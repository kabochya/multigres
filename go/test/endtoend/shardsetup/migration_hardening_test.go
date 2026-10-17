// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardsetup

// PROTOTYPE STUB: end-to-end scenarios for the checks a fence makes before it says a
// tablegroup is quiet: the member that does not know the protocol, and control traffic on
// the tablegroup that holds the coordinator. Delete them with the prototype.

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// oldPooler is a member of a tablegroup that predates the admission protocol: it answers
// every call of the pooler service with Unimplemented, RefreshAdmission included.
type oldPooler struct {
	multipoolerservicepb.UnimplementedMultipoolerServiceServer
}

// scenarioFenceIsNotCleanWhenAMemberDoesNotKnowTheProtocol: a pooler of the tablegroup from
// before the admission protocol has no RefreshAdmission, so it cannot be fenced and it cannot
// say that it is. The fence must not count it as quiet: it is not clean, and says why, and stays
// in place with the pooler that did fence closed. Once the old pooler is gone the same fence,
// retried, is clean.
func scenarioFenceIsNotCleanWhenAMemberDoesNotKnowTheProtocol(t *testing.T, f *migrationFixture) {
	ctx := t.Context()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	multipoolerservicepb.RegisterMultipoolerServiceServer(srv, &oldPooler{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	old := &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: f.def.CellName, Name: "old-version-1"},
		ShardKey:       &clustermetadatapb.ShardKey{Database: f.def.Database, TableGroup: "migrateTG", Shard: "0-inf"},
		Hostname:       "localhost",
		PortMap:        map[string]int32{"grpc": int32(lis.Addr().(*net.TCPAddr).Port)},
		ManagementMode: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED,
	}
	require.NoError(t, f.def.TopoServer.CreateMultipooler(ctx, old))
	// Gone from topology whatever happens, or the shared cluster is not left as found.
	t.Cleanup(func() {
		if err := f.def.TopoServer.UnregisterMultipooler(context.WithoutCancel(ctx), old.Id); err != nil && !errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			t.Errorf("removing the old-version pooler from topology: %v", err)
		}
	})

	before := f.control().version(ctx, "migrateTG")
	_, err = f.control().fenceAt(ctx, "migrateTG", before, 60*time.Second)
	results := requireFenceNotClean(t, err)
	requireMember(t, results, "old-version-1", multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, "Unimplemented")
	requireMember(t, results, "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_CLEAN)
	requireStillFenced(t, f, "migrateTG", before, f.src.grpcPort)

	require.NoError(t, f.def.TopoServer.UnregisterMultipooler(ctx, old.Id))
	resp, err := f.control().fenceWithin(ctx, "migrateTG", 60*time.Second)
	require.NoError(t, err, "without the old pooler the same fence goes through")
	require.Equal(t, admFenced, resp.State.State)
	require.EqualValues(t, before+1, resp.State.Version, "and wrote nothing")
	giveUp(t, f, "migrateTG", before, f.src.grpcPort)
}
