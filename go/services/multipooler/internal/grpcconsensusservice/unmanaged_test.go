// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcconsensusservice

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestConsensusServiceRejectsUnmanaged verifies that consensus RPCs fail with an
// explicit FAILED_PRECONDITION on an unmanaged pooler, which never takes part
// in consensus and has no consensus manager.
func TestConsensusServiceRejectsUnmanaged(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	pm, err := manager.NewMultipoolerManager(slog.New(slog.NewTextHandler(os.Stderr, nil)), &clustermetadata.Multipooler{
		Id:             &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "external"},
		ShardKey:       &clustermetadata.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		ManagementMode: clustermetadata.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED,
	}, &manager.Config{TopoClient: ts, ConnPoolConfig: connpoolmanager.NewConfig(viperutil.NewRegistry())})
	require.NoError(t, err)
	defer pm.ShutdownForTest(ctx)

	svc := &consensusService{manager: pm}
	for name, call := range map[string]func() error{
		"Promote": func() error {
			_, err := svc.Promote(ctx, &consensusdata.PromoteRequest{})
			return err
		},
		"Recruit": func() error {
			_, err := svc.Recruit(ctx, &consensusdata.RecruitRequest{})
			return err
		},
		"SetPrimary": func() error {
			_, err := svc.SetPrimary(ctx, &consensusdata.SetPrimaryRequest{})
			return err
		},
		"UpdateConsensusRule": func() error {
			_, err := svc.UpdateConsensusRule(ctx, &multipoolermanagerdata.UpdateConsensusRuleRequest{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.Error(t, err)
			require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(mterrors.FromGRPC(err)))
			require.ErrorContains(t, err, "not supported on an unmanaged pooler")
		})
	}
}
