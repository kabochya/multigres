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

package poolergateway

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

type adminFixtureClient struct {
	rpc.MultipoolerServiceClient
	calls     int
	proof     string
	operation string
}

func (f *adminFixtureClient) ServingControl(ctx context.Context, r *rpc.ServingControlRequest, _ ...grpc.CallOption) (*rpc.ServingControlResponse, error) {
	f.calls++
	f.operation = r.Operation
	md, _ := metadata.FromOutgoingContext(ctx)
	f.proof = md.Get("x-multigres-serving-key")[0]
	return &rpc.ServingControlResponse{Routing: &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_FENCED}}, nil
}

func (f *adminFixtureClient) GetAuthCredentials(ctx context.Context, r *rpc.GetAuthCredentialsRequest, _ ...grpc.CallOption) (*rpc.GetAuthCredentialsResponse, error) {
	if !r.ServingAdmin {
		panic("application auth must not bypass fencing")
	}
	f.calls++
	md, _ := metadata.FromOutgoingContext(ctx)
	f.proof = md.Get("x-multigres-serving-key")[0]
	return &rpc.GetAuthCredentialsResponse{ScramHash: "test-verifier"}, nil
}

func TestServingControlAndAdminAuthReachFencedManagedAuthority(t *testing.T) {
	lb := newMigrationLB(t, nil)
	p := createTestMultipooler("target", "zone1", constants.DefaultTableGroup, constants.DefaultShard, pb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, p)
	managed := connForTest(t, lb, p)
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_FENCED, 1)
	fake := &adminFixtureClient{}
	managed.client = fake
	pg := &PoolerGateway{loadBalancer: lb, servingToken: strings.Repeat("a", 64)}
	target := &query.Target{ShardKey: p.ShardKey, Mode: query.Mode_MODE_WRITABLE}
	_, err := lb.getConnection(target)
	require.Error(t, err, "ordinary application routing remains fenced")
	_, err = pg.ServingControl(t.Context(), target, &rpc.ServingControlRequest{Operation: "show"})
	require.NoError(t, err)
	require.Equal(t, "show", fake.operation)
	require.Equal(t, pg.servingToken, fake.proof)
	_, err = pg.GetAuthCredentials(t.Context(), &rpc.GetAuthCredentialsRequest{Database: p.ShardKey.Database, Username: "postgres", ServingAdmin: true})
	require.NoError(t, err)
	require.Equal(t, 2, fake.calls)
	pg.servingToken = ""
	_, err = pg.ServingControl(t.Context(), target, &rpc.ServingControlRequest{Operation: "show"})
	require.Error(t, err)
	require.Equal(t, 2, fake.calls)
}
