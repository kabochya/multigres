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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

func newMigrationLB(t *testing.T, onReady func(*pb.ShardKey)) *loadBalancer {
	logger := slog.New(slog.DiscardHandler)
	cache := poolerwatch.New(t.Context(), poolerwatch.Config[*poolerConnection]{Logger: logger})
	lb := newLoadBalancer(loadBalancerOpts{Ctx: t.Context(), LocalCell: "zone1", Logger: logger, Cache: cache, OnLeaderServing: onReady})
	cache.Start(poolerwatch.Hooks[*poolerConnection]{OnLive: func(p *pb.Multipooler, _ *poolerConnection) *poolerConnection {
		conn := &poolerConnection{logger: logger, onHealthUpdate: lb.onPoolerHealthUpdate}
		conn.poolerInfo.Store(&topoclient.MultipoolerInfo{Multipooler: p})
		return conn
	}, OnGone: func(p *pb.Multipooler, _ *poolerConnection, _ poolerwatch.GoneReason) { lb.onPoolerGone(p) }})
	t.Cleanup(cache.Shutdown)
	return lb
}

func migrationFixture(t *testing.T, lb *loadBalancer) (*poolerConnection, *poolerConnection, *poolerConnection, *query.Target) {
	target := createTestMultipooler("target", "zone1", constants.DefaultTableGroup, "0", pb.PoolerType_PRIMARY)
	local := createTestMultipooler("source-local", "zone1", constants.DefaultTableGroup, "0", pb.PoolerType_PRIMARY)
	remote := createTestMultipooler("source-remote", "zone2", constants.DefaultTableGroup, "0", pb.PoolerType_PRIMARY)
	for _, p := range []*pb.Multipooler{local, remote} {
		p.ManagementMode = pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
		p.SourceConnection = "source"
	}
	for _, p := range []*pb.Multipooler{target, local, remote} {
		addPoolerForTest(t, lb, p)
	}
	return connForTest(t, lb, target), connForTest(t, lb, local), connForTest(t, lb, remote), protoutil.NewTarget(constants.DefaultPostgresDatabase, constants.DefaultTableGroup, "0", query.Mode_MODE_WRITABLE)
}

func sourceHealth(conn *poolerConnection, ready bool, sysid string) {
	status := pb.PoolerServingStatus_DISABLED
	if ready {
		status = pb.PoolerServingStatus_SERVING
	}
	conn.processHealthResponse(&rpc.StreamPoolerHealthResponse{PoolerId: conn.PoolerInfo().GetId(), ServingStatus: status, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}, BackendReady: ready, BackendIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: sysid, Database: "postgres"}})
}

func targetPolicy(conn *poolerConnection, mode pb.MigrationMode, term int64) {
	conn.processHealthResponse(&rpc.StreamPoolerHealthResponse{PoolerId: conn.PoolerInfo().GetId(), ServingStatus: pb.PoolerServingStatus_SERVING, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &pb.RuleNumber{CoordinatorTerm: term}}, MigrationRouting: &pb.MigrationRouting{Mode: mode, SourceConnection: "source", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}})
}

func TestMigrationRoutingAllApplicationModesAndLocality(t *testing.T) {
	lb := newMigrationLB(t, nil)
	managed, local, remote, target := migrationFixture(t, lb)
	sourceHealth(local, true, "123")
	sourceHealth(remote, true, "123")
	_, err := lb.getConnection(target)
	require.Error(t, err) // unknown does not mean UNSET.
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_UNSET, 1)
	selected, err := lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, managed, selected)
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, 1)
	for _, mode := range []query.Mode{query.Mode_MODE_WRITABLE, query.Mode_MODE_CONSISTENT, query.Mode_MODE_INCONSISTENT} {
		target.Mode = mode
		selected, err = lb.getConnection(target)
		require.NoError(t, err)
		require.Same(t, local, selected)
	}
	require.False(t, lb.claimsPrimary(local))
	require.True(t, lb.claimsPrimary(managed))
	sourceHealth(local, false, "123")
	selected, err = lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, remote, selected)
	sourceHealth(remote, true, "different")
	_, err = lb.getConnection(target)
	require.Error(t, err)
}

func TestMigrationSourceSurvivesTargetLossAndColdGatewayFailsClosed(t *testing.T) {
	lb := newMigrationLB(t, nil)
	managed, local, remote, target := migrationFixture(t, lb)
	sourceHealth(local, true, "123")
	sourceHealth(remote, true, "123")
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, 1)
	managed.setHealthError(errors.New("target down"))
	selected, err := lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, local, selected)
	removePoolerForTest(t, lb, managed.ID())
	selected, err = lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, local, selected)
	cold := newMigrationLB(t, nil)
	_, coldLocal, coldRemote, coldTarget := migrationFixture(t, cold)
	sourceHealth(coldLocal, true, "123")
	sourceHealth(coldRemote, true, "123")
	_, err = cold.getConnection(coldTarget)
	require.Error(t, err)
}

func TestMigrationFencedBufferReleaseAndControlRouting(t *testing.T) {
	releases := 0
	lb := newMigrationLB(t, func(*pb.ShardKey) { releases++ })
	managed, local, remote, target := migrationFixture(t, lb)
	sourceHealth(local, true, "123")
	sourceHealth(remote, true, "123")
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_FENCED, 1)
	require.Zero(t, releases)
	_, err := lb.getConnection(target)
	require.Error(t, err)
	authority, err := lb.managedControlConnection(target)
	require.NoError(t, err)
	require.Same(t, managed, authority)
	owner, err := lb.getConnectionByID(local.PoolerInfo().GetId())
	require.NoError(t, err)
	require.Same(t, local, owner) // reserved cleanup keeps ownership.
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, 1)
	require.Positive(t, releases)
	targetPolicy(managed, pb.MigrationMode_MIGRATION_MODE_MANAGED, 1)
	selected, err := lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, managed, selected)
}

func TestMigrationObsoleteAuthorityCannotOverwriteAcceptedMode(t *testing.T) {
	lb := newMigrationLB(t, nil)
	old, local, remote, target := migrationFixture(t, lb)
	sourceHealth(local, true, "123")
	sourceHealth(remote, true, "123")
	targetPolicy(old, pb.MigrationMode_MIGRATION_MODE_MANAGED, 1)
	newerInfo := createTestMultipooler("new", "zone1", constants.DefaultTableGroup, "0", pb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, newerInfo)
	newer := connForTest(t, lb, newerInfo)
	targetPolicy(newer, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, 2)
	targetPolicy(old, pb.MigrationMode_MIGRATION_MODE_FENCED, 1)
	selected, err := lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, local, selected)
	removePoolerForTest(t, lb, newer.ID())
	targetPolicy(old, pb.MigrationMode_MIGRATION_MODE_MANAGED, 1)
	selected, err = lb.getConnection(target)
	require.NoError(t, err)
	require.Same(t, local, selected)
}
