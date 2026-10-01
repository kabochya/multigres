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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
)

type fakeApplicationGate struct {
	poolerserver.PoolerController
	open     atomic.Bool
	drainErr error
}

type unavailableCatalogQueries struct{ executor.InternalQueryService }

func (unavailableCatalogQueries) QueryAdmin(context.Context, string) (*sqltypes.Result, error) {
	return nil, errors.New("target unavailable")
}

func (unavailableCatalogQueries) BeginAdmin(context.Context) (executor.InternalTx, error) {
	return nil, errors.New("target unavailable")
}

func TestUnavailableTargetClearsMigrationAdvertisement(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.stateManager = nil
	pm.record = newRecordFromProto(&pb.Multipooler{ShardKey: &pb.ShardKey{Database: "postgres"}, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}})
	pm.healthStreamer = newHealthStreamer(slog.Default(), &pb.ID{}, "default", "0-inf")
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}
	pm.healthStreamer.setMigrationRouting(&pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED})
	var err error
	pm.servingCatalog, err = servingcontrol.New(unavailableCatalogQueries{}, make([]byte, 32))
	require.NoError(t, err)
	g.open.Store(true)
	_ = pm.recoverServingControl(t.Context())
	require.False(t, g.open.Load())
	require.Nil(t, pm.healthStreamer.getState().MigrationRouting, "cold gateways require fresh catalog authority")
}

func (g *fakeApplicationGate) SetApplicationAdmission(allow bool) { g.open.Store(allow) }
func (g *fakeApplicationGate) FenceApplication(context.Context) error {
	g.open.Store(false)
	return g.drainErr
}

func newMigrationGateManager() (*MultipoolerManager, *fakeApplicationGate) {
	g := &fakeApplicationGate{}
	pm := &MultipoolerManager{config: &Config{SourceConnection: "source", MigrationKey: make([]byte, 32)}, qsc: g, actionLock: actionlock.NewActionLock(), record: newRecordFromProto(&pb.Multipooler{ManagementMode: pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, ShardKey: &pb.ShardKey{Database: "postgres"}})}
	pm.stateManager = NewStateManager(slog.Default(), pm.record, func() *pb.ConsensusStatus { return nil })
	return pm, g
}

func TestColdSourceFailsClosedAndWarmSourceRetainsAcceptedState(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) { return nil, errors.New("authority down") }
	_ = pm.recoverServingControl(t.Context())
	require.False(t, g.open.Load())
	require.False(t, pm.sourceModeLoaded.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED, SourceConnection: "source", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}, nil
	}
	_ = pm.recoverServingControl(t.Context())
	require.False(t, g.open.Load(), "mode alone cannot admit an unprepared backend")
	pm.healthStreamer = newHealthStreamer(slog.Default(), &pb.ID{}, "default", "0")
	pm.healthStreamer.backendReady = true
	pm.healthStreamer.backendIdentity = &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}
	_ = pm.recoverServingControl(t.Context())
	require.True(t, g.open.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		t.Fatal("warm refresh must not reopen or reinterpret state")
		return nil, nil
	}
	_ = pm.recoverServingControl(t.Context())
	require.True(t, g.open.Load())
}

func TestRefreshRequiresPreparedIdentityEvenWhenModeAllowsSource(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_UNMANAGED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED, SourceConnection: "source"}, nil
	}
	require.Error(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_UNMANAGED))
	require.False(t, g.open.Load())
	require.False(t, pm.sourceModeLoaded.Load())
}

func TestManagedPeerRejectsDelayedFenceAfterActivation(t *testing.T) {
	pm, _, gate := newTransitionManager(t)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_FENCED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_FENCED}, nil
	}
	require.NoError(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_FENCED))
	require.False(t, gate.open.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_MANAGED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}, nil
	}
	require.Error(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_FENCED), "a delayed command cannot fence a newly activated target")
	require.False(t, gate.open.Load())
}

// Exercise the authenticated production interface with explicit operation IDs.
func (pm *MultipoolerManager) refreshRoutingForTest(ctx context.Context, mode pb.MigrationMode) error {
	ctx = migrationcontrol.AuthorizedContext(ctx, pm.config.MigrationKey)
	md, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, nil)
	ctx = peer.NewContext(metadata.NewIncomingContext(ctx, md), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}})
	_, err := pm.RefreshRouting(ctx, &rpc.RefreshRoutingRequest{
		Database: pm.record.ShardKey().Database, OperationId: mode.String(), ExpectedMode: mode,
	})
	return err
}
