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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

type fakeApplicationGate struct {
	poolerserver.PoolerController
	open     atomic.Bool
	drainErr error
}

func (g *fakeApplicationGate) SetApplicationAdmission(allow bool) { g.open.Store(allow) }
func (g *fakeApplicationGate) FenceApplication(context.Context) error {
	g.open.Store(false)
	return g.drainErr
}

func newMigrationGateManager() (*MultipoolerManager, *fakeApplicationGate) {
	g := &fakeApplicationGate{}
	pm := &MultipoolerManager{config: &Config{SourceConnection: "source"}, qsc: g, actionLock: actionlock.NewActionLock(), record: newRecordFromProto(&pb.Multipooler{ManagementMode: pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, ShardKey: &pb.ShardKey{Database: "postgres"}})}
	pm.stateManager = NewStateManager(slog.Default(), pm.record, func() *pb.ConsensusStatus { return nil })
	return pm, g
}

func TestDelayedUnfenceCannotReopenAfterFence(t *testing.T) {
	pm, g := newMigrationGateManager()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		calls++
		if calls == 1 {
			close(started)
			<-release
			return &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED, SourceConnection: "source"}, nil
		}
		return &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_FENCED, SourceConnection: "source"}, nil
	}
	opened := make(chan error, 1)
	closed := make(chan error, 1)
	go func() { opened <- pm.applySourceGate(t.Context(), true) }()
	<-started
	go func() { closed <- pm.applySourceGate(t.Context(), false) }()
	close(release)
	require.NoError(t, <-opened)
	require.NoError(t, <-closed)
	require.False(t, g.open.Load())
	require.False(t, pm.sourceAdmission.Load())
}

func TestColdSourceFailsClosedAndWarmSourceRetainsAcceptedState(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) { return nil, errors.New("authority down") }
	pm.refreshServingControl(t.Context())
	require.False(t, g.open.Load())
	require.False(t, pm.sourceModeLoaded.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED, SourceConnection: "source"}, nil
	}
	pm.refreshServingControl(t.Context())
	require.True(t, g.open.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		t.Fatal("warm refresh must not reopen or reinterpret state")
		return nil, nil
	}
	pm.refreshServingControl(t.Context())
	require.True(t, g.open.Load())
}

func TestSourceGateMismatchAndIncompleteDrain(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}, nil
	}
	require.Error(t, pm.applySourceGate(t.Context(), true))
	require.False(t, g.open.Load())
	g.drainErr = context.DeadlineExceeded
	require.ErrorIs(t, pm.applySourceGate(t.Context(), false), context.DeadlineExceeded)
	require.False(t, g.open.Load())
	require.False(t, pm.sourceModeLoaded.Load())
	g.drainErr = nil
	require.NoError(t, pm.applySourceGate(t.Context(), false))
	require.NoError(t, pm.applySourceGate(t.Context(), false))
}
