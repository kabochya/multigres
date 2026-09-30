// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0
package manager

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func TestRefreshExactOperationNotJustRepeatedMode(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.healthStreamer = newHealthStreamer(slog.Default(), nil, "default", "0")
	pm.healthStreamer.backendReady = true
	identity := &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}
	pm.healthStreamer.backendIdentity = identity
	state := &pb.MigrationRouting{ActiveRequestId: "new-open", Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED, SourceConnection: "source", SourceIdentity: identity}
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return proto.Clone(state).(*pb.MigrationRouting), nil
	}
	// A delayed request matches the mode, but must not open for a different ID.
	_, err := pm.enforceRouting(t.Context(), &pb.MigrationRouting{ActiveRequestId: "old-open", Mode: state.Mode})
	require.ErrorContains(t, err, "operation/mode mismatch")
	require.False(t, g.open.Load())
	for range 2 {
		ack, err := pm.enforceRouting(t.Context(), proto.Clone(state).(*pb.MigrationRouting))
		require.NoError(t, err)
		require.Equal(t, "new-open", ack.OperationId)
		require.True(t, ack.AdmissionOpen)
	}
	state = &pb.MigrationRouting{ActiveRequestId: "new-fence", Mode: pb.MigrationMode_MIGRATION_MODE_FENCED}
	_, err = pm.enforceRouting(t.Context(), state)
	require.NoError(t, err)
	require.False(t, g.open.Load())
	_, err = pm.enforceRouting(t.Context(), &pb.MigrationRouting{ActiveRequestId: "new-open", Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED})
	require.Error(t, err)
	require.False(t, g.open.Load())
}

func TestRefreshFollowerReplayLagAndOldOperationTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, g := newTransitionManager(t)
		pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
		pm.servingEvents.confirmed = true
		q.state = &pb.MigrationRouting{ActiveRequestId: "old", Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}
		g.open.Store(false)
		expected := &pb.MigrationRouting{ActiveRequestId: "new", Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}
		result := make(chan error, 1)
		go func() { _, err := pm.enforceRouting(t.Context(), expected); result <- err }()
		synctest.Wait()
		require.False(t, g.open.Load(), "matching mode without matching WAL operation cannot authorize admission")
		q.mu.Lock()
		q.state = proto.Clone(expected).(*pb.MigrationRouting)
		q.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
		synctest.Wait()
		require.NoError(t, <-result)
		require.True(t, g.open.Load())
		before := q.begins
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := pm.enforceRouting(ctx, &pb.MigrationRouting{ActiveRequestId: "old", Mode: expected.Mode})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, before, q.begins, "follower enforcement uses read-only local metadata")
	})
}

func TestRefreshColdFollowerCannotOpenObsoleteReplay(t *testing.T) {
	pm, q, g := newTransitionManager(t)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	q.state = &pb.MigrationRouting{ActiveRequestId: "obsolete", Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}
	g.open.Store(false)
	_, err := pm.enforceRouting(t.Context(), proto.Clone(q.state).(*pb.MigrationRouting))
	require.ErrorContains(t, err, "not initialized")
	require.False(t, g.open.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) { return nil, errors.New("authority down") }
	require.Error(t, pm.initializeManagedAdmission(t.Context()))
	require.False(t, g.open.Load())
}

func TestRefreshDrainTimeoutRetainsGateAndExactRetry(t *testing.T) {
	pm, g := newMigrationGateManager()
	state := &pb.MigrationRouting{ActiveRequestId: "fence", Mode: pb.MigrationMode_MIGRATION_MODE_FENCED}
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return proto.Clone(state).(*pb.MigrationRouting), nil
	}
	g.open.Store(true)
	g.drainErr = context.DeadlineExceeded
	_, err := pm.enforceRouting(t.Context(), state)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, g.open.Load())
	require.False(t, pm.sourceModeLoaded.Load())
	g.drainErr = nil
	ack, err := pm.enforceRouting(t.Context(), state)
	require.NoError(t, err)
	require.Equal(t, "fence", ack.OperationId)
	require.False(t, ack.AdmissionOpen)
}

func TestRefreshReadApplySerializedAgainstFence(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.healthStreamer = newHealthStreamer(slog.Default(), nil, "default", "0")
	identity := &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}
	pm.healthStreamer.backendReady = true
	pm.healthStreamer.backendIdentity = identity
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	state := &pb.MigrationRouting{ActiveRequestId: "open", Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED, SourceConnection: "source", SourceIdentity: identity}
	calls := 0
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		mu.Lock()
		s := proto.Clone(state).(*pb.MigrationRouting)
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			close(entered)
			<-release
		}
		return s, nil
	}
	opened, fenced := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := pm.enforceRouting(t.Context(), &pb.MigrationRouting{ActiveRequestId: "open", Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED})
		opened <- err
	}()
	<-entered
	require.False(t, pm.servingMu.TryLock(), "validation and application share local serialization")
	mu.Lock()
	state = &pb.MigrationRouting{ActiveRequestId: "fence", Mode: pb.MigrationMode_MIGRATION_MODE_FENCED}
	mu.Unlock()
	go func() {
		_, err := pm.enforceRouting(t.Context(), &pb.MigrationRouting{ActiveRequestId: "fence", Mode: pb.MigrationMode_MIGRATION_MODE_FENCED})
		fenced <- err
	}()
	close(release)
	require.NoError(t, <-opened)
	require.NoError(t, <-fenced)
	require.False(t, g.open.Load())
	require.False(t, pm.sourceAdmission.Load())
}

func TestRefreshRPCAuthenticationAndRequiredExpectation(t *testing.T) {
	pm, g := newMigrationGateManager()
	pm.config.MigrationKey = make([]byte, 32)
	_, err := pm.RefreshRouting(t.Context(), &rpc.RefreshRoutingRequest{Database: "postgres", OperationId: "x", ExpectedMode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED})
	require.Error(t, err)
	require.False(t, g.open.Load())
}

func TestManagedObservationDoesNotWaitUnderLifecycleLock(t *testing.T) {
	pm, _, _ := newTransitionManager(t)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "monitor")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	// If observation waits for servingMu, a concurrent refresh requiring the
	// action lock forms the bootstrap deadlock. Optional observation must skip.
	done := make(chan struct{})
	go func() { pm.observeManagedPolicy(lockCtx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor observation blocked on enforcement")
	}
}

type failFirstAdmissionGate struct {
	*fakeApplicationGate
	fail atomic.Bool
}

func (g *failFirstAdmissionGate) AdmissionGeneration() uint64 { return 0 }
func (g *failFirstAdmissionGate) ApplyAdmissionGeneration(_ uint64, allow bool) bool {
	if g.fail.Swap(false) {
		return false
	}
	g.open.Store(allow)
	return true
}

func TestRecoveryRetriesAdmissionAfterConfirmedPublication(t *testing.T) {
	pm, _, base := newTransitionManager(t)
	gate := &failFirstAdmissionGate{fakeApplicationGate: base}
	gate.fail.Store(true)
	gate.open.Store(false)
	pm.qsc = gate
	require.Error(t, pm.recoverServingControl(t.Context()))
	require.False(t, gate.open.Load())
	require.NoError(t, pm.recoverServingControl(t.Context()))
	require.True(t, gate.open.Load(), "publication cache must not hide unfinished local enforcement")
}
