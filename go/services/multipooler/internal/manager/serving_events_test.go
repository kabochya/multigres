// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func pendingServingOperation(pm *MultipoolerManager, q *transitionQueries) {
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
	q.migrationID = "migration"
	q.state.ActiveRequestId = "fence"
	q.requests["fence"] = requestRecord{operation: "controller:fence"}
}

func TestServingWaitLostWakeupDuringInitialInspection(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	pendingServingOperation(pm, q)
	// Completion lands after the journal was inspected, before Wait selects.
	q.afterCommit = func() {
		q.mu.Lock()
		r := q.requests["fence"]
		r.done = true
		q.requests["fence"] = r
		q.mu.Unlock()
		pm.notifyServingOperation()
	}
	status, err := pm.WaitServingOperation(t.Context(), "fence")
	require.NoError(t, err)
	require.True(t, status.Completed)
	require.Equal(t, 2, q.begins)
	// Completion before subscription needs only the initial durable inspection.
	status, err = pm.WaitServingOperation(t.Context(), "fence")
	require.NoError(t, err)
	require.True(t, status.Completed)
	require.Equal(t, 3, q.begins)
}

func TestServingWaitMultipleCanceledAndIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, _ := newTransitionManager(t)
		pendingServingOperation(pm, q)
		ctx, cancel := context.WithCancel(t.Context())
		canceled := make(chan error, 1)
		completed := make(chan error, 2)
		go func() { _, err := pm.WaitServingOperation(ctx, "fence"); canceled <- err }()
		for range 2 {
			go func() {
				s, err := pm.WaitServingOperation(t.Context(), "fence")
				if err == nil && !s.Completed {
					err = errors.New("not complete")
				}
				completed <- err
			}()
		}
		synctest.Wait()
		require.Equal(t, 3, q.begins)
		ch := pm.servingOperationChange()
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, 3, q.begins, "idle waiters must not confirm periodically")
		select {
		case <-ch:
			t.Fatal("status confirmation must not notify itself")
		default:
		}
		cancel()
		synctest.Wait()
		require.ErrorIs(t, <-canceled, context.Canceled)
		require.False(t, q.requests["fence"].done)
		require.NoError(t, pm.finishServingRequest(t.Context(), "fence", pb.MigrationMode_MIGRATION_MODE_FENCED, "migration"))
		synctest.Wait()
		for range 2 {
			require.NoError(t, <-completed)
		}
		require.Equal(t, 6, q.begins)
	})
}

func TestServingWaitLeadershipLossAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			pm, q, _ := newTransitionManager(t)
			pendingServingOperation(pm, q)
			pm.shutdownCtx, pm.shutdownCancel = context.WithCancel(t.Context())
			defer pm.shutdownCancel()
			result := make(chan error, 1)
			go func() { _, err := pm.WaitServingOperation(t.Context(), "fence"); result <- err }()
			synctest.Wait()
			if shutdown {
				pm.shutdownCancel()
			} else {
				pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
				require.NoError(t, (migrationLifecycle{pm}).OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}}))
			}
			synctest.Wait()
			require.Error(t, <-result)
			require.False(t, q.requests["fence"].done)
		})
	}
}

func TestServingCommitPublicationAndIdleRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, gate := newTransitionManager(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		snapshot, err := pm.commitRouting(ctx, func(_ *servingcontrol.Catalog, _ executor.InternalTx, s *pb.MigrationRouting) error {
			s.Mode = pb.MigrationMode_MIGRATION_MODE_UNMANAGED
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, q.begins, "committed snapshot needs no follow-up confirmation")
		require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, snapshot.Mode)
		require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, pm.healthStreamer.getState().MigrationRouting.Mode)
		require.False(t, gate.open.Load())
		go pm.runServingControl(ctx)
		synctest.Wait()
		before := q.begins
		for range 10 {
			pm.healthStreamer.Broadcast()
			pm.signalServingRecovery()
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, before, q.begins, "confirmed idle state must reuse its snapshot")
		// A WAL-visible but unacknowledged mutation must first withdraw authority.
		q.failNextCommit = errors.New("lost COMMIT acknowledgment")
		snapshot, err = pm.commitRouting(ctx, func(_ *servingcontrol.Catalog, _ executor.InternalTx, s *pb.MigrationRouting) error {
			s.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED
			return nil
		})
		require.Error(t, err)
		require.Nil(t, snapshot, "uncertain commit cannot return an authoritative snapshot")
		require.Nil(t, pm.healthStreamer.getState().MigrationRouting)
		require.False(t, gate.open.Load())
		synctest.Wait()
		require.Equal(t, pb.MigrationMode_MIGRATION_MODE_MANAGED, pm.healthStreamer.getState().MigrationRouting.Mode)
		require.True(t, gate.open.Load())
	})
}

func TestServingStartupFailureRetriesAreBoundedAndEventDriven(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, gate := newTransitionManager(t)
		gate.open.Store(false)
		q.beginErr = errors.New("catalog unavailable")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go pm.runServingControl(ctx)
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, servingRecoveryAttempts, q.begins)
		require.False(t, gate.open.Load())
		q.beginErr = nil
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, servingRecoveryAttempts, q.begins, "no retry while idle after exhaustion")
		pm.signalServingRecovery() // explicit reconnect/lifecycle event
		synctest.Wait()
		require.True(t, gate.open.Load())
		require.Equal(t, servingRecoveryAttempts+1, q.begins)
	})
}

func TestServingBackendLifecycleWithdrawsAndConfirmsBeforePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, gate := newTransitionManager(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go pm.runServingControl(ctx)
		synctest.Wait()
		require.True(t, gate.open.Load())
		pm.observeServingBackend(ctx, false)
		require.Nil(t, pm.healthStreamer.getState().MigrationRouting)
		require.False(t, gate.open.Load())
		before := q.begins
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, before, q.begins, "known backend outage must not confirm")
		pm.observeServingBackend(ctx, true)
		synctest.Wait()
		require.NotNil(t, pm.healthStreamer.getState().MigrationRouting)
		require.True(t, gate.open.Load())
		require.Equal(t, before+1, q.begins)
	})
}

func TestServingLeadershipLossCancelsInitializationRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, gate := newTransitionManager(t)
		gate.open.Store(false)
		q.beginErr = errors.New("quorum unavailable")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go pm.runServingControl(ctx)
		synctest.Wait()
		require.Equal(t, 1, q.begins)
		pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
		require.NoError(t, (migrationLifecycle{pm}).OnStateChange(ctx, servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}}))
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, 1, q.begins, "obsolete leadership must stop catalog retries")
		require.False(t, gate.open.Load())
		require.Nil(t, pm.healthStreamer.getState().MigrationRouting)
	})
}

func TestServingWaitSupersededRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, q, _ := newTransitionManager(t)
		pendingServingOperation(pm, q)
		result := make(chan error, 1)
		go func() { _, err := pm.WaitServingOperation(t.Context(), "fence"); result <- err }()
		synctest.Wait()
		// Status handles an inactive journal entry without interpreting the
		// notification as completion. Normal mutations still reject supersession
		// of pending fencing through ReserveRequest.
		require.NoError(t, pm.updateRouting(t.Context(), func(_ *servingcontrol.Catalog, _ executor.InternalTx, s *pb.MigrationRouting) error {
			s.ActiveRequestId = "other"
			return nil
		}))
		synctest.Wait()
		require.ErrorContains(t, <-result, "superseded")
	})
}

func TestServingWarmSourceIgnoresRecoveryEventsDuringTargetOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pm, gate := newMigrationGateManager()
		pm.sourceModeLoaded.Store(true)
		pm.sourceAdmission.Store(true)
		gate.open.Store(true)
		pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
			t.Fatal("warm source must not reread authority")
			return nil, nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go pm.runServingControl(ctx)
		for range 5 {
			pm.signalServingRecovery()
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		require.True(t, gate.open.Load())
	})
}

func TestFollowerReplayCannotAuthorizeUnconfirmedActivation(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED // Locally replayed, COMMIT ack unknown.
	gate.open.Store(false)
	pm.observeManagedPolicy(t.Context())
	require.False(t, gate.open.Load(), "local MANAGED visibility is not activation proof")
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) { return nil, errors.New("uncertain authority") }
	require.Error(t, pm.initializeManagedAdmission(t.Context()))
	require.False(t, gate.open.Load())
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_MANAGED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}, nil
	}
	require.NoError(t, pm.initializeManagedAdmission(t.Context()))
	require.True(t, gate.open.Load())
	before := q.begins
	for range 10 {
		pm.observeManagedPolicy(t.Context())
		require.NoError(t, pm.recoverServingControl(t.Context()))
	}
	require.Equal(t, before, q.begins, "idle local observations do not generate confirmation transactions")
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
	q.state.ActiveRequestId = pb.MigrationMode_MIGRATION_MODE_FENCED.String()
	pm.observeManagedPolicy(t.Context())
	require.False(t, gate.open.Load())
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED
	q.state.ActiveRequestId = pb.MigrationMode_MIGRATION_MODE_MANAGED.String()
	pm.observeManagedPolicy(t.Context())
	require.False(t, gate.open.Load(), "replayed activation needs explicit Refresh")
	require.NoError(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED))
	require.True(t, gate.open.Load())
}

func TestServingReopenRequiresConfirmation(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	require.NoError(t, pm.recoverServingControl(t.Context()))
	before := q.begins
	pm.resetServingAuthority()
	require.Nil(t, pm.healthStreamer.getState().MigrationRouting)
	require.NoError(t, pm.recoverServingControl(t.Context()))
	require.Equal(t, before+1, q.begins)
}

func TestDelayedManagedRefreshRejectedAfterFence(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	pm.servingEvents.confirmed = true // Fixture has completed cold initialization.
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED
	q.state.ActiveRequestId = pb.MigrationMode_MIGRATION_MODE_MANAGED.String()
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_MANAGED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}, nil
		}
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_FENCED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_FENCED}, nil
	}
	done := make(chan error, 1)
	go func() { done <- pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED) }()
	<-entered
	fenced := make(chan error, 1)
	// Fence cannot acknowledge until the older check/apply releases servingMu.
	go func() { fenced <- pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_FENCED) }()
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-fenced)
	require.False(t, gate.open.Load())
	require.Error(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED))
	require.False(t, gate.open.Load())
}

func TestManagedActivationFailsClosedDuringBackendRecovery(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	pm.servingEvents.confirmed = true // Fixture has completed cold initialization.
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED
	q.state.ActiveRequestId = pb.MigrationMode_MIGRATION_MODE_MANAGED.String()
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_MANAGED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}, nil
	}
	gate.open.Store(false)
	pm.servingEvents.backendKnown = true
	pm.servingEvents.backendReady = false
	require.ErrorContains(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED), "backend unavailable")
	require.False(t, gate.open.Load())
	pm.servingEvents.backendReady = true
	require.NoError(t, pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED))
	require.True(t, gate.open.Load())
}

type guardedServingTestGate struct {
	*fakeApplicationGate
	mu         sync.Mutex
	generation uint64
}

func (g *guardedServingTestGate) AdmissionGeneration() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}

func (g *guardedServingTestGate) SetApplicationAdmission(allow bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !allow {
		g.generation++
	}
	g.open.Store(allow)
}

func (g *guardedServingTestGate) ApplyAdmissionGeneration(generation uint64, allow bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.generation != generation {
		return false
	}
	g.open.Store(allow)
	return true
}

func TestFollowerValidationCannotCrossAdmissionWithdrawal(t *testing.T) {
	pm, q, base := newTransitionManager(t)
	pm.servingEvents.confirmed = true // Fixture has completed cold initialization.
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED
	q.state.ActiveRequestId = pb.MigrationMode_MIGRATION_MODE_MANAGED.String()
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	gate := &guardedServingTestGate{fakeApplicationGate: base}
	gate.open.Store(false)
	pm.qsc = gate
	entered, release := make(chan struct{}), make(chan struct{})
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		close(entered)
		<-release
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode_MIGRATION_MODE_MANAGED.String(), Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}, nil
	}
	completed := make(chan error, 1)
	go func() { completed <- pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED) }()
	<-entered
	gate.SetApplicationAdmission(false) // Consensus/lifecycle withdraws while RPC is in flight.
	close(release)
	require.ErrorContains(t, <-completed, "admission changed")
	require.False(t, gate.open.Load())
}

type blockingManagedDrainGate struct {
	*fakeApplicationGate
	entered chan struct{}
	release chan struct{}
}

func (g *blockingManagedDrainGate) FenceApplication(context.Context) error {
	g.open.Store(false)
	close(g.entered)
	<-g.release
	return nil
}

func TestManagedRefreshCannotOvertakeAnAcknowledgingDrain(t *testing.T) {
	pm, q, base := newTransitionManager(t)
	pm.servingEvents.confirmed = true // Fixture has completed cold initialization.
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	gate := &blockingManagedDrainGate{fakeApplicationGate: base, entered: make(chan struct{}), release: make(chan struct{})}
	pm.qsc = gate
	var mode atomic.Int32
	var reads atomic.Int32
	mode.Store(int32(pb.MigrationMode_MIGRATION_MODE_FENCED))
	pm.sourceModeReader = func(context.Context) (*pb.MigrationRouting, error) {
		reads.Add(1)
		return &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode(mode.Load()).String(), Mode: pb.MigrationMode(mode.Load())}, nil
	}
	fenced := make(chan error, 1)
	go func() { fenced <- pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_FENCED) }()
	<-gate.entered
	// Both paths use this existing lock for the entire enforcement interval.
	require.False(t, pm.servingMu.TryLock(), "pending drain must retain local serialization")
	mode.Store(int32(pb.MigrationMode_MIGRATION_MODE_MANAGED))
	q.state.Mode = pb.MigrationMode_MIGRATION_MODE_MANAGED
	q.state.ActiveRequestId = pb.MigrationMode_MIGRATION_MODE_MANAGED.String()
	activated := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		activated <- pm.refreshRoutingForTest(t.Context(), pb.MigrationMode_MIGRATION_MODE_MANAGED)
	}()
	<-started
	require.False(t, gate.open.Load())
	close(gate.release)
	require.NoError(t, <-fenced)
	require.NoError(t, <-activated)
	require.True(t, gate.open.Load())
	require.Equal(t, int32(2), reads.Load())
}
