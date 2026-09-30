// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/tools/retry"
)

// All channels are hints, not authority. confirmed is set only while the action
// lock pins leadership and after a synchronous commit. Lifecycle changes clear
// it. Health heartbeats reuse the published immutable snapshot without SQL.
type servingEvents struct {
	mu           sync.Mutex
	recover      chan struct{}
	confirmed    bool
	backendKnown bool
	backendReady bool
	routing      *pb.RoutingState
	observed     *pb.MigrationRouting
	cancel       context.CancelFunc
}

func (pm *MultipoolerManager) servingRecoveryEvent() <-chan struct{} {
	pm.servingEvents.mu.Lock()
	defer pm.servingEvents.mu.Unlock()
	if pm.servingEvents.recover == nil {
		pm.servingEvents.recover = make(chan struct{}, 1)
	}
	return pm.servingEvents.recover
}

func (pm *MultipoolerManager) signalServingRecovery() {
	pm.servingEvents.mu.Lock()
	defer pm.servingEvents.mu.Unlock()
	if pm.servingEvents.recover == nil {
		pm.servingEvents.recover = make(chan struct{}, 1)
	}
	select {
	case pm.servingEvents.recover <- struct{}{}:
	default:
	}
}

func (pm *MultipoolerManager) invalidateServingPublication() {
	pm.servingEvents.mu.Lock()
	pm.servingEvents.confirmed = false
	pm.servingEvents.mu.Unlock()
	if !pm.IsUnmanaged() {
		if g, err := pm.gate(); err == nil {
			g.SetApplicationAdmission(false)
		}
		if pm.healthStreamer != nil {
			pm.healthStreamer.setMigrationRouting(nil)
		}
	}
}

// Open/resume is a new authority boundary even when its cached role is unchanged.
// Caller holds the action lock; unmanaged warm processes retain their accepted
// gate until RefreshRouting explicitly advances it.
func (pm *MultipoolerManager) resetServingAuthority() {
	if pm.IsUnmanaged() {
		return
	}
	pm.invalidateServingPublication()
	pm.servingEvents.mu.Lock()
	pm.servingEvents.backendKnown = false
	pm.servingEvents.mu.Unlock()
}

// Caller holds servingMu and the action lock. No reread separates commit and
// publication. The protobuf is private to this confirmed transaction.
func (pm *MultipoolerManager) publishServingSnapshot(_ context.Context, state *pb.MigrationRouting) {
	if pm.servingAuthority(pm.record.ShardKey().Database) != nil {
		return
	}
	if g, err := pm.gate(); err == nil {
		if state.Mode != pb.MigrationMode_MIGRATION_MODE_UNSET && state.Mode != pb.MigrationMode_MIGRATION_MODE_MANAGED {
			g.SetApplicationAdmission(false)
		}
	}
	if pm.healthStreamer != nil {
		pm.healthStreamer.setMigrationRouting(state)
	}
	pm.servingEvents.mu.Lock()
	pm.servingEvents.confirmed = true
	pm.servingEvents.mu.Unlock()
}

// StateAware is a pure sink: cancel obsolete work and schedule recovery; never
// reenter StateManager or perform SQL under its fanout lock.
type migrationLifecycle struct{ pm *MultipoolerManager }

func (l migrationLifecycle) OnStateChange(_ context.Context, state servingstate.State) error {
	pm := l.pm
	if pm.IsUnmanaged() {
		return nil
	}
	routing := state.Routing.ToProto()
	pm.servingEvents.mu.Lock()
	changed := !proto.Equal(pm.servingEvents.routing, routing)
	if changed {
		pm.servingEvents.routing = routing
		pm.servingEvents.confirmed = false
		pm.servingEvents.observed = nil
		if pm.servingEvents.cancel != nil {
			pm.servingEvents.cancel()
		}
	}
	pm.servingEvents.mu.Unlock()
	if changed {
		pm.invalidateServingPublication()
		pm.notifyServingOperation()
		pm.signalServingRecovery()
	}
	return nil
}

const servingRecoveryAttempts = 5

// Every event gets a bounded, cancellation-aware recovery burst. Exhaustion
// leaves admission closed until an explicit call or a new lifecycle/stream event.
// Stable idle state has no timers and performs no confirmation transactions.
func (pm *MultipoolerManager) runServingControl(ctx context.Context) {
	events := pm.servingRecoveryEvent()
	pm.signalServingRecovery()
	for {
		select {
		case <-ctx.Done():
			return
		case <-events:
		}
		burstCtx, cancel := context.WithCancel(ctx)
		pm.servingEvents.mu.Lock()
		pm.servingEvents.cancel = cancel
		pm.servingEvents.mu.Unlock()
		r := retry.New(100*time.Millisecond, time.Second)
		for attempt, err := range r.Attempts(burstCtx) {
			if err != nil {
				break
			}
			attemptCtx, done := context.WithTimeout(burstCtx, 3*time.Second)
			err = pm.recoverServingControl(attemptCtx)
			done()
			if err == nil || attempt >= servingRecoveryAttempts {
				break
			}
		}
		cancel()
	}
}

func (pm *MultipoolerManager) recoverServingControl(ctx context.Context) error {
	if pm.IsUnmanaged() {
		if pm.sourceModeLoaded.Load() {
			return nil
		}
		_, err := pm.enforceRouting(ctx, nil)
		return err
	}
	if pm.servingAuthority(pm.record.ShardKey().Database) == nil {
		pm.servingMu.Lock()
		defer pm.servingMu.Unlock()
		pm.servingEvents.mu.Lock()
		confirmed := pm.servingEvents.confirmed
		unavailable := pm.servingEvents.backendKnown && !pm.servingEvents.backendReady
		pm.servingEvents.mu.Unlock()
		if unavailable {
			return errors.New("managed backend unavailable")
		}
		if confirmed {
			return nil
		}
		state, err := pm.confirmedRoutingLocked(ctx)
		pm.servingMu.Unlock()
		if err == nil {
			_, err = pm.enforceRouting(ctx, state)
			if err != nil {
				pm.invalidateServingPublication()
			}
		}
		pm.servingMu.Lock()
		if err == nil {
			pm.notifyServingOperation()
		}
		return err
	}
	return pm.initializeManagedAdmission(ctx)
}

// This observes the existing PostgreSQL monitor; it adds no probe or timer.
// Serialize withdrawal with confirmed publication, even when the consensus role
// stays PRIMARY while PostgreSQL is stopped for maintenance.
func (pm *MultipoolerManager) observeServingBackend(ctx context.Context, ready bool) {
	if pm.config == nil || len(pm.config.MigrationKey) == 0 || pm.IsUnmanaged() {
		return
	}
	lockCtx, err := pm.actionLock.Acquire(ctx, "MigrationBackendReadiness")
	if err != nil {
		return
	}
	defer pm.actionLock.Release(lockCtx)
	pm.servingEvents.mu.Lock()
	changed := !pm.servingEvents.backendKnown || pm.servingEvents.backendReady != ready
	pm.servingEvents.backendKnown, pm.servingEvents.backendReady = true, ready
	if changed && !ready && pm.servingEvents.cancel != nil {
		pm.servingEvents.cancel()
	}
	pm.servingEvents.mu.Unlock()
	if !changed {
		return
	}
	if !ready {
		pm.invalidateServingPublication()
		pm.notifyServingOperation()
	}
	pm.signalServingRecovery()
}

// Called by the existing PostgreSQL monitor, without any remote wait or write.
// Even an unconfirmed restrictive row may close admission conservatively.
func (pm *MultipoolerManager) observeManagedPolicy(ctx context.Context) {
	if pm.config == nil || len(pm.config.MigrationKey) == 0 || pm.IsUnmanaged() || pm.servingAuthority(pm.record.ShardKey().Database) == nil {
		return
	}
	// The PostgreSQL monitor can already hold the action lock. Enforcement
	// takes servingMu before that lock, so this optional read must never wait.
	if !pm.servingMu.TryLock() {
		return
	}
	defer pm.servingMu.Unlock()
	c := pm.servingCatalog
	if c == nil {
		var err error
		c, err = servingcontrol.New(pm.qsc.InternalQueryService(), pm.config.MigrationKey)
		if err != nil {
			return
		}
	}
	state, err := c.Routing(ctx)
	if err == nil {
		pm.servingEvents.mu.Lock()
		changed := !proto.Equal(pm.servingEvents.observed, state)
		pm.servingEvents.observed = state
		pm.servingEvents.mu.Unlock()
		if changed {
			pm.signalServingRecovery()
		}
	}
	if err != nil || state.Mode != pb.MigrationMode_MIGRATION_MODE_UNSET && state.Mode != pb.MigrationMode_MIGRATION_MODE_MANAGED {
		if g, gateErr := pm.gate(); gateErr == nil {
			g.SetApplicationAdmission(false)
		}
	}
}
