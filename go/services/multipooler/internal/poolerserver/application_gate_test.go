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

package poolerserver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func newGatedTestServer(t *testing.T) *QueryPoolerServer {
	t.Helper()
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl()
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))
	return s
}

func TestApplicationGateClosedByDefaultWhenAdmissionControlled(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	// Without admission control the gate never closes by itself.
	_, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)

	s = newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl()
	_, err = s.BeginRequest(nil, RequestSingleQuery)
	requireMTF01(t, err)
	_, err = s.BeginRequest(nil, RequestNewReservation)
	requireMTF01(t, err)
}

func TestClosedGateStillAdmitsControlRequestsAndOpenReservedWork(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl()

	// Control and metadata requests (credential lookups) bypass the gate.
	require.NoError(t, s.StartRequest(nil, RequestNewReservation))
	// Work on an existing reserved connection is admitted so a transaction can
	// finish while the pooler drains.
	release, err := s.BeginRequest(nil, RequestExistingReserved)
	require.NoError(t, err)
	release()
}

func TestGateOpensOnlyForTheGenerationItWasReadAt(t *testing.T) {
	s := newGatedTestServer(t)
	read := s.AdmissionGeneration()
	s.CloseApplication()
	require.False(t, s.OpenApplication(read), "a decision read before a close must not open the gate")
	require.False(t, s.ApplicationOpen())
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))
	require.True(t, s.ApplicationOpen())
}

func TestRoleOrServingChangeClosesAdmissionControlledGate(t *testing.T) {
	s := newGatedTestServer(t)
	state := func(role servingstate.RoutingRole, status clustermetadatapb.PoolerServingStatus) servingstate.State {
		return servingstate.State{Routing: servingstate.RoutingState{Role: role}, ServingStatus: status}
	}
	// First transition from the zero state.
	require.NoError(t, s.OnStateChange(t.Context(), state(servingstate.RoutingRoleReplica, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.False(t, s.ApplicationOpen())
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))

	// Same state again: nothing changes.
	require.NoError(t, s.OnStateChange(t.Context(), state(servingstate.RoutingRoleReplica, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.True(t, s.ApplicationOpen())

	// A promotion invalidates the decision, and a read from the previous role
	// cannot reopen the gate.
	previous := s.AdmissionGeneration()
	require.NoError(t, s.OnStateChange(t.Context(), state(servingstate.RoutingRolePrimary, clustermetadatapb.PoolerServingStatus_SERVING)))
	require.False(t, s.ApplicationOpen())
	require.False(t, s.OpenApplication(previous))
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))

	// Losing the backend closes it too.
	require.NoError(t, s.OnStateChange(t.Context(), state(servingstate.RoutingRolePrimary, clustermetadatapb.PoolerServingStatus_DISABLED)))
	require.False(t, s.ApplicationOpen())
}

func TestStateChangeDoesNotTouchGateWithoutAdmissionControl(t *testing.T) {
	s := newStartRequestTestServer()
	require.NoError(t, s.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	require.True(t, s.ApplicationOpen())
}

func TestFenceWaitsForAdmittedRequests(t *testing.T) {
	s := newGatedTestServer(t)
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := s.FenceApplication(t.Context(), time.Minute)
		done <- err
	}()
	require.Eventually(t, func() bool { return !s.ApplicationOpen() }, 2*time.Second, 5*time.Millisecond, "the gate closes immediately")
	select {
	case err := <-done:
		t.Fatalf("fence returned %v while a request was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}

	_, err = s.BeginRequest(nil, RequestSingleQuery)
	requireMTF01(t, err)

	release()
	release() // idempotent
	require.NoError(t, <-done)
}

func TestFenceReturnsWhenCallerCancels(t *testing.T) {
	s := newGatedTestServer(t)
	_, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.FenceApplication(ctx, time.Minute)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, s.ApplicationOpen(), "a failed fence leaves the gate closed")
}

// TestFenceTerminatesReservedConnectionsAtDeadline is the drain deadline: a
// transaction that never finishes is terminated, and only then is the fence
// complete.
func TestFenceTerminatesReservedConnectionsAtDeadline(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	pm.reservedAdd(1) // an open transaction that never concludes
	pm.closeReservedCount = 1
	pm.onCloseReserved = func() { pm.reservedAdd(-1) }
	s.poolManager = pm

	terminated, err := s.FenceApplication(t.Context(), 50*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 1, terminated)
	require.Equal(t, 1, pm.closeReservedCalls)
	require.False(t, s.ApplicationOpen())
}

func TestFenceDoesNotTerminateWhenWorkFinishesInTime(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	pm.reservedAdd(1)
	s.poolManager = pm
	go func() {
		time.Sleep(30 * time.Millisecond)
		pm.reservedAdd(-1)
	}()
	terminated, err := s.FenceApplication(t.Context(), 5*time.Second)
	require.NoError(t, err)
	require.Zero(t, terminated)
	require.Zero(t, pm.closeReservedCalls)
}

// TestFenceNeverAcknowledgesWhileWorkRemains: work that terminating the
// reserved connections does not stop (an admitted request, a single query) makes
// the fence fail rather than report success.
func TestFenceNeverAcknowledgesWhileWorkRemains(t *testing.T) {
	old := forceFinalWait
	forceFinalWait = 50 * time.Millisecond
	defer func() { forceFinalWait = old }()

	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	s.poolManager = pm
	_, err := s.BeginRequest(nil, RequestSingleQuery) // never released
	require.NoError(t, err)

	_, err = s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.ErrorContains(t, err, "still in flight")
	require.False(t, s.ApplicationOpen())
}

// failingKillPoolManager is a pool manager whose terminations fail.
type failingKillPoolManager struct {
	*drainMockPoolManager
	failures atomic.Int64
}

func (m *failingKillPoolManager) ReservedKillFailures() int64 { return m.failures.Load() }

// TestFenceFailsWhenAReservedConnectionCouldNotBeTerminated: a connection whose
// backend was not terminated may still be running a statement, so the fence must
// not be acknowledged even though the connection is gone from the pool.
func TestFenceFailsWhenAReservedConnectionCouldNotBeTerminated(t *testing.T) {
	s := newGatedTestServer(t)
	base := newDrainMockPoolManager()
	pm := &failingKillPoolManager{drainMockPoolManager: base}
	base.reservedAdd(1)
	base.closeReservedCount = 1
	base.onCloseReserved = func() {
		pm.failures.Add(1) // the terminate failed, but the connection was released
		base.reservedAdd(-1)
	}
	s.poolManager = pm

	_, err := s.FenceApplication(t.Context(), 50*time.Millisecond)
	require.ErrorContains(t, err, "could not terminate")
	require.False(t, s.ApplicationOpen())
}

// TestBeginRequestSkipsAccountingWithoutAdmissionControl: only a controlled pooler
// ever waits for requests, so the rest pay nothing for the permit.
func TestBeginRequestSkipsAccountingWithoutAdmissionControl(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
	require.Zero(t, s.activeRequests)
	release()
	require.Nil(t, s.requestsChanged, "a completion nobody waits for allocates no channel")
}
