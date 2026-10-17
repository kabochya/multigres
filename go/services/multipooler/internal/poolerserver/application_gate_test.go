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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func newGatedTestServer(t *testing.T) *QueryPoolerServer {
	t.Helper()
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary // the writable leader: its server is checked
	s.EnableAdmissionControl(false)
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
	s.EnableAdmissionControl(false)
	// Closed until its admission state has been read: one signal, MTF02, whatever closed
	// the gate.
	_, err = s.BeginRequest(nil, RequestSingleQuery)
	requireMTF02(t, err)
	_, err = s.BeginRequest(nil, RequestNewReservation)
	requireMTF02(t, err)
}

func TestClosedGateStillAdmitsControlRequestsAndOpenReservedWork(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl(false)

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
		err := s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: time.Minute})
		done <- err
	}()
	require.Eventually(t, func() bool { return !s.ApplicationOpen() }, 2*time.Second, 5*time.Millisecond, "the gate closes immediately")
	select {
	case err := <-done:
		t.Fatalf("fence returned %v while a request was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}

	_, err = s.BeginRequest(nil, RequestSingleQuery)
	requireMTF02(t, err)

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
	err = s.FenceApplication(ctx, FenceOptions{DrainTimeout: time.Minute})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, s.ApplicationOpen(), "a failed fence leaves the gate closed")
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

// TestShutdownApplicationClosesTheGateForGood: after the shutdown close nothing may
// reopen the gate, whatever generation a late admission read holds.
func TestShutdownApplicationClosesTheGateForGood(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl(false)
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))

	require.NoError(t, s.ShutdownApplication(t.Context()))
	require.False(t, s.ApplicationOpen())
	require.False(t, s.OpenApplication(s.AdmissionGeneration()), "a decision read after the close must not open the gate")
	_, err := s.BeginRequest(nil, RequestSingleQuery)
	requireMTF01(t, err) // shutting down is not a fence: retry elsewhere, as for a failover
}

// TestShutdownApplicationWaitsForAdmittedWork: work admitted before the close is
// drained, and when it does not finish the caller is told.
func TestShutdownApplicationWaitsForAdmittedWork(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl(false)
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)

	short, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, s.ShutdownApplication(short), "work is still running")
	require.False(t, s.ApplicationOpen(), "the gate stays closed when the drain does not finish")

	done := make(chan error, 1)
	go func() { done <- s.ShutdownApplication(t.Context()) }()
	select {
	case err := <-done:
		t.Fatalf("shutdown returned %v while a request was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	require.NoError(t, <-done)
}

// TestControlRequestsPassAClosedGateOnlyWhenTheyReserveNothing: control traffic is
// served while the gate is closed, but one that would start a reservation is gated
// like application work, since the reservation's later requests would pass it.
func TestControlRequestsPassAClosedGateOnlyWhenTheyReserveNothing(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.EnableAdmissionControl(false)
	control := &query.Target{Control: true}

	release, err := s.BeginRequest(control, RequestSingleQuery)
	require.NoError(t, err, "a control query is served while the gate is closed")
	release()

	_, err = s.BeginRequest(control, RequestNewReservation)
	requireMTF02(t, err)
	_, err = s.BeginRequest(&query.Target{}, RequestSingleQuery)
	requireMTF02(t, err)

	// A fence closes it with the same signal.
	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: time.Millisecond})
	require.NoError(t, err)
	_, err = s.BeginRequest(control, RequestNewReservation)
	requireMTF02(t, err)
	_, err = s.BeginRequest(&query.Target{}, RequestSingleQuery)
	requireMTF02(t, err)
	release, err = s.BeginRequest(control, RequestSingleQuery)
	require.NoError(t, err, "control is still served by a fenced gate")
	release()

	// Not serving at all still rejects it: control bypasses the gate, not the
	// pooler's own state.
	s.servingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	_, err = s.BeginRequest(control, RequestSingleQuery)
	requireMTF01(t, err)
}

// TestControlRequestsAreNotWorkAFenceWaitsFor: a control request in flight does not
// hold a fence up.
func TestControlRequestsAreNotWorkAFenceWaitsFor(t *testing.T) {
	s := newGatedTestServer(t)
	release, err := s.BeginRequest(&query.Target{Control: true}, RequestSingleQuery)
	require.NoError(t, err)
	defer release()

	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: time.Second})
	require.NoError(t, err, "a control request must not make the fence wait for its drain deadline")
}

// unexplainedPoolManager is a pool manager whose look for unexplained backends can be
// made to report some, or to fail, and that records the scope and the time it was given.
type unexplainedPoolManager struct {
	*drainMockPoolManager
	unexplained atomic.Int64
	checkErr    atomic.Pointer[error]
	checks      atomic.Int64
	lastBudget  atomic.Int64
}

func (m *unexplainedPoolManager) UnexplainedBackends(ctx context.Context) (int, error) {
	m.checks.Add(1)
	if deadline, ok := ctx.Deadline(); ok {
		m.lastBudget.Store(int64(time.Until(deadline)))
	}
	if e := m.checkErr.Load(); e != nil {
		return 0, *e
	}
	return int(m.unexplained.Load()), nil
}

// TestFenceReportsBusyWhenAReservationIsStillHeld: an open transaction, even one that is idle,
// a temporary table, a session advisory lock, a COPY or a portal is a reservation, and a
// fence never ends a client's state: at the drain deadline it reports busy and leaves the
// gate closed, and the reservation is untouched. Once the client lets it go a fence succeeds.
func TestFenceReportsBusyWhenAReservationIsStillHeld(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	pm.reservedAdd(2) // an idle transaction and a session pinned by an advisory lock
	s.poolManager = pm

	err := s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 50 * time.Millisecond})
	var busy *FenceBusyError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, 2, busy.Reservations)
	require.Zero(t, busy.InFlight)
	require.ErrorContains(t, err, "2 reservations held")
	require.False(t, s.ApplicationOpen(), "the gate stays closed until the coordinator decides")
	require.Zero(t, pm.closeReservedCalls, "nothing the client holds is ended")
	require.Zero(t, pm.checks.Load(), "and the server is not looked at: the fence is already busy")

	pm.reservedAdd(-2)
	require.NoError(t, s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 50 * time.Millisecond}))
}

// TestExistingReservationsPassTheClosedGateWhileAFenceDrains: an ordinary transaction can
// still finish inside the deadline, and the fence then succeeds without having ended or
// refused anything; a new reservation is refused.
func TestExistingReservationsPassTheClosedGateWhileAFenceDrains(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	pm.reservedAdd(1) // an open transaction
	s.poolManager = pm

	done := make(chan error, 1)
	go func() { done <- s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 5 * time.Second}) }()
	require.Eventually(t, func() bool { return !s.ApplicationOpen() }, 2*time.Second, 5*time.Millisecond)

	_, err := s.BeginRequest(nil, RequestNewReservation)
	requireMTF02(t, err)
	release, err := s.BeginRequest(nil, RequestExistingReserved)
	require.NoError(t, err, "a request on an existing reservation passes the closed gate")

	// The transaction commits and releases its reservation.
	release()
	pm.reservedAdd(-1)
	require.NoError(t, <-done, "a transaction that commits inside the deadline does not make the fence abort")
	require.Zero(t, pm.closeReservedCalls)
}

// TestFenceReportsBusyWhenARequestIsStillRunning: running work is neither cancelled nor
// terminated. The fence says so and leaves the gate closed.
func TestFenceReportsBusyWhenARequestIsStillRunning(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	s.poolManager = pm
	release, err := s.BeginRequest(nil, RequestSingleQuery) // a statement that does not finish
	require.NoError(t, err)

	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	var busy *FenceBusyError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, 1, busy.InFlight)
	require.ErrorContains(t, err, "busy: 1 requests in flight")
	require.False(t, s.ApplicationOpen(), "the gate stays closed until the coordinator decides")

	// The statement finishes; a retry succeeds.
	release()
	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.NoError(t, err)
}

// TestFenceReportsBusyWhenAnUnexplainedBackendMayBeRunning: a backend that is not idle
// and that no connection of the pooler explains, or a look for it that cannot be made, is
// not "nothing is running".
func TestFenceReportsBusyWhenAnUnexplainedBackendMayBeRunning(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	s.poolManager = pm

	pm.unexplained.Store(2)
	err := s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	var busy *FenceBusyError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, 2, busy.Unexplained)

	pm.unexplained.Store(0)
	boom := errors.New("admin connection refused")
	pm.checkErr.Store(&boom)
	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.ErrorAs(t, err, &busy)
	require.ErrorIs(t, err, boom, "a look that cannot be made is not a clean one")
	require.False(t, s.ApplicationOpen())

	pm.checkErr.Store(nil)
	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.NoError(t, err)
}

// TestFenceOfAStandbySkipsTheLookAtTheServer: a managed pooler that is not the writable
// leader cannot commit, and the read-only sessions of a replica would abort every fence
// of its cohort, so it does not look at its server at all, reachable or not; the writable
// leader's server is looked at.
func TestFenceOfAStandbySkipsTheLookAtTheServer(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	boom := errors.New("postgres is down")
	pm.checkErr.Store(&boom)
	pm.unexplained.Store(3)
	s.poolManager = pm

	s.routingRole = servingstate.RoutingRoleReplica
	err := s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.NoError(t, err, "a replica, whatever its server shows, must not hold up a fence of its cohort")
	require.Zero(t, pm.checks.Load(), "and its server is not looked at")

	s.routingRole = servingstate.RoutingRolePrimary
	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.ErrorIs(t, err, boom, "the leader's server is looked at")
}

// TestFenceLooksAtAReplicaPromotedWhileItWaits: whether the pooler is a standby is read
// when the look would run, not when the fence started, so a replica promoted during the
// drain, which can now commit, is looked at.
func TestFenceLooksAtAReplicaPromotedWhileItWaits(t *testing.T) {
	s := newGatedTestServer(t)
	s.routingRole = servingstate.RoutingRoleReplica
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	pm.unexplained.Store(1)
	s.poolManager = pm
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 5 * time.Second}) }()
	require.Eventually(t, func() bool { return !s.ApplicationOpen() }, 2*time.Second, 5*time.Millisecond)
	s.mu.Lock()
	s.routingRole = servingstate.RoutingRolePrimary // promoted while the fence waits
	s.mu.Unlock()
	release()

	var busy *FenceBusyError
	require.ErrorAs(t, <-done, &busy)
	require.Equal(t, 1, busy.Unexplained, "the promoted replica's server was looked at")
}

// TestAStreamIsAdmittedByTheGateButNotCounted: a logical replication stream is refused
// while the gate is closed, but it is held for as long as its consumer runs, so it is not
// work in flight that a fence waits for.
func TestAStreamIsAdmittedByTheGateButNotCounted(t *testing.T) {
	s := newGatedTestServer(t)
	require.NoError(t, s.AdmitStream(nil, RequestNewReservation))
	require.Zero(t, s.activeRequests, "a stream is not counted")
	require.NoError(t, s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond}),
		"a fence does not wait for a stream")
	requireMTF02(t, s.AdmitStream(nil, RequestNewReservation))
}

// TestFenceOfAnExternalBackendIsNeverTakenForAStandby: an unmanaged pooler whose readiness
// probe fails reports no routing role, which on a managed pooler is what a dead replica
// looks like. Its source is a writable primary all the same.
func TestFenceOfAnExternalBackendIsNeverTakenForAStandby(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRoleUnknown // the probe is failing
	s.EnableAdmissionControl(true)
	require.True(t, s.OpenApplication(s.AdmissionGeneration()))
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	boom := errors.New("the source cannot be reached")
	pm.checkErr.Store(&boom)
	s.poolManager = pm

	err := s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.ErrorIs(t, err, boom)
}

// TestFenceGivesTheLookItsOwnBudget: the time the look may take comes from the fence's
// options, and is the fixed budget when unset.
func TestFenceGivesTheLookItsOwnBudget(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &unexplainedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	s.poolManager = pm

	err := s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond})
	require.NoError(t, err)
	require.InDelta(t, float64(BackendCheckBudget), float64(pm.lastBudget.Load()), float64(500*time.Millisecond))

	err = s.FenceApplication(t.Context(), FenceOptions{DrainTimeout: 30 * time.Millisecond, BackendCheckTimeout: 700 * time.Millisecond})
	require.NoError(t, err)
	require.InDelta(t, float64(700*time.Millisecond), float64(pm.lastBudget.Load()), float64(300*time.Millisecond))
}
