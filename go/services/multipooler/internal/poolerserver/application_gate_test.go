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

	"github.com/multigres/multigres/go/common/mterrors"
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
	// Closed until its admission state has been read: that is no decision to fence, so
	// it rejects as the failover it follows would.
	_, err = s.BeginRequest(nil, RequestSingleQuery)
	requireMTF01(t, err)
	_, err = s.BeginRequest(nil, RequestNewReservation)
	requireMTF01(t, err)
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
	_, err = s.FenceApplication(ctx, time.Minute)
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
	requireMTF01(t, err)
	_, err = s.BeginRequest(&query.Target{}, RequestSingleQuery)
	requireMTF01(t, err)

	// Once the gate is closed by a fence decision it says so.
	_, err = s.FenceApplication(t.Context(), time.Millisecond)
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

	_, err = s.FenceApplication(t.Context(), time.Second)
	require.NoError(t, err, "a control request must not make the fence wait for its drain deadline")
}

// TestRequestOnAReservationAFenceTerminatedEndsTheSession: a fence terminates the
// reserved connections that are still open at its drain deadline. A session that
// sends its next request before its gateway has seen routing move must be answered
// with the error that ends the session, not one it could retry from.
func TestRequestOnAReservationAFenceTerminatedEndsTheSession(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	pm.reservedAdd(2) // idle reservations: the work the fence has to terminate
	pm.closeReservedCount = 2
	pm.onCloseReserved = func() { pm.reservedAdd(-2) }
	s.poolManager = pm

	require.NoError(t, s.ReservationEndedByFence(1000), "nothing is known about it before the fence")
	_, err := s.FenceApplication(t.Context(), 20*time.Millisecond)
	require.NoError(t, err)

	err = s.ReservationEndedByFence(1000)
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	require.True(t, diag.IsFatal(), "the client connection must be closed")
	require.Equal(t, mterrors.PgSSAdminShutdown, diag.Code)
	require.Error(t, s.ReservationEndedByFence(1001))
	require.NoError(t, s.ReservationEndedByFence(7), "other reservations are unaffected")
	require.NoError(t, s.ReservationEndedByFence(0), "so is a request with none")
}

// abandonedPoolManager is a pool manager whose look for abandoned backends can be made
// to report some, or to fail, and that records how often idle sessions were closed.
type abandonedPoolManager struct {
	*drainMockPoolManager
	abandoned atomic.Int64
	checkErr  atomic.Pointer[error]
	checks    atomic.Int64
}

func (m *abandonedPoolManager) AbandonedBackends(context.Context) (int, error) {
	m.checks.Add(1)
	if e := m.checkErr.Load(); e != nil {
		return 0, *e
	}
	return int(m.abandoned.Load()), nil
}

// TestFenceClosesIdleSessionsAtTheDeadlineAndRemembersThem: a transaction that is open
// and idle when the drain deadline passes is not running anything, so it is closed, and
// the session's next request is answered with the error that ends it. Nothing running
// is touched.
func TestFenceClosesIdleSessionsAtTheDeadlineAndRemembersThem(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &abandonedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	pm.reservedAdd(1) // an open transaction nothing is running on
	pm.closeReservedCount = 1
	pm.onCloseReserved = func() { pm.reservedAdd(-1) }
	s.poolManager = pm

	ended, err := s.FenceApplication(t.Context(), 50*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 1, ended)
	require.False(t, s.ApplicationOpen())
	require.Error(t, s.ReservationEndedByFence(1000), "the session's next request is answered with the error that ends it")
	_, err = s.BeginRequestOnReservation(nil, RequestExistingReserved, 1000)
	require.ErrorContains(t, err, "closed by a migration fence")
}

// TestFenceDoesNotCloseSessionsThatFinishInTime.
func TestFenceDoesNotCloseSessionsThatFinishInTime(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &abandonedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	pm.reservedAdd(1)
	s.poolManager = pm
	go func() {
		time.Sleep(30 * time.Millisecond)
		pm.reservedAdd(-1)
	}()
	ended, err := s.FenceApplication(t.Context(), 5*time.Second)
	require.NoError(t, err)
	require.Zero(t, ended)
	require.Zero(t, pm.closeReservedCalls)
}

// TestFenceReportsBusyWhenARequestIsStillRunning: running work is neither cancelled nor
// terminated. The fence says so and leaves the gate closed, and closes none of the
// idle sessions, which an aborted fence has no business ending.
func TestFenceReportsBusyWhenARequestIsStillRunning(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &abandonedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	pm.reservedAdd(1) // an idle session
	s.poolManager = pm
	release, err := s.BeginRequest(nil, RequestSingleQuery) // a statement that does not finish
	require.NoError(t, err)

	_, err = s.FenceApplication(t.Context(), 30*time.Millisecond)
	var busy *FenceBusyError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, 1, busy.InFlight)
	require.ErrorContains(t, err, "busy: 1 requests in flight")
	require.False(t, s.ApplicationOpen(), "the gate stays closed until the coordinator decides")
	require.Zero(t, pm.closeReservedCalls, "no session is closed by a fence that is going to be aborted")
	require.NoError(t, s.ReservationEndedByFence(1000))

	// The statement finishes; a retry succeeds.
	release()
	pm.reservedAdd(-1)
	_, err = s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.NoError(t, err)
}

// TestFenceReportsBusyWhenAnAbandonedBackendMayStillBeRunning: a backend the pooler gave up
// on that is still on the server, or a look for it that cannot be made, is not "nothing is
// running".
func TestFenceReportsBusyWhenAnAbandonedBackendMayStillBeRunning(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &abandonedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	s.poolManager = pm

	pm.abandoned.Store(2)
	_, err := s.FenceApplication(t.Context(), 30*time.Millisecond)
	var busy *FenceBusyError
	require.ErrorAs(t, err, &busy)
	require.Equal(t, 2, busy.Abandoned)

	pm.abandoned.Store(0)
	boom := errors.New("admin connection refused")
	pm.checkErr.Store(&boom)
	_, err = s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.ErrorAs(t, err, &busy)
	require.ErrorIs(t, err, boom, "a look that cannot be made is not a clean one")
	require.False(t, s.ApplicationOpen())

	pm.checkErr.Store(nil)
	_, err = s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.NoError(t, err)
}

// TestFenceOfAStandbyDoesNotWaitForAServerItCannotReach: a managed pooler that is not the
// writable leader has nothing that can commit, so a server it cannot look at is no
// obstacle; the writable leader's, and an unmanaged pooler's, is.
func TestFenceOfAStandbyDoesNotWaitForAServerItCannotReach(t *testing.T) {
	s := newGatedTestServer(t)
	pm := &abandonedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	boom := errors.New("postgres is down")
	pm.checkErr.Store(&boom)
	s.poolManager = pm

	s.routingRole = servingstate.RoutingRoleReplica
	_, err := s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.NoError(t, err, "a dead replica must not hold up a fence of its cohort")

	s.routingRole = servingstate.RoutingRolePrimary
	_, err = s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.ErrorIs(t, err, boom, "the leader's server is looked at")
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
	pm := &abandonedPoolManager{drainMockPoolManager: newDrainMockPoolManager()}
	boom := errors.New("the source cannot be reached")
	pm.checkErr.Store(&boom)
	s.poolManager = pm

	_, err := s.FenceApplication(t.Context(), 30*time.Millisecond)
	require.ErrorIs(t, err, boom)
}

// TestNoRequestStartsOnAReservationWhileAFenceClosesIt: the check that nothing is in flight
// and the closing of the idle sessions are one step as far as requests are concerned.
func TestNoRequestStartsOnAReservationWhileAFenceClosesIt(t *testing.T) {
	s := newGatedTestServer(t)
	s.mu.Lock()
	s.fenceEnding = true
	s.mu.Unlock()
	_, err := s.BeginRequestOnReservation(nil, RequestExistingReserved, 77)
	require.ErrorContains(t, err, "closed by a migration fence")
	// Control traffic is not application work.
	release, err := s.BeginRequest(&query.Target{Control: true}, RequestSingleQuery)
	require.NoError(t, err)
	release()
}
