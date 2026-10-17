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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
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

// TestClosingTheGateWaitsForNothingAndReportsWhatIsRunning: closing the gate returns at once
// with a request in flight, which is reported, not waited for; new requests are refused, and
// once the request finishes the report is clean.
func TestClosingTheGateWaitsForNothingAndReportsWhatIsRunning(t *testing.T) {
	s := newGatedTestServer(t)
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)

	s.CloseApplication()
	require.False(t, s.ApplicationOpen(), "the gate closes immediately")
	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy)
	require.Equal(t, 1, busy.InFlight)

	_, err = s.BeginRequest(nil, RequestSingleQuery)
	requireMTF02(t, err)

	release()
	release() // idempotent
	busy = s.FenceNow(t.Context())
	require.Nil(t, busy, "nothing is running: the report is clean")
}

// TestFenceNowClosesAnOpenGate: the report is the close and the read in one, so a gate that is open
// is closed by it, and what a member that missed the start answers is the same as the others.
func TestFenceNowClosesAnOpenGate(t *testing.T) {
	s := newGatedTestServer(t)
	require.True(t, s.ApplicationOpen())
	busy := s.FenceNow(t.Context())
	require.Nil(t, busy)
	require.False(t, s.ApplicationOpen(), "the gate is closed")
	requireMTF02(t, func() error { _, err := s.BeginRequest(nil, RequestSingleQuery); return err }())
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
	s.CloseApplication()
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

// TestControlRequestsAreNotWorkAFenceWaitsFor: a control request in flight does not make
// the report busy.
func TestControlRequestsAreNotWorkAFenceWaitsFor(t *testing.T) {
	s := newGatedTestServer(t)
	release, err := s.BeginRequest(&query.Target{Control: true}, RequestSingleQuery)
	require.NoError(t, err)
	defer release()

	busy := s.FenceNow(t.Context())
	require.Nil(t, busy, "a control request does not keep a fence busy")
}

// TestFenceNowSaysBusyWhenAReservationIsHeld: an open transaction, even one that is idle,
// a temporary table, a session advisory lock, a COPY or a portal is a reservation, and a
// fence never ends a client's state: the report says busy and the reservation is untouched.
// Once the client lets it go the report is clean.
func TestFenceNowSaysBusyWhenAReservationIsHeld(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	pm.reservedAdd(2) // an idle transaction and a session pinned by an advisory lock
	s.poolManager = pm
	s.CloseApplication()

	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy)
	require.Equal(t, 2, busy.Reservations)
	require.Zero(t, busy.InFlight)
	require.Contains(t, busy.String(), "2 reservations held")
	require.False(t, s.ApplicationOpen(), "a report leaves the gate closed")
	require.Zero(t, pm.closeReservedCalls, "nothing the client holds is ended")

	pm.reservedAdd(-2)
	busy = s.FenceNow(t.Context())
	require.Nil(t, busy)
}

// TestExistingReservationsPassTheClosedGate: an ordinary transaction can still finish
// after the gate closes, and then the report is clean, without anything having been ended
// or refused; a new reservation is refused.
func TestExistingReservationsPassTheClosedGate(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	pm.reservedAdd(1) // an open transaction
	s.poolManager = pm
	s.CloseApplication()

	_, err := s.BeginRequest(nil, RequestNewReservation)
	requireMTF02(t, err)
	release, err := s.BeginRequest(nil, RequestExistingReserved)
	require.NoError(t, err, "a request on an existing reservation passes the closed gate")
	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy, "the transaction is still open")

	// The transaction commits and releases its reservation.
	release()
	pm.reservedAdd(-1)
	busy = s.FenceNow(t.Context())
	require.Nil(t, busy, "a transaction that commits after the close is no reason to refuse the fence")
	require.Zero(t, pm.closeReservedCalls)
}

// TestAStreamIsAdmittedByTheGateButNotCounted: a logical replication stream is refused
// while the gate is closed, but it is held for as long as its consumer runs, so it is not
// work in flight that a fence waits for.
func TestAStreamIsAdmittedByTheGateButNotCounted(t *testing.T) {
	s := newGatedTestServer(t)
	require.NoError(t, s.AdmitStream(nil, RequestNewReservation))
	require.Zero(t, s.activeRequests, "a stream is not counted")
	busy := s.FenceNow(t.Context())
	require.Nil(t, busy, "a fence does not wait for a stream")
	requireMTF02(t, s.AdmitStream(nil, RequestNewReservation))
}

// TestBusyReportSaysWhatHoldsIt: a gate that is not clean reports the held reservations
// (why each is reserved, for how long, whether a request is running on it, how long ago it
// was used, who holds it, and when its backend started), and the requests in flight, as
// facts, in its message and in the details that travel to the coordinator.
func TestBusyReportSaysWhatHoldsIt(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	s.poolManager = pm
	pm.reservedAdd(2)
	pm.held = []connpoolmanager.HeldReservation{
		{User: "app", Held: reserved.Held{ConnID: 7, Reasons: protoutil.ReasonTransaction, BackendPID: 4242, ClientPort: 51234, ClientConnectionID: 9, HeldFor: 90 * time.Second, IdleFor: 80 * time.Second}},
		{User: "app", Held: reserved.Held{ConnID: 8, Reasons: protoutil.ReasonTempTable | protoutil.ReasonSessionAdvisoryLock, BackendPID: 4243, HeldFor: 10 * time.Second, IdleFor: time.Second}},
	}
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
	defer release()
	s.CloseApplication()

	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy)
	require.Equal(t, 1, busy.InFlight)
	require.Equal(t, 2, busy.Reservations)
	require.Len(t, busy.Held, 2)
	require.Contains(t, busy.String(), "transaction held 1m30s idle 1m20s pid 4242 user app")

	details := busy.Details()
	require.EqualValues(t, 1, details.GetRequestsInFlight())
	require.EqualValues(t, 2, details.GetReservationsHeld())
	require.Zero(t, details.GetReservationsTruncated())
	first, second := details.GetReservations()[0], details.GetReservations()[1]
	require.Equal(t, []string{"transaction"}, first.GetReasons())
	require.Equal(t, 90*time.Second, first.GetHeldFor().AsDuration())
	require.Equal(t, 80*time.Second, first.GetIdleFor().AsDuration())
	require.Equal(t, "app", first.GetUser())
	require.EqualValues(t, 4242, first.GetBackendPid())
	require.EqualValues(t, 7, first.GetReservedConnectionId())
	require.EqualValues(t, 9, first.GetClientConnectionId())
	require.EqualValues(t, 51234, first.GetClientPort(), "the local port of the pooler's connection to the backend")
	require.Zero(t, second.GetClientPort(), "a connection that is not TCP has none")
	require.Equal(t, []string{"temp_table", "session_advisory_lock"}, second.GetReasons())
}

// TestFenceNowReadsTheRequestsBeforeTheReservations: the gate is closed and the requests in flight
// are read together, and the reservations after. A request that creates a reservation and finishes
// between the two reads was counted in flight, so the report is not clean; read the other way
// round, it would create the reservation after the reservations were read and finish before the
// requests were, and a report of nothing would be false.
func TestFenceNowReadsTheRequestsBeforeTheReservations(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	s.poolManager = pm
	release, err := s.BeginRequest(nil, RequestNewReservation) // about to make a reservation
	require.NoError(t, err)
	s.betweenFenceReads = func() {
		pm.reservedAdd(1) // it makes its reservation
		release()         // and finishes
	}

	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy, "the request was in flight when the requests were read")
	require.Equal(t, 1, busy.InFlight)
	require.Equal(t, 1, busy.Reservations, "and the reservation it made is seen")

	// Asked again, with nothing in flight and the reservation left, the report is still busy.
	s.betweenFenceReads = nil
	busy = s.FenceNow(t.Context())
	require.NotNil(t, busy)
	require.Zero(t, busy.InFlight)
	require.Equal(t, 1, busy.Reservations)
}

// TestFenceNowReleasesWhatAControllerEnded: reservations whose backend the server no longer has
// are released when the report asks, so a commit that follows a controller ending an idle
// session is clean and does not wait for the inactivity timeout to find it gone.
func TestFenceNowReleasesWhatAControllerEnded(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	s.poolManager = pm
	pm.reservedAdd(2)
	pm.held = []connpoolmanager.HeldReservation{
		{User: "app", Held: reserved.Held{ConnID: 1, Reasons: protoutil.ReasonTransaction, BackendPID: 4242}},
		{User: "app", Held: reserved.Held{ConnID: 2, Reasons: protoutil.ReasonTransaction, BackendPID: 4243}},
	}
	pm.goneBackends = map[uint32]bool{4242: true}
	s.CloseApplication()

	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy, "one reservation is still held")
	require.Equal(t, 1, busy.Reservations)
	require.Len(t, busy.Held, 1)
	require.EqualValues(t, 4243, busy.Held[0].BackendPID)
	require.Zero(t, busy.HeldNotListed)

	pm.goneBackends[4243] = true
	busy = s.FenceNow(t.Context())
	require.Nil(t, busy, "every held backend is gone: nothing is held")
}

// TestBusyReportListsAtMostTwentyReservations: however many reservations are held, the answer
// lists the longest held ones up to the cap and counts the rest, so it stays small.
func TestBusyReportListsAtMostTwentyReservations(t *testing.T) {
	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	s.poolManager = pm
	pm.reservedAdd(25)
	for i := range 25 {
		pm.held = append(pm.held, connpoolmanager.HeldReservation{User: "app", Held: reserved.Held{ConnID: int64(i), Reasons: protoutil.ReasonTransaction}})
	}

	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy)
	require.Equal(t, 25, busy.Reservations)
	require.Len(t, busy.Held, MaxHeldReservationsReported)
	require.Equal(t, 5, busy.HeldNotListed)
	details := busy.Details()
	require.Len(t, details.GetReservations(), MaxHeldReservationsReported)
	require.EqualValues(t, 5, details.GetReservationsTruncated())
	require.Contains(t, busy.String(), "and 22 more", "the message lists three and counts the rest")
}

// TestWhatABusyFenceReportsHoldsNothingAClientSent: the shape of the answer is facts about
// the reservation and nothing that came from the client. It is pinned to the fields below:
// a field that could hold query text, parameters or a credential must not be added to it
// without this test being changed on purpose.
func TestWhatABusyFenceReportsHoldsNothingAClientSent(t *testing.T) {
	names := func(m protoreflect.MessageDescriptor) []string {
		var out []string
		for i := range m.Fields().Len() {
			out = append(out, string(m.Fields().Get(i).Name()))
		}
		return out
	}
	require.ElementsMatch(t,
		[]string{"requests_in_flight", "reservations_held", "reservations", "reservations_truncated"},
		names((&multipoolerservicepb.FenceBusy{}).ProtoReflect().Descriptor()))
	require.ElementsMatch(t,
		[]string{"reasons", "held_for", "idle_for", "user", "backend_pid", "reserved_connection_id", "client_connection_id", "in_use", "client_port"},
		names((&multipoolerservicepb.HeldReservation{}).ProtoReflect().Descriptor()))
	// The reasons are labels of the pooler's own reservation reasons, never free text.
	labels := map[string]bool{}
	for _, l := range protoutil.ReasonLabels() {
		labels[l.Name] = true
	}
	for _, r := range reasonNames(^uint32(0)) {
		require.True(t, labels[r], r)
	}
}

// TestBusyReportCarriesNothingAClientSent: the reservation of a real connection whose state
// holds text a client sent (session settings that look like SQL) is described without it,
// in the message and in the details alike.
func TestBusyReportCarriesNothingAClientSent(t *testing.T) {
	const secret = "SELECT card_number FROM payments WHERE owner = 'secret-owner'"
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := reserved.NewPool(t.Context(), &reserved.PoolConfig{
		InactivityTimeout: time.Minute,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
	defer pool.Close()
	conn, err := pool.NewConn(t.Context(), nil)
	require.NoError(t, err)
	conn.AddReservationReason(protoutil.ReasonTransaction)
	conn.State().SetSettings(connstate.NewSettings(map[string]string{"application_name": secret}, 0))
	conn.State().SetTrackedVpid(77)

	s := newGatedTestServer(t)
	pm := newDrainMockPoolManager()
	s.poolManager = pm
	pm.reservedAdd(1)
	pm.held = []connpoolmanager.HeldReservation{{User: "app", Held: pool.HeldReservations(time.Now())[0]}}

	busy := s.FenceNow(t.Context())
	require.NotNil(t, busy)
	require.Contains(t, busy.String(), "transaction", "the reservation is described")
	require.NotContains(t, busy.String(), "secret")
	require.NotContains(t, busy.String(), "card_number")
	raw, mErr := proto.Marshal(busy.Details())
	require.NoError(t, mErr)
	require.NotContains(t, string(raw), "secret")
	require.NotContains(t, string(raw), "card_number")
	require.EqualValues(t, 77, busy.Details().GetReservations()[0].GetClientConnectionId())
	conn.Release(reserved.ReleaseCommit, nil)
}
