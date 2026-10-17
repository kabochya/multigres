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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// gatedController is a PoolerController whose application gate is a real
// QueryPoolerServer's, so admission is exercised against the real gate.
type gatedController struct {
	*mockPoolerController
	gate *poolerserver.QueryPoolerServer
}

func (g gatedController) EnableAdmissionControl(external bool) {
	g.gate.EnableAdmissionControl(external)
}
func (g gatedController) AdmissionGeneration() uint64     { return g.gate.AdmissionGeneration() }
func (g gatedController) OpenApplication(gen uint64) bool { return g.gate.OpenApplication(gen) }
func (g gatedController) CloseApplication()               { g.gate.CloseApplication() }
func (g gatedController) ShutdownApplication(ctx context.Context) error {
	return g.gate.ShutdownApplication(ctx)
}
func (g gatedController) ApplicationOpen() bool { return g.gate.ApplicationOpen() }
func (g gatedController) FenceNow(ctx context.Context) *poolerserver.FenceBusy {
	return g.gate.FenceNow(ctx)
}

// admissionHarness is an admission-controlled manager whose authoritative read
// is scripted.
type admissionHarness struct {
	pm    *MultipoolerManager
	tg    string // the tablegroup of the pooler: the default one for a managed pooler
	gate  *poolerserver.QueryPoolerServer
	row   atomic.Pointer[multipoolerservicepb.TablegroupServingState]
	err   atomic.Pointer[error]
	reads atomic.Int32
}

func newAdmissionHarness(t *testing.T, unmanaged bool) *admissionHarness {
	t.Helper()
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	tg := constants.DefaultTableGroup
	if unmanaged {
		tg = "migrateTG"
	}
	initial.ShardKey = &clustermetadatapb.ShardKey{Database: "db", TableGroup: tg, Shard: "0-inf"}
	cfg := &Config{}
	if unmanaged {
		initial.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
		cfg.BackingConnectionName = "src"
		cfg.ExternalBackend = &ExternalBackend{Host: "db", Port: 5432, Database: "db", ExpectedSystemIdentifier: "1"}
	}
	pm, err := NewMultipoolerManager(slog.New(slog.DiscardHandler), initial, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})

	gate := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, initial.Id, tg, "0-inf", nil, 0, false)
	// Serving, so admitted requests are not rejected for the serving state.
	require.NoError(t, gate.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	gate.EnableAdmissionControl(false)
	pm.qsc = gatedController{mockPoolerController: &mockPoolerController{}, gate: gate}

	h := &admissionHarness{pm: pm, tg: tg, gate: gate}
	backing := ""
	if unmanaged {
		backing = "src"
	}
	h.setRow(false, 0, backing)
	pm.admission.reader = func(context.Context) (*multipoolerservicepb.TablegroupServingState, error) {
		h.reads.Add(1)
		if e := h.err.Load(); e != nil {
			return nil, *e
		}
		return h.row.Load(), nil
	}

	// A serving pooler with a ready backend, so admission is the only gate. The
	// streamer would otherwise wait on the manager's original query server.
	pm.healthStreamer.queryServer = nil
	require.NoError(t, pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	pm.healthStreamer.setBackendReady(true)
	return h
}

// stateOf is the state a fenced flag stands for in these tests: a completed fence.
func stateOf(fenced bool) multipoolerservicepb.AdmissionState {
	if fenced {
		return stateFenced
	}
	return stateUnfenced
}

func (h *admissionHarness) setRow(fenced bool, version int64, backing string) {
	h.setRowState(stateOf(fenced), version, backing)
}

func (h *admissionHarness) setRowState(state multipoolerservicepb.AdmissionState, version int64, backing string) {
	h.row.Store(&multipoolerservicepb.TablegroupServingState{
		Tablegroup: h.tg, State: state, Version: version, BackingConnection: backing,
	})
}

func (h *admissionHarness) refresh(fenced bool, version int64) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	return h.refreshState(stateOf(fenced), version)
}

func (h *admissionHarness) refreshState(state multipoolerservicepb.AdmissionState, version int64) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	return h.pm.RefreshAdmission(context.Background(), &multipoolerservicepb.RefreshAdmissionRequest{
		Database: "db", Tablegroup: h.tg, ExpectedState: state, ExpectedVersion: version,
	})
}

func requireCode(t *testing.T, want mtrpcpb.Code, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, want, mterrors.Code(err), "%v", err)
}

func TestAdmissionStartupOpensOnlyWhenTheRowIsUnfenced(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fenced bool
		open   bool
	}{
		{"unfenced opens", false, true},
		{"fenced stays closed", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newAdmissionHarness(t, true)
			require.False(t, h.gate.ApplicationOpen(), "a controlled pooler starts closed")
			h.setRow(tc.fenced, 3, "src")
			_, err := h.pm.enforceAdmission(t.Context(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.open, h.gate.ApplicationOpen())
		})
	}
}

func TestAdmissionUnreadableOrMissingStateNeverOpens(t *testing.T) {
	h := newAdmissionHarness(t, true)
	unreachable := errors.New("default primary unreachable")
	h.err.Store(&unreachable)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.Error(t, err)
	require.False(t, h.gate.ApplicationOpen())

	missing := mterrors.New(mtrpcpb.Code_NOT_FOUND, "tablegroup has no serving row")
	h.err.Store(&missing)
	_, err = h.pm.enforceAdmission(t.Context(), nil)
	require.Error(t, err)
	require.False(t, h.gate.ApplicationOpen())
}

func TestAdmissionBackendUnavailableNeverOpens(t *testing.T) {
	h := newAdmissionHarness(t, true)
	h.pm.healthStreamer.setBackendReady(false)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen(), "an identity-unvalidated backend must not be admitted")

	h.pm.healthStreamer.setBackendReady(true)
	require.NoError(t, h.pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	_, err = h.pm.enforceAdmission(t.Context(), nil)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen())
}

func TestRefreshFenceThenUnfence(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	// The coordinator persists the fence, then asks the pooler to refresh.
	h.setRow(true, 1, "src")
	resp, err := h.refresh(true, 1)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State, "a fence is acknowledged as fenced")
	require.EqualValues(t, 1, resp.Version)
	require.False(t, h.gate.ApplicationOpen())
	require.Equal(t, h.pm.record.Snapshot().GetId().GetName(), resp.PoolerId.GetName())

	// A repeat of the same fence refresh reads again and runs the fence again, which is
	// what makes a repeated fence a resume.
	reads := h.reads.Load()
	resp, err = h.refresh(true, 1)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State)
	require.Greater(t, h.reads.Load(), reads)

	// Unfence.
	h.setRow(false, 2, "src")
	resp, err = h.refresh(false, 2)
	require.NoError(t, err)
	require.Equal(t, stateUnfenced, resp.State)
	require.True(t, h.gate.ApplicationOpen())
}

func TestRefreshExpectationIsNeverPermission(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)

	// The coordinator expects a fence at version 5, but the authority still says
	// unfenced at version 0: nothing is applied and the open gate stays open.
	_, err = h.refresh(true, 5)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.True(t, h.gate.ApplicationOpen())

	// A delayed unfence of an older fence cannot override a newer fence.
	h.setRow(true, 7, "src")
	_, err = h.refresh(false, 6)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.True(t, h.gate.ApplicationOpen(), "the mismatch applied nothing")
	_, err = h.refresh(true, 7)
	require.NoError(t, err)
	require.False(t, h.gate.ApplicationOpen())
	_, err = h.refresh(false, 6)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen(), "a stale unfence must not reopen a fenced pooler")
}

func TestRefreshRejectsMismatchedBacking(t *testing.T) {
	h := newAdmissionHarness(t, true)
	h.setRow(false, 0, "some-other-connection")
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen())

	// A managed pooler is backed by the managed cohort: both are empty.
	m := newAdmissionHarness(t, false)
	m.setRow(false, 0, "src")
	_, err = m.pm.enforceAdmission(t.Context(), nil)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
}

func TestRefreshValidatesRequest(t *testing.T) {
	h := newAdmissionHarness(t, true)
	call := func(db, tg string, version int64) error {
		_, err := h.pm.RefreshAdmission(t.Context(), &multipoolerservicepb.RefreshAdmissionRequest{Database: db, Tablegroup: tg, ExpectedState: stateFenced, ExpectedVersion: version})
		return err
	}
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, call("other", h.tg, 1))
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, call("db", "otherTG", 1))
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, call("db", h.tg, -1))
}

// TestEveryPoolerIsAdmissionControlled: admission control needs no flag, so a pooler built
// with the deprecated flag off is controlled like any other.
func TestEveryPoolerIsAdmissionControlled(t *testing.T) {
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.ShardKey = &clustermetadatapb.ShardKey{Database: "db", TableGroup: constants.DefaultTableGroup, Shard: "0-inf"}
	pm, err := NewMultipoolerManager(slog.New(slog.DiscardHandler), initial, &Config{})
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})
	g, err := pm.applicationGate()
	require.NoError(t, err)
	require.False(t, g.ApplicationOpen(), "closed from the start, until the row is read")
}

// TestLifecycleChangeWithdrawsAdmission: a role change, a serving change or a
// backend reconnect closes the gate and forgets the decision until it is read
// again.
func TestLifecycleChangeWithdrawsAdmission(t *testing.T) {
	h := newAdmissionHarness(t, false)
	h.pm.record.registered.Store(true)
	lifecycle := admissionLifecycle{h.pm}
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	// First sync with the current state: the decision is withdrawn once.
	state := servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}
	require.NoError(t, lifecycle.OnStateChange(t.Context(), state))
	require.False(t, h.gate.ApplicationOpen())
	_, err = h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	// The same state again changes nothing.
	require.NoError(t, lifecycle.OnStateChange(t.Context(), state))
	require.True(t, h.gate.ApplicationOpen())

	// The backend goes away and comes back: closed on the way down, and a read of
	// the state is needed to reopen.
	down := state
	down.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	require.NoError(t, lifecycle.OnStateChange(t.Context(), down))
	require.False(t, h.gate.ApplicationOpen())
	require.NoError(t, lifecycle.OnStateChange(t.Context(), state))
	require.False(t, h.gate.ApplicationOpen(), "recovery of the backend alone never reopens admission")

	// The decision is withdrawn too: an acknowledgment of the old one is invalid.
	reads := h.reads.Load()
	_, err = h.refresh(false, 0)
	require.NoError(t, err)
	require.Greater(t, h.reads.Load(), reads, "a withdrawn decision is re-read, not acknowledged from memory")
	require.True(t, h.gate.ApplicationOpen())
}

// TestOpenDecisionReadBeforeAChangeIsRejected: a read that began before the
// pooler's role changed must not open the gate afterwards.
func TestOpenDecisionReadBeforeAChangeIsRejected(t *testing.T) {
	h := newAdmissionHarness(t, false)
	h.pm.record.registered.Store(true)
	lifecycle := admissionLifecycle{h.pm}
	h.pm.admission.reader = func(context.Context) (*multipoolerservicepb.TablegroupServingState, error) {
		// The role changes while the read is in flight.
		_ = lifecycle.OnStateChange(t.Context(), servingstate.State{
			Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		})
		return h.row.Load(), nil
	}
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	require.False(t, h.gate.ApplicationOpen())
}

// TestRefreshClosesAndReportsWithoutWaitingForWork: a refresh applies the fence and says what the
// pooler holds, in the same call: with a request in flight it closes the gate and answers at once,
// saying that request is running; once the request is over the same refresh answers that nothing is
// held. It fails on a row that is not the one expected, and applies and reports nothing for an
// unfence.
func TestRefreshClosesAndReportsWithoutWaitingForWork(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	release, err := h.gate.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.NoError(t, err)
	h.setRowState(stateFencing, 1, "src")

	started := time.Now()
	resp, err := h.refreshState(stateFencing, 1)
	require.NoError(t, err, "work is in flight, and the refresh does not wait for it")
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, stateFencing, resp.State)
	require.EqualValues(t, 1, resp.Version)
	require.False(t, h.gate.ApplicationOpen(), "the refresh closed the gate")
	require.NotNil(t, resp.Busy, "and said what is still running")
	require.EqualValues(t, 1, resp.Busy.GetRequestsInFlight())

	release()
	resp, err = h.refreshState(stateFencing, 1)
	require.NoError(t, err)
	require.Nil(t, resp.Busy, "nothing is held now")
	require.False(t, h.gate.ApplicationOpen())

	// A refresh about a row that is not the one persisted fails.
	_, err = h.refreshState(stateFencing, 2)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.refreshState(stateFenced, 1)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)

	// An unfence opens the gate and reports nothing.
	h.setRowState(stateUnfenced, 2, "src")
	resp, err = h.refreshState(stateUnfenced, 2)
	require.NoError(t, err)
	require.Nil(t, resp.Busy)
	require.True(t, h.gate.ApplicationOpen())
}

func TestGetServingStateOnlyServedByTheDefaultPrimary(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db"})
	requireCode(t, mtrpcpb.Code_UNAVAILABLE, err) // not the default primary: retryable, after rediscovery
	_, err = h.pm.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "other"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
}

// TestManagedPoolerReadsAdmissionOnlyAfterItIsRegistered: the coordinator fences
// the poolers it finds in topology, so a managed pooler that read an unfenced row and
// opened before it was listed could stay open past a committed fence.
func TestManagedPoolerReadsAdmissionOnlyAfterItIsRegistered(t *testing.T) {
	h := newAdmissionHarness(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.pm.runAdmission(ctx)

	h.pm.signalAdmission()
	testpoll.Never(t, func() bool { return h.reads.Load() > 0 || h.gate.ApplicationOpen() }, 300*time.Millisecond, 20*time.Millisecond,
		"an unregistered managed pooler must not read or open")

	h.pm.record.registered.Store(true)
	h.pm.signalAdmission()
	require.Eventually(t, h.gate.ApplicationOpen, 5*time.Second, 20*time.Millisecond)
}

// TestAdmissionLoopRedecidesAfterTheGateWasClosedBehindIt: a decision recorded for
// an earlier generation is not a decision, whatever happened to cause the close.
func TestAdmissionLoopRedecidesAfterTheGateWasClosedBehindIt(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	// A close that did not go through the lifecycle sink leaves the recorded
	// decision pointing at a generation that no longer exists.
	h.gate.CloseApplication()
	require.False(t, h.gate.ApplicationOpen())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.pm.runAdmission(ctx)
	h.pm.signalAdmission()
	require.Eventually(t, h.gate.ApplicationOpen, 5*time.Second, 20*time.Millisecond)
}

// TestOpeningAndAConcurrentLifecycleChangeNeverLeaveAStaleDecisionOpen: whatever
// the interleaving, an open gate always has a decision recorded for its current
// generation, so the loop can tell it needs another read when it is closed.
func TestOpeningAndAConcurrentLifecycleChangeNeverLeaveAStaleDecisionOpen(t *testing.T) {
	h := newAdmissionHarness(t, true)
	lifecycle := admissionLifecycle{h.pm}
	for i := range 300 {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = h.pm.enforceAdmission(t.Context(), nil)
		}()
		go func() {
			defer wg.Done()
			role := servingstate.RoutingRolePrimary
			if i%2 == 1 {
				role = servingstate.RoutingRoleReplica
			}
			_ = lifecycle.OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: role}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING})
		}()
		wg.Wait()

		h.pm.admission.mu.Lock()
		applied := h.pm.admission.applied
		h.pm.admission.mu.Unlock()
		if h.gate.ApplicationOpen() {
			require.NotNil(t, applied, "iteration %d: the gate is open with no decision recorded", i)
			require.Equal(t, h.gate.AdmissionGeneration(), applied.generation, "iteration %d: the gate is open on a stale decision", i)
		}
		// Reset to the closed, undecided state for the next round.
		_ = lifecycle.OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleUnknown}, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED})
	}
}

// TestStopTopoRegistrationClosesTheGate: a pooler that announces shutdown leaves
// every fence membership, so it must not keep admitting work afterwards.
func TestStopTopoRegistrationClosesTheGate(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	h.pm.StopTopoRegistration(t.Context())
	require.False(t, h.gate.ApplicationOpen())
}

func TestDefaultPrimaryDialOptionIsResolvedOnlyWhenAsked(t *testing.T) {
	var nilConfig *Config
	opt, err := nilConfig.DefaultPrimaryDialOption()
	require.NoError(t, err)
	require.NotNil(t, opt, "no configuration means the insecure internal default")

	calls := 0
	boom := errors.New("tls required but not configured")
	cfg := &Config{DefaultPrimaryTransport: func() (grpc.DialOption, error) { calls++; return nil, boom }}
	require.Zero(t, calls, "constructing the config resolves nothing")
	_, err = cfg.DefaultPrimaryDialOption()
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, calls)
}

// shutdownHarness is an admission harness whose record is registered against a
// recording topology store, so what StopTopoRegistration publishes can be seen.
func newShutdownHarness(t *testing.T) (*admissionHarness, *fakeTopoStore) {
	t.Helper()
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	ts := &fakeTopoStore{}
	initial := newTestPoolerProto(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_SERVING)
	initial.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	h.pm.record = mustNewPoolerRecord(t, ts, initial)
	h.pm.record.Register(t.Context(), func(string) {})
	require.Eventually(t, func() bool { return ts.calls.Load() >= 1 }, time.Second, time.Millisecond)
	return h, ts
}

func announcedShutdown(ts *fakeTopoStore) bool {
	seen := ts.lastSeen.Load()
	return seen != nil && seen.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN
}

// TestStopTopoRegistrationDrainsAdmittedWorkBeforeAnnouncingShutdown: SHUTDOWN
// takes the pooler out of every fence membership, so it must not be published while
// work the pooler admitted can still reach the database.
func TestStopTopoRegistrationDrainsAdmittedWorkBeforeAnnouncingShutdown(t *testing.T) {
	h, ts := newShutdownHarness(t)
	release, err := h.gate.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		require.True(t, h.pm.StopTopoRegistration(t.Context()))
		close(done)
	}()
	require.Eventually(t, func() bool { return !h.gate.ApplicationOpen() }, 2*time.Second, 5*time.Millisecond, "the gate closes at once")
	select {
	case <-done:
		t.Fatal("shutdown finished while admitted work was still running")
	case <-time.After(100 * time.Millisecond):
	}
	require.False(t, announcedShutdown(ts), "not announced while work is in flight")

	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after the work did")
	}
	require.True(t, announcedShutdown(ts))
}

// TestStopTopoRegistrationWithWorkStillRunningFailsClosed: when the work does not
// finish in the budget the record is left as a crashed pooler's would be, and the
// gate does not reopen however the admission loop runs afterwards. This is the path
// a graceful shutdown that timed out, or could not take the action lock, leaves.
func TestStopTopoRegistrationWithWorkStillRunningFailsClosed(t *testing.T) {
	h, ts := newShutdownHarness(t)
	_, err := h.gate.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	require.False(t, h.pm.StopTopoRegistration(ctx), "it tells its caller it left the record alone")

	require.False(t, announcedShutdown(ts), "a pooler with running work must not say it has stopped")
	require.False(t, h.gate.ApplicationOpen())

	// The loop is over, and a decision read now could not open the gate.
	loopCtx, loopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer loopCancel()
	exited := make(chan struct{})
	go func() {
		h.pm.runAdmission(loopCtx)
		close(exited)
	}()
	h.pm.signalAdmission()
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		t.Fatal("the admission loop kept running after shutdown")
	}
	_, err = h.pm.enforceAdmission(t.Context(), nil)
	require.Error(t, err)
	require.False(t, h.gate.ApplicationOpen())
	_, err = h.refresh(false, 0)
	require.Error(t, err, "a refresh cannot reopen a pooler that is shutting down")
	require.False(t, h.gate.ApplicationOpen())
}

// TestReplicaWithADownBackendAcknowledgesAnUnfence: opening is the safe direction, so one
// replica whose postgres is down does not hold up the unfence of its cohort. It records the
// decision and opens; nothing is admitted while it is not serving, and when its backend
// comes back its serving status changes, which closes the gate and reads the row again. A
// pooler that must be able to serve, an unmanaged one, still refuses.
func TestReplicaWithADownBackendAcknowledgesAnUnfence(t *testing.T) {
	h := newAdmissionHarness(t, false)
	h.pm.record.registered.Store(true)
	h.setRow(true, 1, "")
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.False(t, h.gate.ApplicationOpen())

	// Its postgres goes down, and the cohort is unfenced meanwhile.
	require.NoError(t, h.pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	h.setRow(false, 2, "")
	resp, err := h.refresh(false, 2)
	require.NoError(t, err, "a dead replica must not hold up an unfence")
	require.Equal(t, stateUnfenced, resp.State)
	require.True(t, h.gate.ApplicationOpen(), "it opens: the serving status refuses what it cannot serve")

	// An unmanaged pooler has no such latitude.
	u := newAdmissionHarness(t, true)
	u.setRow(false, 2, "src")
	require.NoError(t, u.pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	_, err = u.refresh(false, 2)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
}

// TestAReplicaPromotedWhileAFenceCommitsStaysClosed: a replica read an unfenced row and
// opened. The fence then commits, and before the coordinator's refresh reaches it the
// replica is promoted. The promotion closes its gate and it has to read its row again
// before opening, and the row now says fenced: it stays closed. A coordinator that fenced
// only the leader it knew of would have left it open.
func TestAReplicaPromotedWhileAFenceCommitsStaysClosed(t *testing.T) {
	h := newAdmissionHarness(t, false)
	h.pm.record.registered.Store(true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen(), "the replica read unfenced and opened")

	h.setRow(true, 1, "") // the fence commits
	require.NoError(t, admissionLifecycle{h.pm}.OnStateChange(t.Context(), servingstate.State{
		Routing:       servingstate.RoutingState{Role: servingstate.RoutingRolePrimary},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}))
	require.False(t, h.gate.ApplicationOpen(), "the promotion closes the gate until the row is read again")

	// The admission loop reads again, as it does after any role change.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.pm.runAdmission(ctx)
	h.pm.signalAdmission()
	require.Eventually(t, func() bool { return h.reads.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	testpoll.Never(t, h.gate.ApplicationOpen, 300*time.Millisecond, 20*time.Millisecond, "it read fenced = true and must stay closed")

	// The coordinator's own refresh is then acknowledged from what the pooler enforces.
	resp, err := h.refresh(true, 1)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State)
	require.False(t, h.gate.ApplicationOpen())
}

// TestAPoolerThatCouldNotReadKeepsTryingUntilItCan: a pooler that has not decided retries
// every interval, so a default primary that was not there at start is picked up as soon as
// it is.
func TestAPoolerThatCouldNotReadKeepsTryingUntilItCan(t *testing.T) {
	old := admissionRetryInterval
	admissionRetryInterval = 20 * time.Millisecond
	t.Cleanup(func() { admissionRetryInterval = old })

	h := newAdmissionHarness(t, true)
	unavailable := errors.New("no default primary yet")
	h.err.Store(&unavailable)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.pm.runAdmission(ctx)
	h.pm.signalAdmission()
	require.Eventually(t, func() bool { return h.reads.Load() > 3 }, 5*time.Second, 10*time.Millisecond, "it keeps trying")
	require.False(t, h.gate.ApplicationOpen())

	h.err.Store(nil)
	require.Eventually(t, h.gate.ApplicationOpen, 10*time.Second, 10*time.Millisecond)
}

// TestADecidedPoolerDoesNotReadAgainUntilSomethingChanges: there is no periodic read. An
// open pooler does not notice a fence it was not told of, and a closed one does not notice
// an unfence: the migrator repeats the unfence until none of the members is unreached. A
// managed pooler reads again when its role changes.
func TestADecidedPoolerDoesNotReadAgainUntilSomethingChanges(t *testing.T) {
	old := admissionRetryInterval
	admissionRetryInterval = 20 * time.Millisecond
	t.Cleanup(func() { admissionRetryInterval = old })

	h := newAdmissionHarness(t, false)
	h.pm.record.registered.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.pm.runAdmission(ctx)
	h.pm.signalAdmission()
	require.Eventually(t, h.gate.ApplicationOpen, 10*time.Second, 10*time.Millisecond)
	reads := h.reads.Load()

	h.setRow(true, 1, "") // a fence this pooler is not told of
	testpoll.Never(t, func() bool { return h.reads.Load() != reads || !h.gate.ApplicationOpen() }, 400*time.Millisecond, 10*time.Millisecond,
		"an open pooler that has decided does not read again")

	// A role change is an event: it closes the gate and the pooler reads the fence.
	require.NoError(t, admissionLifecycle{h.pm}.OnStateChange(t.Context(), servingstate.State{
		Routing:       servingstate.RoutingState{Role: servingstate.RoutingRolePrimary},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
	}))
	require.Eventually(t, func() bool { return h.reads.Load() > reads }, 5*time.Second, 10*time.Millisecond)
	testpoll.Never(t, h.gate.ApplicationOpen, 200*time.Millisecond, 10*time.Millisecond, "it read fenced = true and stays closed")

	// Now decided closed, it does not read until something changes either.
	reads = h.reads.Load()
	h.setRow(false, 2, "") // an unfence this pooler is not told of
	testpoll.Never(t, func() bool { return h.reads.Load() != reads || h.gate.ApplicationOpen() }, 400*time.Millisecond, 10*time.Millisecond,
		"a closed pooler that has decided does not read again")
}

// TestAnUnmanagedPoolersDecisionStandsAcrossAReconnect: its source is writable whatever
// role it reports, so a serving change of the backend neither closes its gate nor makes it
// read its row again.
func TestAnUnmanagedPoolersDecisionStandsAcrossAReconnect(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())
	reads := h.reads.Load()

	require.NoError(t, admissionLifecycle{h.pm}.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	require.NoError(t, admissionLifecycle{h.pm}.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	require.True(t, h.gate.ApplicationOpen())
	require.Equal(t, reads, h.reads.Load())
}

// fakeServingReader is a store of serving state for tests.
type fakeServingReader struct {
	rows       map[string]*servingRow
	app        string
	appVersion int64
	err        error
}

func (f *fakeServingReader) GetRow(_ context.Context, _, tg string) (*servingRow, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.rows[tg], nil
}

func (f *fakeServingReader) GetRouting(context.Context, string) (string, int64, bool, error) {
	return f.app, f.appVersion, f.app != "", f.err
}

// TestServingStateReadsAMissingRowAsUnfenced: a cluster that has never fenced anything has no
// row for the tablegroup (or no store of rows yet, which the store reports as the same); that
// reads as unfenced at version zero. Any other failure is an error, never "unfenced".
func TestServingStateReadsAMissingRowAsUnfenced(t *testing.T) {
	req := &multipoolerservicepb.GetServingStateRequest{Database: "db", Tablegroups: []string{"default", "other"}}

	t.Run("no row", func(t *testing.T) {
		resp, err := readServingState(t.Context(), &fakeServingReader{}, req)
		require.NoError(t, err)
		require.Len(t, resp.GetTablegroups(), 2)
		for _, tg := range resp.GetTablegroups() {
			require.Equal(t, stateUnfenced, tg.GetState())
			require.Zero(t, tg.GetVersion())
		}
		require.Equal(t, []string{"default", "other"}, []string{resp.GetTablegroups()[0].GetTablegroup(), resp.GetTablegroups()[1].GetTablegroup()})
		require.Empty(t, resp.GetAppTablegroup())
	})

	t.Run("rows and routing", func(t *testing.T) {
		store := &fakeServingReader{
			rows: map[string]*servingRow{"other": {connection: "src", state: stateFenced, version: 4}},
			app:  "default", appVersion: 7,
		}
		resp, err := readServingState(t.Context(), store, req)
		require.NoError(t, err)
		require.Equal(t, stateUnfenced, resp.GetTablegroups()[0].GetState())
		require.Equal(t, stateFenced, resp.GetTablegroups()[1].GetState())
		require.EqualValues(t, 4, resp.GetTablegroups()[1].GetVersion())
		require.Equal(t, "src", resp.GetTablegroups()[1].GetBackingConnection())
		require.Equal(t, "default", resp.GetAppTablegroup())
		require.EqualValues(t, 7, resp.GetRoutingVersion())
	})

	t.Run("any other failure is an error", func(t *testing.T) {
		_, err := readServingState(t.Context(), &fakeServingReader{err: errors.New("permission denied")}, req)
		require.Error(t, err)
	})

	t.Run("no store configured", func(t *testing.T) {
		defer func(old func(*MultipoolerManager) servingReader) { newServingStore = old }(newServingStore)
		newServingStore = nil
		pm, _ := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		_, err := pm.servingReader()
		requireCode(t, mtrpcpb.Code_UNIMPLEMENTED, err)
	})
}

// TestFencingAndFencedBothKeepTheGateClosed: the pooler cannot tell a completed fence from
// one in progress and does not need to; both close it, and each is acknowledged only against
// the state the row has.
func TestFencingAndFencedBothKeepTheGateClosed(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	h.setRowState(stateFencing, 3, "src")
	resp, err := h.refreshState(stateFencing, 3)
	require.NoError(t, err)
	require.Equal(t, stateFencing, resp.State)
	require.False(t, h.gate.ApplicationOpen(), "FENCING closes the gate")

	// The coordinator completes the fence: FENCED at the same version. A refresh that still
	// expects FENCING is stale and refused; one that expects FENCED is acknowledged.
	h.setRowState(stateFenced, 3, "src")
	_, err = h.refreshState(stateFencing, 3)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	resp, err = h.refreshState(stateFenced, 3)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State)
	require.False(t, h.gate.ApplicationOpen())

	// Only UNFENCED opens it.
	h.setRowState(stateUnfenced, 4, "src")
	_, err = h.refreshState(stateUnfenced, 4)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())
}
