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

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/tools/testpoll"
)

const (
	unfencedState  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	fencingState   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	fencedState    = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	unfencingState = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING
)

// gatedController is a PoolerController whose application gate is a real
// QueryPoolerServer's, so admission is exercised against the real gate.
type gatedController struct {
	*mockPoolerController
	gate *poolerserver.QueryPoolerServer
}

func (g gatedController) EnableAdmissionControl()         { g.gate.EnableAdmissionControl() }
func (g gatedController) AdmissionGeneration() uint64     { return g.gate.AdmissionGeneration() }
func (g gatedController) OpenApplication(gen uint64) bool { return g.gate.OpenApplication(gen) }
func (g gatedController) CloseApplication()               { g.gate.CloseApplication() }
func (g gatedController) ApplicationOpen() bool           { return g.gate.ApplicationOpen() }
func (g gatedController) FenceApplication(ctx context.Context, d time.Duration) (int, error) {
	return g.gate.FenceApplication(ctx, d)
}

// admissionHarness is an admission-controlled manager whose authoritative read
// is scripted.
type admissionHarness struct {
	pm    *MultipoolerManager
	gate  *poolerserver.QueryPoolerServer
	row   atomic.Pointer[multipoolerservicepb.TablegroupServingState]
	err   atomic.Pointer[error]
	reads atomic.Int32
}

func newAdmissionHarness(t *testing.T, unmanaged bool) *admissionHarness {
	t.Helper()
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.ShardKey = &clustermetadatapb.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"}
	cfg := &Config{AdmissionControl: true, AllowNonDefaultTableGroup: true, AdmissionDrainTimeout: time.Second}
	if unmanaged {
		initial.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
		cfg.AdmissionControl = false
		cfg.BackingConnectionName = "src"
		cfg.ExternalBackend = &ExternalBackend{Host: "db", Port: 5432, Database: "db", ExpectedSystemIdentifier: "1"}
	}
	pm, err := NewMultipoolerManager(slog.New(slog.DiscardHandler), initial, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})

	gate := poolerserver.NewQueryPoolerServer(slog.New(slog.DiscardHandler), nil, initial.Id, "migrateTG", "0-inf", nil, 0, false)
	// Serving, so admitted requests are not rejected for the serving state.
	require.NoError(t, gate.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))
	gate.EnableAdmissionControl()
	pm.qsc = gatedController{mockPoolerController: &mockPoolerController{}, gate: gate}

	h := &admissionHarness{pm: pm, gate: gate}
	backing := ""
	if unmanaged {
		backing = "src"
	}
	h.setRow(unfencedState, "r1", backing)
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

func (h *admissionHarness) setRow(state multipoolerservicepb.AdmissionState, requestID, backing string) {
	h.row.Store(&multipoolerservicepb.TablegroupServingState{
		Tablegroup: "migrateTG", AdmissionState: state, RequestId: requestID, BackingConnection: backing,
	})
}

func (h *admissionHarness) refresh(expected multipoolerservicepb.AdmissionState, requestID string) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	return h.pm.RefreshAdmission(context.Background(), &multipoolerservicepb.RefreshAdmissionRequest{
		Database: "db", Tablegroup: "migrateTG", ExpectedState: expected, RequestId: requestID,
	})
}

func requireCode(t *testing.T, want mtrpcpb.Code, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, want, mterrors.Code(err), "%v", err)
}

func TestAdmissionStartupOpensOnlyOnUnfenced(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state multipoolerservicepb.AdmissionState
		open  bool
	}{
		{"unfenced opens", unfencedState, true},
		{"unfencing opens", unfencingState, true},
		{"fencing stays closed", fencingState, false},
		{"fenced stays closed", fencedState, false},
		{"unspecified stays closed", multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newAdmissionHarness(t, true)
			require.False(t, h.gate.ApplicationOpen(), "a controlled pooler starts closed")
			h.setRow(tc.state, "r1", "src")
			_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
			require.NoError(t, err)
			require.Equal(t, tc.open, h.gate.ApplicationOpen())
		})
	}
}

func TestAdmissionUnreadableOrMissingStateNeverOpens(t *testing.T) {
	h := newAdmissionHarness(t, true)
	unreachable := errors.New("default primary unreachable")
	h.err.Store(&unreachable)
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	require.Error(t, err)
	require.False(t, h.gate.ApplicationOpen())

	missing := mterrors.New(mtrpcpb.Code_NOT_FOUND, "tablegroup has no serving row")
	h.err.Store(&missing)
	_, err = h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	require.Error(t, err)
	require.False(t, h.gate.ApplicationOpen())
}

func TestAdmissionBackendUnavailableNeverOpens(t *testing.T) {
	h := newAdmissionHarness(t, true)
	h.pm.healthStreamer.setBackendReady(false)
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen(), "an identity-unvalidated backend must not be admitted")

	h.pm.healthStreamer.setBackendReady(true)
	require.NoError(t, h.pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	_, err = h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen())
}

func TestRefreshFenceThenUnfence(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	// The coordinator persists FENCING, then asks the pooler to refresh.
	h.setRow(fencingState, "fence-1", "src")
	resp, err := h.refresh(fencingState, "fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.AppliedState, "a fence is acknowledged as FENCED")
	require.Equal(t, "fence-1", resp.RequestId)
	require.False(t, h.gate.ApplicationOpen())
	require.Equal(t, h.pm.record.Snapshot().GetId().GetName(), resp.PoolerId.GetName())

	// A repeat of the same refresh is acknowledged without another read.
	reads := h.reads.Load()
	resp, err = h.refresh(fencingState, "fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.AppliedState)
	require.Equal(t, reads, h.reads.Load())

	// Unfence.
	h.setRow(unfencingState, "unfence-1", "src")
	resp, err = h.refresh(unfencingState, "unfence-1")
	require.NoError(t, err)
	require.Equal(t, unfencedState, resp.AppliedState)
	require.True(t, h.gate.ApplicationOpen())
}

func TestRefreshExpectationIsNeverPermission(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	require.NoError(t, err)

	// The coordinator expects FENCING for r2, but the authority still says
	// UNFENCED for r1: nothing is applied and the open gate stays open.
	_, err = h.refresh(fencingState, "r2")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.True(t, h.gate.ApplicationOpen())

	// A delayed unfence of an older request cannot override the newer fence.
	h.setRow(fencedState, "fence-B", "src")
	_, err = h.refresh(unfencingState, "unfence-A")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.True(t, h.gate.ApplicationOpen(), "the mismatch applied nothing")
	_, err = h.refresh(fencedState, "fence-B")
	require.NoError(t, err)
	require.False(t, h.gate.ApplicationOpen())
	_, err = h.refresh(unfencingState, "unfence-A")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen(), "a stale unfence must not reopen a fenced pooler")
}

func TestRefreshRejectsMismatchedBacking(t *testing.T) {
	h := newAdmissionHarness(t, true)
	h.setRow(unfencedState, "r1", "some-other-connection")
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.False(t, h.gate.ApplicationOpen())

	// A managed pooler is backed by the managed cohort: both are empty.
	m := newAdmissionHarness(t, false)
	m.setRow(unfencedState, "r1", "src")
	_, err = m.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
}

func TestRefreshValidatesRequest(t *testing.T) {
	h := newAdmissionHarness(t, true)
	call := func(db, tg string, state multipoolerservicepb.AdmissionState, id string) error {
		_, err := h.pm.RefreshAdmission(t.Context(), &multipoolerservicepb.RefreshAdmissionRequest{Database: db, Tablegroup: tg, ExpectedState: state, RequestId: id})
		return err
	}
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, call("other", "migrateTG", fencingState, "x"))
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, call("db", "otherTG", fencingState, "x"))
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, call("db", "migrateTG", multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "x"))
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, call("db", "migrateTG", fencingState, ""))

	// A managed pooler without admission control refuses.
	plain := newTestManagerForAdmissionOff(t)
	_, err := plain.RefreshAdmission(t.Context(), &multipoolerservicepb.RefreshAdmissionRequest{Database: "db", Tablegroup: "migrateTG", ExpectedState: fencingState, RequestId: "x"})
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
}

func newTestManagerForAdmissionOff(t *testing.T) *MultipoolerManager {
	t.Helper()
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.ShardKey = &clustermetadatapb.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"}
	pm, err := NewMultipoolerManager(slog.New(slog.DiscardHandler), initial, &Config{AllowNonDefaultTableGroup: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})
	return pm
}

// TestLifecycleChangeWithdrawsAdmission: a role change, a serving change or a
// backend reconnect closes the gate and forgets the decision until it is read
// again.
func TestLifecycleChangeWithdrawsAdmission(t *testing.T) {
	h := newAdmissionHarness(t, true)
	lifecycle := admissionLifecycle{h.pm}
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())

	// First sync with the current state: the decision is withdrawn once.
	state := servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}
	require.NoError(t, lifecycle.OnStateChange(t.Context(), state))
	require.False(t, h.gate.ApplicationOpen())
	_, err = h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
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
	_, err = h.refresh(unfencedState, "r1")
	require.NoError(t, err)
	require.Greater(t, h.reads.Load(), reads, "a withdrawn decision is re-read, not acknowledged from memory")
	require.True(t, h.gate.ApplicationOpen())
}

// TestOpenDecisionReadBeforeAChangeIsRejected: a read that began before the
// pooler's role changed must not open the gate afterwards.
func TestOpenDecisionReadBeforeAChangeIsRejected(t *testing.T) {
	h := newAdmissionHarness(t, true)
	lifecycle := admissionLifecycle{h.pm}
	h.pm.admission.reader = func(context.Context) (*multipoolerservicepb.TablegroupServingState, error) {
		// The role changes while the read is in flight.
		_ = lifecycle.OnStateChange(t.Context(), servingstate.State{
			Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		})
		return h.row.Load(), nil
	}
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	require.False(t, h.gate.ApplicationOpen())
}

// TestFenceWaitsForInFlightWorkAndFailsWithoutAcknowledging: a refresh that
// cannot drain returns an error and leaves the gate closed.
func TestFenceDoesNotAcknowledgeWhileWorkIsInFlight(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
	require.NoError(t, err)
	release, err := h.gate.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.NoError(t, err)

	h.setRow(fencingState, "fence-1", "src")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = h.pm.RefreshAdmission(ctx, &multipoolerservicepb.RefreshAdmissionRequest{Database: "db", Tablegroup: "migrateTG", ExpectedState: fencingState, RequestId: "fence-1"})
	require.Error(t, err, "work is still in flight")
	require.False(t, h.gate.ApplicationOpen(), "the gate is closed even though the fence did not complete")

	release()
	resp, err := h.refresh(fencingState, "fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.AppliedState)
}

func TestGetServingStateOnlyServedByTheDefaultPrimary(t *testing.T) {
	h := newAdmissionHarness(t, true)
	_, err := h.pm.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db"})
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.pm.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "other"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
}

func TestParseAdmissionState(t *testing.T) {
	for name, want := range map[string]multipoolerservicepb.AdmissionState{
		"UNFENCED": unfencedState, "FENCING": fencingState, "FENCED": fencedState, "UNFENCING": unfencingState,
	} {
		got, err := parseAdmissionState(name)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := parseAdmissionState("OPEN")
	require.Error(t, err)
}

// TestManagedPoolerReadsAdmissionOnlyAfterItIsRegistered: the coordinator fences
// the poolers it finds in topology, so a managed pooler that read UNFENCED and
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
	_, err := h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
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
			_, _ = h.pm.enforceAdmission(t.Context(), multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
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
