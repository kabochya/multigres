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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// memStore is an in-memory servingStore with the same conditional semantics as
// the SQL store.
type memStore struct {
	mu      sync.Mutex
	rows    map[string]*servingRow
	app     string
	version int64
	hasApp  bool
	// uncertainNext makes the next CompareAndSet apply and then report an error,
	// like a commit whose acknowledgment was lost.
	uncertainNext bool
	// beforeCAS runs just before a compare-and-set, letting a test inject a
	// concurrent writer.
	beforeCAS func()
}

func newMemStore() *memStore {
	return &memStore{rows: map[string]*servingRow{}}
}

func (s *memStore) add(tg string, state multipoolerservicepb.AdmissionState, requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[tg] = &servingRow{state: state, requestID: requestID, updatedAt: time.Now()}
}

func (s *memStore) GetRow(_ context.Context, _, tg string) (*servingRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rows[tg]
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "tablegroup %q has no serving row", tg)
	}
	c := *r
	return &c, nil
}

func (s *memStore) CompareAndSet(_ context.Context, _, tg string, prev multipoolerservicepb.AdmissionState, prevID string, next multipoolerservicepb.AdmissionState, nextID string) (bool, error) {
	if s.beforeCAS != nil {
		s.beforeCAS()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[tg]
	if r == nil || r.state != prev || r.requestID != prevID {
		return false, nil
	}
	if r.requestID != nextID {
		r.history = append(r.history, r.requestID)
		if len(r.history) > requestHistoryLimit {
			r.history = r.history[len(r.history)-requestHistoryLimit:]
		}
	}
	r.state, r.requestID, r.updatedAt = next, nextID, time.Now()
	if s.uncertainNext {
		s.uncertainNext = false
		return false, errors.New("connection lost after commit")
	}
	return true, nil
}

func (s *memStore) GetRouting(context.Context, string) (string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.app, s.version, s.hasApp, nil
}

func (s *memStore) MoveRouting(_ context.Context, _, from, to string) (string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, t := s.rows[from], s.rows[to]
	if !s.hasApp || s.app != from || f == nil || t == nil ||
		f.state != multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED || t.state != multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED {
		return "", 0, false, nil
	}
	s.app, s.version = to, s.version+1
	return s.app, s.version, true, nil
}

// fakeMembers returns a mutable snapshot.
type fakeMembers struct {
	mu    sync.Mutex
	list  []poolerRef
	calls int
	// onSnapshot runs on every call with the call number (1-based).
	onSnapshot func(call int)
	err        error
}

func (m *fakeMembers) Snapshot(context.Context, string, string) ([]poolerRef, error) {
	m.mu.Lock()
	m.calls++
	call, hook := m.calls, m.onSnapshot
	m.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return append([]poolerRef(nil), m.list...), nil
}

func (m *fakeMembers) set(refs ...poolerRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list = refs
}

func ref(name string) poolerRef {
	return poolerRef{id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}}
}

// fakeRefresher answers RefreshAdmission per pooler.
type fakeRefresher struct {
	mu           sync.Mutex
	calls        map[string]int
	unfenceCalls map[string]int
	// behave overrides the default honest acknowledgment.
	behave func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, call int) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

func (f *fakeRefresher) Refresh(_ context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[p.name()]++
	if req.GetExpectedState() == unfencingState {
		if f.unfenceCalls == nil {
			f.unfenceCalls = map[string]int{}
		}
		f.unfenceCalls[p.name()]++
	}
	call := f.calls[p.name()]
	behave := f.behave
	f.mu.Unlock()
	if behave != nil {
		return behave(p, req, call)
	}
	return honestAck(p, req), nil
}

// unfenceCallsTo counts the refreshes that ask a pooler to reopen.
func (f *fakeRefresher) unfenceCallsTo(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unfenceCalls[name]
}

func (f *fakeRefresher) callsTo(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[name]
}

func honestAck(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse {
	return &multipoolerservicepb.RefreshAdmissionResponse{
		PoolerId: p.id, RequestId: req.GetRequestId(), AppliedState: settledState(req.GetExpectedState()),
	}
}

type coordHarness struct {
	c        *admissionCoordinator
	store    *memStore
	members  *fakeMembers
	refresh  *fakeRefresher
	leaderMu sync.Mutex
	leader   error
}

func (h *coordHarness) setLeader(err error) {
	h.leaderMu.Lock()
	defer h.leaderMu.Unlock()
	h.leader = err
}

func (h *coordHarness) leaderCheck() error {
	h.leaderMu.Lock()
	defer h.leaderMu.Unlock()
	return h.leader
}

func newCoordHarness(t *testing.T) *coordHarness {
	t.Helper()
	h := &coordHarness{store: newMemStore(), members: &fakeMembers{}, refresh: &fakeRefresher{}}
	h.store.add("migrateTG", unfencedState, "")
	h.store.add("destTG", unfencedState, "")
	h.store.app, h.store.hasApp = "migrateTG", true
	h.members.set(ref("ext-1"), ref("ext-2"))
	h.c = &admissionCoordinator{
		store: h.store, members: h.members, refresher: h.refresh,
		logger:       slog.New(slog.DiscardHandler),
		isLeader:     h.leaderCheck,
		retryBackoff: time.Millisecond,
	}
	return h
}

func (h *coordHarness) fence(requestID string) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	return h.c.UpdatePoolerAdmission(context.Background(), &multipoolerservicepb.UpdatePoolerAdmissionRequest{
		Database: "db", Tablegroup: "migrateTG", Target: fencedState, RequestId: requestID, Timeout: durationpb.New(5 * time.Second),
	})
}

func (h *coordHarness) unfence(requestID, expected string) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	return h.c.UpdatePoolerAdmission(context.Background(), &multipoolerservicepb.UpdatePoolerAdmissionRequest{
		Database: "db", Tablegroup: "migrateTG", Target: unfencedState, RequestId: requestID, ExpectedRequestId: expected, Timeout: durationpb.New(5 * time.Second),
	})
}

func (h *coordHarness) row(t *testing.T, tg string) *servingRow {
	t.Helper()
	r, err := h.store.GetRow(context.Background(), "db", tg)
	require.NoError(t, err)
	return r
}

func TestCoordinatorFenceCompletesWhenEveryPoolerAcknowledges(t *testing.T) {
	h := newCoordHarness(t)
	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.State.AdmissionState)
	require.Equal(t, "fence-1", resp.State.RequestId)
	require.Len(t, resp.Acks, 2)
	require.Equal(t, fencedState, h.row(t, "migrateTG").state)
	require.Equal(t, 1, h.refresh.callsTo("ext-1"))
}

// failFencingFor makes a pooler fail to acknowledge a fence, and only that: it answers the
// refreshes of an unfence, which is what an abort asks.
func failFencingFor(h *coordHarness, name string, err error) {
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == name && req.GetExpectedState() == fencingState {
			return nil, err
		}
		return honestAck(p, req), nil
	}
}

// TestCoordinatorFenceThatCannotCompleteIsAbortedAndReopens: a member that cannot acknowledge
// the fence (it is busy, or unreachable) aborts it: the tablegroup is back to UNFENCED,
// every member was asked to reopen, the fence's request id is in the history so a delayed
// retry cannot fence again, and the caller is told so and fences again later under a new id.
func TestCoordinatorFenceThatCannotCompleteIsAbortedAndReopens(t *testing.T) {
	for name, cause := range map[string]error{
		"busy":        errors.New("busy: 1 requests in flight, 0 abandoned backends present"),
		"unreachable": errors.New("unreachable"),
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			failFencingFor(h, "ext-2", cause)
			_, err := h.fence("fence-1")
			require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err), "%v", err)
			require.ErrorContains(t, err, "fence aborted")
			require.ErrorContains(t, err, "ext-2")
			require.ErrorContains(t, err, "serving again")

			row := h.row(t, "migrateTG")
			require.Equal(t, unfencedState, row.state, "an aborted fence leaves normal serving, not a stuck FENCING")
			require.Equal(t, "abort-fence-1", row.requestID)
			require.Contains(t, row.history, "fence-1", "the aborted fence is in the history")
			require.Equal(t, fanOutAttempts, h.refresh.callsTo("ext-2")-h.refresh.unfenceCallsTo("ext-2"), "the pooler is retried a bounded number of times")
			require.Equal(t, 1, h.refresh.unfenceCallsTo("ext-1"), "every member is asked to reopen")
			require.Equal(t, 1, h.refresh.unfenceCallsTo("ext-2"), "including the one that could not fence")

			// A delayed retry of the aborted fence must not fence again.
			_, err = h.fence("fence-1")
			require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
			require.ErrorContains(t, err, "was aborted")
			require.Equal(t, unfencedState, h.row(t, "migrateTG").state)

			// A new fence, once the cause is gone, succeeds.
			h.refresh.behave = nil
			resp, err := h.fence("fence-2")
			require.NoError(t, err)
			require.Equal(t, fencedState, resp.State.AdmissionState)
		})
	}
}

// TestCoordinatorDoesNotAskABusyPoolerAgain: a pooler that says it is busy has already waited
// out its drain deadline; asking again only waits another one. A transient failure, in
// contrast, is retried.
func TestCoordinatorDoesNotAskABusyPoolerAgain(t *testing.T) {
	busy := mterrors.Errorf(mtrpcpb.Code_RESOURCE_EXHAUSTED, "busy: 1 requests in flight, 0 abandoned backends present")
	h := newCoordHarness(t)
	failFencingFor(h, "ext-2", busy)
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err), "%v", err)
	require.Equal(t, 1+1, h.refresh.callsTo("ext-2"), "one fencing refresh and one reopening, no retries")

	h = newCoordHarness(t)
	failFencingFor(h, "ext-2", errors.New("unreachable"))
	_, err = h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	require.Equal(t, fanOutAttempts+1, h.refresh.callsTo("ext-2"), "a transient failure is retried")
}

// TestCoordinatorAbortThatCannotReachEveryMemberStaysUnfencingAndCanBeFinished: if the reopening
// itself is incomplete the state is UNFENCING, the error says how to finish, and retrying
// the abort's request id does.
func TestCoordinatorAbortThatCannotReachEveryMemberStaysUnfencingAndCanBeFinished(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-2" {
			return nil, errors.New("unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	require.ErrorContains(t, err, "reopening is incomplete")
	require.ErrorContains(t, err, `request_id="abort-fence-1"`)
	require.Equal(t, unfencingState, h.row(t, "migrateTG").state)

	h.refresh.behave = nil
	resp, err := h.unfence("abort-fence-1", "fence-1")
	require.NoError(t, err)
	require.Equal(t, unfencedState, resp.State.AdmissionState)
	require.Equal(t, unfencedState, h.row(t, "migrateTG").state)
}

// TestCoordinatorAFenceInProgressCanBeAbortedByNamingIt: after a coordinator that died
// mid-fence, GetServingState shows FENCING; an unfence naming that fence aborts it.
func TestCoordinatorAFenceInProgressCanBeAbortedByNamingIt(t *testing.T) {
	h := newCoordHarness(t)
	h.store.add("migrateTG", fencingState, "fence-9")
	_, err := h.unfence("unfence-1", "fence-9")
	require.NoError(t, err)
	require.Equal(t, unfencedState, h.row(t, "migrateTG").state)
	_, err = h.fence("fence-9")
	require.Error(t, err, "and the fence it aborted cannot run again")

	// Naming another request is still refused.
	h.store.add("migrateTG", fencingState, "fence-10")
	_, err = h.unfence("unfence-2", "fence-9")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
}

func TestCoordinatorTransientRefreshFailureIsRetriedWithinTheCall(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, call int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-1" && call == 1 {
			return nil, errors.New("transient")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Equal(t, 2, h.refresh.callsTo("ext-1"))
}

func TestCoordinatorPoolerThatDisappearsFailsFast(t *testing.T) {
	h := newCoordHarness(t)
	// ext-2 answers nothing and is gone from the fresh snapshot.
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-2" {
			h.members.set(ref("ext-1"))
			return nil, errors.New("connection refused")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	require.NotEqual(t, fencedState, h.row(t, "migrateTG").state, "a vanished pooler is not proof it stopped serving, so the fence is not completed")
	require.Equal(t, unfencedState, h.row(t, "migrateTG").state, "it is aborted, and the members that remain reopen")
}

func TestCoordinatorPoolerThatAcknowledgedThenVanishedStillCounts(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		resp := honestAck(p, req)
		if p.name() == "ext-2" {
			h.members.set(ref("ext-1")) // it acknowledged, then went away
		}
		return resp, nil
	}
	_, err := h.fence("fence-1")
	require.NoError(t, err)
}

// TestCoordinatorLateJoinerThatVanishesStaysRequired: a pooler first seen in the
// closing snapshot is required from then on, even if the next enumeration no
// longer lists it, so it cannot slip out unacknowledged.
func TestCoordinatorLateJoinerThatVanishesStaysRequired(t *testing.T) {
	h := newCoordHarness(t)
	h.members.onSnapshot = func(call int) {
		switch call {
		case 2: // the closing snapshot of the first round lists a new pooler
			h.members.set(ref("ext-1"), ref("ext-2"), ref("ext-3"))
		case 3: // and the next enumeration no longer does
			h.members.set(ref("ext-1"), ref("ext-2"))
		}
	}
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-3" {
			return nil, errors.New("connection refused")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence("fence-1")
	require.Error(t, err)
	require.NotEqual(t, fencedState, h.row(t, "migrateTG").state, "the fence is not completed over a pooler that never acknowledged")
}

func TestCoordinatorPoolerJoiningMidOperationIsIncluded(t *testing.T) {
	h := newCoordHarness(t)
	joined := ref("ext-3")
	h.members.onSnapshot = func(call int) {
		if call == 2 { // after the first round
			h.members.set(ref("ext-1"), ref("ext-2"), joined)
		}
	}
	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 3)
	require.Equal(t, 1, h.refresh.callsTo("ext-3"), "the pooler that joined was asked too")
}

// TestCoordinatorAcknowledgmentOfARestartedPoolerStillStands: acknowledgments are
// keyed by pooler id. A pooler that acknowledged and then restarted under the same
// id is safe to count, because the process that replaced it starts after the
// FENCING commit, reads FENCING and stays closed.
func TestCoordinatorAcknowledgmentOfARestartedPoolerStillStands(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		return honestAck(p, req), nil // ext-1 restarts right after this; topology cannot tell
	}
	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 2)
	require.Equal(t, 1, h.refresh.callsTo("ext-1"), "the restarted pooler is not asked again")
}

// TestCoordinatorPoolerRestartedAfterTheFencingCommitCompletesTheFence: a pooler
// whose process was replaced after FENCING was committed answers the refresh from
// its successor, which read FENCING at startup and is already closed; the fence
// completes.
func TestCoordinatorPoolerRestartedAfterTheFencingCommitCompletesTheFence(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, attempt int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-1" && attempt == 1 {
			return nil, errors.New("connection refused: the process is being replaced")
		}
		return honestAck(p, req), nil
	}
	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.State.AdmissionState)
	require.Len(t, resp.Acks, 2)
	require.Equal(t, 2, h.refresh.callsTo("ext-1"))
}

func TestCoordinatorRejectsInexactAcknowledgments(t *testing.T) {
	for name, tamper := range map[string]func(*multipoolerservicepb.RefreshAdmissionResponse){
		"wrong request": func(r *multipoolerservicepb.RefreshAdmissionResponse) { r.RequestId = "other" },
		"wrong state":   func(r *multipoolerservicepb.RefreshAdmissionResponse) { r.AppliedState = unfencedState },
		"wrong pooler": func(r *multipoolerservicepb.RefreshAdmissionResponse) {
			r.PoolerId = &clustermetadatapb.ID{Name: "someone-else"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
				resp := honestAck(p, req)
				tamper(resp)
				return resp, nil
			}
			_, err := h.fence("fence-1")
			require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
			// The same tampering spoils the abort's acknowledgments, so the reopening is
			// incomplete (UNFENCING), unless the tampering is what the abort expects
			// anyway. Never FENCED.
			require.Contains(t, []multipoolerservicepb.AdmissionState{unfencingState, unfencedState}, h.row(t, "migrateTG").state)
		})
	}
}

func TestCoordinatorSameRequestAfterCompletionIsIdempotent(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence("fence-1")
	require.NoError(t, err)
	calls := h.refresh.callsTo("ext-1")
	updated := h.row(t, "migrateTG").updatedAt

	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.State.AdmissionState)
	require.Greater(t, h.refresh.callsTo("ext-1"), calls, "a repeat asks the poolers again")
	require.Equal(t, updated, h.row(t, "migrateTG").updatedAt, "a repeat rewrites nothing")
	require.Len(t, resp.Acks, 2)
}

// TestCoordinatorRepeatOfACompletedOperationVerifiesThePoolers: the repeat is a
// safety net, so it fails when a pooler no longer holds the state.
func TestCoordinatorRepeatOfACompletedOperationVerifiesThePoolers(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence("fence-1")
	require.NoError(t, err)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-2" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err = h.fence("fence-1")
	require.Error(t, err)
	require.Equal(t, fencedState, h.row(t, "migrateTG").state)
}

// TestCoordinatorDelayedRetryOfASupersededRequestIsRejected: a retry of fence F
// that arrives after F and the unfence U that followed must not re-fence the
// serving tablegroup or displace U's request id.
func TestCoordinatorDelayedRetryOfASupersededRequestIsRejected(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence("fence-1")
	require.NoError(t, err)
	_, err = h.unfence("unfence-1", "fence-1")
	require.NoError(t, err)

	_, err = h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
	require.Equal(t, unfencedState, h.row(t, "migrateTG").state)
	require.Equal(t, "unfence-1", h.row(t, "migrateTG").requestID)
}

// TestCoordinatorSameRequestRacingItselfSucceedsOnBothCalls: two calls with one
// request id both fan out; the one that loses the final compare-and-set sees the
// operation already committed and reports success.
func TestCoordinatorSameRequestRacingItselfSucceedsOnBothCalls(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence("fence-1")
	require.NoError(t, err)
	// The row is already FENCED under the same id by the time this call commits.
	row := h.row(t, "migrateTG")
	require.Equal(t, fencedState, row.state)
	h.store.mu.Lock()
	h.store.rows["migrateTG"].state = fencingState // as the loser saw it when it started
	h.store.mu.Unlock()
	h.store.beforeCAS = func() {
		h.store.mu.Lock()
		h.store.rows["migrateTG"].state = fencedState // the winner commits first
		h.store.mu.Unlock()
	}
	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.State.AdmissionState)
}

func TestCoordinatorDelayedUnfenceCannotOverrideNewerFence(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence("fence-A")
	require.NoError(t, err)
	_, err = h.fence("fence-B") // a newer coordinator fences again
	require.NoError(t, err)

	_, err = h.unfence("unfence-A", "fence-A")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	require.Equal(t, fencedState, h.row(t, "migrateTG").state)
	require.Equal(t, "fence-B", h.row(t, "migrateTG").requestID)

	_, err = h.unfence("unfence-B", "")
	require.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))

	resp, err := h.unfence("unfence-B", "fence-B")
	require.NoError(t, err)
	require.Equal(t, unfencedState, resp.State.AdmissionState)
}

func TestCoordinatorUnfenceNeedsACompletedFence(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		return nil, errors.New("down")
	}
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	_, err = h.unfence("unfence-1", "fence-1")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "cannot unfence a fence that never completed")
}

func TestCoordinatorFencePreemptsUnfenceInProgress(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence("f1")
	require.NoError(t, err)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "ext-1" {
			return nil, errors.New("down")
		}
		return honestAck(p, req), nil
	}
	_, err = h.unfence("u1", "f1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	require.Equal(t, unfencingState, h.row(t, "migrateTG").state)

	h.refresh.behave = nil
	resp, err := h.fence("f2")
	require.NoError(t, err)
	require.Equal(t, "f2", resp.State.RequestId)

	// The preempted unfence's retry now loses.
	_, err = h.unfence("u1", "f1")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
}

func TestCoordinatorRequestSupersededWhileFanningOut(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		// A newer coordinator takes over the row while this one waits on poolers.
		h.store.mu.Lock()
		h.store.rows["migrateTG"].requestID = "newer"
		h.store.mu.Unlock()
		return honestAck(p, req), nil
	}
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
	require.Equal(t, fencingState, h.row(t, "migrateTG").state)
	require.Equal(t, "newer", h.row(t, "migrateTG").requestID, "the old coordinator wrote nothing over the newer one")
}

// TestCoordinatorUncertainCommitThenReread: the commit of FENCING succeeded but
// its acknowledgment was lost. The caller re-reads, sees FENCING under its own
// request id, and retries the same request to completion.
func TestCoordinatorUncertainCommitThenReread(t *testing.T) {
	h := newCoordHarness(t)
	h.store.uncertainNext = true
	_, err := h.fence("fence-1")
	require.Error(t, err)
	require.ErrorContains(t, err, "outcome uncertain")
	require.Zero(t, h.refresh.callsTo("ext-1"), "nothing was fanned out after an uncertain commit")

	row := h.row(t, "migrateTG")
	require.Equal(t, fencingState, row.state, "the state is persisted although the call errored")
	require.Equal(t, "fence-1", row.requestID)

	resp, err := h.fence("fence-1")
	require.NoError(t, err)
	require.Equal(t, fencedState, resp.State.AdmissionState)
}

func TestCoordinatorStaleLeaderWritesNothing(t *testing.T) {
	h := newCoordHarness(t)
	h.setLeader(mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "not the default primary pooler"))
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	require.Equal(t, unfencedState, h.row(t, "migrateTG").state)

	// Losing leadership after fanning out stops the terminal write.
	h.setLeader(nil)
	h.refresh.behave = func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, _ int) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		h.setLeader(mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "not the default primary pooler"))
		return honestAck(p, req), nil
	}
	_, err = h.fence("fence-2")
	require.Error(t, err)
	require.Equal(t, fencingState, h.row(t, "migrateTG").state, "FENCED is never committed by a deposed coordinator")
}

func TestCoordinatorConcurrentWriterLosesTheCompareAndSet(t *testing.T) {
	h := newCoordHarness(t)
	h.store.beforeCAS = func() {
		h.store.mu.Lock()
		h.store.rows["migrateTG"].requestID = "someone-else"
		h.store.mu.Unlock()
	}
	_, err := h.fence("fence-1")
	require.Equal(t, mtrpcpb.Code_ABORTED, mterrors.Code(err))
	require.Equal(t, "someone-else", h.row(t, "migrateTG").requestID)
}

func TestCoordinatorValidatesRequests(t *testing.T) {
	h := newCoordHarness(t)
	for name, req := range map[string]*multipoolerservicepb.UpdatePoolerAdmissionRequest{
		"transitional target": {Database: "db", Tablegroup: "migrateTG", Target: fencingState, RequestId: "x"},
		"no request id":       {Database: "db", Tablegroup: "migrateTG", Target: fencedState},
		"no tablegroup":       {Database: "db", Target: fencedState, RequestId: "x"},
		"unfence without cas": {Database: "db", Tablegroup: "migrateTG", Target: unfencedState, RequestId: "x"},
	} {
		_, err := h.c.UpdatePoolerAdmission(context.Background(), req)
		require.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), name)
	}
	_, err := h.c.UpdatePoolerAdmission(context.Background(), &multipoolerservicepb.UpdatePoolerAdmissionRequest{Database: "db", Tablegroup: "missingTG", Target: fencedState, RequestId: "x"})
	require.Equal(t, mtrpcpb.Code_NOT_FOUND, mterrors.Code(err))
}

func TestCoordinatorMembershipFailureAbortsWithoutCompleting(t *testing.T) {
	h := newCoordHarness(t)
	h.members.err = errors.New("topology unavailable")
	_, err := h.fence("fence-1")
	require.Error(t, err)
	require.NotEqual(t, fencedState, h.row(t, "migrateTG").state)
	require.Equal(t, unfencingState, h.row(t, "migrateTG").state, "the abort could not enumerate the members either: it stays UNFENCING")
}

func TestCoordinatorRouting(t *testing.T) {
	h := newCoordHarness(t)
	route := func(from, to string) (*multipoolerservicepb.UpdateMigrationRoutingResponse, error) {
		return h.c.UpdateMigrationRouting(context.Background(), &multipoolerservicepb.UpdateMigrationRoutingRequest{
			Database: "db", FromTablegroup: from, ToTablegroup: to, RequestId: "r",
		})
	}
	_, err := route("migrateTG", "destTG")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "both sides must be fenced")

	_, err = h.fence("f-src")
	require.NoError(t, err)
	_, err = route("migrateTG", "destTG")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "the destination must be fenced too")
	h.store.add("destTG", fencedState, "f-dest")

	_, err = route("destTG", "elsewhereTG")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "from must be the current pointer")

	first, err := route("migrateTG", "destTG")
	require.NoError(t, err)
	require.Equal(t, "destTG", first.AppTablegroup)
	require.EqualValues(t, 1, first.RoutingVersion)
	retry, err := route("migrateTG", "destTG")
	require.NoError(t, err)
	require.Equal(t, first.RoutingVersion, retry.RoutingVersion, "a retry after the move must not bump the version")

	// Routing never unfences anything.
	require.Equal(t, fencedState, h.row(t, "migrateTG").state)
	require.Equal(t, fencedState, h.row(t, "destTG").state)

	_, err = route("migrateTG", "migrateTG")
	require.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
}

func TestCoordinatorRoutingNeedsAPointerAndALeader(t *testing.T) {
	h := newCoordHarness(t)
	h.store.hasApp = false
	_, err := h.c.UpdateMigrationRouting(context.Background(), &multipoolerservicepb.UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: "migrateTG", ToTablegroup: "destTG", RequestId: "r"})
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))

	h = newCoordHarness(t)
	h.store.add("migrateTG", fencedState, "f1")
	h.store.add("destTG", fencedState, "f2")
	h.setLeader(mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "not the default primary pooler"))
	_, err = h.c.UpdateMigrationRouting(context.Background(), &multipoolerservicepb.UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: "migrateTG", ToTablegroup: "destTG", RequestId: "r"})
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
	require.Equal(t, "migrateTG", h.store.app)
}

// TestCoordinatorMovesTrafficIntoAndOutOfTheDefaultTablegroup: the default
// tablegroup is a valid destination. It is fenced like any other, with every pooler
// of the default cohort, the default primary among them, in the fan-out; and it is
// either side of the routing move.
func TestCoordinatorMovesTrafficIntoAndOutOfTheDefaultTablegroup(t *testing.T) {
	h := newCoordHarness(t)
	h.store.add("default", unfencedState, "")
	h.members.set(ref("def-primary"), ref("def-replica-1"), ref("def-replica-2"))
	ctx := context.Background()
	fence := func(tg, id string) {
		t.Helper()
		resp, err := h.c.UpdatePoolerAdmission(ctx, &multipoolerservicepb.UpdatePoolerAdmissionRequest{
			Database: "db", Tablegroup: tg, Target: fencedState, RequestId: id, Timeout: durationpb.New(5 * time.Second),
		})
		require.NoError(t, err, "fence %s", tg)
		require.Equal(t, fencedState, resp.State.AdmissionState)
		require.Len(t, resp.Acks, 3, "every pooler of the cohort acknowledges, the default primary too")
	}
	route := func(from, to, id string) error {
		_, err := h.c.UpdateMigrationRouting(ctx, &multipoolerservicepb.UpdateMigrationRoutingRequest{
			Database: "db", FromTablegroup: from, ToTablegroup: to, RequestId: id,
		})
		return err
	}

	fence("default", "f-default")
	require.Equal(t, 1, h.refresh.callsTo("def-primary"), "the default primary is refreshed like any member")
	require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(route("migrateTG", "default", "r1")), "the source is not fenced yet")
	fence("migrateTG", "f-src")
	require.NoError(t, route("migrateTG", "default", "r1"))
	require.Equal(t, "default", h.store.app)

	// The rollback: the default tablegroup is the source of the move.
	require.NoError(t, route("default", "migrateTG", "r2"))
	require.Equal(t, "migrateTG", h.store.app)

	// And it unfences like any other tablegroup.
	resp, err := h.c.UpdatePoolerAdmission(ctx, &multipoolerservicepb.UpdatePoolerAdmissionRequest{
		Database: "db", Tablegroup: "default", Target: unfencedState, RequestId: "u-default", ExpectedRequestId: "f-default", Timeout: durationpb.New(5 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, unfencedState, resp.State.AdmissionState)
}
