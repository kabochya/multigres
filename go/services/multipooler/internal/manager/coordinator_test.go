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
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// The tablegroups of these tests are "alpha" and "beta": the coordinator is generic over
// the tablegroup it fences, and nothing in it may depend on the name "default".

// memStore is an in-memory servingStore with the same conditional semantics as the SQL
// store.
type memStore struct {
	mu         sync.Mutex
	rows       map[string]*servingRow
	app        string
	appVersion int64
	hasApp     bool
	// uncertainNext makes the next CompareAndSet apply and then report an error, like a
	// commit whose acknowledgment was lost.
	uncertainNext bool
	// beforeCAS runs just before a compare-and-set, letting a test inject a concurrent
	// writer; afterCAS runs just after one that succeeded.
	beforeCAS func()
	afterCAS  func()
	writes    int
	// readErr is returned by GetRow.
	readErr error
}

func newMemStore() *memStore {
	return &memStore{rows: map[string]*servingRow{}}
}

// add sets a tablegroup's row; fenced stands for a completed fence, FENCED.
func (s *memStore) add(tg string, fenced bool, version int64) {
	s.addState(tg, stateOf(fenced), version)
}

func (s *memStore) addState(tg string, state multipoolerservicepb.AdmissionState, version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[tg] = &servingRow{state: state, version: version, updatedAt: time.Now()}
}

func (s *memStore) GetRow(_ context.Context, _, tg string) (*servingRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return nil, s.readErr
	}
	r, ok := s.rows[tg]
	if !ok {
		return nil, nil
	}
	c := *r
	return &c, nil
}

func (s *memStore) CompareAndSet(_ context.Context, _, tg string, prevState multipoolerservicepb.AdmissionState, prevVersion int64, nextState multipoolerservicepb.AdmissionState, nextVersion int64) (bool, error) {
	if s.beforeCAS != nil {
		s.beforeCAS()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rows[tg]
	if r == nil && prevState == stateUnfenced && prevVersion == 0 {
		// A tablegroup that was never fenced has no row: the fence creates it.
		r = &servingRow{state: stateUnfenced}
		s.rows[tg] = r
	}
	if r == nil || r.state != prevState || r.version != prevVersion {
		return false, nil
	}
	r.state, r.version, r.updatedAt = nextState, nextVersion, time.Now()
	s.writes++
	if s.afterCAS != nil {
		after := s.afterCAS
		defer func() { s.mu.Unlock(); after(); s.mu.Lock() }()
	}
	if s.uncertainNext {
		s.uncertainNext = false
		return false, errors.New("connection lost after commit")
	}
	return true, nil
}

func (s *memStore) GetRouting(context.Context, string) (string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.app, s.appVersion, s.hasApp, nil
}

func (s *memStore) MoveRouting(_ context.Context, _, from, to string, expectedVersion int64) (string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, t := s.rows[from], s.rows[to]
	if !s.hasApp || s.app != from || s.appVersion != expectedVersion || f == nil || t == nil || f.state != stateFenced || t.state != stateFenced {
		return "", 0, false, nil
	}
	s.app, s.appVersion = to, s.appVersion+1
	return s.app, s.appVersion, true, nil
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
	mu          sync.Mutex
	fenceCalls  map[string]int
	reopenCalls map[string]int
	// behave overrides the default honest acknowledgment.
	behave func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

func (f *fakeRefresher) Refresh(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	f.mu.Lock()
	calls := &f.fenceCalls
	if req.GetExpectedState() == stateUnfenced {
		calls = &f.reopenCalls
	}
	if *calls == nil {
		*calls = map[string]int{}
	}
	(*calls)[p.name()]++
	behave := f.behave
	f.mu.Unlock()
	if behave != nil {
		return behave(ctx, p, req)
	}
	return honestAck(p, req), nil
}

func (f *fakeRefresher) fencesTo(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fenceCalls[name]
}

func (f *fakeRefresher) reopensTo(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reopenCalls[name]
}

// honestAck is the answer of a pooler that did what it was asked.
func honestAck(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse {
	return &multipoolerservicepb.RefreshAdmissionResponse{PoolerId: p.id, State: req.GetExpectedState(), Version: req.GetExpectedVersion()}
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
	h.store.add("alpha", false, 0)
	h.store.add("beta", false, 0)
	h.store.app, h.store.hasApp = "alpha", true
	// A tablegroup of a writable member and a replica.
	h.members.set(ref("w"), ref("r"))
	h.c = &admissionCoordinator{
		store: h.store, members: h.members, refresher: h.refresh,
		logger:        slog.New(slog.DiscardHandler),
		isLeader:      h.leaderCheck,
		callTimeout:   2 * time.Second,
		reopenTimeout: 200 * time.Millisecond,
	}
	return h
}

func (h *coordHarness) fenceTG(tg string, version int64) (*multipoolerservicepb.FenceTablegroupResponse, error) {
	return h.c.FenceTablegroup(context.Background(), &multipoolerservicepb.FenceTablegroupRequest{
		Database: "db", Tablegroup: tg, ExpectedVersion: version, Timeout: durationpb.New(5 * time.Second),
	})
}

func (h *coordHarness) fence(version int64) (*multipoolerservicepb.FenceTablegroupResponse, error) {
	return h.fenceTG("alpha", version)
}

func (h *coordHarness) unfenceTG(tg string, version int64) (*multipoolerservicepb.UnfenceTablegroupResponse, error) {
	return h.c.UnfenceTablegroup(context.Background(), &multipoolerservicepb.UnfenceTablegroupRequest{
		Database: "db", Tablegroup: tg, ExpectedVersion: version,
	})
}

func (h *coordHarness) unfence(version int64) (*multipoolerservicepb.UnfenceTablegroupResponse, error) {
	return h.unfenceTG("alpha", version)
}

func (h *coordHarness) flip(from, to string, version int64) (*multipoolerservicepb.FlipRoutingResponse, error) {
	return h.c.FlipRouting(context.Background(), &multipoolerservicepb.FlipRoutingRequest{
		Database: "db", FromTablegroup: from, ToTablegroup: to, ExpectedRoutingVersion: version,
	})
}

func (h *coordHarness) row(t *testing.T, tg string) *servingRow {
	t.Helper()
	r, err := h.store.GetRow(context.Background(), "db", tg)
	require.NoError(t, err)
	return r
}

// notClean requires that a fence did not complete, and returns the per-member results the
// caller is told, as the details of the error's status.
func notClean(t *testing.T, err error) map[string]*multipoolerservicepb.MemberFenceResult {
	t.Helper()
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	var incomplete *FenceIncompleteError
	require.ErrorAs(t, err, &incomplete)
	st := incomplete.GRPCStatus()
	require.Equal(t, codes.Aborted, st.Code())
	var detail *multipoolerservicepb.FenceTablegroupResponse
	for _, d := range st.Details() {
		if r, ok := d.(*multipoolerservicepb.FenceTablegroupResponse); ok {
			detail = r
		}
	}
	require.NotNil(t, detail, "the per-member results travel as the details of the error")
	out := map[string]*multipoolerservicepb.MemberFenceResult{}
	for _, m := range detail.Members {
		out[m.GetPoolerId().GetName()] = m
	}
	return out
}

func TestCoordinatorFenceCompletesWhenEveryMemberIsClean(t *testing.T) {
	h := newCoordHarness(t)
	resp, err := h.fence(0)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version, "the fence moved the version")
	require.Len(t, resp.Members, 2, "every member answered, the replica as well as the writable one")
	for _, m := range resp.Members {
		require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, m.Status)
	}
	require.Equal(t, 1, h.refresh.fencesTo("w"))
	require.Equal(t, 1, h.refresh.fencesTo("r"))
	require.Equal(t, stateFenced, h.row(t, "alpha").state)
	require.Equal(t, stateUnfenced, h.row(t, "beta").state, "another tablegroup is untouched")
}

func TestCoordinatorIsGenericOverTheTablegroup(t *testing.T) {
	// Two tablegroups none of which is the control home: a cutover from one to the other,
	// and the rollback, are the same operations.
	h := newCoordHarness(t)
	src, err := h.fenceTG("alpha", 0)
	require.NoError(t, err)
	dst, err := h.fenceTG("beta", 0)
	require.NoError(t, err)
	moved, err := h.flip("alpha", "beta", 0)
	require.NoError(t, err)
	require.Equal(t, "beta", moved.AppTablegroup)
	_, err = h.unfenceTG("beta", dst.State.Version)
	require.NoError(t, err)
	require.Equal(t, stateFenced, h.row(t, "alpha").state, "the source stays fenced")

	_, err = h.fenceTG("beta", h.row(t, "beta").version)
	require.NoError(t, err)
	moved, err = h.flip("beta", "alpha", 1)
	require.NoError(t, err)
	require.Equal(t, "alpha", moved.AppTablegroup)
	require.EqualValues(t, 2, moved.RoutingVersion)
	_, err = h.unfenceTG("alpha", src.State.Version)
	require.NoError(t, err)
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
}

// TestCoordinatorFenceNeverUndoesItself: a fence some member did not complete stays
// fenced, whatever the reason, and says what each member answered. Nothing is reopened.
func TestCoordinatorFenceNeverUndoesItself(t *testing.T) {
	for name, answer := range map[string]struct {
		err    error
		status multipoolerservicepb.MemberFenceResult_Status
		detail string
	}{
		"busy":        {mterrors.New(mtrpcpb.Code_RESOURCE_EXHAUSTED, "1 reservations held"), multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "1 reservations held"},
		"unreachable": {errors.New("pooler unreachable"), multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, "pooler unreachable"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
				if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
					return nil, answer.err
				}
				return honestAck(p, req), nil
			}
			_, err := h.fence(0)
			results := notClean(t, err)
			require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, results["w"].Status)
			require.Equal(t, answer.status, results["r"].Status)
			require.Contains(t, results["r"].Detail, answer.detail)
			require.ErrorContains(t, err, "retry the same fence, or unfence")

			row := h.row(t, "alpha")
			require.Equal(t, stateFencing, row.state, "a fence never undoes itself: the row stays FENCING")
			require.EqualValues(t, 1, row.version)
			require.Zero(t, h.refresh.reopensTo("w"), "nothing is reopened")
			require.Zero(t, h.refresh.reopensTo("r"))

			// The same fence, retried, asks everyone again and succeeds once the member is clean.
			h.refresh.behave = nil
			resp, err := h.fence(0 + 1)
			require.NoError(t, err)
			require.EqualValues(t, 1, resp.State.Version)
			require.Equal(t, 2, h.refresh.fencesTo("w"), "the retry asks every member again")
			require.Equal(t, stateFenced, h.row(t, "alpha").state)
			require.EqualValues(t, 2, h.store.writes, "the retry wrote only the completion, at the same version")
		})
	}
}

// TestCoordinatorAsksEveryMemberEvenWhenOneFails: a failure does not stop the others, and
// each member's own answer is reported.
func TestCoordinatorAsksEveryMemberEvenWhenOneFails(t *testing.T) {
	h := newCoordHarness(t)
	h.members.set(ref("a"), ref("b"), ref("w"))
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		switch p.name() {
		case "a":
			return nil, errors.New("boom")
		case "b":
			// Takes its time: it is not cut short because "a" failed first.
			time.Sleep(200 * time.Millisecond)
			return nil, mterrors.New(mtrpcpb.Code_RESOURCE_EXHAUSTED, "1 requests still running")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence(0)
	results := notClean(t, err)
	require.Len(t, results, 3)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["a"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, results["b"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, results["w"].Status)
	require.NotContains(t, err.Error(), "context canceled")
}

func TestCoordinatorHungMemberIsUnreachableWithinItsCallTimeout(t *testing.T) {
	h := newCoordHarness(t)
	h.c.callTimeout = 300 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "r" {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return nil, errors.New("released")
			}
		}
		return honestAck(p, req), nil
	}
	started := time.Now()
	_, err := h.fence(0)
	results := notClean(t, err)
	require.Less(t, time.Since(started), 2*time.Second, "the call timeout, not the whole request timeout, bounds a hung member")
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["r"].Status)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
}

// TestCoordinatorMemberCallTimeoutFollowsTheRequestsDrain: a member is given the drain the
// request names, plus its look at the server and a margin; and every member is told that
// drain.
func TestCoordinatorMemberCallTimeoutFollowsTheRequestsDrain(t *testing.T) {
	h := newCoordHarness(t)
	h.c.callTimeout = 0
	var (
		mu       sync.Mutex
		deadline []time.Duration
		drains   []time.Duration
	)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if req.GetExpectedState() != stateUnfenced {
			mu.Lock()
			d, _ := ctx.Deadline()
			deadline = append(deadline, time.Until(d))
			drains = append(drains, req.GetDrainTimeout().AsDuration())
			mu.Unlock()
		}
		return honestAck(p, req), nil
	}
	_, err := h.c.FenceTablegroup(context.Background(), &multipoolerservicepb.FenceTablegroupRequest{
		Database: "db", Tablegroup: "alpha", DrainTimeout: durationpb.New(40 * time.Second),
	})
	require.NoError(t, err)
	for i := range deadline {
		require.Equal(t, 40*time.Second, drains[i])
		require.Greater(t, deadline[i], 40*time.Second+poolerserver.BackendCheckBudget, "a member is given its drain, its look and a margin")
	}

	deadline, drains = nil, nil
	_, err = h.unfence(1)
	require.NoError(t, err)
	_, err = h.c.FenceTablegroup(context.Background(), &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: "alpha", ExpectedVersion: 2})
	require.NoError(t, err)
	for i := range deadline {
		require.Equal(t, defaultAdmissionDrainTimeout, drains[i], "an unnamed drain is the default")
	}
}

func TestCoordinatorUnfenceNeedsNoAcknowledgement(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fence(0)
	require.NoError(t, err)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "r" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	resp, err := h.unfence(1)
	require.NoError(t, err, "an unfence has no acknowledgement barrier")
	require.Equal(t, stateUnfenced, resp.State.State)
	require.EqualValues(t, 2, resp.State.Version)
	require.Len(t, resp.Unreached, 1)
	require.Equal(t, "r", resp.Unreached[0].GetName(), "the member it could not ask is reported")
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)

	// The migrator repeats the same unfence until none is unreached.
	h.refresh.behave = nil
	resp, err = h.unfence(1)
	require.NoError(t, err)
	require.Empty(t, resp.Unreached)
	require.EqualValues(t, 2, h.row(t, "alpha").version, "a repeat writes nothing")
}

func TestCoordinatorUnfenceNeedsTheVersionOfTheFence(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.unfence(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)

	first, err := h.fence(0)
	require.NoError(t, err)
	_, err = h.unfence(first.State.Version)
	require.NoError(t, err)
	second, err := h.fence(2)
	require.NoError(t, err)

	// The delayed unfence of the first fence cannot open the second.
	_, err = h.unfence(first.State.Version)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.Equal(t, stateFenced, h.row(t, "alpha").state)
	_, err = h.unfence(second.State.Version)
	require.NoError(t, err)
}

func TestCoordinatorAFenceLeftByADeadCoordinatorIsResumedOrUnfenced(t *testing.T) {
	h := newCoordHarness(t)
	h.store.add("alpha", true, 7) // committed by a coordinator that then died

	resp, err := h.fence(7)
	require.NoError(t, err, "a fence is resumed at the version it has")
	require.EqualValues(t, 7, resp.State.Version, "and the flag and the version are not written again")
	require.Equal(t, 1, h.refresh.fencesTo("w"))

	h.store.add("alpha", true, 9)
	_, err = h.unfence(9)
	require.NoError(t, err, "or unfenced by an unfence naming it")
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
}

func TestCoordinatorFenceOfAnotherVersionIsRejected(t *testing.T) {
	h := newCoordHarness(t)
	for _, version := range []int64{1, 5} {
		_, err := h.fence(version)
		requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	}
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
	require.Zero(t, h.refresh.fencesTo("w"), "a rejected fence asks nobody")
}

func TestCoordinatorMemberThatDisappearsIsNotClean(t *testing.T) {
	h := newCoordHarness(t)
	h.members.onSnapshot = func(call int) {
		if call >= 3 {
			// "r" is gone from the second listing on without having answered.
			h.members.set(ref("w"))
		}
	}
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence(0)
	results := notClean(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["r"].Status)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
}

// TestCoordinatorMemberThatAnsweredThenVanishedStillCounts: a member that closed and then left
// the listing was closed when it counted; it opens again only by reading UNFENCED.
func TestCoordinatorMemberThatAnsweredThenVanishedStillCounts(t *testing.T) {
	h := newCoordHarness(t)
	h.members.onSnapshot = func(call int) {
		if call >= 3 {
			h.members.set(ref("w"))
		}
	}
	resp, err := h.fence(0)
	require.NoError(t, err)
	require.Len(t, resp.Members, 2)
}

func TestCoordinatorLateJoinerThatVanishesStaysRequired(t *testing.T) {
	h := newCoordHarness(t)
	h.members.onSnapshot = func(call int) {
		switch call {
		case 3:
			h.members.set(ref("w"), ref("r"), ref("late"))
		case 4:
			h.members.set(ref("w"), ref("r"))
		}
	}
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if req.GetExpectedState() != stateUnfenced && p.name() == "late" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence(0)
	results := notClean(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["late"].Status)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
}

func TestCoordinatorMemberJoiningMidFenceIsIncluded(t *testing.T) {
	h := newCoordHarness(t)
	h.members.onSnapshot = func(call int) {
		if call == 3 {
			h.members.set(ref("w"), ref("r"), ref("joined"))
		}
	}
	resp, err := h.fence(0)
	require.NoError(t, err)
	require.Len(t, resp.Members, 3)
	require.Equal(t, 1, h.refresh.fencesTo("joined"))
}

func TestCoordinatorAckOfARestartedMemberStillStands(t *testing.T) {
	// A pooler that restarted under the same id after the fence committed reads
	// fenced = true and stays closed; there is nothing to tell the two processes apart
	// by, and nothing needs it.
	h := newCoordHarness(t)
	calls := 0
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
			calls++
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence(0)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestCoordinatorInexactAnswersAreNotClean(t *testing.T) {
	for name, answer := range map[string]func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse{
		"another pooler": func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse {
			r := honestAck(p, req)
			r.PoolerId = ref("impostor").id
			return r
		},
		"not fenced": func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse {
			r := honestAck(p, req)
			r.State = stateUnfenced
			return r
		},
		"another version": func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse {
			r := honestAck(p, req)
			r.Version = req.GetExpectedVersion() + 1
			return r
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
				if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
					return answer(p, req), nil
				}
				return honestAck(p, req), nil
			}
			_, err := h.fence(0)
			results := notClean(t, err)
			require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["r"].Status)
			require.Equal(t, stateFencing, h.row(t, "alpha").state)
		})
	}
}

func TestCoordinatorUncertainCommitThenReread(t *testing.T) {
	h := newCoordHarness(t)
	h.store.uncertainNext = true
	_, err := h.fence(0)
	require.Error(t, err)
	require.ErrorContains(t, err, "outcome uncertain")
	require.Zero(t, h.refresh.fencesTo("w"), "nobody was asked while the outcome was unknown")

	// The write did commit, and nothing undoes it: the caller reads GetServingState, sees
	// FENCING at version 1, and resumes it (or unfences it).
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
	resp, err := h.fence(1)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version)
}

// TestCoordinatorUncertainCompletionIsResumed: the write that completes the fence is the one
// whose acknowledgment is lost. The row says FENCED, and the same fence, repeated, is a
// success that asks the members again.
func TestCoordinatorUncertainCompletionIsResumed(t *testing.T) {
	h := newCoordHarness(t)
	h.store.beforeCAS = func() {
		// The next compare-and-set is the completion: the first one has been made.
		if h.store.writes == 1 {
			h.store.uncertainNext = true
		}
	}
	_, err := h.fence(0)
	require.ErrorContains(t, err, "outcome uncertain")
	require.Equal(t, stateFenced, h.row(t, "alpha").state)
	resp, err := h.fence(0)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 2, h.refresh.fencesTo("w"), "the repeat asked the members again")
}

func TestCoordinatorStaleLeaderWritesNothing(t *testing.T) {
	h := newCoordHarness(t)
	h.setLeader(mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary"))
	writes := h.store.writes
	_, err := h.fence(0)
	requireCode(t, mtrpcpb.Code_UNAVAILABLE, err)
	_, err = h.unfence(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err) // the row is UNFENCED: the state check answers first
	require.Equal(t, writes, h.store.writes)
	require.Zero(t, h.refresh.fencesTo("w"))
}

func TestCoordinatorConcurrentWriterLosesTheCompareAndSet(t *testing.T) {
	h := newCoordHarness(t)
	h.store.beforeCAS = func() {
		// Someone else fences first.
		h.store.mu.Lock()
		h.store.rows["alpha"].state = stateFencing
		h.store.rows["alpha"].version = 1
		h.store.mu.Unlock()
		h.store.beforeCAS = nil
	}
	_, err := h.fence(0)
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	require.Zero(t, h.refresh.fencesTo("w"))
}

// TestCoordinatorFenceIsNotCompletedOverAChangedRow: members that all answered clean for a
// fence whose row changed meanwhile do not complete it: the completion is a compare-and-set
// on the state and the version, and it fails.
func TestCoordinatorFenceIsNotCompletedOverAChangedRow(t *testing.T) {
	for name, move := range map[string]func(r *servingRow){
		"unfenced meanwhile":   func(r *servingRow) { r.state, r.version = stateUnfenced, 2 },
		"another fence":        func(r *servingRow) { r.state, r.version = stateFencing, 3 },
		"another version only": func(r *servingRow) { r.version = 5 },
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
				if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
					h.store.mu.Lock()
					move(h.store.rows["alpha"])
					h.store.mu.Unlock()
				}
				return honestAck(p, req), nil
			}
			_, err := h.fence(0)
			requireCode(t, mtrpcpb.Code_ABORTED, err)
			require.ErrorContains(t, err, "is not FENCED at version 1")
			require.NotEqual(t, stateFenced, h.row(t, "alpha").state, "nothing was written over it")
		})
	}
}

// TestCoordinatorTwoResumesOfTheSameFenceBothSucceed: the loser of the compare-and-set that
// completes a fence finds the row FENCED at its own version, which is what it was after, and
// succeeds. A row that is anything else is still an error.
func TestCoordinatorTwoResumesOfTheSameFenceBothSucceed(t *testing.T) {
	h := newCoordHarness(t)
	h.store.addState("alpha", stateFencing, 1)
	h.store.beforeCAS = func() {
		// The other resume completes the fence first.
		h.store.mu.Lock()
		h.store.rows["alpha"].state = stateFenced
		h.store.mu.Unlock()
		h.store.beforeCAS = nil
	}
	resp, err := h.fence(0)
	require.NoError(t, err, "the fence is complete at the version this call fenced")
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version)
	require.Zero(t, h.store.writes, "the loser wrote nothing")
}

// TestCoordinatorFenceAlreadyCompleteIsNotUndoneByAMemberThatIsNotCleanNow: a retry that finds
// the fence committed (FENCED at v or v+1) succeeds. The members are asked again on a
// best-effort basis, and one that is busy or unreachable now is reported in the response; it
// does not turn the call into an error, because the fence was committed on a clean answer.
func TestCoordinatorFenceAlreadyCompleteIsNotUndoneByAMemberThatIsNotCleanNow(t *testing.T) {
	for name, version := range map[string]int64{"v+1": 5, "v": 4} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.store.addState("alpha", stateFenced, version)
			h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
				switch p.name() {
				case "r":
					return nil, errors.New("pooler unreachable")
				default:
					return nil, mterrors.New(mtrpcpb.Code_RESOURCE_EXHAUSTED, "1 reservations held")
				}
			}
			resp, err := h.fence(4)
			require.NoError(t, err)
			require.Equal(t, stateFenced, resp.State.State)
			require.Equal(t, version, resp.State.Version)
			got := map[string]multipoolerservicepb.MemberFenceResult_Status{}
			for _, m := range resp.Members {
				got[m.GetPoolerId().GetName()] = m.GetStatus()
			}
			require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["w"], "reported, not an error")
			require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["r"])
			require.Zero(t, h.store.writes, "nothing was written")
			require.Equal(t, stateFenced, h.row(t, "alpha").state)
		})
	}
}

// TestCoordinatorFenceIsReadBackBeforeItSucceeds: success needs the row read again and found
// FENCED at the fence's version, so a row that changed after the completion is not a success,
// and neither is a read that fails.
func TestCoordinatorFenceIsReadBackBeforeItSucceeds(t *testing.T) {
	h := newCoordHarness(t)
	h.store.afterCAS = func() {
		// Right after the completion, someone unfences.
		h.store.mu.Lock()
		defer h.store.mu.Unlock()
		if r := h.store.rows["alpha"]; r.state == stateFenced {
			r.state, r.version = stateUnfenced, 2
		}
	}
	_, err := h.fence(0)
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	require.ErrorContains(t, err, "is not FENCED at version 1")

	h = newCoordHarness(t)
	h.store.afterCAS = func() {
		h.store.mu.Lock()
		defer h.store.mu.Unlock()
		if h.store.rows["alpha"].state == stateFenced {
			h.store.readErr = errors.New("connection lost")
		}
	}
	_, err = h.fence(0)
	require.ErrorContains(t, err, "read back the fence")
}

func TestCoordinatorFlipNeedsBothSidesFencedAndTheRoutingVersion(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.flip("alpha", "beta", 0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.ErrorContains(t, err, "requires both tablegroups FENCED")

	_, err = h.fenceTG("alpha", 0)
	require.NoError(t, err)
	_, err = h.flip("alpha", "beta", 0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.fenceTG("beta", 0)
	require.NoError(t, err)

	_, err = h.flip("beta", "alpha", 0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.ErrorContains(t, err, "not \"beta\"", "from must be where the pointer is")
	_, err = h.flip("alpha", "beta", 4)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.ErrorContains(t, err, "expected version 4", "and the routing version must be the one the caller read")

	moved, err := h.flip("alpha", "beta", 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, moved.RoutingVersion)
}

// TestCoordinatorFlipRetryAfterTheMoveSucceeds: a flip whose reply was lost is repeated, and
// finds the traffic already on the destination at the version after the one it expected; that
// is its own success, and nothing is written. A flip the other way, or a stale one, is not.
func TestCoordinatorFlipRetryAfterTheMoveSucceeds(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fenceTG("alpha", 0)
	require.NoError(t, err)
	_, err = h.fenceTG("beta", 0)
	require.NoError(t, err)
	first, err := h.flip("alpha", "beta", 0)
	require.NoError(t, err)

	// The destination may already have been unfenced by then.
	_, err = h.unfenceTG("beta", 1)
	require.NoError(t, err)

	again, err := h.flip("alpha", "beta", 0)
	require.NoError(t, err, "a retry of the flip that committed")
	require.Equal(t, first.AppTablegroup, again.AppTablegroup)
	require.Equal(t, first.RoutingVersion, again.RoutingVersion)
	require.Equal(t, "beta", h.store.app)
	require.EqualValues(t, 1, h.store.appVersion, "and nothing was written")

	_, err = h.flip("alpha", "beta", 1)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.flip("beta", "alpha", 1)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err) // moving it back needs both sides fenced again
	_, err = h.flip("alpha", "beta", 5)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
}

// TestCoordinatorFlipRefusesAnIncompleteFence: a tablegroup that is FENCING, however it got
// there (a member was busy, the coordinator was lost), is not a completed fence and the flip
// refuses it, so a migrator that takes it for one cannot move the traffic. A resumed fence
// completes it, and then the flip is accepted.
func TestCoordinatorFlipRefusesAnIncompleteFence(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fenceTG("alpha", 0)
	require.NoError(t, err)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
			return nil, mterrors.New(mtrpcpb.Code_RESOURCE_EXHAUSTED, "1 reservations held")
		}
		return honestAck(p, req), nil
	}
	_, err = h.fenceTG("beta", 0)
	notClean(t, err)
	require.Equal(t, stateFencing, h.row(t, "beta").state)

	_, err = h.flip("alpha", "beta", 0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.ErrorContains(t, err, "its fence is not complete")
	require.Equal(t, "alpha", h.store.app, "the traffic did not move")

	// The store, asked directly for the same move, refuses it too.
	_, _, moved, err := h.store.MoveRouting(context.Background(), "db", "alpha", "beta", 0)
	require.NoError(t, err)
	require.False(t, moved, "the store refuses it as well")

	h.refresh.behave = nil
	_, err = h.fenceTG("beta", 0)
	require.NoError(t, err, "the same fence, resumed, completes")
	require.Equal(t, stateFenced, h.row(t, "beta").state)
	_, err = h.flip("alpha", "beta", 0)
	require.NoError(t, err)
}

// TestCoordinatorRefusesMoreThanOneUnmanagedMember: exactly one unmanaged
// pooler is supported per tablegroup. A fence of a tablegroup with two is refused before anything
// is written; one that appears while a fence runs leaves it FENCING, for the migrator to see.
func TestCoordinatorRefusesMoreThanOneUnmanagedMember(t *testing.T) {
	unmanaged := func(name string) poolerRef {
		r := ref(name)
		r.record = &clustermetadatapb.Multipooler{ManagementMode: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED}
		return r
	}
	h := newCoordHarness(t)
	h.members.set(unmanaged("u1"), unmanaged("u2"))
	writes := h.store.writes
	_, err := h.fence(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.ErrorContains(t, err, "2 unmanaged poolers (u1, u2); exactly one is supported")
	require.Equal(t, writes, h.store.writes, "refused before anything is written")
	require.Zero(t, h.refresh.fencesTo("u1"))

	// One unmanaged member, and managed ones beside it, is a different tablegroup shape that
	// is not refused: only the count of unmanaged poolers matters.
	h.members.set(unmanaged("u1"), ref("m1"))
	_, err = h.fence(0)
	require.NoError(t, err)

	// A second one registers while a fence runs.
	h = newCoordHarness(t)
	h.members.set(unmanaged("u1"))
	h.members.onSnapshot = func(call int) {
		if call == 3 {
			h.members.set(unmanaged("u1"), unmanaged("u2"))
		}
	}
	_, err = h.fence(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.Equal(t, stateFencing, h.row(t, "alpha").state, "the fence is left FENCING, not completed")
}

func TestCoordinatorFlipChecksAreRepeatedWhereTheyCommit(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fenceTG("alpha", 0)
	require.NoError(t, err)
	_, err = h.fenceTG("beta", 0)
	require.NoError(t, err)
	// One side is unfenced between the coordinator's check and the commit.
	h.store.mu.Lock()
	moveStore := h.store
	h.store.mu.Unlock()
	h.c.store = &flipRacingStore{memStore: moveStore, beforeMove: func() {
		moveStore.mu.Lock()
		moveStore.rows["beta"].state = stateUnfenced
		moveStore.mu.Unlock()
	}}
	_, err = h.flip("alpha", "beta", 0)
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	require.Equal(t, "alpha", h.store.app, "the pointer did not move")
}

// flipRacingStore changes the world just before the flip commits.
type flipRacingStore struct {
	*memStore
	beforeMove func()
}

func (s *flipRacingStore) MoveRouting(ctx context.Context, db, from, to string, expectedVersion int64) (string, int64, bool, error) {
	s.beforeMove()
	return s.memStore.MoveRouting(ctx, db, from, to, expectedVersion)
}

func TestCoordinatorValidatesRequests(t *testing.T) {
	h := newCoordHarness(t)
	ctx := context.Background()
	_, err := h.c.FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{Database: "db"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: "alpha", ExpectedVersion: -1})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.UnfenceTablegroup(ctx, &multipoolerservicepb.UnfenceTablegroupRequest{Database: "db"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.FlipRouting(ctx, &multipoolerservicepb.FlipRoutingRequest{Database: "db", FromTablegroup: "alpha", ToTablegroup: "alpha"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.FlipRouting(ctx, &multipoolerservicepb.FlipRoutingRequest{Database: "db", FromTablegroup: "alpha"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
}

func TestCoordinatorNamesNoTablegroupSpecially(t *testing.T) {
	// The control home is a role, not a name the protocol knows: a tablegroup called
	// "default" is fenced exactly like any other.
	h := newCoordHarness(t)
	h.store.add("default", false, 0)
	resp, err := h.fenceTG("default", 0)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
}

func TestCoordinatorFenceOfATablegroupNeverFencedCreatesItsRow(t *testing.T) {
	h := newCoordHarness(t)
	resp, err := h.fenceTG("gamma", 0)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version)

	_, err = h.fenceTG("delta", 3)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.unfenceTG("epsilon", 0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.flip("alpha", "zeta", 0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
}

// TestCoordinatorRefusesAFenceWhenEitherDurabilityPolicyIsTooWeak: the fenced tablegroup's
// policy and the control home's both have to need at least two nodes, when managed, and the
// refusal is made before anything is written.
func TestCoordinatorRefusesAFenceWhenEitherDurabilityPolicyIsTooWeak(t *testing.T) {
	type policy struct {
		nodes   int
		managed bool
		err     error
	}
	for name, tc := range map[string]struct {
		alpha, home policy
		refused     string
	}{
		"both strong":                         {alpha: policy{2, true, nil}, home: policy{2, true, nil}},
		"fenced tablegroup too weak":          {alpha: policy{1, true, nil}, home: policy{2, true, nil}, refused: `the durability policy of "alpha" needs 1 node(s)`},
		"control home too weak":               {alpha: policy{2, true, nil}, home: policy{1, true, nil}, refused: `the durability policy of "default" needs 1 node(s)`},
		"unmanaged tablegroup is exempt":      {alpha: policy{0, false, nil}, home: policy{2, true, nil}},
		"unmanaged still needs a strong home": {alpha: policy{0, false, nil}, home: policy{1, true, nil}, refused: `the durability policy of "default"`},
		"a policy that cannot be read":        {alpha: policy{0, true, errors.New("no policy")}, home: policy{2, true, nil}, refused: "could not be read"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.c.requiredNodes = func(_ context.Context, _, tg string) (int, bool, error) {
				p := tc.alpha
				if tg == "default" {
					p = tc.home
				}
				return p.nodes, p.managed, p.err
			}
			writes := h.store.writes
			_, err := h.fence(0)
			if tc.refused == "" {
				require.NoError(t, err)
				return
			}
			requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
			require.ErrorContains(t, err, tc.refused)
			require.Equal(t, writes, h.store.writes, "refused before anything is written")
			require.Zero(t, h.refresh.fencesTo("w"))
			require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
		})
	}

	// Fencing the control home itself asks about it once.
	h := newCoordHarness(t)
	h.store.add("default", false, 0)
	var asked []string
	h.c.requiredNodes = func(_ context.Context, _, tg string) (int, bool, error) {
		asked = append(asked, tg)
		return 2, true, nil
	}
	_, err := h.fenceTG("default", 0)
	require.NoError(t, err)
	require.Equal(t, []string{"default"}, asked)
}

// TestRequiredNodesOfATablegroup: an unmanaged tablegroup has no cohort and so no policy; a
// managed tablegroup other than the default one is held to the policy the database was
// bootstrapped with.
func TestRequiredNodesOfATablegroup(t *testing.T) {
	managedRec := func(name string, mode clustermetadatapb.PoolerManagementMode) poolerRef {
		return poolerRef{
			id:     &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
			record: &clustermetadatapb.Multipooler{ManagementMode: mode},
		}
	}
	pm, _ := newTestManagerWithMock(t, "default", "0-inf")
	ctx := context.Background()

	n, managed, err := pm.requiredNodes(ctx, &fakeMembers{list: []poolerRef{managedRec("u1", clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED)}}, "db", "migrateTG")
	require.NoError(t, err)
	require.False(t, managed)
	require.Zero(t, n)

	// A managed tablegroup other than the default one, with a database that has no policy:
	// unreadable, so it cannot be fenced.
	_, managed, err = pm.requiredNodes(ctx, &fakeMembers{list: []poolerRef{managedRec("m1", clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED)}}, "db", "other")
	require.True(t, managed)
	require.Error(t, err)

	// The default tablegroup's own policy is the committed rule's; with none cached it
	// needs no nodes, which a fence refuses.
	n, managed, err = pm.requiredNodes(ctx, &fakeMembers{}, "db", "default")
	require.NoError(t, err)
	require.True(t, managed)
	require.Less(t, n, 2)
}

// TestCoordinatorFenceAndUnfenceStateTables: each call recognizes the exact rows its own
// success, or a retry of it, leaves, and rejects everything else, so a retry of a call whose
// reply was lost succeeds without writing more than it must, while a delayed retry of a fence
// that was undone still fails. The version is only ever changed by the coordinator's own
// compare-and-set, and a fence completes (FENCING to FENCED) at the version it has.
func TestCoordinatorFenceAndUnfenceStateTables(t *testing.T) {
	type row struct {
		state   multipoolerservicepb.AdmissionState
		version int64
	}
	U, G, F := stateUnfenced, stateFencing, stateFenced
	bad := mtrpcpb.Code_FAILED_PRECONDITION
	for name, tc := range map[string]struct {
		call      string // "fence" or "unfence"
		expected  int64
		start     row
		ok        bool
		end       row
		writes    int // writes the call makes
		fanOut    bool
		reopened  bool
		errorCode mtrpcpb.Code
	}{
		"fence: UNFENCED at v writes FENCING at v+1, fans out, and completes it": {call: "fence", expected: 4, start: row{U, 4}, ok: true, end: row{F, 5}, writes: 2, fanOut: true},
		"fence: FENCING at v+1 is a resume: every member again, then FENCED":     {call: "fence", expected: 4, start: row{G, 5}, ok: true, end: row{F, 5}, writes: 1, fanOut: true},
		"fence: FENCING at v resumes for a caller that re-read":                  {call: "fence", expected: 4, start: row{G, 4}, ok: true, end: row{F, 4}, writes: 1, fanOut: true},
		"fence: FENCED at v+1 is done: members asked again, nothing written":     {call: "fence", expected: 4, start: row{F, 5}, ok: true, end: row{F, 5}, fanOut: true},
		"fence: FENCED at v is done for a caller that re-read":                   {call: "fence", expected: 4, start: row{F, 4}, ok: true, end: row{F, 4}, fanOut: true},
		"fence: UNFENCED at v+1 is rejected":                                     {call: "fence", expected: 4, start: row{U, 5}, errorCode: bad, end: row{U, 5}},
		"fence: UNFENCED at v+2 is rejected":                                     {call: "fence", expected: 4, start: row{U, 6}, errorCode: bad, end: row{U, 6}},
		"fence: FENCING at v+2 is another fence and is rejected":                 {call: "fence", expected: 4, start: row{G, 6}, errorCode: bad, end: row{G, 6}},
		"fence: FENCED at v+2 is another fence and is rejected":                  {call: "fence", expected: 4, start: row{F, 6}, errorCode: bad, end: row{F, 6}},
		"fence: anything older is rejected":                                      {call: "fence", expected: 4, start: row{U, 3}, errorCode: bad, end: row{U, 3}},
		"unfence: FENCED at v writes UNFENCED at v+1":                            {call: "unfence", expected: 4, start: row{F, 4}, ok: true, end: row{U, 5}, writes: 1, reopened: true},
		"unfence: FENCING at v writes UNFENCED at v+1":                           {call: "unfence", expected: 4, start: row{G, 4}, ok: true, end: row{U, 5}, writes: 1, reopened: true},
		"unfence: UNFENCED at v+1 is done, and the members are asked":            {call: "unfence", expected: 4, start: row{U, 5}, ok: true, end: row{U, 5}, reopened: true},
		"unfence: UNFENCED at v is rejected":                                     {call: "unfence", expected: 4, start: row{U, 4}, errorCode: bad, end: row{U, 4}},
		"unfence: UNFENCED at v+2 is rejected":                                   {call: "unfence", expected: 4, start: row{U, 6}, errorCode: bad, end: row{U, 6}},
		"unfence: FENCED at v+1 is another fence and is rejected":                {call: "unfence", expected: 4, start: row{F, 5}, errorCode: bad, end: row{F, 5}},
		"unfence: FENCING at v+1 is another fence and is rejected":               {call: "unfence", expected: 4, start: row{G, 5}, errorCode: bad, end: row{G, 5}},
		"unfence: FENCED at v+2 is rejected":                                     {call: "unfence", expected: 4, start: row{F, 6}, errorCode: bad, end: row{F, 6}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.store.addState("alpha", tc.start.state, tc.start.version)
			writes := h.store.writes
			var err error
			if tc.call == "fence" {
				_, err = h.fence(tc.expected)
			} else {
				_, err = h.unfence(tc.expected)
			}
			if tc.ok {
				require.NoError(t, err)
			} else {
				requireCode(t, tc.errorCode, err)
			}
			got := h.row(t, "alpha")
			require.Equal(t, tc.end, row{got.state, got.version})
			require.Equal(t, tc.writes, h.store.writes-writes, "writes")
			require.Equal(t, tc.fanOut, h.refresh.fencesTo("w") == 1, "members asked to fence")
			require.Equal(t, tc.reopened, h.refresh.reopensTo("w") == 1, "members asked to reopen")
		})
	}
}

// TestCoordinatorRepeatingEveryCallEndsInTheSameState: a migrator that does not know whether
// a call took effect repeats it, and ends where it would have if it had not.
func TestCoordinatorRepeatingEveryCallEndsInTheSameState(t *testing.T) {
	h := newCoordHarness(t)
	first, err := h.fence(0)
	require.NoError(t, err)
	again, err := h.fence(0)
	require.NoError(t, err, "a retry of the same request, whose reply was lost")
	require.Equal(t, first.State.Version, again.State.Version)

	_, err = h.unfence(first.State.Version)
	require.NoError(t, err)
	_, err = h.unfence(first.State.Version)
	require.NoError(t, err, "the same unfence again")
	require.EqualValues(t, 2, h.row(t, "alpha").version)
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)

	// The delayed fence of the first request cannot fence again now that it was undone.
	_, err = h.fence(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
}

// TestDefaultPrimaryLostMidFenceLeavesItFencedForTheMigrator: nothing undoes a fence whose
// coordinator was lost. The migrator, which is waiting on the call, gets the error, reads
// GetServingState, and unfences at the version it read, from the new default primary; a
// delayed retry of the fence it was making is then rejected.
func TestDefaultPrimaryLostMidFenceLeavesItFencedForTheMigrator(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if req.GetExpectedState() != stateUnfenced && p.name() == "r" {
			// This default primary is deposed while the fence runs.
			h.setLeader(mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary"))
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.fence(0)
	notClean(t, err)
	require.Equal(t, stateFencing, h.row(t, "alpha").state, "nothing undoes it but the migrator, and it is not complete")

	// The deposed coordinator refuses to write, retryably: it is not the default primary.
	_, err = h.unfence(1)
	requireCode(t, mtrpcpb.Code_UNAVAILABLE, err)

	// The new default primary, asked by the migrator after it read the state.
	h.setLeader(nil)
	h.refresh.behave = nil
	_, err = h.unfence(h.row(t, "alpha").version)
	require.NoError(t, err)
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)

	_, err = h.fence(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	resp, err := h.fence(h.row(t, "alpha").version)
	require.NoError(t, err, "a fresh fence at the current version works")
	require.Equal(t, stateFenced, resp.State.State)
}
