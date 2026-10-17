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
	"google.golang.org/grpc/codes"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
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
	fenceCalls  map[string]int // refreshes a start asked
	reportCalls map[string]int // refreshes a commit asked
	reopenCalls map[string]int
	// stage is the call of the coordinator that is running: "start" or "commit".
	stage atomic.Value
	// behave overrides the default honest acknowledgment.
	behave func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

func (f *fakeRefresher) Refresh(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	f.mu.Lock()
	calls := &f.fenceCalls
	switch {
	case req.GetExpectedState() == stateUnfenced:
		calls = &f.reopenCalls
	case f.stage.Load() == "commit":
		calls = &f.reportCalls
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

func (f *fakeRefresher) reportsTo(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reportCalls[name]
}

func (f *fakeRefresher) reopensTo(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reopenCalls[name]
}

// honestAck is the answer of a pooler that did what it was asked.
func honestAck(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) *multipoolerservicepb.RefreshAdmissionResponse {
	return &multipoolerservicepb.RefreshAdmissionResponse{
		PoolerId: p.id, State: req.GetExpectedState(), Version: req.GetExpectedVersion(),
	}
}

// starting is whether the refresh being answered is a start's, and committing whether it is a
// commit's.
func (f *fakeRefresher) starting() bool   { return f.stage.Load() == "start" }
func (f *fakeRefresher) committing() bool { return f.stage.Load() == "commit" }

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

func (h *coordHarness) startTG(tg string, version int64) (*multipoolerservicepb.StartFenceResponse, error) {
	h.refresh.stage.Store("start")
	return h.c.StartFence(context.Background(), &multipoolerservicepb.StartFenceRequest{
		Database: "db", Tablegroup: tg, ExpectedVersion: version,
	})
}

func (h *coordHarness) start(version int64) (*multipoolerservicepb.StartFenceResponse, error) {
	return h.startTG("alpha", version)
}

func (h *coordHarness) commitTG(tg string, version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	h.refresh.stage.Store("commit")
	return h.c.CommitFence(context.Background(), &multipoolerservicepb.CommitFenceRequest{
		Database: "db", Tablegroup: tg, Version: version,
	})
}

func (h *coordHarness) commit(version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	return h.commitTG("alpha", version)
}

// fenceTG starts the fence at version and commits it, once each, as a migrator with nothing
// to wait for does: it returns the commit's response, or the error of the call that failed.
func (h *coordHarness) fenceTG(tg string, version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	started, err := h.startTG(tg, version)
	if err != nil {
		return nil, err
	}
	return h.commitTG(tg, started.GetState().GetVersion())
}

func (h *coordHarness) fence(version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
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

// notClean requires that a start or a commit did not complete, and returns the per-member
// results the caller is told, as the details of the error's status.
func notClean(t *testing.T, err error) map[string]*multipoolerservicepb.MemberFenceResult {
	t.Helper()
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	var incomplete *FenceIncompleteError
	require.ErrorAs(t, err, &incomplete)
	st := incomplete.GRPCStatus()
	require.Equal(t, codes.Aborted, st.Code())
	var members []*multipoolerservicepb.MemberFenceResult
	var found bool
	for _, d := range st.Details() {
		switch r := d.(type) {
		case *multipoolerservicepb.StartFenceResponse:
			members, found = r.Members, true
		case *multipoolerservicepb.CommitFenceResponse:
			members, found = r.Members, true
		}
	}
	require.True(t, found, "the per-member results travel as the details of the error")
	out := map[string]*multipoolerservicepb.MemberFenceResult{}
	for _, m := range members {
		out[m.GetPoolerId().GetName()] = m
	}
	return out
}

// busyAnswer is the answer of a pooler whose gate is closed and that holds something.
func busyAnswer(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest, holds *multipoolerservicepb.FenceBusy) *multipoolerservicepb.RefreshAdmissionResponse {
	r := honestAck(p, req)
	r.Busy = holds
	return r
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
	require.Equal(t, 1, h.refresh.fencesTo("w"), "the start closed each member once")
	require.Equal(t, 1, h.refresh.fencesTo("r"))
	require.Equal(t, 1, h.refresh.reportsTo("w"), "the commit asked each member once")
	require.Equal(t, 1, h.refresh.reportsTo("r"))
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

// TestCoordinatorStartAsksEveryMemberAndSucceedsWhateverTheyHold: a start asks every member, once,
// at the version it wrote, and returns the row FENCING; a member that is busy does not fail it: the
// start does not wait for work, it closes and reports.
func TestCoordinatorStartAsksEveryMemberAndSucceedsWhateverTheyHold(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "r" {
			return busyAnswer(p, req, &multipoolerservicepb.FenceBusy{ReservationsHeld: 1}), nil
		}
		return honestAck(p, req), nil
	}
	resp, err := h.start(0)
	require.NoError(t, err)
	require.Equal(t, stateFencing, resp.State.State, "a start does not complete the fence")
	require.EqualValues(t, 1, resp.State.Version)
	require.Len(t, resp.Members, 2)
	got := map[string]multipoolerservicepb.MemberFenceResult_Status{}
	for _, m := range resp.Members {
		got[m.GetPoolerId().GetName()] = m.Status
	}
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, got["w"])
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["r"], "reported, not an error")
	require.Equal(t, 1, h.refresh.fencesTo("w"))
	require.Zero(t, h.refresh.reportsTo("w"), "that is all a start asks")
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
	require.EqualValues(t, 1, h.store.writes)
}

// TestCoordinatorStartIsIdempotent: a start is repeated by a migrator that does not know whether it
// took effect, that read the row it left, or that wants the members that missed it reached: it asks
// every member again and writes nothing; once the fence is complete it succeeds and asks nobody. The
// caller passes back the version it read, never one it computed.
func TestCoordinatorStartIsIdempotent(t *testing.T) {
	h := newCoordHarness(t)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if h.refresh.starting() && p.name() == "r" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.start(0)
	results := notClean(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, results["w"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["r"].Status)
	require.Contains(t, results["r"].Detail, "pooler unreachable")
	require.ErrorContains(t, err, "call StartFence again")
	row := h.row(t, "alpha")
	require.Equal(t, stateFencing, row.state, "a start never undoes itself: the row stays FENCING")
	require.EqualValues(t, 1, row.version)
	require.Zero(t, h.refresh.reopensTo("w"), "nothing is reopened")

	h.refresh.behave = nil
	resp, err := h.start(0)
	require.NoError(t, err, "the same start, repeated, reaches the member that missed it")
	require.EqualValues(t, 1, resp.State.Version)
	require.Equal(t, 2, h.refresh.fencesTo("w"), "the repeat asks every member again")
	require.EqualValues(t, 1, h.store.writes, "and writes nothing")
	again, err := h.start(1)
	require.NoError(t, err, "so is a start that names the version of the row it read, which is FENCING")
	require.EqualValues(t, 1, again.State.Version)
	require.EqualValues(t, 1, h.store.writes)

	_, err = h.commit(1)
	require.NoError(t, err)
	asked := h.refresh.fencesTo("w")
	done, err := h.start(0)
	require.NoError(t, err, "a start of a fence that is complete succeeds")
	require.Equal(t, stateFenced, done.State.State)
	require.Empty(t, done.Members, "and asks nobody")
	done, err = h.start(1)
	require.NoError(t, err, "whichever of the two versions it names")
	require.Empty(t, done.Members)
	require.Equal(t, asked, h.refresh.fencesTo("w"))
}

// TestCoordinatorCommitNeverUndoesTheFenceAndSaysWhatEachMemberAnswered: a commit that some
// member does not allow writes nothing and leaves the row FENCING, whatever the reason, and says
// what each member answered; asked again once the member is clean it completes.
func TestCoordinatorCommitNeverUndoesTheFenceAndSaysWhatEachMemberAnswered(t *testing.T) {
	holds := &multipoolerservicepb.FenceBusy{RequestsInFlight: 0, ReservationsHeld: 1}
	for name, answer := range map[string]struct {
		respond func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
		status  multipoolerservicepb.MemberFenceResult_Status
		detail  string
	}{
		"busy": {func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
			return busyAnswer(p, req, holds), nil
		}, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "1 reservations held"},
		"unreachable": {func(p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
			return nil, errors.New("pooler unreachable")
		}, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, "pooler unreachable"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			_, err := h.start(0)
			require.NoError(t, err)
			writes := h.store.writes
			h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
				if h.refresh.committing() && p.name() == "r" {
					return answer.respond(p, req)
				}
				return honestAck(p, req), nil
			}
			_, err = h.commit(1)
			results := notClean(t, err)
			require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, results["w"].Status)
			require.Equal(t, answer.status, results["r"].Status)
			require.Contains(t, results["r"].Detail, answer.detail)
			require.ErrorContains(t, err, "call CommitFence again, or unfence")

			row := h.row(t, "alpha")
			require.Equal(t, stateFencing, row.state, "the fence is not undone: the row stays FENCING")
			require.EqualValues(t, 1, row.version)
			require.Equal(t, writes, h.store.writes, "a commit that is refused writes nothing")
			require.Zero(t, h.refresh.reopensTo("w"), "nothing is reopened")

			// Asked again, the commit asks every member again and completes once the member is clean.
			h.refresh.behave = nil
			resp, err := h.commit(1)
			require.NoError(t, err)
			require.Equal(t, stateFenced, resp.State.State)
			require.EqualValues(t, 1, resp.State.Version, "the version does not change")
			require.Equal(t, 2, h.refresh.reportsTo("w"), "the second commit asks every member again")
			require.Equal(t, writes+1, h.store.writes, "and writes only the completion")
		})
	}
}

// TestCoordinatorCommitAsksEveryMemberEvenWhenOneFails: a failure does not stop the others, and
// each member's own answer is reported.
func TestCoordinatorCommitAsksEveryMemberEvenWhenOneFails(t *testing.T) {
	h := newCoordHarness(t)
	h.members.set(ref("a"), ref("b"), ref("w"))
	_, err := h.start(0)
	require.NoError(t, err)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		switch p.name() {
		case "a":
			return nil, errors.New("boom")
		case "b":
			// Takes its time: it is not cut short because "a" failed first.
			time.Sleep(200 * time.Millisecond)
			return busyAnswer(p, req, &multipoolerservicepb.FenceBusy{RequestsInFlight: 1}), nil
		}
		return honestAck(p, req), nil
	}
	_, err = h.commit(1)
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
	_, err := h.start(0)
	results := notClean(t, err)
	require.Less(t, time.Since(started), 2*time.Second, "the call timeout bounds a hung member")
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["r"].Status)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
}

// TestCoordinatorMemberCallsAreShort: neither a start nor a commit waits for work, so a member is
// given a short constant to answer in.
func TestCoordinatorMemberCallsAreShort(t *testing.T) {
	h := newCoordHarness(t)
	h.c.callTimeout = 0
	var (
		mu   sync.Mutex
		left []time.Duration
	)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		mu.Lock()
		d, _ := ctx.Deadline()
		left = append(left, time.Until(d))
		mu.Unlock()
		return honestAck(p, req), nil
	}
	_, err := h.start(0)
	require.NoError(t, err)
	_, err = h.commit(1)
	require.NoError(t, err)
	require.Len(t, left, 4, "every member is asked by the start and by the commit")
	for _, d := range left {
		require.Positive(t, d)
		require.LessOrEqual(t, d, defaultMemberCallTimeout, "a member is given a short time to answer in")
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
	h.store.addState("alpha", stateFencing, 7) // started by a coordinator that then died

	resp, err := h.commit(7)
	require.NoError(t, err, "a started fence is committed at the version it has")
	require.EqualValues(t, 7, resp.State.Version, "and the version is not written again")
	require.Equal(t, 1, h.refresh.reportsTo("w"))

	h.store.addState("alpha", stateFencing, 9)
	_, err = h.start(8)
	require.NoError(t, err, "or started again, which closes the members once more")
	require.Equal(t, 1, h.refresh.fencesTo("w"))
	_, err = h.unfence(9)
	require.NoError(t, err, "or unfenced by an unfence naming it")
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
}

func TestCoordinatorFenceOfAnotherVersionIsRejected(t *testing.T) {
	h := newCoordHarness(t)
	for _, version := range []int64{1, 5} {
		_, err := h.start(version)
		requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	}
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)
	require.Zero(t, h.refresh.fencesTo("w"), "a rejected start asks nobody")
}

// TestCoordinatorCommitAtAnotherVersionIsRejected: a commit names the version of the fence it
// completes, and nothing else is one: not a fence that was never started, not another version
// of this tablegroup, not a version that is not one, and nobody is asked.
func TestCoordinatorCommitAtAnotherVersionIsRejected(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.commit(1)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err) // nothing was started
	_, err = h.start(0)
	require.NoError(t, err)
	for _, version := range []int64{2, 7} {
		_, err = h.commit(version)
		requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	}
	_, err = h.commit(0)
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
	require.Zero(t, h.refresh.reportsTo("w"), "a commit at another version asks nobody")
	require.EqualValues(t, 1, h.store.writes)
}

// startThenWatchListings starts the fence of "alpha" and then resets the listing counter, so that a
// hook on the listings runs on the commit's: its first listing is call 1 and the one that follows
// the round is call 2.
func (h *coordHarness) startThenWatchListings(t *testing.T, hook func(call int)) {
	t.Helper()
	_, err := h.start(0)
	require.NoError(t, err)
	h.members.mu.Lock()
	h.members.calls = 0
	h.members.mu.Unlock()
	h.members.onSnapshot = hook
}

func TestCoordinatorMemberThatDisappearsIsNotClean(t *testing.T) {
	h := newCoordHarness(t)
	h.startThenWatchListings(t, func(call int) {
		if call >= 2 {
			// "r" is gone from the second listing on without having answered.
			h.members.set(ref("w"))
		}
	})
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if h.refresh.committing() && p.name() == "r" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.commit(1)
	results := notClean(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["r"].Status)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
}

// TestCoordinatorMemberThatAnsweredThenVanishedStillCounts: a member that was clean and then left
// the listing was closed when it counted; it opens again only by reading UNFENCED.
func TestCoordinatorMemberThatAnsweredThenVanishedStillCounts(t *testing.T) {
	h := newCoordHarness(t)
	h.startThenWatchListings(t, func(call int) {
		if call >= 2 {
			h.members.set(ref("w"))
		}
	})
	resp, err := h.commit(1)
	require.NoError(t, err)
	require.Len(t, resp.Members, 2)
}

func TestCoordinatorLateJoinerThatVanishesStaysRequired(t *testing.T) {
	h := newCoordHarness(t)
	h.startThenWatchListings(t, func(call int) {
		switch call {
		case 2:
			h.members.set(ref("w"), ref("r"), ref("late"))
		case 3:
			h.members.set(ref("w"), ref("r"))
		}
	})
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if h.refresh.committing() && p.name() == "late" {
			return nil, errors.New("pooler unreachable")
		}
		return honestAck(p, req), nil
	}
	_, err := h.commit(1)
	results := notClean(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, results["late"].Status)
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
}

// TestCoordinatorCommitReachesAMemberThatJoinedAfterTheStart: a pooler that registered after the
// start never got the start; a commit lists the poolers afresh and asks every one to apply the row,
// so it closes the straggler itself and the member is counted by what it answers.
func TestCoordinatorCommitReachesAMemberThatJoinedAfterTheStart(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.start(0)
	require.NoError(t, err)
	h.members.set(ref("w"), ref("r"), ref("joined"))
	held := &multipoolerservicepb.FenceBusy{ReservationsHeld: 1}
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "joined" {
			return busyAnswer(p, req, held), nil
		}
		return honestAck(p, req), nil
	}
	_, err = h.commit(1)
	results := notClean(t, err)
	require.Len(t, results, 3)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, results["joined"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, results["w"].Status)
	require.Equal(t, 1, h.refresh.reportsTo("joined"), "the commit asked the member that joined, and the refresh closed it")
	require.Equal(t, stateFencing, h.row(t, "alpha").state)

	// Once it holds nothing the commit completes.
	h.refresh.behave = nil
	resp, err := h.commit(1)
	require.NoError(t, err)
	require.Len(t, resp.Members, 3)
	require.Equal(t, stateFenced, h.row(t, "alpha").state)
}

func TestCoordinatorMemberJoiningMidCommitIsIncluded(t *testing.T) {
	h := newCoordHarness(t)
	h.startThenWatchListings(t, func(call int) {
		if call == 2 {
			h.members.set(ref("w"), ref("r"), ref("joined"))
		}
	})
	resp, err := h.commit(1)
	require.NoError(t, err)
	require.Len(t, resp.Members, 3)
	require.Equal(t, 1, h.refresh.reportsTo("joined"))
}

func TestCoordinatorAckOfARestartedMemberStillStands(t *testing.T) {
	// A pooler that restarted under the same id after the fence committed reads
	// fenced = true and stays closed; there is nothing to tell the two processes apart
	// by, and nothing needs it.
	h := newCoordHarness(t)
	calls := 0
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if h.refresh.committing() && p.name() == "r" {
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
		for _, stage := range []string{"start", "commit"} {
			t.Run(name+" at the "+stage, func(t *testing.T) {
				h := newCoordHarness(t)
				asked := func(req *multipoolerservicepb.RefreshAdmissionRequest) bool {
					if stage == "start" {
						return h.refresh.starting()
					}
					return h.refresh.committing()
				}
				h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
					if asked(req) && p.name() == "r" {
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
}

func TestCoordinatorUncertainCommitThenReread(t *testing.T) {
	h := newCoordHarness(t)
	h.store.uncertainNext = true
	_, err := h.start(0)
	require.Error(t, err)
	require.ErrorContains(t, err, "outcome uncertain")
	require.Zero(t, h.refresh.fencesTo("w"), "nobody was asked while the outcome was unknown")

	// The write did commit, and nothing undoes it: the caller reads GetServingState, sees
	// FENCING at version 1, and starts it again, which resumes it (or unfences it).
	require.Equal(t, stateFencing, h.row(t, "alpha").state)
	_, err = h.start(0)
	require.NoError(t, err)
	resp, err := h.commit(1)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version)
}

// TestCoordinatorUncertainCompletionIsResumed: the write that completes the fence is the one
// whose acknowledgment is lost. The row says FENCED, and the same commit, repeated, is a
// success.
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
	resp, err := h.commit(1)
	require.NoError(t, err)
	require.Equal(t, stateFenced, resp.State.State)
	require.Equal(t, 1, h.refresh.reportsTo("w"), "a commit of a complete fence asks nobody")
}

func TestCoordinatorStaleLeaderWritesNothing(t *testing.T) {
	h := newCoordHarness(t)
	h.setLeader(mterrors.New(mtrpcpb.Code_UNAVAILABLE, "not the default primary"))
	writes := h.store.writes
	_, err := h.start(0)
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
	_, err := h.start(0)
	requireCode(t, mtrpcpb.Code_ABORTED, err)
	require.Zero(t, h.refresh.fencesTo("w"))
}

// TestCoordinatorFenceIsNotCompletedOverAChangedRow: members that all answered clean to a
// commit whose row changed meanwhile do not complete it: the completion is a compare-and-set
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
				if h.refresh.committing() && p.name() == "r" {
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

// TestCoordinatorACommitThatLosesToAnotherCommitSucceeds: a member that already sees FENCED refuses
// the refresh of a commit that was running at the same time as another's, so the member is not
// clean; the coordinator reads the row again, finds the fence FENCED at its version, and succeeds
// rather than answering ABORTED.
func TestCoordinatorACommitThatLosesToAnotherCommitSucceeds(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.start(0)
	require.NoError(t, err)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "r" {
			// The other commit completes the fence; this member now reads FENCED.
			h.store.mu.Lock()
			h.store.rows["alpha"].state = stateFenced
			h.store.mu.Unlock()
			return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "persisted admission is FENCED, not the expected FENCING")
		}
		return honestAck(p, req), nil
	}
	resp, err := h.commit(1)
	require.NoError(t, err, "the fence is complete at the version")
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version)
}

// TestCoordinatorTwoCommitsOfTheSameFenceBothSucceed: the loser of the compare-and-set that
// completes a fence finds the row FENCED at its own version, which is what it was after, and
// succeeds. A row that is anything else is still an error.
func TestCoordinatorTwoCommitsOfTheSameFenceBothSucceed(t *testing.T) {
	h := newCoordHarness(t)
	h.store.addState("alpha", stateFencing, 1)
	h.store.beforeCAS = func() {
		// The other resume completes the fence first.
		h.store.mu.Lock()
		h.store.rows["alpha"].state = stateFenced
		h.store.mu.Unlock()
		h.store.beforeCAS = nil
	}
	resp, err := h.commit(1)
	require.NoError(t, err, "the fence is complete at the version this call committed")
	require.Equal(t, stateFenced, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version)
	require.Zero(t, h.store.writes, "the loser wrote nothing")
}

// TestCoordinatorACompleteFenceIsSuccessAndAsksNobody: a start or a commit that finds the fence
// complete succeeds, asks no member, and writes nothing: whether a member is clean now is not
// the question of a fence that was committed on every member's clean answer.
func TestCoordinatorACompleteFenceIsSuccessAndAsksNobody(t *testing.T) {
	h := newCoordHarness(t)
	h.store.addState("alpha", stateFenced, 5)

	started, err := h.start(4)
	require.NoError(t, err)
	require.Equal(t, stateFenced, started.State.State)
	require.EqualValues(t, 5, started.State.Version)
	require.Empty(t, started.Members)

	committed, err := h.commit(5)
	require.NoError(t, err)
	require.Equal(t, stateFenced, committed.State.State)
	require.Empty(t, committed.Members)

	_, err = h.start(6)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err) // FENCED at v-1 is another fence
	require.Zero(t, h.refresh.fencesTo("w"))
	require.Zero(t, h.refresh.reportsTo("w"))
	require.Zero(t, h.store.writes, "nothing was written")
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
// refuses it, so a migrator that takes it for one cannot move the traffic. A fence that is
// started again and committed completes, and then the flip is accepted.
func TestCoordinatorFlipRefusesAnIncompleteFence(t *testing.T) {
	h := newCoordHarness(t)
	_, err := h.fenceTG("alpha", 0)
	require.NoError(t, err)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if h.refresh.committing() && p.name() == "r" {
			return busyAnswer(p, req, &multipoolerservicepb.FenceBusy{ReservationsHeld: 1}), nil
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
	require.NoError(t, err, "the same fence, started again and committed, completes")
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
	_, err := h.c.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "db"})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "db", Tablegroup: "alpha", ExpectedVersion: -1})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.CommitFence(ctx, &multipoolerservicepb.CommitFenceRequest{Database: "db", Version: 1})
	requireCode(t, mtrpcpb.Code_INVALID_ARGUMENT, err)
	_, err = h.c.CommitFence(ctx, &multipoolerservicepb.CommitFenceRequest{Database: "db", Tablegroup: "alpha"})
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
		call      string // "start", "commit" or "unfence"
		version   int64  // the version the call names
		start     row
		ok        bool
		end       row
		writes    int // writes the call makes
		closed    bool
		reported  bool
		reopened  bool
		errorCode mtrpcpb.Code
	}{
		"start: UNFENCED at v writes FENCING at v+1 and closes the members": {call: "start", version: 4, start: row{U, 4}, ok: true, end: row{G, 5}, writes: 1, closed: true},
		"start: FENCING at v+1 is a repeat: every member asked again":       {call: "start", version: 4, start: row{G, 5}, ok: true, end: row{G, 5}, closed: true},
		"start: FENCING at v is a repeat for a caller that read it":         {call: "start", version: 4, start: row{G, 4}, ok: true, end: row{G, 4}, closed: true},
		"start: FENCED at v+1 is done: nobody asked, nothing written":       {call: "start", version: 4, start: row{F, 5}, ok: true, end: row{F, 5}},
		"start: FENCED at v is done for a caller that read it":              {call: "start", version: 4, start: row{F, 4}, ok: true, end: row{F, 4}},
		"start: UNFENCED at v+1 is rejected":                                {call: "start", version: 4, start: row{U, 5}, errorCode: bad, end: row{U, 5}},
		"start: UNFENCED at v+2 is rejected":                                {call: "start", version: 4, start: row{U, 6}, errorCode: bad, end: row{U, 6}},
		"start: FENCING at v+2 is another fence and is rejected":            {call: "start", version: 4, start: row{G, 6}, errorCode: bad, end: row{G, 6}},
		"start: FENCED at v+2 is another fence and is rejected":             {call: "start", version: 4, start: row{F, 6}, errorCode: bad, end: row{F, 6}},
		"start: anything older is rejected":                                 {call: "start", version: 4, start: row{U, 3}, errorCode: bad, end: row{U, 3}},
		"commit: FENCING at v is committed: members asked, FENCED written":  {call: "commit", version: 5, start: row{G, 5}, ok: true, end: row{F, 5}, writes: 1, reported: true},
		"commit: FENCED at v is done: nobody asked, nothing written":        {call: "commit", version: 5, start: row{F, 5}, ok: true, end: row{F, 5}},
		"commit: UNFENCED at v is not a fence and is rejected":              {call: "commit", version: 5, start: row{U, 5}, errorCode: bad, end: row{U, 5}},
		"commit: FENCING at v+1 is another fence and is rejected":           {call: "commit", version: 5, start: row{G, 6}, errorCode: bad, end: row{G, 6}},
		"commit: FENCING at v-1 is an older fence and is rejected":          {call: "commit", version: 5, start: row{G, 4}, errorCode: bad, end: row{G, 4}},
		"commit: FENCED at v+1 is another fence and is rejected":            {call: "commit", version: 5, start: row{F, 6}, errorCode: bad, end: row{F, 6}},
		"unfence: FENCED at v writes UNFENCED at v+1":                       {call: "unfence", version: 4, start: row{F, 4}, ok: true, end: row{U, 5}, writes: 1, reopened: true},
		"unfence: FENCING at v writes UNFENCED at v+1":                      {call: "unfence", version: 4, start: row{G, 4}, ok: true, end: row{U, 5}, writes: 1, reopened: true},
		"unfence: UNFENCED at v+1 is done, and the members are asked":       {call: "unfence", version: 4, start: row{U, 5}, ok: true, end: row{U, 5}, reopened: true},
		"unfence: UNFENCED at v is rejected":                                {call: "unfence", version: 4, start: row{U, 4}, errorCode: bad, end: row{U, 4}},
		"unfence: UNFENCED at v+2 is rejected":                              {call: "unfence", version: 4, start: row{U, 6}, errorCode: bad, end: row{U, 6}},
		"unfence: FENCED at v+1 is another fence and is rejected":           {call: "unfence", version: 4, start: row{F, 5}, errorCode: bad, end: row{F, 5}},
		"unfence: FENCING at v+1 is another fence and is rejected":          {call: "unfence", version: 4, start: row{G, 5}, errorCode: bad, end: row{G, 5}},
		"unfence: FENCED at v+2 is rejected":                                {call: "unfence", version: 4, start: row{F, 6}, errorCode: bad, end: row{F, 6}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCoordHarness(t)
			h.store.addState("alpha", tc.start.state, tc.start.version)
			writes := h.store.writes
			var err error
			switch tc.call {
			case "start":
				_, err = h.start(tc.version)
			case "commit":
				_, err = h.commit(tc.version)
			default:
				_, err = h.unfence(tc.version)
			}
			if tc.ok {
				require.NoError(t, err)
			} else {
				requireCode(t, tc.errorCode, err)
			}
			got := h.row(t, "alpha")
			require.Equal(t, tc.end, row{got.state, got.version})
			require.Equal(t, tc.writes, h.store.writes-writes, "writes")
			require.Equal(t, tc.closed, h.refresh.fencesTo("w") == 1, "members asked to close")
			require.Equal(t, tc.reported, h.refresh.reportsTo("w") == 1, "members asked what they hold")
			require.Equal(t, tc.reopened, h.refresh.reopensTo("w") == 1, "members asked to reopen")
		})
	}
}

// TestCoordinatorRepeatingEveryCallEndsInTheSameState: a migrator that does not know whether
// a call took effect repeats it, and ends where it would have if it had not.
func TestCoordinatorRepeatingEveryCallEndsInTheSameState(t *testing.T) {
	h := newCoordHarness(t)
	first, err := h.start(0)
	require.NoError(t, err)
	again, err := h.start(0)
	require.NoError(t, err, "a retry of the same start, whose reply was lost")
	require.Equal(t, first.State.Version, again.State.Version)
	committed, err := h.commit(first.State.Version)
	require.NoError(t, err)
	again2, err := h.commit(first.State.Version)
	require.NoError(t, err, "a retry of the same commit")
	require.Equal(t, committed.State.Version, again2.State.Version)
	_, err = h.start(0)
	require.NoError(t, err, "a start after the commit is a success too")

	_, err = h.unfence(first.State.Version)
	require.NoError(t, err)
	_, err = h.unfence(first.State.Version)
	require.NoError(t, err, "the same unfence again")
	require.EqualValues(t, 2, h.row(t, "alpha").version)
	require.Equal(t, stateUnfenced, h.row(t, "alpha").state)

	// The delayed start and commit of the first fence cannot fence again now that it was undone.
	_, err = h.start(0)
	requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
	_, err = h.commit(1)
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
		if h.refresh.committing() && p.name() == "r" {
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

// TestCoordinatorBusyMemberResultSaysWhatHoldsIt: a member that is busy says what holds its
// gate in its answer, and the per-member result carries it to the controller as it is.
func TestCoordinatorBusyMemberResultSaysWhatHoldsIt(t *testing.T) {
	h := newCoordHarness(t)
	h.members.set(ref("a"), ref("w"))
	holds := (&poolerserver.FenceBusy{
		InFlight:     1,
		Reservations: 1,
		Held: []connpoolmanager.HeldReservation{{User: "app", Held: reserved.Held{
			ConnID: 11, Reasons: protoutil.ReasonTransaction, BackendPID: 777,
			HeldFor: time.Minute, IdleFor: 45 * time.Second, ClientPort: 51234,
		}}},
	}).Details()
	_, err := h.start(0)
	require.NoError(t, err)
	h.refresh.behave = func(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
		if p.name() == "a" && h.refresh.committing() {
			return busyAnswer(p, req, holds), nil
		}
		return honestAck(p, req), nil
	}
	_, err = h.commit(1)
	results := notClean(t, err)

	a := results["a"]
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, a.Status)
	require.NotNil(t, a.Busy)
	require.EqualValues(t, 1, a.Busy.GetRequestsInFlight())
	require.EqualValues(t, 1, a.Busy.GetReservationsHeld())
	require.Len(t, a.Busy.GetReservations(), 1)
	got := a.Busy.GetReservations()[0]
	require.Equal(t, []string{"transaction"}, got.GetReasons())
	require.EqualValues(t, 777, got.GetBackendPid())
	require.Equal(t, 45*time.Second, got.GetIdleFor().AsDuration())
	require.Equal(t, "app", got.GetUser())
	require.EqualValues(t, 51234, got.GetClientPort(), "the port that names the connection reaches the controller as it is")
	require.Equal(t, "1 requests in flight, 1 reservations held", a.Detail)
	require.Nil(t, results["w"].Busy)
}
