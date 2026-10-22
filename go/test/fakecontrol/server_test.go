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

package fakecontrol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

const (
	// opA and opB are two operations: UUIDs, as the contract requires.
	opA = "6f1c0b3a-6a3e-4b8e-9d0f-0a1b2c3d4e5f"
	opB = "b2d4f6a8-1c3e-4a5b-8c7d-9e0f1a2b3c4d"
)

func newFake() *Server {
	s := New()
	s.AddTablegroup("db", "migrateTG", "ext-1", "ext-2")
	s.AddTablegroup("db", "destTG", "dest-1", "dest-2", "dest-3")
	s.SetHome("db", "migrateTG")
	return s
}

func scope(tg string) *multipoolerservicepb.AdmissionScope {
	return &multipoolerservicepb.AdmissionScope{Tablegroup: tg}
}

func startReq(op, tg string, version int64) *multipoolerservicepb.StartFenceRequest {
	return &multipoolerservicepb.StartFenceRequest{Database: "db", OperationId: op, Scope: scope(tg), ExpectedVersion: version}
}

func commitReq(op, tg string, version int64) *multipoolerservicepb.CommitFenceRequest {
	return &multipoolerservicepb.CommitFenceRequest{Database: "db", OperationId: op, Scope: scope(tg), Version: version}
}

func unfenceReq(op, tg string, version int64) *multipoolerservicepb.UnfenceScopeRequest {
	return &multipoolerservicepb.UnfenceScopeRequest{Database: "db", OperationId: op, Scope: scope(tg), ExpectedVersion: version}
}

func switchReq(op, from, to string, version int64) *multipoolerservicepb.SwitchRoutingRequest {
	return &multipoolerservicepb.SwitchRoutingRequest{
		Database: "db", OperationId: op, ExpectedRoutingVersion: version,
		Rule: &multipoolerservicepb.RoutingRule{FromTablegroup: from, ToTablegroup: to, ToShard: "0-inf"},
	}
}

func revertReq(op string, version int64) *multipoolerservicepb.RevertRoutingRequest {
	return &multipoolerservicepb.RevertRoutingRequest{Database: "db", OperationId: op, ExpectedRoutingVersion: version}
}

func foldReq(op string, version int64) *multipoolerservicepb.FoldRoutingRequest {
	return &multipoolerservicepb.FoldRoutingRequest{Database: "db", OperationId: op, ExpectedRoutingVersion: version}
}

func cleanupReq(op string) *multipoolerservicepb.CleanupOperationRequest {
	return &multipoolerservicepb.CleanupOperationRequest{Database: "db", OperationId: op}
}

// fence starts the fence at version and commits it, once each, as a controller with nothing to
// wait for does.
func fence(ctx context.Context, s *Server, op, tg string, version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	started, err := s.StartFence(ctx, startReq(op, tg, version))
	if err != nil {
		return nil, err
	}
	return s.CommitFence(ctx, commitReq(op, tg, started.Fence.Version))
}

func code(err error) codes.Code { return status.Code(err) }

// routing reads the whole routing state.
func routing(t *testing.T, s *Server) *multipoolerservicepb.GetRoutingStateResponse {
	t.Helper()
	resp, err := s.GetRoutingState(t.Context(), &multipoolerservicepb.GetRoutingStateRequest{Database: "db"})
	require.NoError(t, err)
	return resp
}

// fenceOf reads the fence of a tablegroup, nil when it has none.
func fenceOf(t *testing.T, s *Server, tg string) *multipoolerservicepb.AdmissionFence {
	t.Helper()
	for _, f := range routing(t, s).Fences {
		if f.Scope.Tablegroup == tg {
			return f
		}
	}
	return nil
}

func requireFence(t *testing.T, s *Server, tg, op string, state multipoolerservicepb.AdmissionState, version int64) {
	t.Helper()
	f := fenceOf(t, s, tg)
	require.NotNil(t, f, "%s has a fence", tg)
	require.Equal(t, op, f.OperationId, "%s is held by this operation", tg)
	require.Equal(t, state, f.State, "%s", tg)
	require.Equal(t, version, f.Version, "%s", tg)
}

// fenceBoth fences the source and the destination for the operation, from no fence.
func fenceBoth(t *testing.T, s *Server, op string) {
	t.Helper()
	_, err := fence(t.Context(), s, op, "migrateTG", 0)
	require.NoError(t, err)
	_, err = fence(t.Context(), s, op, "destTG", 0)
	require.NoError(t, err)
}

func TestCutoverAndRollback(t *testing.T) {
	ctx := t.Context()
	s := newFake()

	src, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	require.Equal(t, stateFenced, src.Fence.State)
	require.EqualValues(t, 1, src.Fence.Version)
	require.Equal(t, opA, src.Fence.OperationId)
	require.Equal(t, "migrateTG", src.Fence.Scope.Tablegroup)
	require.Len(t, src.Members, 2, "every member answered")
	dest, err := fence(ctx, s, opA, "destTG", 0)
	require.NoError(t, err)
	require.Len(t, dest.Members, 3)
	for _, m := range dest.Members {
		require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, m.Status)
	}

	route, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	require.Equal(t, "destTG", route.EffectiveTablegroup)
	require.EqualValues(t, 1, route.RoutingVersion)
	state := routing(t, s)
	require.Equal(t, "migrateTG", state.HomeTablegroup, "the home does not move until the fold")
	require.Len(t, state.Rules, 1)
	require.Equal(t, opA, state.Rules[0].OperationId)
	require.EqualValues(t, 1, state.Rules[0].CreatedVersion)
	require.Equal(t, "destTG", state.Rules[0].Rule.ToTablegroup)

	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "destTG", dest.Fence.Version))
	require.NoError(t, err)
	requireFence(t, s, "destTG", opA, stateUnfenced, 2)
	requireFence(t, s, "migrateTG", opA, stateFenced, 1) // the source stays closed

	// Rollback: fence the destination again (it is this operation's UNFENCED row, taken at the
	// version read), revert, unfence the source. The source is still fenced from the cutover.
	_, err = fence(ctx, s, opA, "destTG", 2)
	require.NoError(t, err)
	reverted, err := s.RevertRouting(ctx, revertReq(opA, 1))
	require.NoError(t, err)
	require.EqualValues(t, 2, reverted.RoutingVersion)
	require.Equal(t, "migrateTG", reverted.EffectiveTablegroup)
	require.Empty(t, routing(t, s).Rules)
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", src.Fence.Version))
	require.NoError(t, err)
	requireFence(t, s, "migrateTG", opA, stateUnfenced, 2)
	requireFence(t, s, "destTG", opA, stateFenced, 3)
}

func TestFoldAndCleanup(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	fenceBoth(t, s, opA)
	_, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "destTG", 1))
	require.NoError(t, err)

	folded, err := s.FoldRouting(ctx, foldReq(opA, 1))
	require.NoError(t, err)
	require.EqualValues(t, 2, folded.RoutingVersion)
	require.Equal(t, "destTG", folded.HomeTablegroup)
	state := routing(t, s)
	require.Equal(t, "destTG", state.HomeTablegroup)
	require.Empty(t, state.Rules, "the routing is empty again")
	requireFence(t, s, "migrateTG", opA, stateFenced, 1) // the source stays fenced after the fold

	// The cleanup deletes every fence of the operation, the FENCED source included, and says how many.
	cleaned, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err)
	require.EqualValues(t, 2, cleaned.FencesDeleted)
	require.Empty(t, routing(t, s).Fences, "the scopes serve again")
	again, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err, "an operation with nothing left cleans up")
	require.Zero(t, again.FencesDeleted)
}

// memberResults reads the per-member results out of the details of an ABORTED fence.
func memberResults(t *testing.T, err error) map[string]*multipoolerservicepb.MemberFenceResult {
	t.Helper()
	require.Equal(t, codes.Aborted, code(err))
	out := map[string]*multipoolerservicepb.MemberFenceResult{}
	for _, d := range status.Convert(err).Details() {
		var members []*multipoolerservicepb.MemberFenceResult
		switch resp := d.(type) {
		case *multipoolerservicepb.StartFenceResponse:
			members = resp.Members
		case *multipoolerservicepb.CommitFenceResponse:
			members = resp.Members
		}
		for _, m := range members {
			out[m.PoolerId.Name] = m
		}
	}
	require.NotEmpty(t, out, "the per-member results are in the error's details")
	return out
}

func TestIncompleteFenceStaysFencingAndReportsEveryMember(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.FailRefresh("ext-1", true)
	s.BusyMember("ext-2", "2 reservations held")

	// A member that does not answer fails the start, which asks the others and says so; a busy
	// member does not fail it, it is reported.
	_, err := s.StartFence(ctx, startReq(opA, "migrateTG", 0))
	got := memberResults(t, err)
	require.Len(t, got, 2, "every member is asked, none is skipped because another failed")
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["ext-1"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["ext-2"].Status)
	requireFence(t, s, "migrateTG", opA, stateFencing, 1) // a fence that cannot complete stays FENCING: the controller decides

	// Starting again reaches the member that missed it; the commit then says what is held.
	s.FailRefresh("ext-1", false)
	_, err = s.StartFence(ctx, startReq(opA, "migrateTG", 0))
	require.NoError(t, err)
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	got = memberResults(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, got["ext-1"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["ext-2"].Status)
	require.Equal(t, "2 reservations held", got["ext-2"].Detail)
	requireFence(t, s, "migrateTG", opA, stateFencing, 1)

	// The controller commits again once the busy member is clean, and nothing was written by the
	// repeats.
	s.BusyMember("ext-2", "")
	resp, err := s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Fence.Version)

	// Or it unfences, and the delayed repeat of the undone fence is rejected.
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 1))
	require.NoError(t, err)
	_, err = s.StartFence(ctx, startReq(opA, "migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err))
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

func TestFenceInProgressIsResumedAtItsVersion(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	// A coordinator that died after committing the fence leaves the row FENCING.
	s.mu.Lock()
	s.databases["db"].fences["migrateTG"] = &fenceRow{operationID: opA, state: stateFencing, version: 7}
	s.mu.Unlock()

	_, err := s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 6))
	require.Equal(t, codes.FailedPrecondition, code(err), "naming another version is refused")
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 6))
	require.Equal(t, codes.FailedPrecondition, code(err), "so is a commit at another version")
	resp, err := s.CommitFence(ctx, commitReq(opA, "migrateTG", 7))
	require.NoError(t, err, "a fence is committed at the version it has")
	require.EqualValues(t, 7, resp.Fence.Version, "and the version does not change")
	require.Equal(t, stateFenced, resp.Fence.State, "it is FENCED once every member is clean")

	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 7))
	require.NoError(t, err)
	requireFence(t, s, "migrateTG", opA, stateUnfenced, 8)
}

func TestDelayedUnfenceCannotOpenANewerFence(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	a, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", a.Fence.Version))
	require.NoError(t, err)
	// A newer coordinator fences again.
	b, err := fence(ctx, s, opA, "migrateTG", fenceOf(t, s, "migrateTG").Version)
	require.NoError(t, err)

	// The old coordinator's unfence names the fence it knew about.
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", a.Fence.Version))
	require.Equal(t, codes.FailedPrecondition, code(err))
	requireFence(t, s, "migrateTG", opA, stateFenced, b.Fence.Version)
}

func TestUnfenceReportsTheMembersItCouldNotReachAndRepeats(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	f, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	s.FailRefresh("ext-1", true)
	resp, err := s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", f.Fence.Version))
	require.NoError(t, err, "an unfence has no acknowledgement barrier")
	require.Equal(t, stateUnfenced, resp.Fence.State)
	require.Len(t, resp.Unreached, 1)
	require.Equal(t, "ext-1", resp.Unreached[0].Name)

	// The controller repeats the same unfence until none is unreached.
	s.FailRefresh("ext-1", false)
	resp, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", f.Fence.Version))
	require.NoError(t, err)
	require.Empty(t, resp.Unreached)
}

func TestFenceAndUnfenceAreSafeToRepeat(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	first, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	again, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err, "a retry of the same request: the tablegroup is fenced at the version after")
	require.Equal(t, first.Fence.Version, again.Fence.Version, "and nothing was written")

	_, err = fence(ctx, s, opA, "migrateTG", 5)
	require.Equal(t, codes.FailedPrecondition, code(err))
	second, err := s.CommitFence(ctx, commitReq(opA, "migrateTG", first.Fence.Version))
	require.NoError(t, err, "a commit of a fence that is complete: done")
	require.Equal(t, first.Fence.Version, second.Fence.Version)

	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", first.Fence.Version))
	require.NoError(t, err)
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", first.Fence.Version))
	require.NoError(t, err, "unfenced at the version after: already done")
	requireFence(t, s, "migrateTG", opA, stateUnfenced, 2)

	_, err = fence(ctx, s, opA, "migrateTG", 0)
	require.Equal(t, codes.FailedPrecondition, code(err), "the delayed retry of an undone fence is refused")
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

// TestFenceAlreadyCompleteIsSuccessAndAsksNobody: like the real coordinator, a fence that was
// committed (FENCED) is not undone by a member that is busy or unreachable on a later call: a
// start at the version before, or a commit at the version, succeed and ask nobody.
func TestFenceAlreadyCompleteIsSuccessAndAsksNobody(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	s.BusyMember("ext-1", "1 reservations held")
	s.FailRefresh("ext-2", true)

	started, err := s.StartFence(ctx, startReq(opA, "migrateTG", 0))
	require.NoError(t, err, "FENCED at the version after is done")
	require.Equal(t, stateFenced, started.Fence.State)
	require.Empty(t, started.Members)
	committed, err := s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	require.NoError(t, err, "FENCED at the version is done")
	require.Equal(t, stateFenced, committed.Fence.State)
	require.Empty(t, committed.Members)
	requireFence(t, s, "migrateTG", opA, stateFenced, 1) // nothing was written
}

// TestOwnershipAndTakeover: a scope has one fence, held by one operation. Another operation takes
// it over only from an UNFENCED row, with the version it read.
func TestOwnershipAndTakeover(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := s.StartFence(ctx, startReq(opA, "migrateTG", 0))
	require.NoError(t, err)

	// FENCING for A: B can neither start, commit nor unfence, at any version.
	for _, v := range []int64{0, 1, 2} {
		_, err = s.StartFence(ctx, startReq(opB, "migrateTG", v))
		require.Equal(t, codes.FailedPrecondition, code(err), "a scope that is FENCING for another operation, version %d", v)
	}
	_, err = s.CommitFence(ctx, commitReq(opB, "migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err))
	_, err = s.UnfenceScope(ctx, unfenceReq(opB, "migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err))

	// FENCED for A: the same.
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	require.NoError(t, err)
	_, err = s.StartFence(ctx, startReq(opB, "migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "a scope that is FENCED for another operation")
	requireFence(t, s, "migrateTG", opA, stateFenced, 1)

	// A unfences: the row stays, UNFENCED at the next version, still A's.
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 1))
	require.NoError(t, err)
	requireFence(t, s, "migrateTG", opA, stateUnfenced, 2)
	_, err = s.StartFence(ctx, startReq(opB, "migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "B names a version it did not read")
	_, err = s.StartFence(ctx, startReq(opB, "migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "version 0 is for a scope with no fence")

	// B takes the UNFENCED row over, with the version it read.
	started, err := s.StartFence(ctx, startReq(opB, "migrateTG", 2))
	require.NoError(t, err)
	require.Equal(t, opB, started.Fence.OperationId)
	require.EqualValues(t, 3, started.Fence.Version)
	requireFence(t, s, "migrateTG", opB, stateFencing, 3)

	// A's delayed calls are refused: the scope is B's now.
	_, err = s.StartFence(ctx, startReq(opA, "migrateTG", 2))
	require.Equal(t, codes.FailedPrecondition, code(err))
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 2))
	require.Equal(t, codes.FailedPrecondition, code(err), "A's unfence of its undone fence does not reach B's")
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 3))
	require.Equal(t, codes.FailedPrecondition, code(err), "nor does one that names B's version")
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 3))
	require.Equal(t, codes.FailedPrecondition, code(err))
	requireFence(t, s, "migrateTG", opB, stateFencing, 3)

	// A's cleanup takes nothing of B's.
	cleaned, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err)
	require.Zero(t, cleaned.FencesDeleted)
	requireFence(t, s, "migrateTG", opB, stateFencing, 3)
}

// TestAnExplicitUnfencedRowStaysUntilCleanup: an unfence does not delete the fence, and a scope
// with no fence is read as unfenced at version 0 (it is simply not listed).
func TestAnExplicitUnfencedRowStaysUntilCleanup(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	require.Empty(t, routing(t, s).Fences, "a scope with no fence is not listed")
	_, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 1))
	require.NoError(t, err)
	state := routing(t, s)
	require.Len(t, state.Fences, 1)
	requireFence(t, s, "migrateTG", opA, stateUnfenced, 2)

	cleaned, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err)
	require.EqualValues(t, 1, cleaned.FencesDeleted)
	require.Nil(t, fenceOf(t, s, "migrateTG"))
	// With no fence the scope is at version 0 again, and starts from it.
	started, err := s.StartFence(ctx, startReq(opB, "migrateTG", 0))
	require.NoError(t, err)
	require.EqualValues(t, 1, started.Fence.Version)
}

func TestSwitchRules(t *testing.T) {
	ctx := t.Context()
	s := newFake()

	_, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "both sides must be fenced")
	_, err = fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the destination must be fenced too")
	_, err = fence(ctx, s, opB, "destTG", 0)
	require.NoError(t, err)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "and fenced for this operation: it is B's")
	_, err = s.UnfenceScope(ctx, unfenceReq(opB, "destTG", 1))
	require.NoError(t, err)
	_, err = fence(ctx, s, opA, "destTG", 2)
	require.NoError(t, err, "A takes the destination over")

	_, err = s.SwitchRouting(ctx, switchReq(opA, "destTG", "migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "from must be the home tablegroup")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 5))
	require.Equal(t, codes.FailedPrecondition, code(err), "the routing version must be the one the caller read")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "elsewhereTG", 0))
	require.Equal(t, codes.NotFound, code(err), "the tablegroup of a rule must be in the catalog")

	moved, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	again, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err, "a retry after the switch is a success")
	require.Equal(t, moved.RoutingVersion, again.RoutingVersion, "and writes nothing")
	require.Equal(t, "destTG", again.EffectiveTablegroup)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "a switch naming a later version is refused")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "otherTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "this operation already has a rule, a different one is not a retry")
	_, err = s.SwitchRouting(ctx, switchReq(opB, "migrateTG", "destTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "at most one rule applies at a time")

	other := switchReq(opA, "migrateTG", "destTG", 0)
	other.Rule.ToShard = "other-shard"
	_, err = s.SwitchRouting(ctx, other)
	require.Equal(t, codes.FailedPrecondition, code(err), "a retry asks for the rule that was switched")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "migrateTG", 1))
	require.Equal(t, codes.InvalidArgument, code(err))
	bad := switchReq(opA, "migrateTG", "destTG", 1)
	bad.Rule.ToShard = ""
	_, err = s.SwitchRouting(ctx, bad)
	require.Equal(t, codes.InvalidArgument, code(err), "a rule names the shard it sends the traffic to")
}

func TestSwitchRefusesTheDefaultTablegroupAndAHomeThatIsNotFrom(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.AddTablegroup("db", "default", "def-1", "def-2")
	s.SetHome("db", "default")
	for _, tg := range []string{"default", "destTG"} {
		_, err := fence(ctx, s, opA, tg, 0)
		require.NoError(t, err)
	}
	_, err := s.SwitchRouting(ctx, switchReq(opA, "default", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the traffic of the default tablegroup cannot be routed away")

	empty := New()
	empty.AddTablegroup("db", "migrateTG", "ext-1")
	empty.AddTablegroup("db", "destTG", "dest-1")
	_, err = fence(ctx, empty, opA, "migrateTG", 0)
	require.NoError(t, err)
	_, err = fence(ctx, empty, opA, "destTG", 0)
	require.NoError(t, err)
	_, err = empty.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "a database with no home has nothing to route from")
}

// TestSwitchRefusesAnIncompleteFence: FENCING on either side is not a completed fence.
func TestSwitchRefusesAnIncompleteFence(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.FailRefresh("dest-2", true)
	_, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	_, err = fence(ctx, s, opA, "destTG", 0)
	require.Equal(t, codes.Aborted, code(err))
	requireFence(t, s, "destTG", opA, stateFencing, 1)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "a switch refuses a tablegroup that is only FENCING")

	s.FailRefresh("dest-2", false)
	_, err = fence(ctx, s, opA, "destTG", 0)
	require.NoError(t, err, "the same fence, started again and committed, completes")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err, "and then the switch is accepted")
}

func TestRevertRules(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	fenceBoth(t, s, opA)

	_, err := s.RevertRouting(ctx, revertReq(opA, 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "there is no rule to revert")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)

	_, err = s.RevertRouting(ctx, revertReq(opB, 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "B has no rule")
	_, err = s.RevertRouting(ctx, revertReq(opA, 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the routing version must be the one the caller read")

	// Reverting needs both tablegroups of the rule FENCED, like switching: the destination was
	// unfenced after the switch.
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "destTG", 1))
	require.NoError(t, err)
	_, err = s.RevertRouting(ctx, revertReq(opA, 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "the destination is unfenced")
	_, err = s.StartFence(ctx, startReq(opA, "destTG", 2))
	require.NoError(t, err)
	_, err = s.RevertRouting(ctx, revertReq(opA, 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "the destination is only FENCING")
	_, err = s.CommitFence(ctx, commitReq(opA, "destTG", 3))
	require.NoError(t, err)

	reverted, err := s.RevertRouting(ctx, revertReq(opA, 1))
	require.NoError(t, err)
	require.EqualValues(t, 2, reverted.RoutingVersion)
	require.Equal(t, "migrateTG", reverted.EffectiveTablegroup)
	again, err := s.RevertRouting(ctx, revertReq(opA, 1))
	require.NoError(t, err, "a retry after the revert is a success")
	require.EqualValues(t, 2, again.RoutingVersion, "and writes nothing")
	_, err = s.RevertRouting(ctx, revertReq(opA, 2))
	require.Equal(t, codes.FailedPrecondition, code(err), "a revert naming the version after is refused")

	// The reverted operation can switch again, from the version it reads.
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 2))
	require.NoError(t, err)
}

func TestFoldRules(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := s.FoldRouting(ctx, foldReq(opA, 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "there is no rule to fold")

	fenceBoth(t, s, opA)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	_, err = s.FoldRouting(ctx, foldReq(opB, 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "B has no rule")
	_, err = s.FoldRouting(ctx, foldReq(opA, 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the routing version must be the one the caller read")

	// No fence is needed: the effective tablegroup is the same before and after. Both are still
	// FENCED here, and the fold does not care.
	folded, err := s.FoldRouting(ctx, foldReq(opA, 1))
	require.NoError(t, err)
	require.EqualValues(t, 2, folded.RoutingVersion)
	require.Equal(t, "destTG", folded.HomeTablegroup)
	require.Equal(t, "destTG", routing(t, s).HomeTablegroup)

	again, err := s.FoldRouting(ctx, foldReq(opA, 1))
	require.NoError(t, err, "a retry after the fold is a success")
	require.EqualValues(t, 2, again.RoutingVersion, "and writes nothing")
	require.Equal(t, "destTG", again.HomeTablegroup)
	_, err = s.FoldRouting(ctx, foldReq(opA, 2))
	require.Equal(t, codes.FailedPrecondition, code(err))

	// A fold would refuse to move the traffic: the rule of this operation does not apply to the home.
	s.mu.Lock()
	d := s.databases["db"]
	d.rules = []*ruleRow{{operationID: opB, from: "migrateTG", to: "destTG", toShard: "0-inf", createdVersion: 2}}
	s.mu.Unlock()
	_, err = s.FoldRouting(ctx, foldReq(opB, 2))
	require.Equal(t, codes.FailedPrecondition, code(err), "the rule moves a tablegroup that is not the home")
}

func TestCleanupRules(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	fenceBoth(t, s, opA)
	_, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)

	_, err = s.CleanupOperation(ctx, cleanupReq(opA))
	require.Equal(t, codes.FailedPrecondition, code(err), "refused while the operation has a rule")
	require.Len(t, routing(t, s).Fences, 2, "and nothing is deleted")

	_, err = s.RevertRouting(ctx, revertReq(opA, 1))
	require.NoError(t, err)
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 1))
	require.NoError(t, err)
	_, err = s.StartFence(ctx, startReq(opA, "migrateTG", 2))
	require.NoError(t, err)
	_, err = s.CleanupOperation(ctx, cleanupReq(opA))
	require.Equal(t, codes.FailedPrecondition, code(err), "refused while one of its fences is FENCING")
	require.Len(t, routing(t, s).Fences, 2, "and nothing is deleted, the FENCED one included")

	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 3))
	require.NoError(t, err)
	cleaned, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err, "a FENCED fence is deleted too")
	require.EqualValues(t, 2, cleaned.FencesDeleted)
	require.Empty(t, routing(t, s).Fences)
}

func TestGetRoutingState(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	state := routing(t, s)
	require.Equal(t, "migrateTG", state.HomeTablegroup)
	require.Zero(t, state.RoutingVersion)
	require.False(t, state.NotModified)
	require.Empty(t, state.Fences, "a tablegroup with no fence is unfenced at version zero, and not listed")

	empty, err := s.GetRoutingState(ctx, &multipoolerservicepb.GetRoutingStateRequest{Database: "other"})
	require.NoError(t, err)
	require.Empty(t, empty.HomeTablegroup)
	require.Zero(t, empty.RoutingVersion)

	fenceBoth(t, s, opA)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)

	// A caller that holds the current routing version is told nothing changed, and gets no home or
	// rules; the fences always come, since they change without a change of the routing version.
	got, err := s.GetRoutingState(ctx, &multipoolerservicepb.GetRoutingStateRequest{Database: "db", SinceVersion: 1})
	require.NoError(t, err)
	require.True(t, got.NotModified)
	require.EqualValues(t, 1, got.RoutingVersion)
	require.Empty(t, got.HomeTablegroup)
	require.Empty(t, got.Rules)
	require.Len(t, got.Fences, 2)
	require.Equal(t, "destTG", got.Fences[0].Scope.Tablegroup, "fences are listed in order of tablegroup")

	for _, since := range []int64{0, 2} {
		got, err = s.GetRoutingState(ctx, &multipoolerservicepb.GetRoutingStateRequest{Database: "db", SinceVersion: since})
		require.NoError(t, err)
		require.False(t, got.NotModified, "since_version %d is not the current version", since)
		require.Equal(t, "migrateTG", got.HomeTablegroup)
		require.Len(t, got.Rules, 1)
	}
	require.NoError(t, func() error {
		_, err := s.UnfenceScope(ctx, unfenceReq(opA, "destTG", 1))
		return err
	}())
	got, err = s.GetRoutingState(ctx, &multipoolerservicepb.GetRoutingStateRequest{Database: "db", SinceVersion: 1})
	require.NoError(t, err)
	require.True(t, got.NotModified)
	require.Equal(t, stateUnfenced, got.Fences[0].State, "a fence change is visible with no change of the routing version")
}

// TestARetryIsRecognizedByTheOperationsStateNotByTheVersion: a fold that is retried succeeds, and a
// revert that names the same version is refused, though the routing version is the one after.
func TestARetryIsRecognizedByTheOperationsStateNotByTheVersion(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	fenceBoth(t, s, opA)
	_, err := s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	_, err = s.FoldRouting(ctx, foldReq(opA, 1))
	require.NoError(t, err)
	_, err = s.FoldRouting(ctx, foldReq(opA, 1))
	require.NoError(t, err, "the fold, retried")

	_, err = s.RevertRouting(ctx, revertReq(opA, 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "a revert is not the retry of a fold")
	require.ErrorContains(t, err, "it is FOLDED at routing version 2")
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 2))
	require.Equal(t, codes.FailedPrecondition, code(err), "a folded operation cannot switch again")
	require.ErrorContains(t, err, "folded")

	// And a revert, retried, is not taken for a fold.
	r := newFake()
	fenceBoth(t, r, opA)
	_, err = r.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	_, err = r.RevertRouting(ctx, revertReq(opA, 1))
	require.NoError(t, err)
	_, err = r.FoldRouting(ctx, foldReq(opA, 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "a fold is not the retry of a revert")
	require.ErrorContains(t, err, "it is REVERTED at routing version 2")
}

// TestRevertAndFoldOfAnOperationThatNeverSwitchedAreRefused: another operation's change bumped the
// routing version to the one after the expected, which is not a trace of this operation's.
func TestRevertAndFoldOfAnOperationThatNeverSwitchedAreRefused(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.AddTablegroup("db", "thirdTG", "third-1")
	_, err := fence(ctx, s, opB, "thirdTG", 0) // B is recorded, as PREPARE
	require.NoError(t, err)
	fenceBoth(t, s, opA)
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)

	const opC = "0c0c0c0c-1d1d-4e4e-8f8f-a0a0b1b1c2c2" // nothing is recorded of it
	for op, trace := range map[string]string{opB: "it is PREPARE at routing version 0", opC: "nothing is recorded of it"} {
		_, err = s.RevertRouting(ctx, revertReq(op, 0))
		require.Equal(t, codes.FailedPrecondition, code(err), "revert by %s", op)
		require.ErrorContains(t, err, trace)
		_, err = s.FoldRouting(ctx, foldReq(op, 0))
		require.Equal(t, codes.FailedPrecondition, code(err), "fold by %s", op)
		require.ErrorContains(t, err, trace)
	}
	require.Len(t, routing(t, s).Rules, 1, "A's rule is untouched")
	require.EqualValues(t, 1, routing(t, s).RoutingVersion)
}

// TestAReplayAfterCleanupIsRefused: a cleaned-up operation is finished for good. A scope with no fence
// is at version zero, so a delayed start of the operation would otherwise fence it again.
func TestAReplayAfterCleanupIsRefused(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := fence(ctx, s, opA, "migrateTG", 0)
	require.NoError(t, err)
	cleaned, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err)
	require.EqualValues(t, 1, cleaned.FencesDeleted)

	calls := map[string]error{}
	_, calls["start"] = s.StartFence(ctx, startReq(opA, "migrateTG", 0))
	_, calls["commit"] = s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	_, calls["unfence"] = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 1))
	_, calls["switch"] = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", 0))
	_, calls["revert"] = s.RevertRouting(ctx, revertReq(opA, 0))
	_, calls["fold"] = s.FoldRouting(ctx, foldReq(opA, 0))
	for name, err := range calls {
		require.Equal(t, codes.FailedPrecondition, code(err), name)
		require.ErrorContains(t, err, "cleaned up", name)
	}
	require.Empty(t, routing(t, s).Fences, "the replay fenced nothing")

	again, err := s.CleanupOperation(ctx, cleanupReq(opA))
	require.NoError(t, err, "the cleanup itself is repeated")
	require.Zero(t, again.FencesDeleted)

	// An operation that was never recorded is cleaned up too, and ends.
	fresh, err := s.CleanupOperation(ctx, cleanupReq(opB))
	require.NoError(t, err)
	require.Zero(t, fresh.FencesDeleted)
	_, err = s.StartFence(ctx, startReq(opB, "migrateTG", 0))
	require.ErrorContains(t, err, "cleaned up")
}

func TestRequestsAreValidated(t *testing.T) {
	ctx := t.Context()
	s := newFake()

	// An operation id that is not a UUID.
	_, err := s.StartFence(ctx, startReq("not-a-uuid", "migrateTG", 0))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.CommitFence(ctx, commitReq("", "migrateTG", 1))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.UnfenceScope(ctx, unfenceReq("x", "migrateTG", 1))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.SwitchRouting(ctx, switchReq("x", "migrateTG", "destTG", 0))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.RevertRouting(ctx, revertReq("x", 0))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.FoldRouting(ctx, foldReq("x", 0))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.CleanupOperation(ctx, cleanupReq("x"))
	require.Equal(t, codes.InvalidArgument, code(err))

	// A scope with no tablegroup, a bad version.
	_, err = s.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "db", OperationId: opA})
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.StartFence(ctx, startReq(opA, "migrateTG", -1))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 0))
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.SwitchRouting(ctx, switchReq(opA, "migrateTG", "destTG", -1))
	require.Equal(t, codes.InvalidArgument, code(err))

	// A scope below the tablegroup.
	for _, sc := range []*multipoolerservicepb.AdmissionScope{
		{Tablegroup: "migrateTG", Shard: "0-inf"},
		{Tablegroup: "migrateTG", Table: "public.t"},
	} {
		_, err = s.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "db", OperationId: opA, Scope: sc})
		require.Equal(t, codes.Unimplemented, code(err))
	}

	// A tablegroup that is not in the catalog and has no fence.
	_, err = s.StartFence(ctx, startReq(opA, "nope", 0))
	require.Equal(t, codes.NotFound, code(err))
	_, err = s.CommitFence(ctx, commitReq(opA, "nope", 1))
	require.Equal(t, codes.NotFound, code(err))
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "nope", 1))
	require.Equal(t, codes.NotFound, code(err))
	require.Nil(t, fenceOf(t, s, "nope"), "nothing was created")

	// A known tablegroup with no fence: commit and unfence find nothing to act on.
	_, err = s.CommitFence(ctx, commitReq(opA, "migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err))
	_, err = s.UnfenceScope(ctx, unfenceReq(opA, "migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

// fenceOver is fence over the generated client.
func fenceOver(t *testing.T, client multipoolerservicepb.MultipoolerServiceClient, op, tg string, version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	t.Helper()
	started, err := client.StartFence(t.Context(), startReq(op, tg, version))
	if err != nil {
		return nil, err
	}
	return client.CommitFence(t.Context(), commitReq(op, tg, started.Fence.Version))
}

// TestOverGRPC checks the fake serves the real generated service, which is what
// a controller under development dials, per-member results included.
func TestOverGRPC(t *testing.T) {
	s := newFake()
	addr := Start(t, s)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := multipoolerservicepb.NewMultipoolerServiceClient(conn)

	_, err = fenceOver(t, client, opA, "migrateTG", 0)
	require.NoError(t, err)
	s.FailRefresh("dest-2", true)
	_, err = fenceOver(t, client, opA, "destTG", 0)
	got := memberResults(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["dest-2"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, got["dest-1"].Status)
	resp, err := client.GetRoutingState(t.Context(), &multipoolerservicepb.GetRoutingStateRequest{Database: "db"})
	require.NoError(t, err)
	require.Len(t, resp.Fences, 2)
	require.Equal(t, stateFencing, resp.Fences[0].State, "the fence that could not complete stays FENCING")
	require.Equal(t, stateFenced, resp.Fences[1].State)

	s.FailRefresh("dest-2", false)
	_, err = fenceOver(t, client, opA, "destTG", 0)
	require.NoError(t, err)
	_, err = client.SwitchRouting(t.Context(), switchReq(opA, "migrateTG", "destTG", 0))
	require.NoError(t, err)
	_, err = client.FoldRouting(t.Context(), foldReq(opA, 1))
	require.NoError(t, err)
	cleaned, err := client.CleanupOperation(t.Context(), cleanupReq(opA))
	require.NoError(t, err)
	require.EqualValues(t, 2, cleaned.FencesDeleted)
}
