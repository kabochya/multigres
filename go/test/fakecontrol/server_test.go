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

func newFake() *Server {
	s := New()
	s.AddTablegroup("db", "migrateTG", "srcDbConn", "ext-1", "ext-2")
	s.AddTablegroup("db", "destTG", "", "dest-1", "dest-2", "dest-3")
	s.SetAppTablegroup("db", "migrateTG")
	return s
}

func startReq(tg string, version int64) *multipoolerservicepb.StartFenceRequest {
	return &multipoolerservicepb.StartFenceRequest{Database: "db", Tablegroup: tg, ExpectedVersion: version}
}

func commitReq(tg string, version int64) *multipoolerservicepb.CommitFenceRequest {
	return &multipoolerservicepb.CommitFenceRequest{Database: "db", Tablegroup: tg, Version: version}
}

// fence starts the fence at version and commits it, once each, as a controller with nothing to
// wait for does.
func fence(ctx context.Context, s *Server, tg string, version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	started, err := s.StartFence(ctx, startReq(tg, version))
	if err != nil {
		return nil, err
	}
	return s.CommitFence(ctx, commitReq(tg, started.State.Version))
}

func unfence(tg string, version int64) *multipoolerservicepb.UnfenceTablegroupRequest {
	return &multipoolerservicepb.UnfenceTablegroupRequest{Database: "db", Tablegroup: tg, ExpectedVersion: version}
}

func flip(from, to string, version int64) *multipoolerservicepb.FlipRoutingRequest {
	return &multipoolerservicepb.FlipRoutingRequest{Database: "db", FromTablegroup: from, ToTablegroup: to, ExpectedRoutingVersion: version}
}

func code(err error) codes.Code { return status.Code(err) }

func state(t *testing.T, s *Server, tg string) *multipoolerservicepb.TablegroupServingState {
	t.Helper()
	resp, err := s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db", Tablegroups: []string{tg}})
	require.NoError(t, err)
	return resp.Tablegroups[0]
}

func TestCutoverAndRollback(t *testing.T) {
	ctx := t.Context()
	s := newFake()

	src, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, src.State.State)
	require.EqualValues(t, 1, src.State.Version)
	require.Len(t, src.Members, 2, "every member answered")
	dest, err := fence(ctx, s, "destTG", 0)
	require.NoError(t, err)
	require.Len(t, dest.Members, 3)
	for _, m := range dest.Members {
		require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, m.Status)
	}

	route, err := s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.NoError(t, err)
	require.Equal(t, "destTG", route.AppTablegroup)
	require.EqualValues(t, 1, route.RoutingVersion)

	_, err = s.UnfenceTablegroup(ctx, unfence("destTG", dest.State.Version))
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, state(t, s, "destTG").State)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, state(t, s, "migrateTG").State, "the source stays closed")

	// Rollback is the same sequence with the sides exchanged. The source is still
	// fenced from the cutover.
	_, err = fence(ctx, s, "destTG", state(t, s, "destTG").Version)
	require.NoError(t, err)
	route, err = s.FlipRouting(ctx, flip("destTG", "migrateTG", 1))
	require.NoError(t, err)
	require.EqualValues(t, 2, route.RoutingVersion)
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", src.State.Version))
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, state(t, s, "migrateTG").State)
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

func TestIncompleteFenceStaysFencedAndReportsEveryMember(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.FailRefresh("ext-1", true)
	s.BusyMember("ext-2", "2 reservations held")

	// A member that does not answer fails the start, which asks the others and says so; a busy
	// member does not fail it, it is reported.
	_, err := s.StartFence(ctx, startReq("migrateTG", 0))
	got := memberResults(t, err)
	require.Len(t, got, 2, "every member is asked, none is skipped because another failed")
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["ext-1"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["ext-2"].Status)
	row := state(t, s, "migrateTG")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, row.State, "a fence that cannot complete stays FENCING: the controller decides")
	require.EqualValues(t, 1, row.Version)

	// Starting again reaches the member that missed it; the commit then says what is held.
	s.FailRefresh("ext-1", false)
	_, err = s.StartFence(ctx, startReq("migrateTG", 0))
	require.NoError(t, err)
	_, err = s.CommitFence(ctx, commitReq("migrateTG", 1))
	got = memberResults(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, got["ext-1"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["ext-2"].Status)
	require.Equal(t, "2 reservations held", got["ext-2"].Detail)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, state(t, s, "migrateTG").State)

	// The controller commits again once the busy member is clean, and nothing was written by the
	// repeats.
	s.BusyMember("ext-2", "")
	resp, err := s.CommitFence(ctx, commitReq("migrateTG", 1))
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.State.Version)

	// Or it unfences, and the delayed repeat of the undone fence is rejected.
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", 1))
	require.NoError(t, err)
	_, err = s.StartFence(ctx, startReq("migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err))
	_, err = s.CommitFence(ctx, commitReq("migrateTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

func TestFenceInProgressIsResumedAtItsVersion(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	// A coordinator that died after committing the fence leaves the row FENCING.
	s.mu.Lock()
	r := s.databases["db"].rows["migrateTG"]
	r.state, r.version = stateFencing, 7
	s.mu.Unlock()

	_, err := s.UnfenceTablegroup(ctx, unfence("migrateTG", 6))
	require.Equal(t, codes.FailedPrecondition, code(err), "naming another version is refused")
	_, err = s.CommitFence(ctx, commitReq("migrateTG", 6))
	require.Equal(t, codes.FailedPrecondition, code(err), "so is a commit at another version")
	resp, err := s.CommitFence(ctx, commitReq("migrateTG", 7))
	require.NoError(t, err, "a fence is committed at the version it has")
	require.EqualValues(t, 7, resp.State.Version, "and the version does not change")
	require.Equal(t, stateFenced, resp.State.State, "it is FENCED once every member is clean")

	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", 7))
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, state(t, s, "migrateTG").State)
}

func TestDelayedUnfenceCannotOpenANewerFence(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	a, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", a.State.Version))
	require.NoError(t, err)
	// A newer coordinator fences again.
	b, err := fence(ctx, s, "migrateTG", state(t, s, "migrateTG").Version)
	require.NoError(t, err)

	// The old coordinator's unfence names the fence it knew about.
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", a.State.Version))
	require.Equal(t, codes.FailedPrecondition, code(err))
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, state(t, s, "migrateTG").State)
	require.Equal(t, b.State.Version, state(t, s, "migrateTG").Version)
}

func TestUnfenceReportsTheMembersItCouldNotReachAndRepeats(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	f, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	s.FailRefresh("ext-1", true)
	resp, err := s.UnfenceTablegroup(ctx, unfence("migrateTG", f.State.Version))
	require.NoError(t, err, "an unfence has no acknowledgement barrier")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, resp.State.State)
	require.Len(t, resp.Unreached, 1)
	require.Equal(t, "ext-1", resp.Unreached[0].Name)

	// The controller repeats the same unfence until none is unreached.
	s.FailRefresh("ext-1", false)
	resp, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", f.State.Version))
	require.NoError(t, err)
	require.Empty(t, resp.Unreached)
}

func TestFlipRules(t *testing.T) {
	ctx := t.Context()
	s := newFake()

	_, err := s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "both sides must be fenced")
	_, err = fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the destination must be fenced too")
	_, err = fence(ctx, s, "destTG", 0)
	require.NoError(t, err)

	_, err = s.FlipRouting(ctx, flip("destTG", "elsewhereTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "from must be the current application tablegroup")
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 5))
	require.Equal(t, codes.FailedPrecondition, code(err), "the routing version must be the one the caller read")

	moved, err := s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.NoError(t, err)
	again, err := s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.NoError(t, err, "a retry after the move is a success")
	require.Equal(t, moved.RoutingVersion, again.RoutingVersion, "and writes nothing")
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 1))
	require.Equal(t, codes.FailedPrecondition, code(err), "a flip naming a later version is refused")

	_, err = s.FlipRouting(ctx, flip("migrateTG", "migrateTG", 1))
	require.Equal(t, codes.InvalidArgument, code(err))
}

// TestFlipRefusesAnIncompleteFence: FENCING on either side is not a completed fence.
func TestFlipRefusesAnIncompleteFence(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.FailRefresh("dest-2", true)
	_, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	_, err = fence(ctx, s, "destTG", 0)
	require.Equal(t, codes.Aborted, code(err))
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, state(t, s, "destTG").State)
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "a flip refuses a tablegroup that is only FENCING")

	s.FailRefresh("dest-2", false)
	_, err = fence(ctx, s, "destTG", 0)
	require.NoError(t, err, "the same fence, started again and committed, completes")
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.NoError(t, err, "and then the flip is accepted")
}

func TestGetServingStateReadsAMissingRowAsUnfenced(t *testing.T) {
	s := newFake()
	resp, err := s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db"})
	require.NoError(t, err)
	require.Empty(t, resp.Tablegroups)
	require.Equal(t, "migrateTG", resp.AppTablegroup)

	for _, req := range []*multipoolerservicepb.GetServingStateRequest{
		{Database: "db", Tablegroups: []string{"nope"}},
		{Database: "other", Tablegroups: []string{"x"}},
	} {
		got, err := s.GetServingState(t.Context(), req)
		require.NoError(t, err, "like the real coordinator, a tablegroup with no row is unfenced at version zero")
		require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, got.Tablegroups[0].State)
		require.Zero(t, got.Tablegroups[0].Version)
	}
	empty, err := s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "other"})
	require.NoError(t, err)
	require.Empty(t, empty.AppTablegroup)
	require.Zero(t, empty.RoutingVersion)
}

func TestFenceValidatesRequests(t *testing.T) {
	s := newFake()
	_, err := s.StartFence(t.Context(), &multipoolerservicepb.StartFenceRequest{Database: "db"})
	require.Equal(t, codes.InvalidArgument, code(err))
	_, err = s.CommitFence(t.Context(), &multipoolerservicepb.CommitFenceRequest{Database: "db", Tablegroup: "migrateTG"})
	require.Equal(t, codes.InvalidArgument, code(err))
	resp, err := fence(t.Context(), s, "nope", 0)
	require.NoError(t, err, "a tablegroup that was never fenced is unfenced at version zero, and its first fence creates the row")
	require.EqualValues(t, 1, resp.State.Version)
}

// fenceOver is fence over the generated client.
func fenceOver(t *testing.T, client multipoolerservicepb.MultipoolerServiceClient, tg string, version int64) (*multipoolerservicepb.CommitFenceResponse, error) {
	t.Helper()
	started, err := client.StartFence(t.Context(), startReq(tg, version))
	if err != nil {
		return nil, err
	}
	return client.CommitFence(t.Context(), commitReq(tg, started.State.Version))
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

	_, err = fenceOver(t, client, "migrateTG", 0)
	require.NoError(t, err)
	s.FailRefresh("dest-2", true)
	_, err = fenceOver(t, client, "destTG", 0)
	got := memberResults(t, err)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["dest-2"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, got["dest-1"].Status)
	resp, err := client.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db", Tablegroups: []string{"migrateTG", "destTG"}})
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.Tablegroups[0].State)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, resp.Tablegroups[1].State, "the fence that could not complete stays FENCING")
}

func TestFenceAndUnfenceAreSafeToRepeat(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	first, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	again, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err, "a retry of the same request: the tablegroup is fenced at the version after")
	require.Equal(t, first.State.Version, again.State.Version, "and nothing was written")

	_, err = fence(ctx, s, "migrateTG", 5)
	require.Equal(t, codes.FailedPrecondition, code(err))
	second, err := s.CommitFence(ctx, commitReq("migrateTG", first.State.Version))
	require.NoError(t, err, "a commit of a fence that is complete: done")
	require.Equal(t, first.State.Version, second.State.Version)

	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", first.State.Version))
	require.NoError(t, err)
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", first.State.Version))
	require.NoError(t, err, "unfenced at the version after: already done")
	require.EqualValues(t, 2, state(t, s, "migrateTG").Version)

	_, err = fence(ctx, s, "migrateTG", 0)
	require.Equal(t, codes.FailedPrecondition, code(err), "the delayed retry of an undone fence is refused")
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

// TestFenceAlreadyCompleteIsSuccessAndAsksNobody: like the real coordinator, a fence that was
// committed (FENCED) is not undone by a member that is busy or unreachable on a later call: a
// start at the version before, or a commit at the version, succeed and ask nobody.
func TestFenceAlreadyCompleteIsSuccessAndAsksNobody(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := fence(ctx, s, "migrateTG", 0)
	require.NoError(t, err)
	s.BusyMember("ext-1", "1 reservations held")
	s.FailRefresh("ext-2", true)

	started, err := s.StartFence(ctx, startReq("migrateTG", 0))
	require.NoError(t, err, "FENCED at the version after is done")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, started.State.State)
	require.Empty(t, started.Members)
	committed, err := s.CommitFence(ctx, commitReq("migrateTG", 1))
	require.NoError(t, err, "FENCED at the version is done")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, committed.State.State)
	require.Empty(t, committed.Members)
	require.EqualValues(t, 1, state(t, s, "migrateTG").Version, "nothing was written")
}
