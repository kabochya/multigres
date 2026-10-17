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

func fence(tg string, version int64) *multipoolerservicepb.FenceTablegroupRequest {
	return &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: tg, ExpectedVersion: version}
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

	src, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, src.State.State)
	require.EqualValues(t, 1, src.State.Version)
	require.Len(t, src.Members, 2, "every member answered")
	dest, err := s.FenceTablegroup(ctx, fence("destTG", 0))
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
	_, err = s.FenceTablegroup(ctx, fence("destTG", state(t, s, "destTG").Version))
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
		if resp, ok := d.(*multipoolerservicepb.FenceTablegroupResponse); ok {
			for _, m := range resp.Members {
				out[m.PoolerId.Name] = m
			}
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

	_, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	got := memberResults(t, err)
	require.Len(t, got, 2, "every member is asked, none is skipped because another failed")
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["ext-1"].Status)
	require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["ext-2"].Status)
	require.Equal(t, "2 reservations held", got["ext-2"].Detail)
	row := state(t, s, "migrateTG")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, row.State, "a fence that cannot complete stays FENCING: the controller decides")
	require.EqualValues(t, 1, row.Version)

	// The controller retries the same fence, which resumes and asks everyone again.
	s.FailRefresh("ext-1", false)
	_, err = s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.Equal(t, codes.Aborted, code(err), "still busy")
	s.BusyMember("ext-2", "")
	resp, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.State.Version, "and nothing was written by the retries")

	// Or it unfences, and the delayed retry of the undone fence is rejected.
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", 1))
	require.NoError(t, err)
	_, err = s.FenceTablegroup(ctx, fence("migrateTG", 0))
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
	resp, err := s.FenceTablegroup(ctx, fence("migrateTG", 7))
	require.NoError(t, err, "a fence is resumed at the version it has")
	require.EqualValues(t, 7, resp.State.Version, "and the version does not change")
	require.Equal(t, stateFenced, resp.State.State, "it is FENCED once every member is clean")

	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", 7))
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, state(t, s, "migrateTG").State)
}

func TestDelayedUnfenceCannotOpenANewerFence(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	a, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", a.State.Version))
	require.NoError(t, err)
	// A newer coordinator fences again.
	b, err := s.FenceTablegroup(ctx, fence("migrateTG", state(t, s, "migrateTG").Version))
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
	f, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
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
	_, err = s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the destination must be fenced too")
	_, err = s.FenceTablegroup(ctx, fence("destTG", 0))
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
	_, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	_, err = s.FenceTablegroup(ctx, fence("destTG", 0))
	require.Equal(t, codes.Aborted, code(err))
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, state(t, s, "destTG").State)
	_, err = s.FlipRouting(ctx, flip("migrateTG", "destTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "a flip refuses a tablegroup that is only FENCING")

	s.FailRefresh("dest-2", false)
	_, err = s.FenceTablegroup(ctx, fence("destTG", 0))
	require.NoError(t, err, "the same fence, resumed, completes")
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
	_, err := s.FenceTablegroup(t.Context(), &multipoolerservicepb.FenceTablegroupRequest{Database: "db"})
	require.Equal(t, codes.InvalidArgument, code(err))
	resp, err := s.FenceTablegroup(t.Context(), fence("nope", 0))
	require.NoError(t, err, "a tablegroup that was never fenced is unfenced at version zero, and its first fence creates the row")
	require.EqualValues(t, 1, resp.State.Version)
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

	_, err = client.FenceTablegroup(t.Context(), fence("migrateTG", 0))
	require.NoError(t, err)
	s.FailRefresh("dest-2", true)
	_, err = client.FenceTablegroup(t.Context(), fence("destTG", 0))
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
	first, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	again, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err, "a retry of the same request: the tablegroup is fenced at the version after")
	require.Equal(t, first.State.Version, again.State.Version, "and nothing was written")

	_, err = s.FenceTablegroup(ctx, fence("migrateTG", 5))
	require.Equal(t, codes.FailedPrecondition, code(err))

	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", first.State.Version))
	require.NoError(t, err)
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", first.State.Version))
	require.NoError(t, err, "unfenced at the version after: already done")
	require.EqualValues(t, 2, state(t, s, "migrateTG").Version)

	_, err = s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err), "the delayed retry of an undone fence is refused")
	_, err = s.UnfenceTablegroup(ctx, unfence("migrateTG", 0))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

// TestFenceAlreadyCompleteSucceedsWhenAMemberIsNotCleanNow: like the real coordinator, a fence
// that was committed (FENCED) is not undone by a member that is busy or unreachable on a later
// ask: the call succeeds and reports the members.
func TestFenceAlreadyCompleteSucceedsWhenAMemberIsNotCleanNow(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := s.FenceTablegroup(ctx, fence("migrateTG", 0))
	require.NoError(t, err)
	s.BusyMember("ext-1", "1 reservations held")
	s.FailRefresh("ext-2", true)

	for _, version := range []int64{0, 1} {
		resp, err := s.FenceTablegroup(ctx, fence("migrateTG", version))
		require.NoError(t, err, "FENCED at the expected version or the one after is done")
		require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.State)
		got := map[string]multipoolerservicepb.MemberFenceResult_Status{}
		for _, m := range resp.Members {
			got[m.GetPoolerId().GetName()] = m.GetStatus()
		}
		require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_BUSY, got["ext-1"])
		require.Equal(t, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, got["ext-2"])
	}
	require.EqualValues(t, 1, state(t, s, "migrateTG").Version, "nothing was written")
}
