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

const (
	fenced    = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	fencing   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	unfenced  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	unfencing = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING
)

func newFake() *Server {
	s := New()
	s.AddTablegroup("db", "migrateTG", "srcDbConn", "ext-1", "ext-2")
	s.AddTablegroup("db", "destTG", "", "dest-1", "dest-2", "dest-3")
	s.SetAppTablegroup("db", "migrateTG")
	return s
}

func fence(tg, id string) *multipoolerservicepb.UpdatePoolerAdmissionRequest {
	return &multipoolerservicepb.UpdatePoolerAdmissionRequest{Database: "db", Tablegroup: tg, Target: fenced, RequestId: id}
}

func unfence(tg, id, expected string) *multipoolerservicepb.UpdatePoolerAdmissionRequest {
	return &multipoolerservicepb.UpdatePoolerAdmissionRequest{Database: "db", Tablegroup: tg, Target: unfenced, RequestId: id, ExpectedRequestId: expected}
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

	resp, err := s.UpdatePoolerAdmission(ctx, fence("destTG", "f-dest"))
	require.NoError(t, err)
	require.Equal(t, fenced, resp.State.AdmissionState)
	require.Len(t, resp.Acks, 3)
	_, err = s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f-src"))
	require.NoError(t, err)

	route, err := s.UpdateMigrationRouting(ctx, &multipoolerservicepb.UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: "migrateTG", ToTablegroup: "destTG", RequestId: "r1"})
	require.NoError(t, err)
	require.Equal(t, "destTG", route.AppTablegroup)
	require.EqualValues(t, 1, route.RoutingVersion)

	_, err = s.UpdatePoolerAdmission(ctx, unfence("destTG", "u-dest", "f-dest"))
	require.NoError(t, err)
	require.Equal(t, unfenced, state(t, s, "destTG").AdmissionState)
	require.Equal(t, fenced, state(t, s, "migrateTG").AdmissionState, "the source stays closed")

	// Rollback is the same sequence with the roles swapped.
	_, err = s.UpdatePoolerAdmission(ctx, fence("destTG", "f-dest-2"))
	require.NoError(t, err)
	route, err = s.UpdateMigrationRouting(ctx, &multipoolerservicepb.UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: "destTG", ToTablegroup: "migrateTG", RequestId: "r2"})
	require.NoError(t, err)
	require.EqualValues(t, 2, route.RoutingVersion)
	_, err = s.UpdatePoolerAdmission(ctx, unfence("migrateTG", "u-src", "f-src"))
	require.NoError(t, err)
	require.Equal(t, unfenced, state(t, s, "migrateTG").AdmissionState)
}

func TestIncompleteFenceLeavesFencingAndResumesWithSameRequestID(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	s.FailRefresh("ext-2", true)

	_, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f-src"))
	require.Equal(t, codes.Aborted, code(err))
	got := state(t, s, "migrateTG")
	require.Equal(t, fencing, got.AdmissionState, "an incomplete fence is distinguishable from FENCED")
	require.Equal(t, "f-src", got.RequestId)

	// Nothing may be rerouted while the fence is incomplete.
	_, err = s.UpdatePoolerAdmission(ctx, fence("destTG", "f-dest"))
	require.NoError(t, err)
	_, err = s.UpdateMigrationRouting(ctx, &multipoolerservicepb.UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: "migrateTG", ToTablegroup: "destTG", RequestId: "r1"})
	require.Equal(t, codes.FailedPrecondition, code(err))

	// An unfence cannot undo a fence that never completed.
	_, err = s.UpdatePoolerAdmission(ctx, unfence("migrateTG", "u-src", "f-src"))
	require.Equal(t, codes.FailedPrecondition, code(err))

	s.FailRefresh("ext-2", false)
	resp, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f-src"))
	require.NoError(t, err)
	require.Equal(t, fenced, resp.State.AdmissionState)
}

func TestSameRequestIDIsIdempotent(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	first, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f-src"))
	require.NoError(t, err)
	again, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f-src"))
	require.NoError(t, err)
	require.Equal(t, first.State.UpdatedAt.AsTime(), again.State.UpdatedAt.AsTime(), "a repeat must not rewrite state")
}

func TestDelayedUnfenceCannotOverrideNewerFence(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "fence-A"))
	require.NoError(t, err)
	// A newer coordinator fences again under its own request id.
	_, err = s.UpdatePoolerAdmission(ctx, fence("migrateTG", "fence-B"))
	require.NoError(t, err)

	// The old coordinator's unfence names the fence it knew about.
	_, err = s.UpdatePoolerAdmission(ctx, unfence("migrateTG", "unfence-A", "fence-A"))
	require.Equal(t, codes.FailedPrecondition, code(err))
	require.Equal(t, fenced, state(t, s, "migrateTG").AdmissionState)
	require.Equal(t, "fence-B", state(t, s, "migrateTG").RequestId)

	// An unfence must name the fence it undoes.
	_, err = s.UpdatePoolerAdmission(ctx, unfence("migrateTG", "unfence-B", ""))
	require.Equal(t, codes.InvalidArgument, code(err))
}

func TestFencePreemptsUnfenceInProgress(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	_, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f1"))
	require.NoError(t, err)
	s.FailRefresh("ext-1", true)
	_, err = s.UpdatePoolerAdmission(ctx, unfence("migrateTG", "u1", "f1"))
	require.Equal(t, codes.Aborted, code(err))
	require.Equal(t, unfencing, state(t, s, "migrateTG").AdmissionState)

	s.FailRefresh("ext-1", false)
	resp, err := s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f2"))
	require.NoError(t, err)
	require.Equal(t, fenced, resp.State.AdmissionState)
	require.Equal(t, "f2", resp.State.RequestId)

	// The preempted unfence's retry now loses.
	_, err = s.UpdatePoolerAdmission(ctx, unfence("migrateTG", "u1", "f1"))
	require.Equal(t, codes.FailedPrecondition, code(err))
}

func TestRoutingRules(t *testing.T) {
	ctx := t.Context()
	s := newFake()
	route := func(from, to string) (*multipoolerservicepb.UpdateMigrationRoutingResponse, error) {
		return s.UpdateMigrationRouting(ctx, &multipoolerservicepb.UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: from, ToTablegroup: to, RequestId: "r"})
	}

	_, err := route("migrateTG", "destTG")
	require.Equal(t, codes.FailedPrecondition, code(err), "both sides must be fenced")
	_, err = s.UpdatePoolerAdmission(ctx, fence("migrateTG", "f1"))
	require.NoError(t, err)
	_, err = route("migrateTG", "destTG")
	require.Equal(t, codes.FailedPrecondition, code(err), "the destination must be fenced too")
	_, err = s.UpdatePoolerAdmission(ctx, fence("destTG", "f2"))
	require.NoError(t, err)

	_, err = route("destTG", "elsewhereTG")
	require.Equal(t, codes.FailedPrecondition, code(err), "from must be the current application tablegroup")

	first, err := route("migrateTG", "destTG")
	require.NoError(t, err)
	retry, err := route("migrateTG", "destTG")
	require.NoError(t, err)
	require.Equal(t, first.RoutingVersion, retry.RoutingVersion, "a retry after the move must not bump the version")

	_, err = route("migrateTG", "migrateTG")
	require.Equal(t, codes.InvalidArgument, code(err))
}

func TestGetServingStateReadsRoutingOnlyWhenNoTablegroupsRequested(t *testing.T) {
	s := newFake()
	resp, err := s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db"})
	require.NoError(t, err)
	require.Empty(t, resp.Tablegroups)
	require.Equal(t, "migrateTG", resp.AppTablegroup)

	_, err = s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db", Tablegroups: []string{"nope"}})
	require.Equal(t, codes.NotFound, code(err))
	empty, err := s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "other"})
	require.NoError(t, err)
	require.Empty(t, empty.AppTablegroup)
	require.Zero(t, empty.RoutingVersion)
	_, err = s.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "other", Tablegroups: []string{"x"}})
	require.Equal(t, codes.NotFound, code(err))
}

func TestUpdatePoolerAdmissionValidatesRequests(t *testing.T) {
	s := newFake()
	for name, req := range map[string]*multipoolerservicepb.UpdatePoolerAdmissionRequest{
		"transitional target": {Database: "db", Tablegroup: "migrateTG", Target: fencing, RequestId: "x"},
		"no target":           {Database: "db", Tablegroup: "migrateTG", RequestId: "x"},
		"no request id":       {Database: "db", Tablegroup: "migrateTG", Target: fenced},
	} {
		_, err := s.UpdatePoolerAdmission(t.Context(), req)
		require.Equal(t, codes.InvalidArgument, code(err), name)
	}
	_, err := s.UpdatePoolerAdmission(t.Context(), fence("nope", "x"))
	require.Equal(t, codes.NotFound, code(err))
}

// TestOverGRPC checks the fake serves the real generated service, which is what
// a controller under development dials.
func TestOverGRPC(t *testing.T) {
	s := newFake()
	addr := Start(t, s)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := multipoolerservicepb.NewMultipoolerServiceClient(conn)

	_, err = client.UpdatePoolerAdmission(t.Context(), fence("migrateTG", "f1"))
	require.NoError(t, err)
	s.FailRefresh("dest-2", true)
	_, err = client.UpdatePoolerAdmission(t.Context(), fence("destTG", "f2"))
	require.Equal(t, codes.Aborted, code(err))
	resp, err := client.GetServingState(t.Context(), &multipoolerservicepb.GetServingStateRequest{Database: "db", Tablegroups: []string{"migrateTG", "destTG"}})
	require.NoError(t, err)
	require.Equal(t, fenced, resp.Tablegroups[0].AdmissionState)
	require.Equal(t, fencing, resp.Tablegroups[1].AdmissionState)
}
