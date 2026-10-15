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

package shardsetup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// controlClient talks to the default primary's serving-control RPCs.
type controlClient struct {
	multipoolerservicepb.MultipoolerServiceClient
	conn *grpc.ClientConn
}

func newControlClient(t *testing.T, grpcPort int) *controlClient {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &controlClient{MultipoolerServiceClient: multipoolerservicepb.NewMultipoolerServiceClient(conn), conn: conn}
}

func (c *controlClient) fence(ctx context.Context, tg, requestID string) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	return c.fenceWithin(ctx, tg, requestID, 40*time.Second)
}

// fenceWithin is fence with the coordinator's own deadline for the call.
func (c *controlClient) fenceWithin(ctx context.Context, tg, requestID string, timeout time.Duration) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	return c.UpdatePoolerAdmission(ctx, &multipoolerservicepb.UpdatePoolerAdmissionRequest{
		Database: "postgres", Tablegroup: tg, Target: stateOf("FENCED"), RequestId: requestID, Timeout: durationpb.New(timeout),
	})
}

func (c *controlClient) unfence(ctx context.Context, tg, requestID, expected string) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	return c.UpdatePoolerAdmission(ctx, &multipoolerservicepb.UpdatePoolerAdmissionRequest{
		Database: "postgres", Tablegroup: tg, Target: stateOf("UNFENCED"), RequestId: requestID, ExpectedRequestId: expected, Timeout: durationpb.New(40 * time.Second),
	})
}

func (c *controlClient) route(ctx context.Context, from, to, requestID string) (*multipoolerservicepb.UpdateMigrationRoutingResponse, error) {
	return c.UpdateMigrationRouting(ctx, &multipoolerservicepb.UpdateMigrationRoutingRequest{
		Database: "postgres", FromTablegroup: from, ToTablegroup: to, RequestId: requestID,
	})
}

func (c *controlClient) state(ctx context.Context, tgs ...string) *multipoolerservicepb.GetServingStateResponse {
	resp, err := c.GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: "postgres", Tablegroups: tgs})
	if err != nil {
		panic(err)
	}
	return resp
}

// tryState is state that reports a failure instead of panicking, for polling
// across a failover.
func (c *controlClient) tryState(ctx context.Context, tgs ...string) (*multipoolerservicepb.GetServingStateResponse, error) {
	return c.GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: "postgres", Tablegroups: tgs})
}

// seedRouting sets the routing pointer, as the prototype's setup does.
func seedRouting(t *testing.T, s *ShardSetup, appTablegroup string) {
	t.Helper()
	ctx := t.Context()
	conn := s.ConnectPrimaryAdmin(t)
	_, err := conn.Exec(ctx, `INSERT INTO multigres.proto_routing (database, app_tablegroup, version) VALUES ($1, $2, 1)
ON CONFLICT (database) DO UPDATE SET app_tablegroup = EXCLUDED.app_tablegroup, version = 1`, s.Database, appTablegroup)
	require.NoError(t, err)
}

// startControlledDefaultCohort starts the default cohort with application admission
// control, as the destination of a migration: its poolers start closed and open once
// the default tablegroup's admission row, seeded here as UNFENCED, says so.
func startControlledDefaultCohort(t *testing.T, opts ...SetupOption) *ShardSetup {
	t.Helper()
	// Three poolers: failing over needs a majority of the outgoing cohort.
	all := append([]SetupOption{
		WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"),
		WithMultipoolerExtraArgs("--admission-control", "--admission-drain-timeout=3s"),
	}, opts...)
	def, cleanup := NewIsolated(t, all...)
	t.Cleanup(cleanup)
	setServingRow(t, def, "default", "", "UNFENCED", "r-def0")
	requireAdmits(t, def.GetPrimary(t).Multipooler.GrpcPort, "the default cohort must admit once its row says UNFENCED")
	return def
}

// TestCoordinatorCutoverAndRollback drives the serving-control RPCs of the
// default primary against a real unmanaged pooler and the default cohort as the
// destination: fence both sides, move the routing pointer, unfence the destination,
// then roll back. It checks each step both in the persisted state and in what the
// poolers actually admit, and that the fenced default primary keeps serving the
// control plane throughout.
func TestCoordinatorCutoverAndRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	def := startControlledDefaultCohort(t)
	primary := def.GetPrimary(t).Multipooler.GrpcPort
	ctl := newControlClient(t, primary)

	srcPort := startExternalPostgres(t)
	seedBackingConnection(t, def, "coordinator-src", externalDSN(srcPort), externalSystemIdentifier(t, srcPort))
	setServingRow(t, def, "migrateTG", "coordinator-src", "UNFENCED", "r-src")
	seedRouting(t, def, "migrateTG")
	src := startUnmanagedPooler(t, def, "coordinator-src-1", "postgres", "coordinator-src")
	waitBackendReady(t, src)
	requireAdmits(t, src.grpcPort, "the source must admit while UNFENCED")
	admitsDefault := func() bool {
		for _, inst := range def.Multipoolers {
			if admits(t, inst.Multipooler.GrpcPort) {
				return true
			}
		}
		return false
	}

	st := ctl.state(ctx, "migrateTG", "default")
	require.Equal(t, "migrateTG", st.AppTablegroup)
	require.EqualValues(t, 1, st.RoutingVersion)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Tablegroups[0].AdmissionState)
	require.Equal(t, "coordinator-src", st.Tablegroups[0].BackingConnection)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Tablegroups[1].AdmissionState)

	// Cutover: fence the destination, fence the source, route, unfence the
	// destination.
	resp, err := ctl.fence(ctx, "default", "fence-def-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 3, "every pooler of the default cohort acknowledged, the default primary among them")
	require.False(t, admitsDefault(), "the fenced default cohort refuses application work")
	requireRejectedAsAdmissionClosed(t, primary)
	// The fenced default primary still answers the control plane.
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, ctl.state(ctx, "default").Tablegroups[0].AdmissionState)

	resp, err = ctl.fence(ctx, "migrateTG", "fence-src-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 1)
	require.False(t, admits(t, src.grpcPort))
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, ctl.state(ctx, "migrateTG").Tablegroups[0].AdmissionState)

	moved, err := ctl.route(ctx, "migrateTG", "default", "route-1")
	require.NoError(t, err)
	require.Equal(t, "default", moved.AppTablegroup)
	require.EqualValues(t, 2, moved.RoutingVersion)
	retry, err := ctl.route(ctx, "migrateTG", "default", "route-1")
	require.NoError(t, err)
	require.EqualValues(t, 2, retry.RoutingVersion, "a retry does not bump the version")
	require.False(t, admitsDefault(), "routing never unfences anything")

	// A delayed unfence of the source, naming a fence that is not the current
	// one, is refused and changes nothing.
	_, err = ctl.unfence(ctx, "migrateTG", "late-unfence", "some-older-fence")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.False(t, admits(t, src.grpcPort))

	_, err = ctl.unfence(ctx, "default", "unfence-def-1", "fence-def-1")
	require.NoError(t, err)
	requireAdmits(t, primary, "the destination must admit after its unfence")
	require.False(t, admits(t, src.grpcPort), "the source stays closed")

	// Rollback: fence the destination, route back, unfence the source.
	_, err = ctl.fence(ctx, "default", "fence-def-2")
	require.NoError(t, err)
	require.False(t, admitsDefault())
	moved, err = ctl.route(ctx, "default", "migrateTG", "route-2")
	require.NoError(t, err)
	require.Equal(t, "migrateTG", moved.AppTablegroup)
	require.EqualValues(t, 3, moved.RoutingVersion)
	_, err = ctl.unfence(ctx, "migrateTG", "unfence-src-1", "fence-src-1")
	require.NoError(t, err)
	requireAdmits(t, src.grpcPort, "the source must admit after rollback")
	require.False(t, admitsDefault(), "the default cohort stays closed after the rollback")

	// Control RPCs are served by the default primary only.
	_, err = newControlClient(t, src.grpcPort).fence(ctx, "migrateTG", "x")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	for name, inst := range def.Multipoolers {
		if inst.Multipooler.GrpcPort == primary {
			continue
		}
		_, err = newControlClient(t, inst.Multipooler.GrpcPort).fence(ctx, "migrateTG", "x")
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "a default replica (%s) must refuse: %v", name, err)
	}
}
