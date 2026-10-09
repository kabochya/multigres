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

// TestCoordinatorCutoverAndRollback drives the serving-control RPCs of the
// default primary against real unmanaged poolers: fence both sides, move the
// routing pointer, unfence the destination, then roll back. It checks each step
// both in the persisted state and in what the poolers actually admit.
func TestCoordinatorCutoverAndRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	ctl := newControlClient(t, s.PrimaryMultipooler(t).GrpcPort)

	srcPort, dstPort := startExternalPostgres(t), startExternalPostgres(t)
	seedBackingConnection(t, s, "w5-src", externalDSN(srcPort), externalSystemIdentifier(t, srcPort))
	seedBackingConnection(t, s, "w5-dst", externalDSN(dstPort), externalSystemIdentifier(t, dstPort))
	setServingRow(t, s, "migrateTG", "w5-src", "UNFENCED", "r-src")
	setServingRow(t, s, "destTG", "w5-dst", "FENCED", "r-dst")
	seedRouting(t, s, "migrateTG")

	src1 := startUnmanagedPooler(t, s, "w5-src-1", "postgres", "w5-src")
	src2 := startUnmanagedPooler(t, s, "w5-src-2", "postgres", "w5-src")
	dst1 := startUnmanagedPooler(t, s, "w5-dst-1", "postgres", "w5-dst", "--table-group=destTG")
	for _, p := range []*unmanagedPooler{src1, src2, dst1} {
		waitBackendReady(t, p)
	}
	requireAdmits(t, src1.grpcPort, "the source must admit while UNFENCED")
	requireAdmits(t, src2.grpcPort, "the source must admit while UNFENCED")
	requireStaysClosed(t, dst1.grpcPort, 3*time.Second, "the destination starts FENCED")

	st := ctl.state(ctx, "migrateTG", "destTG")
	require.Equal(t, "migrateTG", st.AppTablegroup)
	require.EqualValues(t, 1, st.RoutingVersion)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Tablegroups[0].AdmissionState)
	require.Equal(t, "w5-src", st.Tablegroups[0].BackingConnection)

	// Cutover: fence the destination, fence the source, route, unfence the
	// destination.
	resp, err := ctl.fence(ctx, "destTG", "fence-dst-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 1)
	resp, err = ctl.fence(ctx, "migrateTG", "fence-src-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 2, "both source poolers acknowledged")
	require.False(t, admits(t, src1.grpcPort))
	require.False(t, admits(t, src2.grpcPort))
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, ctl.state(ctx, "migrateTG").Tablegroups[0].AdmissionState)

	moved, err := ctl.route(ctx, "migrateTG", "destTG", "route-1")
	require.NoError(t, err)
	require.Equal(t, "destTG", moved.AppTablegroup)
	require.EqualValues(t, 2, moved.RoutingVersion)
	retry, err := ctl.route(ctx, "migrateTG", "destTG", "route-1")
	require.NoError(t, err)
	require.EqualValues(t, 2, retry.RoutingVersion, "a retry does not bump the version")
	require.False(t, admits(t, dst1.grpcPort), "routing never unfences anything")

	// A delayed unfence of the source, naming a fence that is not the current
	// one, is refused and changes nothing.
	_, err = ctl.unfence(ctx, "migrateTG", "late-unfence", "some-older-fence")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.False(t, admits(t, src1.grpcPort))

	_, err = ctl.unfence(ctx, "destTG", "unfence-dst-1", "fence-dst-1")
	require.NoError(t, err)
	requireAdmits(t, dst1.grpcPort, "the destination must admit after its unfence")
	require.False(t, admits(t, src1.grpcPort), "the source stays closed")

	// Rollback: fence the destination, route back, unfence the source.
	_, err = ctl.fence(ctx, "destTG", "fence-dst-2")
	require.NoError(t, err)
	require.False(t, admits(t, dst1.grpcPort))
	moved, err = ctl.route(ctx, "destTG", "migrateTG", "route-2")
	require.NoError(t, err)
	require.Equal(t, "migrateTG", moved.AppTablegroup)
	require.EqualValues(t, 3, moved.RoutingVersion)
	_, err = ctl.unfence(ctx, "migrateTG", "unfence-src-1", "fence-src-1")
	require.NoError(t, err)
	requireAdmits(t, src1.grpcPort, "the source must admit after rollback")
	requireAdmits(t, src2.grpcPort, "the source must admit after rollback")

	// Control RPCs are served by the default primary only.
	_, err = newControlClient(t, src1.grpcPort).fence(ctx, "migrateTG", "x")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
}
