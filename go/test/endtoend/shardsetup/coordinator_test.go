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

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// controlClient talks to the default primary's serving-control RPCs. Its fence, unfence
// and flip read the version they are to name from the serving state, as a migrator does,
// and the ...At forms take it explicitly, for the scenarios about stale versions.
type controlClient struct {
	multipoolerservicepb.MultipoolerServiceClient
	conn *grpc.ClientConn
	t    *testing.T
	// drain is how long its fences wait for the members to be clean, asking CommitFence again
	// until then; zero is 3 seconds.
	drain time.Duration
}

func newControlClient(t *testing.T, grpcPort int) *controlClient {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &controlClient{MultipoolerServiceClient: multipoolerservicepb.NewMultipoolerServiceClient(conn), conn: conn, t: t}
}

// version is the version of a tablegroup's row.
func (c *controlClient) version(ctx context.Context, tg string) int64 {
	c.t.Helper()
	return c.state(ctx, tg).Tablegroups[0].Version
}

// fence fences a tablegroup at the version it reads now.
func (c *controlClient) fence(ctx context.Context, tg string) (*multipoolerservicepb.CommitFenceResponse, error) {
	c.t.Helper()
	return c.fenceWithin(ctx, tg, 40*time.Second)
}

// fenceWithin is fence with a deadline for the whole of it. The start names the version of the row
// as it was read, whatever its state: a fence that was left is started again, and one that is
// complete succeeds.
func (c *controlClient) fenceWithin(ctx context.Context, tg string, timeout time.Duration) (*multipoolerservicepb.CommitFenceResponse, error) {
	c.t.Helper()
	return c.fenceAt(ctx, tg, c.version(ctx, tg), timeout)
}

// fenceAt fences a tablegroup from the version the caller names: it starts the fence, and asks
// CommitFence again until it succeeds or the controller's wait (drain) has passed.
func (c *controlClient) fenceAt(ctx context.Context, tg string, version int64, timeout time.Duration) (*multipoolerservicepb.CommitFenceResponse, error) {
	return c.fenceWithDrain(ctx, tg, version, timeout, c.drain)
}

// fenceWithDrain is fenceAt with the time to wait for the members to be clean; zero is 3 seconds.
// What it returns is the commit's answer, or the start's error if the start did not complete.
func (c *controlClient) fenceWithDrain(ctx context.Context, tg string, version int64, timeout, wait time.Duration) (*multipoolerservicepb.CommitFenceResponse, error) {
	if wait <= 0 {
		wait = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	started, err := c.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "postgres", Tablegroup: tg, ExpectedVersion: version})
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		resp, err := c.CommitFence(ctx, &multipoolerservicepb.CommitFenceRequest{Database: "postgres", Tablegroup: tg, Version: started.State.Version})
		if err == nil || status.Code(err) != codes.Aborted || time.Now().After(deadline) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// memberResults returns the per-member results of a fence call that did not complete, from the
// details of its error.
func memberResults(t *testing.T, err error) map[string]*multipoolerservicepb.MemberFenceResult {
	t.Helper()
	require.Equal(t, codes.Aborted, status.Code(err), "%v", err)
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
			out[m.GetPoolerId().GetName()] = m
		}
	}
	require.NotEmpty(t, out, "the per-member results are the details of the error: %v", err)
	return out
}

// unfenceAt unfences a tablegroup at the version the caller names.
func (c *controlClient) unfenceAt(ctx context.Context, tg string, version int64) (*multipoolerservicepb.UnfenceTablegroupResponse, error) {
	return c.UnfenceTablegroup(ctx, &multipoolerservicepb.UnfenceTablegroupRequest{Database: "postgres", Tablegroup: tg, ExpectedVersion: version})
}

// flip moves the routing pointer at the routing version it reads now.
func (c *controlClient) flip(ctx context.Context, from, to string) (*multipoolerservicepb.FlipRoutingResponse, error) {
	c.t.Helper()
	return c.flipAt(ctx, from, to, c.state(ctx).RoutingVersion)
}

// flipAt moves the routing pointer at the routing version the caller names.
func (c *controlClient) flipAt(ctx context.Context, from, to string, version int64) (*multipoolerservicepb.FlipRoutingResponse, error) {
	return c.FlipRouting(ctx, &multipoolerservicepb.FlipRoutingRequest{
		Database: "postgres", FromTablegroup: from, ToTablegroup: to, ExpectedRoutingVersion: version,
	})
}

// state reads the serving state and fails the test when it cannot; it must be called from the
// test's own goroutine.
func (c *controlClient) state(ctx context.Context, tgs ...string) *multipoolerservicepb.GetServingStateResponse {
	c.t.Helper()
	resp, err := c.GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: "postgres", Tablegroups: tgs})
	require.NoError(c.t, err, "reading the serving state")
	return resp
}

// tryState is state that reports a failure instead of failing the test, for polling
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
	acquireFixtureSlot(t)
	// Three poolers unless the caller says otherwise: failing over needs a majority of
	// the outgoing cohort.
	all := append([]SetupOption{
		WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"),
	}, opts...)
	def, cleanup := NewIsolated(t, all...)
	t.Cleanup(cleanup)
	// No row for the default tablegroup is written: none reads as unfenced.
	requireAdmits(t, def.GetPrimary(t).Multipooler.GrpcPort, "the default cohort must admit with no admission state written")
	return def
}

// scenarioCoordinatorCallsAreSafeToRepeat: a migrator that does not know whether a call took
// effect, because its deadline passed or the reply was lost, repeats it and ends in the state
// it would have ended in. Fence and unfence recognize the row their own success leaves; a
// delayed retry of a fence that was undone is still rejected; a flip that already committed,
// asked again, succeeds and writes nothing, and a flip from a state that is no longer current
// is refused.
func scenarioCoordinatorCallsAreSafeToRepeat(t *testing.T, f *migrationFixture) {
	ctx := t.Context()
	ctl := f.control()

	// Fence, then the same request again, twice.
	v := ctl.version(ctx, "migrateTG")
	first, err := ctl.fenceAt(ctx, "migrateTG", v, 30*time.Second)
	require.NoError(t, err)
	for i := range 2 {
		again, err := ctl.fenceAt(ctx, "migrateTG", v, 30*time.Second)
		require.NoError(t, err, "repeat %d", i)
		require.Equal(t, first.State.Version, again.State.Version)
		require.Empty(t, again.Members, "a fence that is complete is answered without asking the members")
	}
	require.EqualValues(t, v+1, ctl.version(ctx, "migrateTG"), "nothing was written by the repeats")

	// Unfence, then the same unfence again, twice.
	_, err = ctl.unfenceAt(ctx, "migrateTG", first.State.Version)
	require.NoError(t, err)
	for i := range 2 {
		_, err := ctl.unfenceAt(ctx, "migrateTG", first.State.Version)
		require.NoError(t, err, "repeat %d", i)
	}
	require.EqualValues(t, v+2, ctl.version(ctx, "migrateTG"))
	requireAdmits(t, f.src.grpcPort, "the repeated unfence leaves the source open")

	// The delayed retry of the first fence cannot fence again now that it was undone.
	_, err = ctl.fenceAt(ctx, "migrateTG", v, 30*time.Second)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Equal(t, admUnfenced, ctl.state(ctx, "migrateTG").Tablegroups[0].State)

	// A flip that committed, asked again, succeeds and writes nothing.
	_, err = ctl.fence(ctx, "migrateTG")
	require.NoError(t, err)
	_, err = ctl.fence(ctx, "default")
	require.NoError(t, err)
	routingVersion := ctl.state(ctx).RoutingVersion
	moved, err := ctl.flipAt(ctx, "migrateTG", "default", routingVersion)
	require.NoError(t, err)
	require.Equal(t, "default", moved.AppTablegroup)
	for i := range 2 {
		again, err := ctl.flipAt(ctx, "migrateTG", "default", routingVersion)
		require.NoError(t, err, "repeat %d", i)
		require.Equal(t, moved.RoutingVersion, again.RoutingVersion)
	}
	require.Equal(t, moved.RoutingVersion, ctl.state(ctx).RoutingVersion, "nothing was written by the repeats")

	// A flip that names a version the routing held before the move, and no longer holds, is
	// refused: the move back is not the move that was asked for.
	_, err = ctl.flipAt(ctx, "default", "migrateTG", routingVersion)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	// What is left (traffic on the default tablegroup, both sides fenced) is what the next
	// reset undoes.
}
