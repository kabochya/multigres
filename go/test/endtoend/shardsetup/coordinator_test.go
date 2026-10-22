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

	"github.com/multigres/multigres/go/common/protometadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// controlClient talks to the default primary's admission and routing RPCs, for one operation.
// Its fence, unfence and switch read the version they are to name from the routing state, as a
// migrator does, and the ...At forms take it explicitly, for the scenarios about stale versions.
type controlClient struct {
	multipoolerservicepb.MultipoolerServiceClient
	conn *grpc.ClientConn
	t    *testing.T
	// operation is the operation every call of the client belongs to.
	operation string
	// drain is how long its fences wait for the members to be clean, asking CommitFence again
	// until then; zero is 3 seconds.
	drain time.Duration
}

// newControlClientFor is a client of the given operation.
func newControlClientFor(t *testing.T, grpcPort int, operation string) *controlClient {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &controlClient{MultipoolerServiceClient: multipoolerservicepb.NewMultipoolerServiceClient(conn), conn: conn, t: t, operation: operation}
}

func scopeOf(tablegroup string) *multipoolerservicepb.AdmissionScope {
	return &multipoolerservicepb.AdmissionScope{Tablegroup: tablegroup}
}

// fenceOf is the fence of a tablegroup in a routing state, nil when it has none.
func fenceOf(resp *multipoolerservicepb.GetRoutingStateResponse, tg string) *multipoolerservicepb.AdmissionFence {
	for _, f := range resp.GetFences() {
		if f.GetScope().GetTablegroup() == tg {
			return f
		}
	}
	return nil
}

// stateOfFence is the state of a tablegroup's fence: unfenced when it has none.
func stateOfFence(resp *multipoolerservicepb.GetRoutingStateResponse, tg string) multipoolerservicepb.AdmissionState {
	if f := fenceOf(resp, tg); f != nil {
		return f.GetState()
	}
	return admUnfenced
}

// effectiveTablegroup is where the traffic goes: the home tablegroup, or the destination of the
// rule that applies to it.
func effectiveTablegroup(resp *multipoolerservicepb.GetRoutingStateResponse) string {
	for _, r := range resp.GetRules() {
		if r.GetRule().GetFromTablegroup() == resp.GetHomeTablegroup() {
			return r.GetRule().GetToTablegroup()
		}
	}
	return resp.GetHomeTablegroup()
}

// version is the version of a tablegroup's fence: zero when it has none.
func (c *controlClient) version(ctx context.Context, tg string) int64 {
	c.t.Helper()
	return fenceOf(c.state(ctx), tg).GetVersion()
}

// fence fences a tablegroup at the version it reads now.
func (c *controlClient) fence(ctx context.Context, tg string) (*multipoolerservicepb.CommitFenceResponse, error) {
	c.t.Helper()
	return c.fenceWithin(ctx, tg, 40*time.Second)
}

// fenceWithin is fence with a deadline for the whole of it. The start names the version of the fence
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
	started, err := c.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "postgres", OperationId: c.operation, Scope: scopeOf(tg), ExpectedVersion: version})
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		resp, err := c.CommitFence(ctx, &multipoolerservicepb.CommitFenceRequest{Database: "postgres", OperationId: c.operation, Scope: scopeOf(tg), Version: started.Fence.Version})
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
func (c *controlClient) unfenceAt(ctx context.Context, tg string, version int64) (*multipoolerservicepb.UnfenceScopeResponse, error) {
	return c.UnfenceScope(ctx, &multipoolerservicepb.UnfenceScopeRequest{Database: "postgres", OperationId: c.operation, Scope: scopeOf(tg), ExpectedVersion: version})
}

// switchRouting moves the traffic of from to to at the routing version it reads now.
func (c *controlClient) switchRouting(ctx context.Context, from, to string) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	c.t.Helper()
	return c.switchRoutingAt(ctx, from, to, c.state(ctx).RoutingVersion)
}

// switchRoutingAt moves the traffic at the routing version the caller names.
func (c *controlClient) switchRoutingAt(ctx context.Context, from, to string, version int64) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	return c.SwitchRouting(ctx, &multipoolerservicepb.SwitchRoutingRequest{
		Database: "postgres", OperationId: c.operation, ExpectedRoutingVersion: version,
		Rule: &multipoolerservicepb.RoutingRule{FromTablegroup: from, ToTablegroup: to, ToShard: "0-inf"},
	})
}

// revertRouting puts the traffic back on the home tablegroup at the routing version it reads now.
func (c *controlClient) revertRouting(ctx context.Context) (*multipoolerservicepb.RevertRoutingResponse, error) {
	c.t.Helper()
	return c.RevertRouting(ctx, &multipoolerservicepb.RevertRoutingRequest{Database: "postgres", OperationId: c.operation, ExpectedRoutingVersion: c.state(ctx).RoutingVersion})
}

// cleanup deletes the operation's fences.
func (c *controlClient) cleanup(ctx context.Context) (*multipoolerservicepb.CleanupOperationResponse, error) {
	return c.CleanupOperation(ctx, &multipoolerservicepb.CleanupOperationRequest{Database: "postgres", OperationId: c.operation})
}

// fenceRow is the fence of a tablegroup; a tablegroup with no fence is unfenced at version zero.
func (c *controlClient) fenceRow(ctx context.Context, tg string) *multipoolerservicepb.AdmissionFence {
	c.t.Helper()
	return fenceOrUnfenced(c.state(ctx), tg)
}

// fenceOrUnfenced is the fence of a tablegroup in a routing state; a tablegroup with no fence is
// unfenced at version zero.
func fenceOrUnfenced(resp *multipoolerservicepb.GetRoutingStateResponse, tg string) *multipoolerservicepb.AdmissionFence {
	if f := fenceOf(resp, tg); f != nil {
		return f
	}
	return &multipoolerservicepb.AdmissionFence{Scope: scopeOf(tg), State: admUnfenced}
}

// state reads the routing state and fails the test when it cannot; it must be called from the
// test's own goroutine.
func (c *controlClient) state(ctx context.Context) *multipoolerservicepb.GetRoutingStateResponse {
	c.t.Helper()
	resp, err := c.GetRoutingState(ctx, &multipoolerservicepb.GetRoutingStateRequest{Database: "postgres"})
	require.NoError(c.t, err, "reading the routing state")
	return resp
}

// tryState is state that reports a failure instead of failing the test, for polling
// across a failover.
func (c *controlClient) tryState(ctx context.Context) (*multipoolerservicepb.GetRoutingStateResponse, error) {
	return c.GetRoutingState(ctx, &multipoolerservicepb.GetRoutingStateRequest{Database: "postgres"})
}

// seedHome sets the home tablegroup, which receives the traffic when no rule applies, as the
// prototype's setup does, and creates the metadata tables for it.
func seedHome(t *testing.T, s *ShardSetup, home string) {
	t.Helper()
	ctx := t.Context()
	conn := s.ConnectPrimaryAdmin(t)
	require.NoError(t, protometadata.EnsureRoutingSchema(ctx, func(ctx context.Context, sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}))
	tag, err := conn.Exec(ctx, `UPDATE multigres.catalog_state
SET home_tablegroup = (SELECT oid FROM multigres.tablegroup WHERE name = $1), routing_version = 0`, home)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(), "the catalog state of the database, with %q in the catalog", home)
}

// startControlledDefaultCohort starts the default cohort with application admission
// control, as the destination of a migration: its poolers start closed and open once
// they read that the default tablegroup has no fence.
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
	// No fence for the default tablegroup is written: none reads as unfenced.
	requireAdmits(t, def.GetPrimary(t).Multipooler.GrpcPort, "the default cohort must admit with no admission state written")
	return def
}

// scenarioCoordinatorCallsAreSafeToRepeat: a migrator that does not know whether a call took
// effect, because its deadline passed or the reply was lost, repeats it and ends in the state
// it would have ended in. Fence and unfence recognize the fence their own success leaves; a
// delayed retry of a fence that was undone is still rejected; a switch that already committed,
// asked again, succeeds and writes nothing, and a switch from a state that is no longer current
// is refused; so are a revert and a fold.
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
		require.Equal(t, first.Fence.Version, again.Fence.Version)
		require.Empty(t, again.Members, "a fence that is complete is answered without asking the members")
	}
	require.EqualValues(t, v+1, ctl.version(ctx, "migrateTG"), "nothing was written by the repeats")

	// Unfence, then the same unfence again, twice.
	_, err = ctl.unfenceAt(ctx, "migrateTG", first.Fence.Version)
	require.NoError(t, err)
	for i := range 2 {
		_, err := ctl.unfenceAt(ctx, "migrateTG", first.Fence.Version)
		require.NoError(t, err, "repeat %d", i)
	}
	require.EqualValues(t, v+2, ctl.version(ctx, "migrateTG"))
	requireAdmits(t, f.src.grpcPort, "the repeated unfence leaves the source open")

	// The delayed retry of the first fence cannot fence again now that it was undone.
	_, err = ctl.fenceAt(ctx, "migrateTG", v, 30*time.Second)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Equal(t, admUnfenced, stateOfFence(ctl.state(ctx), "migrateTG"))
	require.NotNil(t, fenceOf(ctl.state(ctx), "migrateTG"), "an unfence leaves the fence, UNFENCED")

	// A switch that committed, asked again, succeeds and writes nothing.
	_, err = ctl.fence(ctx, "migrateTG")
	require.NoError(t, err)
	_, err = ctl.fence(ctx, "default")
	require.NoError(t, err)
	routingVersion := ctl.state(ctx).RoutingVersion
	moved, err := ctl.switchRoutingAt(ctx, "migrateTG", "default", routingVersion)
	require.NoError(t, err)
	require.Equal(t, "default", moved.EffectiveTablegroup)
	for i := range 2 {
		again, err := ctl.switchRoutingAt(ctx, "migrateTG", "default", routingVersion)
		require.NoError(t, err, "repeat %d", i)
		require.Equal(t, moved.RoutingVersion, again.RoutingVersion)
	}
	require.Equal(t, moved.RoutingVersion, ctl.state(ctx).RoutingVersion, "nothing was written by the repeats")

	// A switch that names a version the routing held before the move, and no longer holds, is
	// refused: another switch is not the move that was asked for.
	_, err = ctl.switchRoutingAt(ctx, "default", "migrateTG", routingVersion)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)

	// A revert and a fold that committed, asked again, succeed and write nothing.
	reverted, err := ctl.RevertRouting(ctx, &multipoolerservicepb.RevertRoutingRequest{Database: "postgres", OperationId: ctl.operation, ExpectedRoutingVersion: moved.RoutingVersion})
	require.NoError(t, err)
	again, err := ctl.RevertRouting(ctx, &multipoolerservicepb.RevertRoutingRequest{Database: "postgres", OperationId: ctl.operation, ExpectedRoutingVersion: moved.RoutingVersion})
	require.NoError(t, err, "a retry of the revert that committed")
	require.Equal(t, reverted.RoutingVersion, again.RoutingVersion)
	require.Equal(t, "migrateTG", effectiveTablegroup(ctl.state(ctx)))
	// What is left (both sides fenced, no rule) is what the next reset undoes.
}
