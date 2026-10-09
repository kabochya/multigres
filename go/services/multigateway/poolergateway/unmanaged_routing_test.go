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

package poolergateway

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

const unmanagedMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED

func unmanagedPooler(name, cell, tableGroup string) *clustermetadatapb.Multipooler {
	p := createTestMultipooler(name, cell, tableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	p.ManagementMode = unmanagedMode
	return p
}

// reportHealth reports a live health snapshot for conn.
func reportHealth(conn *poolerConnection, status clustermetadatapb.PoolerServingStatus, admissionClosed bool) {
	conn.processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId:        conn.PoolerInfo().Id,
		ServingStatus:   status,
		RoutingState:    &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
		AdmissionClosed: admissionClosed,
	})
}

func writableTarget(tableGroup string) *query.Target {
	return protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
}

func TestUnmanagedSelectionRequiresServingAndAnOpenGate(t *testing.T) {
	lb := newTestLB(t, "zone1")
	open := unmanagedPooler("open", "zone1", "migrateTG")
	closed := unmanagedPooler("closed", "zone1", "migrateTG")
	down := unmanagedPooler("down", "zone1", "migrateTG")
	for _, p := range []*clustermetadatapb.Multipooler{open, closed, down} {
		addPoolerForTest(t, lb, p)
	}
	reportHealth(connForTest(t, lb, closed), clustermetadatapb.PoolerServingStatus_SERVING, true)
	reportHealth(connForTest(t, lb, down), clustermetadatapb.PoolerServingStatus_DISABLED, false)

	// Only a serving pooler with an open gate is eligible, whatever the mode.
	reportHealth(connForTest(t, lb, open), clustermetadatapb.PoolerServingStatus_SERVING, false)
	for _, mode := range []query.Mode{query.Mode_MODE_WRITABLE, query.Mode_MODE_CONSISTENT, query.Mode_MODE_INCONSISTENT} {
		conn, err := lb.getConnection(protoutil.NewTarget(constants.DefaultPostgresDatabase, "migrateTG", constants.DefaultShard, mode))
		require.NoError(t, err, mode.String())
		assert.Equal(t, poolerID(open), conn.ID(), mode.String())
	}

	// With every gate closed the error is the bufferable no-primary one.
	reportHealth(connForTest(t, lb, open), clustermetadatapb.PoolerServingStatus_SERVING, true)
	_, err := lb.getConnection(writableTarget("migrateTG"))
	require.Error(t, err)
	assert.True(t, isNoWritablePrimaryError(err), "a fenced unmanaged shard must buffer, not fail: %v", err)
}

func TestUnmanagedSelectionPrefersTheLocalCell(t *testing.T) {
	lb := newTestLB(t, "zone1")
	local := unmanagedPooler("local", "zone1", "migrateTG")
	remote := unmanagedPooler("remote", "zone2", "migrateTG")
	addPoolerForTest(t, lb, local)
	addPoolerForTest(t, lb, remote)
	reportHealth(connForTest(t, lb, local), clustermetadatapb.PoolerServingStatus_SERVING, false)
	reportHealth(connForTest(t, lb, remote), clustermetadatapb.PoolerServingStatus_SERVING, false)
	for range 20 {
		conn, err := lb.getConnection(writableTarget("migrateTG"))
		require.NoError(t, err)
		require.Equal(t, poolerID(local), conn.ID())
	}
}

func TestUnmanagedShardNeverBecomesALeaderAndOtherShardsAreUnaffected(t *testing.T) {
	lb := newTestLB(t, "zone1")
	ext := unmanagedPooler("ext", "zone1", "migrateTG")
	addPoolerForTest(t, lb, ext)
	reportHealth(connForTest(t, lb, ext), clustermetadatapb.PoolerServingStatus_SERVING, false)

	managed := createTestMultipooler("managed", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, managed)
	simulateHealthUpdate(connForTest(t, lb, managed), clustermetadatapb.PoolerServingStatus_SERVING,
		managed.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})

	conn, err := lb.getConnection(writableTarget(constants.DefaultTableGroup))
	require.NoError(t, err)
	assert.Equal(t, poolerID(managed), conn.ID(), "the default cohort still routes to its leader")
	assert.False(t, lb.claimsPrimary(connForTest(t, lb, ext)))
}

func TestManagedLeaderWithClosedGateBuffersInsteadOfBouncing(t *testing.T) {
	lb := newTestLB(t, "zone1")
	leader := createTestMultipooler("dest-1", "zone1", "destTG", constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)
	simulateHealthUpdate(connForTest(t, lb, leader), clustermetadatapb.PoolerServingStatus_SERVING,
		leader.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})
	conn, err := lb.getConnection(writableTarget("destTG"))
	require.NoError(t, err)
	require.Equal(t, poolerID(leader), conn.ID())

	// A fenced managed leader is held, not tried.
	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, AdmissionClosed: true,
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
	})
	_, err = lb.getConnection(writableTarget("destTG"))
	require.Error(t, err)
	assert.True(t, isNoWritablePrimaryError(err))
}

func TestUnmanagedPoolerAdmittingAgainDrainsTheBuffer(t *testing.T) {
	var drained atomic.Int32
	var key atomic.Pointer[clustermetadatapb.ShardKey]
	lb := newTestLBWithLeaderServing(t, "zone1", func(sk *clustermetadatapb.ShardKey) {
		drained.Add(1)
		key.Store(sk)
	})
	ext := unmanagedPooler("ext", "zone1", "migrateTG")
	addPoolerForTest(t, lb, ext)

	reportHealth(connForTest(t, lb, ext), clustermetadatapb.PoolerServingStatus_SERVING, true)
	require.Zero(t, drained.Load(), "a closed gate does not release requests")
	reportHealth(connForTest(t, lb, ext), clustermetadatapb.PoolerServingStatus_DISABLED, false)
	require.Zero(t, drained.Load(), "neither does a pooler that is not serving")

	reportHealth(connForTest(t, lb, ext), clustermetadatapb.PoolerServingStatus_SERVING, false)
	require.EqualValues(t, 1, drained.Load())
	assert.Equal(t, "migrateTG", key.Load().GetTableGroup())
}

func TestManagedLeaderWithClosedGateDoesNotDrainTheBuffer(t *testing.T) {
	var drained atomic.Int32
	lb := newTestLBWithLeaderServing(t, "zone1", func(*clustermetadatapb.ShardKey) { drained.Add(1) })
	leader := createTestMultipooler("dest-1", "zone1", "destTG", constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)
	rs := &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}}
	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, AdmissionClosed: true, RoutingState: rs,
	})
	require.Zero(t, drained.Load())
	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, AdmissionClosed: false, RoutingState: rs,
	})
	require.Positive(t, drained.Load())
}

func TestAppRoutingRewriteAndVersions(t *testing.T) {
	r := NewAppRouting("postgres")

	// Unknown: application targets fail closed with a bufferable error.
	pending := writableTarget(PendingAppTableGroup)
	err := r.Rewrite(pending)
	require.Error(t, err)
	assert.True(t, isNoWritablePrimaryError(err))
	// Credential lookups wait for routing too, rather than asking the default cohort.
	assert.Equal(t, PendingAppTableGroup, r.AuthTableGroup())
	var nilRouting *AppRouting
	assert.Equal(t, constants.DefaultTableGroup, nilRouting.AuthTableGroup(), "without application routing it is the default")

	require.True(t, r.Update("migrateTG", 1))
	tg, known := r.Current()
	require.True(t, known)
	require.Equal(t, "migrateTG", tg)

	// The pending placeholder and the current group resolve to the current one.
	require.NoError(t, r.Rewrite(pending))
	assert.Equal(t, "migrateTG", pending.GetShardKey().GetTableGroup())
	assert.Equal(t, "migrateTG", r.AuthTableGroup())

	// Other tablegroups are not application targets.
	meta := writableTarget(constants.DefaultTableGroup)
	require.NoError(t, r.Rewrite(meta))
	assert.Equal(t, constants.DefaultTableGroup, meta.GetShardKey().GetTableGroup())

	// A same-version or older read changes nothing.
	assert.False(t, r.Update("destTG", 1))
	assert.False(t, r.Update("destTG", 0))
	tg, _ = r.Current()
	assert.Equal(t, "migrateTG", tg)

	// A newer one moves traffic, and a request planned for the old tablegroup is
	// re-resolved to the new one.
	stale := writableTarget("migrateTG")
	require.True(t, r.Update("destTG", 2))
	require.NoError(t, r.Rewrite(stale))
	assert.Equal(t, "destTG", stale.GetShardKey().GetTableGroup())

	// A nil AppRouting is a no-op, so ordinary gateways are unaffected.
	var none *AppRouting
	other := writableTarget("migrateTG")
	require.NoError(t, none.Rewrite(other))
	assert.Equal(t, "migrateTG", other.GetShardKey().GetTableGroup())
	assert.Equal(t, constants.DefaultTableGroup, none.AuthTableGroup())
}

func TestAppRoutingCallbacksRunInOrderOnChangeOnly(t *testing.T) {
	r := NewAppRouting("postgres")
	var got []string
	r.OnChange(func(old, new string) { got = append(got, "a:"+old+">"+new) })
	r.OnChange(func(old, new string) { got = append(got, "b:"+old+">"+new) })
	r.Update("migrateTG", 1)
	r.Update("migrateTG", 2) // a newer version of the same pointer is not a change
	r.Update("destTG", 3)
	assert.Equal(t, []string{"a:>migrateTG", "b:>migrateTG", "a:migrateTG>destTG", "b:migrateTG>destTG"}, got)
}

// TestRoutingFlipReleasesBufferedRequestsToTheNewTablegroup is the cutover from a
// gateway's point of view: a request planned for the source finds it fenced and
// waits; when routing moves, it is released and retried against the destination,
// which now owns the traffic.
func TestRoutingFlipReleasesBufferedRequestsToTheNewTablegroup(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}

	src := unmanagedPooler("src", "zone1", "migrateTG")
	dst := unmanagedPooler("dst", "zone1", "destTG")
	addPoolerForTest(t, lb, src)
	addPoolerForTest(t, lb, dst)
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING, true) // fenced
	reportHealth(connForTest(t, lb, dst), clustermetadatapb.PoolerServingStatus_SERVING, true) // not yet unfenced

	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)
	routing.Update("migrateTG", 1)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	target := writableTarget("migrateTG") // planned before the cutover
	// The buffer is keyed by the shard key the request waited under; keep our own
	// copy, since the request's target is rewritten when routing moves.
	bufferKey := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: "migrateTG", Shard: constants.DefaultShard}
	selected := make(chan *poolerConnection, 1)
	done := make(chan error, 1)
	go func() {
		done <- pg.withBuffering(ctx, target, true, false, func(conn *poolerConnection) error {
			selected <- conn
			return nil
		})
	}()

	// It is held, not served and not failed.
	require.Eventually(t, func() bool {
		probe, stop := context.WithCancel(ctx)
		stop()
		_, err := failoverBuffer.WaitIfAlreadyBuffering(probe, bufferKey)
		return errors.Is(err, context.Canceled)
	}, 5*time.Second, time.Millisecond, "the request should be buffering")
	select {
	case err := <-done:
		t.Fatalf("request returned while the source was fenced: %v", err)
	default:
	}

	// The destination admits, and routing moves to it.
	reportHealth(connForTest(t, lb, dst), clustermetadatapb.PoolerServingStatus_SERVING, false)
	require.True(t, routing.Update("destTG", 2))

	require.NoError(t, <-done)
	got := <-selected
	assert.Equal(t, poolerID(dst), got.ID(), "the held request ends up on the destination")
	assert.Equal(t, "destTG", target.GetShardKey().GetTableGroup(), "and its target was re-resolved")
}

// TestRoutingFlipBeforeTheDestinationOpensHoldsTheRequestAgain: routing moves
// while the destination is still fenced. The released request must wait again on
// the destination instead of spending its retries on it, and be served when the
// destination admits.
func TestRoutingFlipBeforeTheDestinationOpensHoldsTheRequestAgain(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}

	src := unmanagedPooler("src", "zone1", "migrateTG")
	dst := unmanagedPooler("dst", "zone1", "destTG")
	addPoolerForTest(t, lb, src)
	addPoolerForTest(t, lb, dst)
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING, true)
	reportHealth(connForTest(t, lb, dst), clustermetadatapb.PoolerServingStatus_SERVING, true)

	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)
	routing.Update("migrateTG", 1)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	target := writableTarget("migrateTG")
	selected := make(chan *poolerConnection, 1)
	done := make(chan error, 1)
	go func() {
		done <- pg.withBuffering(ctx, target, true, false, func(conn *poolerConnection) error {
			selected <- conn
			return nil
		})
	}()

	bufferedOn := func(tg string) func() bool {
		key := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: tg, Shard: constants.DefaultShard}
		return func() bool {
			probe, stop := context.WithCancel(ctx)
			stop()
			_, err := failoverBuffer.WaitIfAlreadyBuffering(probe, key)
			return errors.Is(err, context.Canceled)
		}
	}
	require.Eventually(t, bufferedOn("migrateTG"), 5*time.Second, time.Millisecond, "held on the source")

	// Routing moves while the destination is still closed.
	require.True(t, routing.Update("destTG", 2))
	require.Eventually(t, bufferedOn("destTG"), 5*time.Second, time.Millisecond, "held again, now on the destination")
	select {
	case err := <-done:
		t.Fatalf("request returned while the destination was fenced: %v", err)
	default:
	}

	reportHealth(connForTest(t, lb, dst), clustermetadatapb.PoolerServingStatus_SERVING, false)
	require.NoError(t, <-done)
	assert.Equal(t, poolerID(dst), (<-selected).ID())
}

func TestColdGatewayFailsClosedUntilRoutingIsKnown(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	src := unmanagedPooler("src", "zone1", "migrateTG")
	addPoolerForTest(t, lb, src)
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING, false)

	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	target := writableTarget(PendingAppTableGroup)
	selected := make(chan *poolerConnection, 1)
	done := make(chan error, 1)
	go func() {
		done <- pg.withBuffering(ctx, target, true, false, func(conn *poolerConnection) error {
			selected <- conn
			return nil
		})
	}()
	select {
	case err := <-done:
		t.Fatalf("a gateway that does not know the routing served or failed: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// The pooler's health stream (which has no address in this test) may have
	// reported an error meanwhile; report it healthy again as routing is learned.
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING, false)
	routing.Update("migrateTG", 1)
	require.NoError(t, <-done)
	assert.Equal(t, poolerID(src), (<-selected).ID())
}

// TestOnlyUnmanagedShardsAreScannedForUnmanagedMembers: the scan for unmanaged
// members is per-request work, so a shard whose poolers are all managed must not
// pay it, while an unmanaged shard is recognized from its first health report.
func TestOnlyUnmanagedShardsAreScannedForUnmanagedMembers(t *testing.T) {
	lb := newTestLB(t, "zone1")
	managed := createTestMultipooler("m1", "zone1", "default", constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	unmanaged := unmanagedPooler("u1", "zone1", "migrateTG")
	addPoolerForTest(t, lb, managed)
	addPoolerForTest(t, lb, unmanaged)
	reportHealth(connForTest(t, lb, managed), clustermetadatapb.PoolerServingStatus_SERVING, false)
	reportHealth(connForTest(t, lb, unmanaged), clustermetadatapb.PoolerServingStatus_SERVING, false)

	summaryOf := func(p *clustermetadatapb.Multipooler) *shardSummary {
		lb.mu.Lock()
		defer lb.mu.Unlock()
		return lb.shards[shardKeyOf(p.GetShardKey())]
	}
	require.NotNil(t, summaryOf(managed))
	assert.False(t, summaryOf(managed).unmanaged.Load())
	require.NotNil(t, summaryOf(unmanaged))
	assert.True(t, summaryOf(unmanaged).unmanaged.Load())

	conn, err := lb.getConnection(writableTarget("migrateTG"))
	require.NoError(t, err)
	assert.Equal(t, poolerID(unmanaged), conn.ID())
}

// TestControlRequestsAreNotHeldBackByAClosedGate: a credential lookup is not
// application work, so a fence must not make new logins wait.
func TestControlRequestsAreNotHeldBackByAClosedGate(t *testing.T) {
	lb := newTestLB(t, "zone1")
	closed := unmanagedPooler("closed", "zone1", "migrateTG")
	addPoolerForTest(t, lb, closed)
	reportHealth(connForTest(t, lb, closed), clustermetadatapb.PoolerServingStatus_SERVING, true)

	_, err := lb.getConnection(writableTarget("migrateTG"))
	require.Error(t, err)
	assert.True(t, isNoWritablePrimaryError(err), "application work waits for the gate")

	conn, err := lb.getControlConnection(writableTarget("migrateTG"))
	require.NoError(t, err)
	assert.Equal(t, poolerID(closed), conn.ID())

	// A pooler that is not serving at all still cannot answer.
	reportHealth(connForTest(t, lb, closed), clustermetadatapb.PoolerServingStatus_DISABLED, true)
	_, err = lb.getControlConnection(writableTarget("migrateTG"))
	require.Error(t, err)
}
