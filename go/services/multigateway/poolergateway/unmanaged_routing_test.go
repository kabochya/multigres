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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	gatewaybuffer "github.com/multigres/multigres/go/services/multigateway/buffer"
	"github.com/multigres/multigres/go/tools/viperutil"
)

const unmanagedMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED

func unmanagedPooler(name, cell, tableGroup string) *clustermetadatapb.Multipooler {
	p := createTestMultipooler(name, cell, tableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	p.ManagementMode = unmanagedMode
	return p
}

// reportHealth reports a live health snapshot for an unmanaged pooler's connection: it
// reports role PRIMARY (with no rule) while it serves, that is while its backend is ready, and
// none otherwise.
func reportHealth(conn *poolerConnection, status clustermetadatapb.PoolerServingStatus, admissionClosed bool) {
	resp := &multipoolerservice.StreamPoolerHealthResponse{
		PoolerId:        conn.PoolerInfo().Id,
		ServingStatus:   status,
		AdmissionClosed: admissionClosed,
	}
	if status == clustermetadatapb.PoolerServingStatus_SERVING {
		resp.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
	}
	conn.processHealthResponse(resp)
}

// controlTarget is a control request's target: fixed to its tablegroup and served
// by a pooler whose application gate is closed.
func controlTarget(tableGroup string) *query.Target {
	t := protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	t.Control = true
	return t
}

func writableTarget(tableGroup string) *query.Target {
	return protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
}

// TestFencedUnmanagedLeaderIsNeverSelectedForApplicationTraffic: an unmanaged pooler is its
// shard's leader like any single-node shard's, so application traffic goes to it only while
// it serves and its gate is open. Fenced, it is never selected whatever the mode, and the error
// is the bufferable one a missing leader returns, so requests wait for it to admit again
// instead of failing. There are no replicas to read from.
func TestFencedUnmanagedLeaderIsNeverSelectedForApplicationTraffic(t *testing.T) {
	lb := newTestLB(t, "zone1")
	ext := unmanagedPooler("ext", "zone1", "migrateTG")
	addPoolerForTest(t, lb, ext)
	conn := connForTest(t, lb, ext)

	reportHealth(conn, clustermetadatapb.PoolerServingStatus_SERVING, false)
	for _, mode := range []query.Mode{query.Mode_MODE_WRITABLE, query.Mode_MODE_CONSISTENT} {
		got, err := lb.getConnection(protoutil.NewTarget(constants.DefaultPostgresDatabase, "migrateTG", constants.DefaultShard, mode))
		require.NoError(t, err, mode.String())
		assert.Equal(t, poolerID(ext), got.ID(), mode.String())
	}
	_, err := lb.getConnection(protoutil.NewTarget(constants.DefaultPostgresDatabase, "migrateTG", constants.DefaultShard, query.Mode_MODE_INCONSISTENT))
	require.Error(t, err, "there is no replica of a shard of one")

	// Fenced: never selected, and the failure is the one that is buffered.
	reportHealth(conn, clustermetadatapb.PoolerServingStatus_SERVING, true)
	for _, mode := range []query.Mode{query.Mode_MODE_WRITABLE, query.Mode_MODE_CONSISTENT} {
		_, err = lb.getConnection(protoutil.NewTarget(constants.DefaultPostgresDatabase, "migrateTG", constants.DefaultShard, mode))
		require.Error(t, err, mode.String())
		assert.True(t, isAdmissionClosedError(err), "a fenced unmanaged shard must buffer, not fail: %v", err)
	}

	// Not serving at all: an ordinary outage, also buffered.
	reportHealth(conn, clustermetadatapb.PoolerServingStatus_DISABLED, false)
	_, err = lb.getConnection(writableTarget("migrateTG"))
	require.Error(t, err)

	// The control path is not held back by the closed gate.
	reportHealth(conn, clustermetadatapb.PoolerServingStatus_SERVING, true)
	got, err := lb.getControlConnection(controlTarget("migrateTG"))
	require.NoError(t, err)
	assert.Equal(t, poolerID(ext), got.ID())
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
	assert.Equal(t, PendingAppTableGroup, r.AuthTableGroup("postgres"))
	var nilRouting *AppRouting
	assert.Equal(t, constants.DefaultTableGroup, nilRouting.AuthTableGroup("postgres"), "without application routing it is the default")

	require.True(t, r.Update("migrateTG", 1))
	tg, known := r.Current()
	require.True(t, known)
	require.Equal(t, "migrateTG", tg)

	// The pending placeholder and the current group resolve to the current one.
	require.NoError(t, r.Rewrite(pending))
	assert.Equal(t, "migrateTG", pending.GetShardKey().GetTableGroup())
	assert.Equal(t, "migrateTG", r.AuthTableGroup("postgres"))
	assert.Equal(t, constants.DefaultTableGroup, r.AuthTableGroup("other"), "a database the routing does not follow keeps the default")

	// Control targets are not application targets, whatever their tablegroup.
	meta := controlTarget(constants.DefaultTableGroup)
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
	assert.Equal(t, constants.DefaultTableGroup, none.AuthTableGroup("postgres"))
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
	// The connections' own health streams fail against the test's address and mark the
	// poolers unhealthy whenever they get to it, so the state this test wants is reported
	// again continuously instead of once: a single report could be overwritten before the
	// released request selected a pooler.
	var srcClosed, dstClosed atomic.Bool
	srcClosed.Store(true) // fenced
	dstClosed.Store(true) // not yet unfenced
	srcConn, dstConn := connForTest(t, lb, src), connForTest(t, lb, dst)
	var reportMu sync.Mutex
	report := func() {
		reportMu.Lock()
		defer reportMu.Unlock()
		reportHealth(srcConn, clustermetadatapb.PoolerServingStatus_SERVING, srcClosed.Load())
		reportHealth(dstConn, clustermetadatapb.PoolerServingStatus_SERVING, dstClosed.Load())
	}
	report()
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-t.Context().Done():
				return
			case <-tick.C:
				report()
			}
		}
	}()

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
	dstClosed.Store(false)
	report()
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
	// The connections' own health streams fail against the test's address and mark the
	// poolers unhealthy whenever they get to it, so the state is reported again continuously
	// instead of once: a single report could be overwritten before the released request
	// selected a pooler.
	srcConn, dstConn := connForTest(t, lb, src), connForTest(t, lb, dst)
	var dstClosed atomic.Bool
	dstClosed.Store(true)
	var reportMu sync.Mutex
	report := func() {
		reportMu.Lock()
		defer reportMu.Unlock()
		reportHealth(srcConn, clustermetadatapb.PoolerServingStatus_SERVING, true)
		reportHealth(dstConn, clustermetadatapb.PoolerServingStatus_SERVING, dstClosed.Load())
	}
	report()
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-t.Context().Done():
				return
			case <-tick.C:
				report()
			}
		}
	}()

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

	dstClosed.Store(false)
	report()
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

// TestRollbackRightAfterACutoverStillHoldsRequests: with the default timing guard
// between buffer cycles, a second closure of the same tablegroup shortly after the
// first used to be skipped, so requests bounced instead of waiting. Closures of the
// application tablegroup are deliberate, so the guard must not apply to them.
func TestRollbackRightAfterACutoverStillHoldsRequests(t *testing.T) {
	config := gatewaybuffer.NewConfig(viperutil.NewRegistry())
	config.Enabled.Set(true)
	config.Window.Set(10 * time.Second)
	config.Size.Set(10)
	config.MaxFailoverDuration.Set(10 * time.Second)
	config.MinTimeBetweenFailovers.Set(time.Hour) // far longer than this test
	config.DrainConcurrency.Set(1)
	failoverBuffer := gatewaybuffer.New(t.Context(), config, slog.New(slog.DiscardHandler))
	t.Cleanup(failoverBuffer.Shutdown)

	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	src := unmanagedPooler("src", "zone1", "migrateTG")
	dst := unmanagedPooler("dst", "zone1", "destTG")
	addPoolerForTest(t, lb, src)
	addPoolerForTest(t, lb, dst)
	// The connections' own health streams fail against the test's address and mark
	// the poolers unhealthy whenever they get to it, so the state this test wants is
	// reported again continuously instead of once.
	var srcClosed, dstClosed atomic.Bool
	srcClosed.Store(true) // fenced
	srcConn, dstConn := connForTest(t, lb, src), connForTest(t, lb, dst)
	var reportMu sync.Mutex // a report that read the old state must not land after a newer one
	report := func() {
		reportMu.Lock()
		defer reportMu.Unlock()
		reportHealth(srcConn, clustermetadatapb.PoolerServingStatus_SERVING, srcClosed.Load())
		reportHealth(dstConn, clustermetadatapb.PoolerServingStatus_SERVING, dstClosed.Load())
	}
	report()
	go func() {
		tick := time.NewTicker(200 * time.Microsecond) // the window in which a failed stream leaves the pooler unhealthy
		defer tick.Stop()
		for {
			select {
			case <-t.Context().Done():
				return
			case <-tick.C:
				report()
			}
		}
	}()
	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)
	routing.Update("migrateTG", 1)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	run := func(target *query.Target) (<-chan *poolerConnection, <-chan error) {
		selected, done := make(chan *poolerConnection, 1), make(chan error, 1)
		go func() {
			done <- pg.withBuffering(ctx, target, true, false, func(conn *poolerConnection) error {
				selected <- conn
				return nil
			})
		}()
		return selected, done
	}
	bufferedOn := func(tg string) func() bool {
		key := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: tg, Shard: constants.DefaultShard}
		return func() bool {
			probe, stop := context.WithCancel(ctx)
			stop()
			_, err := failoverBuffer.WaitIfAlreadyBuffering(probe, key)
			return errors.Is(err, context.Canceled)
		}
	}

	// The cutover: a request waits on the fenced source and is released by the flip.
	selected, done := run(writableTarget("migrateTG"))
	require.Eventually(t, bufferedOn("migrateTG"), 5*time.Second, time.Millisecond)
	require.True(t, routing.Update("destTG", 2))
	require.NoError(t, <-done)
	require.Equal(t, poolerID(dst), (<-selected).ID())
	// The cutover's buffer drains in the background once its request has finished
	// retrying; a request that comes while it still drains is retried at once and
	// does not wait, which is not what this test is about.
	idleOn := func(tg string) func() bool {
		key := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: tg, Shard: constants.DefaultShard}
		return func() bool {
			retryDone, err := failoverBuffer.WaitIfAlreadyBuffering(ctx, key)
			return retryDone == nil && err == nil
		}
	}
	require.Eventually(t, idleOn("migrateTG"), 5*time.Second, time.Millisecond, "the cutover's buffer never went idle")

	// The rollback, moments later: the destination is fenced, routing goes back to
	// the source, which is still fenced until its unfence.
	dstClosed.Store(true)
	report()
	require.True(t, routing.Update("migrateTG", 3))
	selected, done = run(writableTarget("destTG"))
	require.Eventually(t, bufferedOn("migrateTG"), 5*time.Second, time.Millisecond, "the request must wait again, within the timing guard")
	select {
	case err := <-done:
		t.Fatalf("the request returned while neither side admitted: %v", err)
	default:
	}
	srcClosed.Store(false)
	report()
	require.NoError(t, <-done)
	require.Equal(t, poolerID(src), (<-selected).ID())
}

// TestAppRoutingDoesNotRewriteOtherDatabases: routing follows one database.
func TestAppRoutingDoesNotRewriteOtherDatabases(t *testing.T) {
	r := NewAppRouting("appdb")
	r.Update("destTG", 1)
	other := protoutil.NewTarget("elsewhere", PendingAppTableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	require.NoError(t, r.Rewrite(other))
	assert.Equal(t, PendingAppTableGroup, other.GetShardKey().GetTableGroup(), "left alone")

	mine := protoutil.NewTarget("appdb", PendingAppTableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	require.NoError(t, r.Rewrite(mine))
	assert.Equal(t, "destTG", mine.GetShardKey().GetTableGroup())
}

// TestAdmissionClosureIsToldApartFromALostLeader: only a closed application gate,
// seen by the gateway's own selection or reported by the pooler as MTF02, is
// exempt from the timing guard; a lost leader is not, and both are bufferable.
func TestAdmissionClosureIsToldApartFromALostLeader(t *testing.T) {
	lb := newTestLB(t, "zone1")

	// A managed leader that is fenced.
	leader := createTestMultipooler("dest-1", "zone1", "destTG", constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)
	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, AdmissionClosed: true,
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
	})
	_, err := lb.getConnection(writableTarget("destTG"))
	assert.True(t, isAdmissionClosedError(err), "a fenced managed leader")
	assert.True(t, isNoWritablePrimaryError(err), "and it is still bufferable")

	// A shard with no leader at all.
	_, err = lb.getConnection(writableTarget("noLeaderTG"))
	require.Error(t, err)
	assert.False(t, isAdmissionClosedError(err), "no leader is not a closed gate")
	assert.True(t, isNoWritablePrimaryError(err))

	// An unmanaged shard: fenced is a closure, a pooler that is not serving is not.
	fenced := unmanagedPooler("fenced", "zone1", "migrateTG")
	down := unmanagedPooler("down", "zone1", "downTG")
	addPoolerForTest(t, lb, fenced)
	addPoolerForTest(t, lb, down)
	reportHealth(connForTest(t, lb, fenced), clustermetadatapb.PoolerServingStatus_SERVING, true)
	reportHealth(connForTest(t, lb, down), clustermetadatapb.PoolerServingStatus_DISABLED, false)
	_, err = lb.getConnection(writableTarget("migrateTG"))
	assert.True(t, isAdmissionClosedError(err), "every unmanaged pooler is fenced")
	_, err = lb.getConnection(writableTarget("downTG"))
	require.Error(t, err)
	assert.False(t, isAdmissionClosedError(err), "an unmanaged pooler that is down is an outage")
	assert.True(t, isNoWritablePrimaryError(err))

	// The pooler's own rejections.
	target := writableTarget("destTG")
	assert.Equal(t, actionBuffer, classifyError(mterrors.MTF02.New(), target, false))
	assert.True(t, isAdmissionClosedError(mterrors.MTF02.New()))
	assert.Equal(t, actionBuffer, classifyError(mterrors.MTF01.New(), target, false))
	assert.False(t, isAdmissionClosedError(mterrors.MTF01.New()), "a failover keeps the guard")
}

// TestLeaderLossOnAnApplicationTablegroupKeepsTheTimingGuard: the guard against
// buffering again soon after a cycle is lifted for a fence only. A managed
// application tablegroup that loses its leader twice in a row is a flapping
// failover and is not buffered the second time.
func TestLeaderLossOnAnApplicationTablegroupKeepsTheTimingGuard(t *testing.T) {
	config := gatewaybuffer.NewConfig(viperutil.NewRegistry())
	config.Enabled.Set(true)
	config.Window.Set(10 * time.Second)
	config.Size.Set(10)
	config.MaxFailoverDuration.Set(10 * time.Second)
	config.MinTimeBetweenFailovers.Set(time.Hour)
	config.DrainConcurrency.Set(1)
	failoverBuffer := gatewaybuffer.New(t.Context(), config, slog.New(slog.DiscardHandler))
	t.Cleanup(failoverBuffer.Shutdown)

	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)
	routing.Update("destTG", 1)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	run := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			done <- pg.withBuffering(ctx, writableTarget("destTG"), true, false, func(*poolerConnection) error { return nil })
		}()
		return done
	}
	bufferedOn := func() bool {
		key := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: "destTG", Shard: constants.DefaultShard}
		probe, stop := context.WithCancel(ctx)
		stop()
		_, err := failoverBuffer.WaitIfAlreadyBuffering(probe, key)
		return errors.Is(err, context.Canceled)
	}

	// The first loss is buffered and ends when a leader appears.
	done := run()
	require.Eventually(t, bufferedOn, 5*time.Second, time.Millisecond, "the first loss buffers")
	leader := createTestMultipooler("dest-1", "zone1", "destTG", constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)
	simulateHealthUpdate(connForTest(t, lb, leader), clustermetadatapb.PoolerServingStatus_SERVING,
		leader.Id, &clustermetadatapb.RuleNumber{CoordinatorTerm: 1})
	require.NoError(t, <-done)

	// The leader is lost again within the guard: the request is not held.
	removePoolerForTest(t, lb, poolerID(leader))
	select {
	case err := <-run():
		require.Error(t, err)
		assert.True(t, isNoWritablePrimaryError(err))
	case <-time.After(5 * time.Second):
		t.Fatal("the second loss was buffered despite the timing guard")
	}
	assert.False(t, bufferedOn(), "no buffer was started")
}

// TestRoutingMovedBeforeTheWaitStartsOpensNoBuffer: the wait context ends at once
// when routing has already moved, so the buffer, which starts nothing for an ended
// context, is not opened for the tablegroup that lost the traffic.
func TestRoutingMovedBeforeTheWaitStartsOpensNoBuffer(t *testing.T) {
	moved := make(chan struct{})
	close(moved)
	ctx, stop := contextUntil(t.Context(), moved)
	defer stop()
	require.Error(t, ctx.Err(), "ended before it was returned, not on a goroutine's schedule")

	open := make(chan struct{})
	ctx, stop = contextUntil(t.Context(), open)
	defer stop()
	require.NoError(t, ctx.Err())

	failoverBuffer := newTestFailoverBuffer(t, 10)
	key := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: "migrateTG", Shard: constants.DefaultShard}
	ended, stopEnded := contextUntil(t.Context(), moved)
	defer stopEnded()
	_, err := failoverBuffer.WaitForFailoverEnd(ended, key)
	require.ErrorIs(t, err, context.Canceled)
	probe, stopProbe := context.WithCancel(t.Context())
	stopProbe()
	_, err = failoverBuffer.WaitIfAlreadyBuffering(probe, key)
	assert.NotErrorIs(t, err, context.Canceled, "no buffer exists for the tablegroup")
}

// TestControlTargetsAreNeverRewritten: the default tablegroup can be the application
// tablegroup, so a tablegroup says nothing about which kind of traffic a target is.
// An application target on it follows routing; a control target on it does not, in
// either direction of a cutover.
func TestControlTargetsAreNeverRewritten(t *testing.T) {
	r := NewAppRouting(constants.DefaultPostgresDatabase)
	r.Update("migrateTG", 1)

	control := controlTarget(constants.DefaultTableGroup)
	require.NoError(t, r.Rewrite(control))
	assert.Equal(t, constants.DefaultTableGroup, control.GetShardKey().GetTableGroup())

	// The cutover into the default tablegroup: application traffic is there now.
	require.True(t, r.Update(constants.DefaultTableGroup, 2))
	app := writableTarget("migrateTG")
	require.NoError(t, r.Rewrite(app))
	assert.Equal(t, constants.DefaultTableGroup, app.GetShardKey().GetTableGroup())
	require.NoError(t, r.Rewrite(control))
	assert.Equal(t, constants.DefaultTableGroup, control.GetShardKey().GetTableGroup())

	// The rollback: application traffic leaves it. A request that was bound for it
	// follows, a control request does not.
	require.True(t, r.Update("migrateTG", 3))
	app = writableTarget(constants.DefaultTableGroup)
	require.NoError(t, r.Rewrite(app))
	assert.Equal(t, "migrateTG", app.GetShardKey().GetTableGroup(), "an application request bound for default follows the traffic back")
	require.NoError(t, r.Rewrite(control))
	assert.Equal(t, constants.DefaultTableGroup, control.GetShardKey().GetTableGroup(), "control traffic stays on default")

	// Control traffic is not held for unknown routing either: the pointer is only
	// needed to find where application traffic goes.
	cold := NewAppRouting(constants.DefaultPostgresDatabase)
	require.NoError(t, cold.Rewrite(controlTarget(constants.DefaultTableGroup)))
	require.Error(t, cold.Rewrite(writableTarget(PendingAppTableGroup)))
}

// TestFencedDefaultLeaderStillServesControlTraffic: with the default tablegroup
// fenced, its leader refuses application work but is still the one that answers
// control traffic: NOTIFY, the routing poll and LISTEN, all marked as control.
func TestFencedDefaultLeaderStillServesControlTraffic(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	leader := createTestMultipooler("def-1", "zone1", constants.DefaultTableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)
	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, AdmissionClosed: true,
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
	})

	// Application work is refused.
	_, err := lb.getConnection(writableTarget(constants.DefaultTableGroup))
	require.Error(t, err)
	assert.True(t, isAdmissionClosedError(err))

	// Control traffic through the buffering path reaches it at once.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var served *poolerConnection
	require.NoError(t, pg.withBuffering(ctx, controlTarget(constants.DefaultTableGroup), true, false, func(conn *poolerConnection) error {
		served = conn
		return nil
	}))
	require.NotNil(t, served)
	assert.Equal(t, poolerID(leader), served.ID())

	// The direct entry point, used by the routing poll and the LISTEN manager.
	direct, err := pg.GetConnection(controlTarget(constants.DefaultTableGroup))
	require.NoError(t, err)
	assert.Equal(t, poolerID(leader), direct.ID())
}

// TestRoutingMovingBetweenAFailedAttemptAndTheNextDoesNotBufferTheNewShard: an error
// from the old tablegroup says nothing about the one the request is pointed at after
// routing moved. The request must try the new one at once rather than wait for it
// to recover from a failure it never had.
func TestRoutingMovingBetweenAFailedAttemptAndTheNextDoesNotBufferTheNewShard(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	dst := unmanagedPooler("dst", "zone1", "destTG")
	addPoolerForTest(t, lb, dst)
	reportHealth(connForTest(t, lb, dst), clustermetadatapb.PoolerServingStatus_SERVING, false)
	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)
	routing.Update("migrateTG", 1)

	// The first selection fails, as the fenced source's would, and routing moves in
	// that very moment: before the request's next attempt.
	calls := 0
	selectConn := func(target *query.Target) (*poolerConnection, error) {
		calls++
		if calls == 1 {
			routing.Update("destTG", 2)
			return nil, newAdmissionClosedError("the source is fenced")
		}
		return lb.getConnection(target)
	}
	done := make(chan error, 1)
	go func() {
		done <- pg.withBufferingSelecting(t.Context(), writableTarget("migrateTG"), true, false, selectConn, func(*poolerConnection) error { return nil })
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the request waited for the destination to recover from the source's failure")
	}
	assert.Equal(t, 2, calls)
}

// TestLeaderThatHasNotReadItsAdmissionStateIsHeld: after an ordinary failover the new
// leader reports SERVING and PRIMARY while its gate is still closed, until it has read the
// admission state. It is not selected, and the buffer is not drained toward it: requests
// released into a closed gate would only bounce. A closed gate is one signal, whatever
// closed it.
func TestLeaderThatHasNotReadItsAdmissionStateIsHeld(t *testing.T) {
	var drained atomic.Int32
	lb := newTestLBWithLeaderServing(t, "zone1", func(*clustermetadatapb.ShardKey) { drained.Add(1) })
	leader := createTestMultipooler("dest-1", "zone1", "destTG", constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	addPoolerForTest(t, lb, leader)
	rs := &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}}
	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, AdmissionClosed: true, RoutingState: rs,
	})
	require.Zero(t, drained.Load(), "the buffer is not drained toward a closed gate")
	_, err := lb.getConnection(writableTarget("destTG"))
	require.Error(t, err)
	assert.True(t, isNoWritablePrimaryError(err), "bufferable")
	assert.True(t, isAdmissionClosedError(err), "a closed gate, whatever closed it")

	connForTest(t, lb, leader).processHealthResponse(&multipoolerservice.StreamPoolerHealthResponse{
		PoolerId: leader.Id, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING, RoutingState: rs,
	})
	require.Positive(t, drained.Load(), "it drains once the gate is open")
	conn, err := lb.getConnection(writableTarget("destTG"))
	require.NoError(t, err)
	assert.Equal(t, poolerID(leader), conn.ID())
}

// TestUnmanagedPoolerThatHasNotReadItsAdmissionStateIsNotSelected: after a readiness flap
// an unmanaged member is back SERVING with its gate closed until it has read the state.
func TestUnmanagedPoolerThatHasNotReadItsAdmissionStateIsNotSelected(t *testing.T) {
	lb := newTestLB(t, "zone1")
	ext := unmanagedPooler("ext", "zone1", "migrateTG")
	addPoolerForTest(t, lb, ext)
	reportHealth(connForTest(t, lb, ext), clustermetadatapb.PoolerServingStatus_SERVING, true)
	_, err := lb.getConnection(writableTarget("migrateTG"))
	require.Error(t, err)
	assert.True(t, isNoWritablePrimaryError(err))
	assert.True(t, isAdmissionClosedError(err), "a closed gate, whatever closed it")
}

// bufferingOn reports whether a request is held in the buffer for a tablegroup of the
// application's database.
func bufferingOn(t *testing.T, fb *gatewaybuffer.Buffer, tg string) bool {
	t.Helper()
	probe, stop := context.WithCancel(t.Context())
	stop()
	key := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: tg, Shard: constants.DefaultShard}
	_, err := fb.WaitIfAlreadyBuffering(probe, key)
	return errors.Is(err, context.Canceled)
}

// TestARequestNeverSucceedsWithoutRunning: routing moves while a request waits, every time
// it waits, so that the attempts run out on moves. However many moves there are, the request
// either runs on a pooler and returns its result, or fails with an error the client retries;
// it never returns success without having run.
func TestARequestNeverSucceedsWithoutRunning(t *testing.T) {
	for _, moves := range []int{1, 2, 3, 4, 6} {
		t.Run(fmt.Sprintf("%d moves", moves), func(t *testing.T) {
			failoverBuffer := newTestFailoverBuffer(t, 10)
			lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
			pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}

			// A tablegroup per position of the routing, all of them closed except the last
			// one, which the request is served by if it gets there.
			tgs := []string{"tg0"}
			for i := 1; i <= moves; i++ {
				tgs = append(tgs, fmt.Sprintf("tg%d", i))
			}
			conns := map[string]*poolerConnection{}
			for _, tg := range tgs {
				p := unmanagedPooler("pooler-"+tg, "zone1", tg)
				addPoolerForTest(t, lb, p)
				conns[tg] = connForTest(t, lb, p)
			}
			last := tgs[len(tgs)-1]
			var reportMu sync.Mutex
			report := func() {
				reportMu.Lock()
				defer reportMu.Unlock()
				for tg, c := range conns {
					reportHealth(c, clustermetadatapb.PoolerServingStatus_SERVING, tg != last)
				}
			}
			report()
			go func() {
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-t.Context().Done():
						return
					case <-tick.C:
						report()
					}
				}
			}()

			routing := NewAppRouting(constants.DefaultPostgresDatabase)
			pg.SetAppRouting(routing)
			routing.Update(tgs[0], 1)

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			ran := 0
			var result error
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				result = pg.withBuffering(ctx, writableTarget(tgs[0]), true, false, func(*poolerConnection) error {
					ran++
					return nil
				})
			}()
			// Each time the request is held for a tablegroup, routing moves on to the next
			// (or, when it is not held there, after a moment).
			for i := 1; i <= moves; i++ {
				deadline := time.Now().Add(300 * time.Millisecond)
				for time.Now().Before(deadline) && !bufferingOn(t, failoverBuffer, tgs[i-1]) {
					select {
					case <-finished:
						deadline = time.Time{}
					case <-time.After(time.Millisecond):
					}
				}
				select {
				case <-finished:
				default:
					routing.Update(tgs[i], int64(i+1))
				}
			}
			<-finished
			err := result
			if err == nil {
				require.Equal(t, 1, ran, "a request that reports success must have run")
				return
			}
			require.Zero(t, ran)
			require.True(t, mterrors.IsErrorCode(err, mterrors.MTF01.ID, mterrors.MTF02.ID) || isNoWritablePrimaryError(err) || mterrors.Code(err) == mtrpcpb.Code_UNAVAILABLE,
				"the failure is one the client retries: %v", err)
		})
	}
}

// TestARequestHeldOnceWaitsAgainOnTheTablegroupItIsMovedTo: a request that has waited for
// one tablegroup and then finds routing moved between two attempts waits for the new one too,
// instead of spending its attempts on it back to back.
func TestARequestHeldOnceWaitsAgainOnTheTablegroupItIsMovedTo(t *testing.T) {
	failoverBuffer := newTestFailoverBuffer(t, 10)
	lb := newTestLBWithLeaderServing(t, "zone1", failoverBuffer.StopBuffering)
	pg := &PoolerGateway{loadBalancer: lb, buffer: failoverBuffer, logger: slog.New(slog.DiscardHandler)}
	dst := unmanagedPooler("dst", "zone1", "destTG")
	addPoolerForTest(t, lb, dst)
	dstConn := connForTest(t, lb, dst)
	var dstClosed atomic.Bool
	dstClosed.Store(true)
	var reportMu sync.Mutex
	report := func() {
		reportMu.Lock()
		defer reportMu.Unlock()
		reportHealth(dstConn, clustermetadatapb.PoolerServingStatus_SERVING, dstClosed.Load())
	}
	report()
	go func() {
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-t.Context().Done():
				return
			case <-tick.C:
				report()
			}
		}
	}()
	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)
	routing.Update("migrateTG", 1)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	srcKey := &clustermetadatapb.ShardKey{Database: constants.DefaultPostgresDatabase, TableGroup: "migrateTG", Shard: constants.DefaultShard}
	calls := 0
	selectConn := func(target *query.Target) (*poolerConnection, error) {
		calls++
		switch calls {
		case 1:
			// The source is fenced: the request waits for it.
			return nil, newAdmissionClosedError("the source is fenced")
		case 2:
			// The wait is over (the source reopened) and the pooler is closed again, and
			// routing moves in the same moment: between this attempt and the next.
			routing.Update("destTG", 2)
			return nil, newAdmissionClosedError("the source is fenced again")
		}
		return lb.getConnection(target)
	}
	done := make(chan error, 1)
	go func() {
		done <- pg.withBufferingSelecting(ctx, writableTarget("migrateTG"), true, false, selectConn, func(*poolerConnection) error { return nil })
	}()
	require.Eventually(t, func() bool { return bufferingOn(t, failoverBuffer, "migrateTG") }, 5*time.Second, time.Millisecond, "held for the source")
	failoverBuffer.StopBuffering(srcKey) // the wait for the source ends without a routing move

	require.Eventually(t, func() bool { return bufferingOn(t, failoverBuffer, "destTG") }, 5*time.Second, time.Millisecond,
		"the request waits for the destination, which is closed, instead of using up its attempts on it")
	dstClosed.Store(false)
	report()
	require.NoError(t, <-done)
}
