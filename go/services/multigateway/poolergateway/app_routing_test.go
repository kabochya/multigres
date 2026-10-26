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
	"log/slog"
	"sync"
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

func unmanagedPooler(name, cell, tableGroup string) *clustermetadatapb.Multipooler {
	p := createTestMultipooler(name, cell, tableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	p.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	return p
}

// reportHealth reports a live health snapshot for an unmanaged pooler's connection: it
// reports role PRIMARY (with no rule) while it serves, that is while its backend is ready, and
// none otherwise.
func reportHealth(conn *poolerConnection, status clustermetadatapb.PoolerServingStatus) {
	resp := &multipoolerservice.StreamPoolerHealthResponse{PoolerId: conn.PoolerInfo().Id, ServingStatus: status}
	if status == clustermetadatapb.PoolerServingStatus_SERVING {
		resp.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
	}
	conn.processHealthResponse(resp)
}

func writableTarget(tableGroup string) *query.Target {
	return protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
}

// TestWhatChangesInStepWithTheRoutingIsNeverBehindIt: the planner's tablegroup is updated in
// the same step as the routing view, so a reader that sees the new tablegroup through the
// routing never finds the planner still on the old one, which would let a statement planned at
// the old tablegroup meet reservations recorded at the new one.
func TestWhatChangesInStepWithTheRoutingIsNeverBehindIt(t *testing.T) {
	r := NewAppRouting("db")
	var planner atomic.Int64 // the version the planner plans for
	var version atomic.Int64
	r.OnChangeInStep(func(_, _ string) {
		time.Sleep(50 * time.Microsecond) // the planner takes a moment to update
		planner.Store(version.Load())
	})
	stop := make(chan struct{})
	var behind atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			// What a reader of the routing sees, then what the planner says.
			_, seen, known := r.snapshot()
			if known && planner.Load() < seen {
				behind.Add(1)
			}
		}
	})
	for v := int64(1); v <= 300; v++ {
		tg := "a"
		if v%2 == 0 {
			tg = "b"
		}
		version.Store(v)
		r.Update(tg, v)
	}
	close(stop)
	wg.Wait()
	require.Zero(t, behind.Load(), "the planner never lags the routing view")
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

	// A same-version or older read changes nothing.
	assert.False(t, r.Update("destTG", 1))
	assert.False(t, r.Update("destTG", 0))
	tg, _ = r.Current()
	assert.Equal(t, "migrateTG", tg)

	// A newer one changes the tablegroup, and a request planned for the old tablegroup is
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

// TestColdGatewayFailsClosedUntilRoutingIsKnown: before the routing is known, an application request
// fails with a bufferable error instead of reaching a pooler, and once it is known a request is
// served by the tablegroup it names.
func TestColdGatewayFailsClosedUntilRoutingIsKnown(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.New(slog.DiscardHandler)}
	src := unmanagedPooler("src", "zone1", "migrateTG")
	addPoolerForTest(t, lb, src)
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING)

	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	pg.SetAppRouting(routing)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	served := make(chan *poolerConnection, 1)
	serve := func(conn *poolerConnection) error {
		served <- conn
		return nil
	}
	err := pg.withBuffering(ctx, writableTarget(PendingAppTableGroup), true, false, serve)
	require.Error(t, err, "a gateway that does not know the routing neither served nor failed it open")
	require.Empty(t, served)

	// The pooler's health stream (which has no address in this test) may have reported an
	// error meanwhile; report it healthy again as routing is learned.
	reportHealth(connForTest(t, lb, src), clustermetadatapb.PoolerServingStatus_SERVING)
	routing.Update("migrateTG", 1)
	require.NoError(t, pg.withBuffering(ctx, writableTarget(PendingAppTableGroup), true, false, serve))
	assert.Equal(t, poolerID(src), (<-served).ID())
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
