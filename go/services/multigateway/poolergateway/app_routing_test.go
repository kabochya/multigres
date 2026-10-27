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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/protoutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

func unmanagedPooler(name, cell, tableGroup string) *clustermetadatapb.Multipooler {
	p := createTestMultipooler(name, cell, tableGroup, constants.DefaultShard, clustermetadatapb.PoolerType_PRIMARY)
	p.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	return p
}

// controlTarget is a control request's target: fixed to its tablegroup.
func controlTarget(tableGroup string) *query.Target {
	t := protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
	t.Control = true
	return t
}

func writableTarget(tableGroup string) *query.Target {
	return protoutil.NewTarget(constants.DefaultPostgresDatabase, tableGroup, constants.DefaultShard, query.Mode_MODE_WRITABLE)
}

// TestWhatChangesInStepWithTheRoutingIsNeverBehindIt: the planner's tablegroup is updated in
// the same step as the routing view, so a reader that sees the new tablegroup through the
// routing never finds the planner still on the old one, which would let a statement planned at
// the old tablegroup meet reservations recorded at the new one. With the update made after the
// lock is released, as an ordinary OnChange callback is, readers do see it behind.
func TestWhatChangesInStepWithTheRoutingIsNeverBehindIt(t *testing.T) {
	for name, register := range map[string]func(r *AppRouting, f func(old, new string)){
		"in step":        func(r *AppRouting, f func(old, new string)) { r.OnChangeInStep(f) },
		"after the lock": func(r *AppRouting, f func(old, new string)) { r.OnChange(f) },
	} {
		t.Run(name, func(t *testing.T) {
			r := NewAppRouting("db")
			var planner atomic.Int64 // the version the planner plans for
			var version atomic.Int64
			register(r, func(_, _ string) {
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
			if name == "in step" {
				require.Zero(t, behind.Load(), "the planner never lags the routing view")
			} else {
				t.Logf("the control: an after-the-lock callback lagged the routing view %d times", behind.Load())
			}
		})
	}
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
