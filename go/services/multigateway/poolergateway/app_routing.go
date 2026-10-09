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
	"time"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// PROTOTYPE STUB: gateway-side application routing for the unmanaged pooler
// prototype.
//
// All application SQL of a database goes to one tablegroup: the one the default
// primary's routing pointer currently names. The gateway polls that pointer; on a
// change it swaps the tablegroup its planner targets and rewrites in-flight
// retries. Safety does not depend on how fresh the pointer is: a gateway that is
// behind only reaches a fenced pooler, which rejects with a bufferable error.
// Real routing propagation belongs to the sharding design; delete this file with
// the stub when it lands.

// PendingAppTableGroup is the tablegroup application SQL is planned against
// before the first successful read of the routing pointer. Requests for it fail
// closed with a bufferable error until the pointer is known.
const PendingAppTableGroup = "__routing_pending__"

// AppRouting holds the gateway's view of one database's application tablegroup.
type AppRouting struct {
	database string

	mu       sync.Mutex
	current  string
	version  int64
	known    bool
	aliases  map[string]bool
	onChange []func(old, new string)
}

// NewAppRouting returns routing state for database, unknown until first Update.
func NewAppRouting(database string) *AppRouting {
	return &AppRouting{database: database, aliases: map[string]bool{PendingAppTableGroup: true}}
}

// OnChange registers a callback run, outside the lock and in registration
// order, when the application tablegroup changes (old is empty on the first
// read).
func (r *AppRouting) OnChange(f func(old, new string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onChange = append(r.onChange, f)
}

// Current returns the application tablegroup and whether it is known yet.
func (r *AppRouting) Current() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, r.known
}

// Update records a read of the routing pointer. A version at or below the one
// already applied is ignored, so an older read arriving late changes nothing.
func (r *AppRouting) Update(tablegroup string, version int64) bool {
	if tablegroup == "" {
		return false
	}
	r.mu.Lock()
	if r.known && version <= r.version {
		r.mu.Unlock()
		return false
	}
	old := r.current
	changed := !r.known || old != tablegroup
	r.current, r.version, r.known = tablegroup, version, true
	r.aliases[tablegroup] = true
	callbacks := append([]func(old, new string){}, r.onChange...)
	r.mu.Unlock()
	if changed {
		for _, cb := range callbacks {
			cb(old, tablegroup)
		}
	}
	return changed
}

// Rewrite points an application target at the current application tablegroup.
// A target for another tablegroup (the default cohort's metadata, a different
// database) is left alone. Before the pointer is known the request fails closed
// with a bufferable error. Targets are built per call, so it swaps the target's
// key in place; the old key, which a buffered request may hold, is not edited.
func (r *AppRouting) Rewrite(target *query.Target) error {
	if r == nil || target == nil || target.GetShardKey() == nil {
		return nil
	}
	sk := target.GetShardKey()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.aliases[sk.GetTableGroup()] {
		return nil
	}
	if !r.known {
		return newNoWritablePrimaryError("application routing for database=%s is not known yet", r.database)
	}
	// Replace the key rather than edit it: a buffered request holds the old one.
	target.ShardKey = &clustermetadatapb.ShardKey{Database: sk.GetDatabase(), TableGroup: r.current, Shard: sk.GetShard()}
	return nil
}

// AuthTableGroup is the tablegroup whose poolers answer credential lookups: the
// application tablegroup once known, else the default one.
func (r *AppRouting) AuthTableGroup() string {
	if r == nil {
		return constants.DefaultTableGroup
	}
	if tg, known := r.Current(); known {
		return tg
	}
	return constants.DefaultTableGroup
}

// SetAppRouting wires application routing into the gateway. On a change it
// releases requests buffered for the tablegroup that lost the traffic, so they
// retry against the new one immediately instead of waiting out the buffer window.
func (pg *PoolerGateway) SetAppRouting(r *AppRouting) {
	pg.appRouting = r
	r.OnChange(func(old, _ string) {
		if old == "" || old == PendingAppTableGroup {
			// The pointer became known: release everything held while it was not.
			pg.drainTableGroup(PendingAppTableGroup)
			return
		}
		pg.drainTableGroup(old)
	})
}

// drainTableGroup stops buffering for every shard of a tablegroup.
func (pg *PoolerGateway) drainTableGroup(tablegroup string) {
	if pg.buffer == nil {
		return
	}
	pg.loadBalancer.mu.Lock()
	var keys []*clustermetadatapb.ShardKey
	for _, summary := range pg.loadBalancer.shards {
		if summary.shardKey.GetTableGroup() == tablegroup {
			keys = append(keys, summary.shardKey)
		}
	}
	pg.loadBalancer.mu.Unlock()
	for _, key := range keys {
		pg.buffer.StopBuffering(key)
	}
	// Requests held while routing was unknown are keyed by the pending group.
	if tablegroup == PendingAppTableGroup {
		pg.buffer.StopBuffering(&clustermetadatapb.ShardKey{Database: pg.appRouting.database, TableGroup: PendingAppTableGroup, Shard: constants.DefaultShard})
	}
}

// RoutingPoller reads the routing pointer from the default primary.
type RoutingPoller struct {
	pg       *PoolerGateway
	routing  *AppRouting
	database string
	interval time.Duration
	logger   *slog.Logger
	// onChange runs after the pointer changed (plan cache invalidation).
	onChange func()
	// read fetches the routing pointer; a seam for tests.
	read func(ctx context.Context) (*multipoolerservicepb.GetServingStateResponse, error)
}

// NewRoutingPoller builds a poller; call Run in a goroutine.
func NewRoutingPoller(pg *PoolerGateway, routing *AppRouting, database string, interval time.Duration, logger *slog.Logger, onChange func()) *RoutingPoller {
	p := &RoutingPoller{pg: pg, routing: routing, database: database, interval: interval, logger: logger, onChange: onChange}
	p.read = p.readFromDefaultPrimary
	return p
}

// readFromDefaultPrimary asks the default primary for the routing pointer only.
func (p *RoutingPoller) readFromDefaultPrimary(ctx context.Context) (*multipoolerservicepb.GetServingStateResponse, error) {
	conn, err := p.pg.GetConnection(&query.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: p.database, TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard},
		Mode:     query.Mode_MODE_WRITABLE,
	})
	if err != nil {
		return nil, err
	}
	return conn.ServiceClient().GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: p.database})
}

// Run polls until ctx is cancelled. A failed read keeps the last known routing;
// before the first successful read the gateway fails closed.
func (p *RoutingPoller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		p.PollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// PollOnce performs one read and applies it.
func (p *RoutingPoller) PollOnce(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := p.read(callCtx)
	if err != nil {
		p.logger.DebugContext(ctx, "routing poll failed", "error", err)
		return
	}
	if p.routing.Update(resp.GetAppTablegroup(), resp.GetRoutingVersion()) {
		p.logger.InfoContext(ctx, "application routing changed",
			"tablegroup", resp.GetAppTablegroup(), "version", resp.GetRoutingVersion())
		if p.onChange != nil {
			p.onChange()
		}
	}
}
