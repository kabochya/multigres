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
	"fmt"
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
// primary's routing pointer currently names, which may be the default tablegroup
// itself. The gateway polls that pointer; on a change it swaps the tablegroup its
// planner targets and rewrites in-flight retries. Control traffic (NOTIFY, the
// routing poll, LISTEN) is not application traffic: its targets are marked, stay on
// the default tablegroup and are never rewritten. Which tablegroup a request is
// for says nothing about which kind it is. Safety does not depend on how fresh the pointer is: a gateway that is
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
	onChange []func(old, new string)
	// changed is closed, and replaced, whenever the application tablegroup
	// changes: a waiter that resolved the tablegroup before the change learns
	// that what it resolved is stale.
	changed chan struct{}
}

// NewAppRouting returns routing state for database, unknown until first Update.
func NewAppRouting(database string) *AppRouting {
	return &AppRouting{database: database, changed: make(chan struct{})}
}

// OnChange registers a callback run, outside the lock and in registration
// order, when the application tablegroup changes (old is empty on the first
// read).
func (r *AppRouting) OnChange(f func(old, new string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onChange = append(r.onChange, f)
}

// IsStaleApplicationTarget reports whether tablegroup of database, as the target of
// an application reservation, has lost the traffic: the routing follows that
// database, knows where traffic goes now, and it is not this tablegroup. Whether a
// reservation is an application one is for the caller to know: control reservations
// on the same tablegroup are not affected by routing.
func (r *AppRouting) IsStaleApplicationTarget(database, tablegroup string) bool {
	if r == nil || database != r.database {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.known && tablegroup != r.current
}

// Current returns the application tablegroup and whether it is known yet.
func (r *AppRouting) Current() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, r.known
}

// snapshot returns the applied tablegroup and version, and whether any is known.
func (r *AppRouting) snapshot() (tablegroup string, version int64, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, r.version, r.known
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
	if changed {
		close(r.changed)
		r.changed = make(chan struct{})
	}
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
// A control target and a target of another database are left alone. Before the
// pointer is known the request fails closed with a bufferable error. Targets are built per call, so it swaps the target's
// key in place; the old key, which a buffered request may hold, is not edited.
func (r *AppRouting) Rewrite(target *query.Target) error {
	_, err := r.rewrite(target)
	return err
}

// rewrite is Rewrite that also returns a channel closed when the application
// tablegroup next changes, captured together with the rewrite so no change can
// fall between them. It is nil when the target is not an application target.
func (r *AppRouting) rewrite(target *query.Target) (<-chan struct{}, error) {
	if r == nil || target == nil || target.GetShardKey() == nil || target.GetControl() {
		return nil, nil
	}
	sk := target.GetShardKey()
	r.mu.Lock()
	defer r.mu.Unlock()
	// Routing follows one database; another database's targets are not rewritten.
	if sk.GetDatabase() != r.database {
		return nil, nil
	}
	if !r.known {
		return r.changed, newNoWritablePrimaryError("application routing for database=%s is not known yet", r.database)
	}
	// Replace the key rather than edit it: a buffered request holds the old one.
	target.ShardKey = &clustermetadatapb.ShardKey{Database: sk.GetDatabase(), TableGroup: r.current, Shard: sk.GetShard()}
	return r.changed, nil
}

// AuthTableGroup is the tablegroup whose poolers answer credential lookups for
// database: the application tablegroup. Before it is known the lookup targets
// the pending tablegroup and waits like any other request, rather than
// consulting the default cohort, whose roles and password verifiers need not
// match the ones the application database uses. Without application routing, or
// for a database it does not follow, it is the default one.
func (r *AppRouting) AuthTableGroup(database string) string {
	if r == nil || database != r.database {
		return constants.DefaultTableGroup
	}
	if tg, known := r.Current(); known {
		return tg
	}
	return PendingAppTableGroup
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
		if summary.shardKey.GetDatabase() == pg.appRouting.database && summary.shardKey.GetTableGroup() == tablegroup {
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
	// lastWarning is the key of the condition last warned about (touched only by the
	// polling loop).
	lastWarning string
	// failures counts consecutive failed reads (touched only by the polling loop).
	failures int
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
		Control:  true,
	})
	if err != nil {
		return nil, err
	}
	return conn.ServiceClient().GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: p.database})
}

// Run polls until ctx is cancelled. A failed read keeps the last known routing;
// before the first successful read the gateway fails closed.
func (p *RoutingPoller) Run(ctx context.Context) {
	for {
		p.PollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.nextPollDelay()):
		}
	}
}

// coldPollInterval is how often a gateway that does not yet know the routing
// retries, however long the steady-state interval is: until the first read it
// cannot authenticate or serve application traffic.
const coldPollInterval = 500 * time.Millisecond

func (p *RoutingPoller) nextPollDelay() time.Duration {
	if _, known := p.routing.Current(); !known && p.interval > coldPollInterval {
		return coldPollInterval
	}
	return p.interval
}

// warnOnce logs a warning unless the previous warning had the same key, so a
// condition that persists across polls is reported once, not on every poll. A
// successful, applicable read resets it.
func (p *RoutingPoller) warnOnce(ctx context.Context, key, msg string, args ...any) {
	if p.lastWarning == key {
		return
	}
	p.lastWarning = key
	p.logger.WarnContext(ctx, msg, args...)
}

// PollOnce performs one read and applies it.
func (p *RoutingPoller) PollOnce(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := p.read(callCtx)
	if err != nil {
		// Warned on the first failure and then every tenth, so an outage is
		// visible without a line per poll.
		if p.failures++; p.failures == 1 || p.failures%10 == 0 {
			p.logger.WarnContext(ctx, "routing poll failed; keeping the last known routing",
				"error", err, "consecutive_failures", p.failures)
		}
		return
	}
	p.failures = 0
	if resp.GetAppTablegroup() == "" {
		p.warnOnce(ctx, "empty", "the default primary has no application routing pointer; application traffic stays held until one is set",
			"database", p.database)
		return
	}
	if current, version, known := p.routing.snapshot(); known && resp.GetRoutingVersion() < version {
		// Ignored, not applied: an older read arriving late must change nothing, and
		// a pointer that really was reset has to be brought forward by an operator.
		p.warnOnce(ctx, fmt.Sprintf("backwards/%d/%s", resp.GetRoutingVersion(), resp.GetAppTablegroup()),
			"routing version went backwards; ignoring it until it passes the applied one",
			"applied_version", version, "read_version", resp.GetRoutingVersion(), "current", current, "read", resp.GetAppTablegroup())
		return
	}
	if current, version, known := p.routing.snapshot(); known && resp.GetRoutingVersion() == version && resp.GetAppTablegroup() != current {
		// Update ignores it: a version is applied once. Seeing a different
		// tablegroup under the same version means the routing row was rewritten
		// without bumping it, which an operator needs to hear about.
		p.warnOnce(ctx, fmt.Sprintf("same-version/%d/%s", version, resp.GetAppTablegroup()),
			"routing row changed without a version bump; ignoring it until the version rises",
			"version", version, "current", current, "read", resp.GetAppTablegroup())
		return
	}
	p.lastWarning = ""
	if p.routing.Update(resp.GetAppTablegroup(), resp.GetRoutingVersion()) {
		p.logger.InfoContext(ctx, "application routing changed",
			"tablegroup", resp.GetAppTablegroup(), "version", resp.GetRoutingVersion())
		if p.onChange != nil {
			p.onChange()
		}
	}
}
