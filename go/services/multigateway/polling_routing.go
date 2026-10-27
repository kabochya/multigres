// Copyright 2025 Supabase, Inc.
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

package multigateway

import (
	"context"
	"log/slog"
	"time"

	"github.com/spf13/pflag"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// PROTOTYPE STUB: a routing source that follows the routing state of the prototype metadata
// on the default primary, enabled by --routing-poll-database. The tablegroup application queries
// go to is the home tablegroup, or the destination of the rule that applies to it. It is the
// source that follows a migration; the static source (prototype_routing.go) never changes the
// tablegroup while the gateway runs. Delete this file when real routing propagation lands.

func init() { RegisterRoutingSource(func() RoutingSource { return &pollingRouting{} }) }

type pollingRouting struct {
	database viperutil.Value[string]
	interval viperutil.Value[time.Duration]
}

func (r *pollingRouting) RegisterFlags(fs *pflag.FlagSet, reg *viperutil.Registry) {
	r.database = viperutil.Configure(reg, "routing-poll-database", viperutil.Options[string]{
		Default:  "",
		FlagName: "routing-poll-database",
		Dynamic:  false,
	})
	r.interval = viperutil.Configure(reg, "routing-poll-interval", viperutil.Options[time.Duration]{
		Default:  500 * time.Millisecond,
		FlagName: "routing-poll-interval",
		Dynamic:  false,
	})
	fs.String("routing-poll-database", r.database.Default(), "follow the application routing of this database on the default primary: application queries go to the tablegroup its routing state resolves to and fail closed until it is known (prototype)")
	fs.Duration("routing-poll-interval", r.interval.Default(), "how often to read the application routing state")
	viperutil.BindFlags(fs, r.database, r.interval)
}

func (r *pollingRouting) Start(ctx context.Context, sink RoutingSink, control ControlPlane, logger *slog.Logger) error {
	db := r.database.Get()
	if db == "" {
		return nil
	}
	if err := sink.Follow(db); err != nil {
		return err
	}
	go (&routingPoller{sink: sink, control: control, database: db, interval: r.interval.Get(), logger: logger}).run(ctx)
	logger.InfoContext(ctx, "following application routing", "database", db)
	return nil
}

// routingPoller reads the routing state from the default primary and pushes the tablegroup it
// resolves to the sink.
//
// The gateway's routing is monotonic on purpose: the sink ignores a version at or below the one it
// holds, because the catalog publishes only what is durable, so that the version of a new catalog
// leader is never lower than the old one's, and a read from a default primary that has been deposed
// cannot roll the gateway back. The price is a known limit: a catalog whose catalog_state is seeded
// again starts its routing_version over, below what the gateways hold, and they keep the routing
// they have; recovering from it means restarting the gateways.
type routingPoller struct {
	sink     RoutingSink
	control  ControlPlane
	database string
	interval time.Duration
	logger   *slog.Logger
	// since is the routing_version the sink has applied, which lets the default primary answer a
	// read that would change nothing without the state.
	since int64
}

// effectiveTablegroup resolves where application traffic goes: the home tablegroup, or the
// destination of the rule that applies to it, applied once. It is empty when the database has no
// home tablegroup.
func effectiveTablegroup(resp *multipoolerservicepb.GetRoutingStateResponse) string {
	home := resp.GetHomeTablegroup()
	for _, r := range resp.GetRules() {
		if r.GetRule().GetFromTablegroup() == home {
			return r.GetRule().GetToTablegroup()
		}
	}
	return home
}

// run polls until ctx is cancelled. A failed read keeps the last known routing; before the
// first successful read the gateway fails closed.
func (p *routingPoller) run(ctx context.Context) {
	for {
		p.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.interval):
		}
	}
}

// pollOnce performs one read and pushes it. The sink ignores a version at or below the one
// applied, so an older read arriving late is harmless.
func (p *routingPoller) pollOnce(ctx context.Context) {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	client, err := p.control.DefaultPrimary(p.database)
	if err != nil {
		p.logger.DebugContext(ctx, "routing poll failed; keeping the last known routing", "error", err)
		return
	}
	resp, err := client.GetRoutingState(callCtx, &multipoolerservicepb.GetRoutingStateRequest{Database: p.database, SinceVersion: p.since})
	if err != nil {
		p.logger.DebugContext(ctx, "routing poll failed; keeping the last known routing", "error", err)
		return
	}
	if resp.GetNotModified() {
		return
	}
	tablegroup := effectiveTablegroup(resp)
	if tablegroup == "" {
		return
	}
	changed, err := p.sink.Update(p.database, tablegroup, resp.GetRoutingVersion())
	if err != nil {
		p.logger.WarnContext(ctx, "routing update refused", "error", err)
		return
	}
	p.since = resp.GetRoutingVersion()
	if changed {
		p.logger.InfoContext(ctx, "application routing changed",
			"tablegroup", tablegroup, "version", resp.GetRoutingVersion())
	}
}
