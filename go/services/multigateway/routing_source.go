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

package multigateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/executor"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// A RoutingSource follows where the application's traffic goes and pushes what it learns to
// the gateway as versioned updates. Which tablegroup carries a database's application SQL
// is decided elsewhere (the sharding design), and how the gateway learns it is not decided
// here: nothing is registered by default, and application SQL then goes to the default
// tablegroup, as it always did. The gateway owns what an update does (the routing view,
// the planner's tablegroup, the requests that are waiting), so a source never touches
// those.
type RoutingSource interface {
	// RegisterFlags adds the source's flags, configured in reg so that the config file and
	// the environment apply to them as to every other flag of the gateway.
	RegisterFlags(fs *pflag.FlagSet, reg *viperutil.Registry)
	// Start begins following; it must not block, and stops what it started when ctx ends.
	// A source that was not configured does nothing.
	Start(ctx context.Context, sink RoutingSink, control ControlPlane, logger *slog.Logger) error
}

// RoutingSink is where a source pushes the routing it learns.
type RoutingSink interface {
	// Follow tells the gateway that the application routing of database comes from this
	// source: application queries for it fail closed, with an error that holds them in the
	// buffer, until the first Update. A gateway follows one database in this version, and
	// Follow of another is an error. Follow must be called from Start, synchronously, before
	// the gateway serves: wiring the routing into the pooler gateway is not synchronized
	// with the queries that read it.
	Follow(database string) error
	// Update records the application tablegroup of a database at a version. A version at
	// or below the one applied is ignored, so an older read arriving late changes nothing.
	// It reports whether the application tablegroup changed.
	Update(database, tablegroup string, version int64) (changed bool, err error)
}

// ControlPlane is how a source reaches the default primary, over the control path: a closed
// application gate does not hold such a request back.
type ControlPlane interface {
	DefaultPrimary(database string) (multipoolerservicepb.MultipoolerServiceClient, error)
}

// routingSourceFactories build the registered sources. Each gateway builds its own, so that
// two gateways in one process (tests, say) do not share the flag values of a source.
var routingSourceFactories []func() RoutingSource

// RegisterRoutingSource registers a source, by a function that builds one; it is meant to be
// called from an init function.
func RegisterRoutingSource(newSource func() RoutingSource) {
	routingSourceFactories = append(routingSourceFactories, newSource)
}

// gatewayRouting is the gateway's side of the sink and the control plane.
type gatewayRouting struct {
	executor *executor.Executor
	pg       *poolergateway.PoolerGateway

	mu       sync.Mutex
	database string
	routing  *poolergateway.AppRouting
}

func (g *gatewayRouting) Follow(database string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.routing != nil {
		if g.database == database {
			return nil
		}
		return fmt.Errorf("this gateway follows the application routing of database %q only, not %q", g.database, database)
	}
	routing := poolergateway.NewAppRouting(database)
	// Until the first update the planner targets a pending tablegroup that fails closed.
	g.executor.SetApplicationTableGroup(database, poolergateway.PendingAppTableGroup)
	// Plan against the new tablegroup first, in the same step as the routing view changes,
	// then release what was held for the old one.
	routing.OnChangeInStep(func(_, tg string) {
		g.executor.SetApplicationTableGroup(database, tg)
	})
	g.pg.SetAppRouting(routing)
	g.database, g.routing = database, routing
	return nil
}

func (g *gatewayRouting) Update(database, tablegroup string, version int64) (bool, error) {
	g.mu.Lock()
	routing, followed := g.routing, g.database
	g.mu.Unlock()
	if routing == nil || followed != database {
		return false, fmt.Errorf("the application routing of database %q is not being followed", database)
	}
	return routing.Update(tablegroup, version), nil
}

func (g *gatewayRouting) DefaultPrimary(database string) (multipoolerservicepb.MultipoolerServiceClient, error) {
	conn, err := g.pg.GetConnection(&query.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: database, TableGroup: constants.DefaultTableGroup, Shard: constants.DefaultShard},
		Mode:     query.Mode_MODE_WRITABLE,
		Control:  true,
	})
	if err != nil {
		return nil, err
	}
	return conn.ServiceClient(), nil
}
