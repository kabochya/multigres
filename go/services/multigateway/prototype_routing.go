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
	"errors"
	"log/slog"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// PROTOTYPE STUB: a routing source that sends the application's SQL of one database to one
// tablegroup, both named in the gateway's configuration (--routing-database and
// --routing-tablegroup). It is how a gateway serves a database from an unmanaged
// tablegroup until the routing metadata that follows a migration lands: the tablegroup does not
// change while the gateway runs. Delete this file when real routing propagation lands.

func init() { RegisterRoutingSource(func() RoutingSource { return &staticRouting{} }) }

type staticRouting struct {
	database   viperutil.Value[string]
	tablegroup viperutil.Value[string]
}

func (r *staticRouting) RegisterFlags(fs *pflag.FlagSet, reg *viperutil.Registry) {
	r.database = viperutil.Configure(reg, "routing-database", viperutil.Options[string]{
		Default:  "",
		FlagName: "routing-database",
		Dynamic:  false,
	})
	r.tablegroup = viperutil.Configure(reg, "routing-tablegroup", viperutil.Options[string]{
		Default:  "",
		FlagName: "routing-tablegroup",
		Dynamic:  false,
	})
	fs.String("routing-database", r.database.Default(), "send the application queries of this database to --routing-tablegroup instead of the default tablegroup (prototype)")
	fs.String("routing-tablegroup", r.tablegroup.Default(), "the tablegroup that serves the application queries of --routing-database, for example one served by an unmanaged pooler (prototype)")
	viperutil.BindFlags(fs, r.database, r.tablegroup)
}

func (r *staticRouting) Start(ctx context.Context, sink RoutingSink, logger *slog.Logger) error {
	db, tg := r.database.Get(), r.tablegroup.Get()
	if db == "" && tg == "" {
		return nil
	}
	if db == "" || tg == "" {
		return errors.New("--routing-database and --routing-tablegroup are set together")
	}
	if err := sink.Follow(db); err != nil {
		return err
	}
	if _, err := sink.Update(db, tg, 1); err != nil {
		return err
	}
	logger.InfoContext(ctx, "application routing", "database", db, "tablegroup", tg)
	return nil
}
