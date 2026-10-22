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
	"log/slog"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

func newStaticRouting(t *testing.T, args ...string) *staticRouting {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	r := &staticRouting{}
	r.RegisterFlags(fs, viperutil.NewRegistry())
	require.NoError(t, fs.Parse(args))
	return r
}

// TestTheStaticRoutingSourceIsRegisteredAndOffUnlessConfigured: it adds its two flags, and
// without them it starts nothing, so the gateway routes application SQL to the default
// tablegroup as it always did.
func TestTheStaticRoutingSourceIsRegisteredAndOffUnlessConfigured(t *testing.T) {
	var found int
	for _, newSource := range routingSourceFactories {
		if _, ok := newSource().(*staticRouting); ok {
			found++
		}
	}
	require.Equal(t, 1, found, "registered by its init function")

	// Each gateway builds a source of its own: the flag values of one are not those of another.
	var factory func() RoutingSource
	for _, newSource := range routingSourceFactories {
		if _, ok := newSource().(*staticRouting); ok {
			factory = newSource
		}
	}
	one, two := factory().(*staticRouting), factory().(*staticRouting)
	require.NotSame(t, one, two)
	fsOne, fsTwo := pflag.NewFlagSet("one", pflag.ContinueOnError), pflag.NewFlagSet("two", pflag.ContinueOnError)
	one.RegisterFlags(fsOne, viperutil.NewRegistry())
	two.RegisterFlags(fsTwo, viperutil.NewRegistry())
	require.NoError(t, fsOne.Parse([]string{"--routing-database=one"}))
	require.NoError(t, fsTwo.Parse([]string{"--routing-database=two"}))
	require.Equal(t, "one", one.database.Get())
	require.Equal(t, "two", two.database.Get())

	off := newStaticRouting(t)
	sink := newTestGatewayRouting()
	require.NoError(t, off.Start(t.Context(), sink, slog.Default()), "not configured: nothing started, nothing needed")
	_, err := sink.Update("app", "migrateTG", 1)
	require.Error(t, err, "nothing is followed")
}

// TestTheStaticRoutingSourceSendsTheDatabaseToItsTablegroup: configured, the source follows
// the database and sets its tablegroup before the gateway serves.
func TestTheStaticRoutingSourceSendsTheDatabaseToItsTablegroup(t *testing.T) {
	r := newStaticRouting(t, "--routing-database=app", "--routing-tablegroup=migrateTG")
	require.Equal(t, "app", r.database.Get())
	require.Equal(t, "migrateTG", r.tablegroup.Get())

	sink := newTestGatewayRouting()
	require.NoError(t, r.Start(t.Context(), sink, slog.Default()))
	tg, known := sink.routing.Current()
	require.True(t, known, "known when Start returns, so no request meets an unknown routing")
	require.Equal(t, "migrateTG", tg)
}

// TestTheStaticRoutingSourceNeedsBothFlags: one of the two alone is a mistake, not a default.
func TestTheStaticRoutingSourceNeedsBothFlags(t *testing.T) {
	for _, args := range [][]string{{"--routing-database=app"}, {"--routing-tablegroup=migrateTG"}} {
		require.ErrorContains(t, newStaticRouting(t, args...).Start(t.Context(), newTestGatewayRouting(), slog.Default()), "set together")
	}
}
