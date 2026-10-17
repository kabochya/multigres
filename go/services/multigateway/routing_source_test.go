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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multigateway/executor"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
)

func newTestGatewayRouting() *gatewayRouting {
	return &gatewayRouting{
		executor: executor.NewExecutor(nil, slog.New(slog.DiscardHandler), 0),
		pg:       &poolergateway.PoolerGateway{},
	}
}

// TestSinkAppliesVersionedUpdatesOfTheDatabaseItFollows: a source pushes updates for a
// database it asked the gateway to follow; an older version changes nothing, and nothing is
// accepted for a database that is not followed.
func TestSinkAppliesVersionedUpdatesOfTheDatabaseItFollows(t *testing.T) {
	g := newTestGatewayRouting()

	_, err := g.Update("db", "tg", 1)
	require.Error(t, err, "nothing is followed yet")

	require.NoError(t, g.Follow("db"))
	require.NoError(t, g.Follow("db"), "following twice is following once")
	require.ErrorContains(t, g.Follow("other"), "follows the application routing of database \"db\" only")

	changed, err := g.Update("db", "migrateTG", 2)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = g.Update("db", "default", 1)
	require.NoError(t, err)
	require.False(t, changed, "a version below the applied one is ignored")
	changed, err = g.Update("db", "migrateTG", 2)
	require.NoError(t, err)
	require.False(t, changed, "the same version changes nothing")
	changed, err = g.Update("db", "default", 3)
	require.NoError(t, err)
	require.True(t, changed)

	_, err = g.Update("other", "tg", 9)
	require.Error(t, err)
}
