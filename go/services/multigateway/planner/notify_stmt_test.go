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

package planner

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestPlan_NotifyStaysOnTheDefaultTableGroup: LISTEN subscribes on the default
// cohort, so NOTIFY must not follow the application tablegroup when routing
// moves it elsewhere.
func TestPlan_NotifyStaysOnTheDefaultTableGroup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	p.SetDefaultTableGroup("destTG")
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "NOTIFY chan, 'x'"
	plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
	require.NoError(t, err)
	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "expected Route, got %T", plan.Primitive)
	assert.Equal(t, "default", route.GetTableGroup())

	other := "SELECT 1"
	plan, err = p.Plan(other, parseOne(t, other), conn, PlanOptions{})
	require.NoError(t, err)
	route, ok = plan.Primitive.(*engine.Route)
	require.True(t, ok)
	assert.Equal(t, "destTG", route.GetTableGroup(), "application statements follow routing")
}
