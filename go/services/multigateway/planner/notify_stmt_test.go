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
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	p.SetApplicationTableGroup(conn.Database(), "destTG")

	sql := "NOTIFY chan, 'x'"
	plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
	require.NoError(t, err)
	notify, ok := plan.Primitive.(*engine.NotifyPrimitive)
	require.True(t, ok, "expected NotifyPrimitive, got %T", plan.Primitive)
	assert.Equal(t, "default", notify.GetTableGroup())
	assert.True(t, plan.ExecInfo.Control, "NOTIFY is control traffic: application routing never moves it")
	assert.True(t, notify.DefersInTransaction(), "inside a transaction it waits for the commit while the application is elsewhere")

	other := "SELECT 1"
	plan, err = p.Plan(other, parseOne(t, other), conn, PlanOptions{})
	require.NoError(t, err)
	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok)
	assert.Equal(t, "destTG", route.GetTableGroup(), "application statements follow routing")
	assert.False(t, plan.ExecInfo.Control, "application statements are not control traffic")
}

// TestPlan_NotifyIsNotDeferredOnceTheApplicationIsOnTheDefaultTableGroup: after a
// cutover into the default tablegroup a NOTIFY in a transaction runs on the
// transaction's own backend, atomically, so nothing is held back.
func TestPlan_NotifyIsNotDeferredOnceTheApplicationIsOnTheDefaultTableGroup(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	sql := "NOTIFY chan, 'x'"
	plan, err := p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
	require.NoError(t, err)
	notify, ok := plan.Primitive.(*engine.NotifyPrimitive)
	require.True(t, ok)
	assert.False(t, notify.DefersInTransaction())
	assert.True(t, plan.ExecInfo.Control)

	// And the rollback of that cutover: the application is elsewhere again.
	p.SetApplicationTableGroup(conn.Database(), "migrateTG")
	plan, err = p.Plan(sql, parseOne(t, sql), conn, PlanOptions{})
	require.NoError(t, err)
	assert.True(t, plan.Primitive.(*engine.NotifyPrimitive).DefersInTransaction())
}
