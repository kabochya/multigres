// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connpoolmanager

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
)

const unexplainedQueryPattern = `SELECT pid FROM pg_stat_activity.*`

func pidRows(pids ...uint32) [][]any {
	rows := make([][]any, len(pids))
	for i, pid := range pids {
		rows[i] = []any{strconv.FormatUint(uint64(pid), 10)}
	}
	return rows
}

func TestUnexplainedBackendsSQL(t *testing.T) {
	require.Contains(t, unexplainedBackendsSQL, "backend_type = 'client backend' OR backend_type IS NULL",
		"a role that cannot see other roles' sessions gets a NULL backend_type for them")
	require.Contains(t, unexplainedBackendsSQL, "pid <> pg_backend_pid()")
	require.Contains(t, unexplainedBackendsSQL, "datname = current_database()", "only this database")
	require.Contains(t, unexplainedBackendsSQL, "state IS DISTINCT FROM 'idle'", "an unreadable state is not idle")
	require.NotContains(t, unexplainedBackendsSQL, "current_user", "the pooler's own role, and its admin role, are looked at: its connections are explained by the snapshot")
	require.NotContains(t, unexplainedBackendsSQL, "usename")
}

// TestUnexplainedBackends: a backend that is not idle is explained when it is the
// backend of one of the pooler's open connections, and not otherwise, including when
// the connection that had it is closed.
func TestUnexplainedBackends(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	m := newTestManagerWithConfig(t, server, nil)
	defer m.Close()
	ctx := context.Background()

	conn, err := m.GetRegularConn(ctx, "app", nil, nil)
	require.NoError(t, err)
	pid := conn.Conn.ProcessID()
	require.NotZero(t, pid)

	look := func() (int, error) {
		c, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		return m.UnexplainedBackends(c)
	}

	server.AddQueryPattern(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, nil))
	n, err := look()
	require.NoError(t, err)
	require.Zero(t, n, "nothing is running")

	server.RemoveQueryPattern(unexplainedQueryPattern)
	server.AddQueryPattern(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows(pid)))
	n, err = look()
	require.NoError(t, err)
	require.Zero(t, n, "a borrowed connection's backend is the pooler's own")

	conn.Recycle()
	n, err = look()
	require.NoError(t, err)
	require.Zero(t, n, "so is an idle one's")

	server.RemoveQueryPattern(unexplainedQueryPattern)
	server.AddQueryPattern(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows(pid, pid+100000)))
	n, err = look()
	require.NoError(t, err)
	require.Equal(t, 1, n, "a backend nothing explains")

	// A connection closed under a statement no longer explains its backend.
	conn2, err := m.GetRegularConn(ctx, "app", nil, nil)
	require.NoError(t, err)
	pid2 := conn2.Conn.ProcessID()
	server.RemoveQueryPattern(unexplainedQueryPattern)
	server.AddQueryPattern(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows(pid2)))
	n, err = look()
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, conn2.Conn.Close())
	conn2.Taint()
	n, err = look()
	require.NoError(t, err)
	require.Equal(t, 1, n, "the pooler closed that connection, and its backend is still running")
}

// TestUnexplainedBackendsRidesOutAConnectionBeingOpened: a backend that is unexplained
// for a moment (its connection is not yet in the snapshot) is looked for again before
// the fence is told it is there.
func TestUnexplainedBackendsRidesOutAConnectionBeingOpened(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	m := newTestManagerWithConfig(t, server, nil)
	defer m.Close()
	ctx := context.Background()

	conn, err := m.GetRegularConn(ctx, "app", nil, nil)
	require.NoError(t, err)
	pid := conn.Conn.ProcessID()
	conn.Recycle()

	server.AddQueryPattern(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows(pid+100000)))
	go func() {
		time.Sleep(250 * time.Millisecond)
		server.RemoveQueryPattern(unexplainedQueryPattern)
		server.AddQueryPattern(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows(pid)))
	}()
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	n, err := m.UnexplainedBackends(c)
	require.NoError(t, err)
	require.Zero(t, n)
}

// TestUnexplainedBackendsFailsClosed: a look that cannot be made is an error, never zero.
func TestUnexplainedBackendsFailsClosed(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	m := newTestManagerWithConfig(t, server, nil)
	defer m.Close()

	server.RejectQueryPattern(unexplainedQueryPattern, "permission denied for view pg_stat_activity")
	c, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := m.UnexplainedBackends(c)
	require.Error(t, err)
}

// TestUnexplainedBackendsReportsWhatItFoundWhenTheDeadlineCutsTheLastAttempt: an attempt that
// completed and found a backend nothing explains is still the answer when a later attempt is
// cut short by the deadline: the look says there are some, not that it failed.
func TestUnexplainedBackendsReportsWhatItFoundWhenTheDeadlineCutsTheLastAttempt(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	m := newTestManagerWithConfig(t, server, nil)
	defer m.Close()

	var calls atomic.Int32
	server.AddQueryPatternWithCallback(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows(4_000_000)), func(string) {
		if calls.Add(1) > 1 {
			time.Sleep(2 * time.Second) // every attempt after the first is slow
		}
	})
	c, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	n, err := m.UnexplainedBackends(c)
	require.NoError(t, err, "the first attempt completed and found one")
	require.Equal(t, 1, n)
	require.GreaterOrEqual(t, calls.Load(), int32(2), "a later attempt was started and cut short")

	// With nothing found before the deadline, it is an error, never zero.
	server.RemoveQueryPattern(unexplainedQueryPattern)
	server.AddQueryPatternWithCallback(unexplainedQueryPattern, fakepgserver.MakeResult([]string{"pid"}, pidRows()), func(string) { time.Sleep(2 * time.Second) })
	c2, cancel2 := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel2()
	_, err = m.UnexplainedBackends(c2)
	require.Error(t, err)
}
