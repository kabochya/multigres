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

package queryserving

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// A session's reserved connections must be live. When the pooler loses the connection
// that holds state of the client's session (a temporary table, a session advisory lock),
// the session ends with FATAL 57P01 so the client reconnects, instead of carrying on
// on a fresh backend without that state. When it only held a transaction, the client
// gets 40001 and retries the transaction.
//
// The loss is caused four ways: the pooler reaps the idle reservation, the backend is
// terminated, a planned failover, and a SIGKILLed primary. Each is run on the simple
// protocol and on two extended-protocol modes of pgx.

const rlLockKey = 7142

var rlModes = []struct {
	name string
	mode pgx.QueryExecMode
}{
	{"simple", pgx.QueryExecModeSimpleProtocol},
	{"cache_statement", pgx.QueryExecModeCacheStatement},
	{"exec_unnamed", pgx.QueryExecModeExec},
}

// rlSession is one client connection of a test.
type rlSession struct {
	conn *pgx.Conn
}

func rlOpen(t *testing.T, ctx context.Context, dsn string, mode pgx.QueryExecMode) *rlSession {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = mode
	c, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close(context.Background()) })
	return &rlSession{conn: c}
}

// run executes sql in the session's protocol mode (Query, since pgx runs an Exec
// without arguments in the simple protocol) and returns the first column of the first
// row as text, "" when there are none.
func (s *rlSession) run(ctx context.Context, sql string, args ...any) (string, error) {
	c, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := s.conn.Query(c, sql, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var out string
	if rows.Next() {
		vals, verr := rows.Values()
		if verr != nil {
			return "", verr
		}
		out = fmt.Sprint(vals[0])
	}
	rows.Close()
	return out, rows.Err()
}

func (s *rlSession) mustRun(t *testing.T, ctx context.Context, sql string, args ...any) string {
	t.Helper()
	out, err := s.run(ctx, sql, args...)
	require.NoError(t, err, "%s", sql)
	return out
}

func rlPgError(t *testing.T, err error) *pgconn.PgError {
	t.Helper()
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "expected a PostgreSQL error, got %T: %v", err, err)
	return pgErr
}

// requireSessionEnded asserts that err is FATAL 57P01 naming the lost state, and that the
// gateway closed the client's connection.
func (s *rlSession) requireSessionEnded(t *testing.T, ctx context.Context, err error, lostState string) {
	t.Helper()
	require.Error(t, err, "the statement after the loss must fail")
	pgErr := rlPgError(t, err)
	require.Equal(t, "FATAL", pgErr.Severity, "got %+v", pgErr)
	require.Equal(t, "57P01", pgErr.Code, "got %+v", pgErr)
	require.Contains(t, pgErr.Message, lostState)
	require.Contains(t, pgErr.Message, "reconnect")

	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = s.conn.Exec(c, "SELECT 1")
	require.Error(t, err, "the gateway must have closed the connection after the FATAL")
	require.True(t, s.conn.IsClosed())
}

func rlAdminQuery(t *testing.T, ctx context.Context, pgPort int, sql string) string {
	t.Helper()
	dsn := shardsetup.GetTestUserDSN("localhost", pgPort, "sslmode=disable", "connect_timeout=5")
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "ADMIN CONNECT ERROR: " + err.Error()
	}
	defer c.Close(ctx)
	var out string
	if err := c.QueryRow(ctx, sql, pgx.QueryExecModeSimpleProtocol).Scan(&out); err != nil {
		return "ADMIN QUERY ERROR: " + err.Error()
	}
	return out
}

// rlEnv is a cluster and a way to lose the reserved connection of a session.
type rlEnv struct {
	setup *shardsetup.ShardSetup
	// cause makes the pooler lose the reservation of the session whose backend has the
	// given pid. pgPort is the postgres the backend lives on.
	cause func(t *testing.T, ctx context.Context, pid string, pgPort int)
}

func (e *rlEnv) dsn() string {
	return shardsetup.GetTestUserDSN("localhost", e.setup.ClientPort(), "sslmode=disable", "connect_timeout=5")
}

// lose runs a scenario on a session of the mode: establish builds the session's
// state and the statement the client is going to run next, after the loss, is returned
// by establish as well.
func (e *rlEnv) lose(t *testing.T, ctx context.Context, mode pgx.QueryExecMode, establish func(a *rlSession) (next string)) (*rlSession, error) {
	t.Helper()
	a := rlOpen(t, ctx, e.dsn(), mode)
	next := establish(a)
	pid := a.mustRun(t, ctx, "SELECT pg_backend_pid()")
	port := e.setup.RefreshPrimary(t).Pgctld.PgPort
	e.cause(t, ctx, pid, port)
	_, err := a.run(ctx, next)
	return a, err
}

// scenario: session advisory lock. The lock must be free once the session ended.
func (e *rlEnv) advisoryLock(t *testing.T, ctx context.Context, mode pgx.QueryExecMode) {
	t.Helper()
	b := rlOpen(t, ctx, e.dsn(), pgx.QueryExecModeSimpleProtocol)
	a, err := e.lose(t, ctx, mode, func(a *rlSession) string {
		a.mustRun(t, ctx, "SELECT pg_advisory_lock($1)", int64(rlLockKey))
		require.Equal(t, "false", b.mustRun(t, ctx, "SELECT pg_try_advisory_lock($1)", int64(rlLockKey)),
			"B must not get the lock while A holds it")
		return "SELECT 1"
	})
	a.requireSessionEnded(t, ctx, err, "session_advisory_lock")

	// The lock is gone with the backend, and a client that retries on 40001 is not
	// let into the critical section: its next statement would have failed above.
	require.Equal(t, "true", b.mustRun(t, ctx, "SELECT pg_try_advisory_lock($1)", int64(rlLockKey)), "the lock must be free")
	b.mustRun(t, ctx, "SELECT pg_advisory_unlock($1)", int64(rlLockKey))
}

// scenario: a temporary table created with IF NOT EXISTS. Without the FATAL the next
// CREATE TEMP TABLE IF NOT EXISTS silently makes an empty table.
func (e *rlEnv) tempTableIfNotExists(t *testing.T, ctx context.Context, mode pgx.QueryExecMode, name string) {
	t.Helper()
	a, err := e.lose(t, ctx, mode, func(a *rlSession) string {
		a.mustRun(t, ctx, fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (id int)", name))
		a.mustRun(t, ctx, fmt.Sprintf("INSERT INTO %s VALUES (1)", name))
		return fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (id int)", name)
	})
	a.requireSessionEnded(t, ctx, err, "temp_table")
}

// scenario: a temporary table shadows a permanent one under its unqualified name. Without
// the FATAL the statement after the loss reads or writes the permanent table.
func (e *rlEnv) tempTableShadowsPermanent(t *testing.T, ctx context.Context, mode pgx.QueryExecMode, name string) {
	t.Helper()
	admin := rlOpen(t, ctx, e.dsn(), pgx.QueryExecModeSimpleProtocol)
	admin.mustRun(t, ctx, fmt.Sprintf("CREATE TABLE public.%s (id int)", name))
	admin.mustRun(t, ctx, fmt.Sprintf("INSERT INTO public.%s VALUES (1)", name))
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.conn.Exec(c, fmt.Sprintf("DROP TABLE IF EXISTS public.%s", name))
	})

	a, err := e.lose(t, ctx, mode, func(a *rlSession) string {
		a.mustRun(t, ctx, fmt.Sprintf("CREATE TEMP TABLE %s (id int)", name))
		a.mustRun(t, ctx, fmt.Sprintf("INSERT INTO %s VALUES (100)", name))
		require.Equal(t, "100", a.mustRun(t, ctx, fmt.Sprintf("SELECT max(id) FROM %s", name)), "the temp table shadows")
		return fmt.Sprintf("INSERT INTO %s VALUES (999)", name)
	})
	a.requireSessionEnded(t, ctx, err, "temp_table")

	require.Equal(t, "1", admin.mustRun(t, ctx, fmt.Sprintf("SELECT count(*) FROM public.%s", name)), "the permanent table is untouched")
	require.Equal(t, "1", admin.mustRun(t, ctx, fmt.Sprintf("SELECT max(id) FROM public.%s", name)))
}

// scenario: only a transaction is lost. The client gets 40001, the transaction is failed
// until ROLLBACK, the connection stays open and new statement texts work afterwards (no
// statement keeps failing with 40001).
func (e *rlEnv) transactionOnly(t *testing.T, ctx context.Context, mode pgx.QueryExecMode) {
	t.Helper()
	a := rlOpen(t, ctx, e.dsn(), mode)
	_, err := a.conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	pid := a.mustRun(t, ctx, "SELECT pg_backend_pid()")
	e.cause(t, ctx, pid, e.setup.RefreshPrimary(t).Pgctld.PgPort)

	_, err = a.run(ctx, "SELECT 4001")
	require.Error(t, err)
	pgErr := rlPgError(t, err)
	require.Equal(t, "ERROR", pgErr.Severity, "a lost transaction does not end the session: %+v", pgErr)
	require.Equal(t, "40001", pgErr.Code, "got %+v", pgErr)
	require.False(t, a.conn.IsClosed())

	// The transaction is failed: new statements are refused, not failed with 40001 again.
	for i := 0; i < 3; i++ {
		_, err = a.run(ctx, fmt.Sprintf("SELECT %d", 4100+i))
		require.Error(t, err)
		require.NotEqual(t, "40001", rlPgError(t, err).Code, "a stale reservation must not keep failing statements: %v", err)
	}
	_, err = a.conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.Equal(t, fmt.Sprint(4200+i), a.mustRun(t, ctx, fmt.Sprintf("SELECT %d", 4200+i)), "new statement texts work after ROLLBACK")
	}
	// and the next transaction runs on the backend that has the traffic
	_, err = a.conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	require.Equal(t, "1", a.mustRun(t, ctx, "SELECT 1"))
	_, err = a.conn.Exec(ctx, "COMMIT")
	require.NoError(t, err)
}

// scenario: the sticky 40001 bug. Describe never cleared the lost reservation, so
// with pgx's default mode every statement of a new text failed with 40001 for as long as
// the session lived. A session that held only a transaction recovers; one that held state
// is ended.
func (e *rlEnv) describeDoesNotStick(t *testing.T, ctx context.Context) {
	t.Helper()
	a := rlOpen(t, ctx, e.dsn(), pgx.QueryExecModeCacheStatement)
	a.mustRun(t, ctx, "SELECT pg_advisory_lock($1)", int64(rlLockKey+1))
	pid := a.mustRun(t, ctx, "SELECT pg_backend_pid()")
	e.cause(t, ctx, pid, e.setup.RefreshPrimary(t).Pgctld.PgPort)
	_, err := a.run(ctx, "SELECT 5001") // a text pgx has not described yet
	a.requireSessionEnded(t, ctx, err, "session_advisory_lock")
}

// causes for a cluster of two poolers with a short reserved-connection inactivity timeout.
func rlIdleReap(t *testing.T, ctx context.Context, _ string, _ int) {
	time.Sleep(6 * time.Second) // > 2s inactivity timeout; the reaper ticks at timeout/10
}

func rlTerminateBackend(t *testing.T, ctx context.Context, pid string, pgPort int) {
	t.Logf("pg_terminate_backend(%s): %s", pid, rlAdminQuery(t, ctx, pgPort, "SELECT pg_terminate_backend("+pid+")"))
	time.Sleep(time.Second)
}

func TestReservationLoss_IdleReapAndTerminate(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultipoolerExtraArgs("--connpool-user-reserved-inactivity-timeout=2s"),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)
	ctx := utils.WithTimeout(t, 20*time.Minute)

	causes := []struct {
		name  string
		cause func(t *testing.T, ctx context.Context, pid string, pgPort int)
	}{
		{"idle_reap", rlIdleReap},
		{"terminate_backend", rlTerminateBackend},
	}
	for _, c := range causes {
		env := &rlEnv{setup: setup, cause: c.cause}
		for _, m := range rlModes {
			prefix := c.name + "/" + m.name + "/"
			tag := strings.ReplaceAll(c.name+"_"+m.name, "-", "_")
			t.Run(prefix+"advisory_lock", func(t *testing.T) { env.advisoryLock(t, ctx, m.mode) })
			t.Run(prefix+"temp_table_if_not_exists", func(t *testing.T) { env.tempTableIfNotExists(t, ctx, m.mode, "rl_tmp_"+tag) })
			t.Run(prefix+"temp_table_shadows_permanent", func(t *testing.T) { env.tempTableShadowsPermanent(t, ctx, m.mode, "rl_shadow_"+tag) })
			t.Run(prefix+"transaction_only", func(t *testing.T) { env.transactionOnly(t, ctx, m.mode) })
		}
		t.Run(c.name+"/describe_does_not_stick", func(t *testing.T) { env.describeDoesNotStick(t, ctx) })
	}
}

func newReservationLossFailoverCluster(t *testing.T) (*shardsetup.ShardSetup, func()) {
	t.Helper()
	clustersetup.RequireMultigresTopology(t, "failover")
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithMultiadmin(),
		// A long inactivity timeout so the idle reaper cannot be the cause of the loss.
		shardsetup.WithMultipoolerExtraArgs("--connpool-drain-grace-period=1s", "--connpool-user-reserved-inactivity-timeout=10m"),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForQueryServing(t)
	require.NotNil(t, setup.GetPrimary(t))
	return setup, cleanup
}

func TestReservationLoss_PlannedFailover(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup, cleanup := newReservationLossFailoverCluster(t)
	defer cleanup()
	ctx := utils.WithTimeout(t, 30*time.Minute)
	env := &rlEnv{setup: setup, cause: func(t *testing.T, ctx context.Context, _ string, _ int) {
		triggerFailover(t, setup)
		time.Sleep(2 * time.Second)
	}}
	for _, m := range rlModes {
		t.Run(m.name+"/advisory_lock", func(t *testing.T) { env.advisoryLock(t, ctx, m.mode) })
		t.Run(m.name+"/transaction_only", func(t *testing.T) { env.transactionOnly(t, ctx, m.mode) })
	}
}

func TestReservationLoss_UnplannedPrimaryKill(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup, cleanup := newReservationLossFailoverCluster(t)
	defer cleanup()
	ctx := utils.WithTimeout(t, 30*time.Minute)
	env := &rlEnv{setup: setup, cause: func(t *testing.T, ctx context.Context, _ string, _ int) {
		primary := setup.GetPrimary(t).Name
		setup.KillPostgres(t, primary)
		require.Eventually(t, func() bool {
			c, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
			if err != nil {
				return false
			}
			defer c.Close(ctx)
			var n int
			return c.QueryRow(ctx, "SELECT 1").Scan(&n) == nil
		}, 90*time.Second, time.Second, "the gateway must serve again after the primary was killed")
		t.Logf("primary was %s; primary after recovery: %s", primary, setup.RefreshPrimary(t).Name)
	}}
	for _, m := range rlModes {
		t.Run(m.name+"/advisory_lock", func(t *testing.T) { env.advisoryLock(t, ctx, m.mode) })
		t.Run(m.name+"/transaction_only", func(t *testing.T) { env.transactionOnly(t, ctx, m.mode) })
	}
}
