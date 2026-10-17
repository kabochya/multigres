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

package shardsetup

// PROTOTYPE STUB: end-to-end scenarios for the look a fence makes for backends that nothing
// of the pooler's explains, against a real PostgreSQL. Delete them with the prototype.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/stretchr/testify/require"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	mig "github.com/multigres/multigres/go/test/migrator"
)

// connectSendingCancelRequests connects with a driver that, when a statement's context
// is cancelled, sends the server a cancel request. pgx's default only closes the
// socket, which is a different thing for a pooler to see: no cancel is attempted.
func connectSendingCancelRequests(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: pgConn, DeadlineDelay: 10 * time.Second}
	}
	return pgx.ConnectConfig(ctx, cfg)
}

// terminateBackends ends the backends of the source whose query carries marker and waits for
// them to be gone. It is meant for cleanups too, which run after the test's own context ended.
func terminateBackends(t *testing.T, admin *pgx.Conn, marker string) {
	t.Helper()
	ctx := context.WithoutCancel(t.Context())
	_, err := admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%"+marker+"%' AND pid <> pg_backend_pid()")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var n int
		err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%"+marker+"%' AND pid <> pg_backend_pid()").Scan(&n)
		return err == nil && n == 0
	}, 20*time.Second, 200*time.Millisecond, "the backends of %s are still running", marker)
}

// scenarioTheLookForUnexplainedBackends: before a fence says a tablegroup is quiet it looks,
// on the server, for client backends that nothing of the pooler's owns. Each case runs on the
// same cluster, one after the other, and leaves the tablegroup unfenced:
//
//   - a statement that swallows the cancel the pooler sends it, and keeps running after the
//     pooler's connection to it is closed, makes the fence busy;
//   - another role's running statement does too (the pooler's role sees it because it has
//     pg_read_all_stats), and the migrator waits for it;
//   - a session in another database is not the database's work, and does not.
func scenarioTheLookForUnexplainedBackends(t *testing.T, f *migrationFixture) {
	ctx := t.Context()

	t.Run("a statement that swallows the cancel", func(t *testing.T) {
		f := f.with(t)
		conn, err := connectSendingCancelRequests(ctx, appDSN(f.gatewayPort))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		stmtCtx, cancelStmt := context.WithCancel(ctx)
		stmtDone := make(chan error, 1)
		go func() {
			// Everything the loop does is inside nested handlers: the client's cancel and the
			// gateway's can both arrive, a few milliseconds apart, and the second can land
			// while the first is being handled.
			_, err := conn.Exec(stmtCtx, `DO $$ /* ignores-cancel-marker */
DECLARE stop timestamptz := clock_timestamp() + interval '120 seconds';
BEGIN
  LOOP
    BEGIN
      BEGIN
        BEGIN
          IF clock_timestamp() >= stop THEN EXIT; END IF;
          PERFORM pg_sleep(0.2);
        EXCEPTION WHEN query_canceled THEN NULL;
        END;
      EXCEPTION WHEN query_canceled THEN NULL;
      END;
    EXCEPTION WHEN query_canceled THEN NULL;
    END;
  END LOOP;
END $$`)
			stmtDone <- err
		}()
		t.Cleanup(func() { terminateBackends(t, f.srcAdmin, "ignores-cancel-marker") })
		require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "ignores-cancel-marker") == 1 },
			20*time.Second, 200*time.Millisecond, "the statement never started")
		// The statement is inside its handlers (asleep), not still initializing: a cancel before
		// that would end it.
		require.Eventually(t, func() bool {
			var n int
			require.NoError(t, f.srcAdmin.QueryRow(ctx,
				"SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%ignores-cancel-marker%' AND wait_event = 'PgSleep' AND pid <> pg_backend_pid()").Scan(&n))
			return n == 1
		}, 20*time.Second, 50*time.Millisecond, "the statement never reached its sleep")

		// The client gives up: the pooler cancels the statement, which swallows it, and after
		// its grace closes the connection on a backend that is still running.
		cancelStmt()
		select {
		case <-stmtDone:
		case <-time.After(30 * time.Second):
			t.Fatal("the client's statement never returned")
		}
		require.Equal(t, 1, activeBackends(t, f.srcAdmin, "ignores-cancel-marker"), "the statement is still running on the server")

		before := f.control().version(ctx, "migrateTG")
		_, err = f.control().fenceAt(ctx, "migrateTG", before, 60*time.Second)
		requireMember(t, requireFenceNotClean(t, err), "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "1 unexplained backends present")
		requireStillFenced(t, f, "migrateTG", before, f.src.grpcPort)
		require.Equal(t, 1, activeBackends(t, f.srcAdmin, "ignores-cancel-marker"), "the backend was left alone")

		// The backend is gone: the same fence, retried, is clean.
		terminateBackends(t, f.srcAdmin, "ignores-cancel-marker")
		resp, err := f.control().fenceWithin(ctx, "migrateTG", 60*time.Second)
		require.NoError(t, err)
		require.Equal(t, admFenced, resp.State.State)
		require.EqualValues(t, before+1, resp.State.Version, "the retry wrote nothing")
		require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", resp.State.Version))
	})

	t.Run("another role's running statement", func(t *testing.T) {
		f := f.with(t)
		_, err := f.srcAdmin.Exec(ctx, "CREATE ROLE bystander LOGIN PASSWORD 'bystander-secret'")
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = f.srcAdmin.Exec(context.WithoutCancel(ctx), "DROP ROLE IF EXISTS bystander") })
		bystander, err := pgx.Connect(ctx, fmt.Sprintf("postgres://bystander:bystander-secret@127.0.0.1:%d/postgres?sslmode=disable", f.srcPort))
		require.NoError(t, err)
		t.Cleanup(func() { _ = bystander.Close(context.Background()) })

		// Idle, it holds nothing back.
		_, err = bystander.Exec(ctx, "SELECT 1")
		require.NoError(t, err)
		resp, err := f.control().fenceWithin(ctx, "migrateTG", 60*time.Second)
		require.NoError(t, err, "an idle session of another role does not hold the fence back")
		require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", resp.State.Version))

		// Running, it makes the fence busy, whatever role it is.
		stopBystander, cancelBystander := context.WithCancel(ctx)
		t.Cleanup(cancelBystander)
		go func() { _, _ = bystander.Exec(stopBystander, "SELECT pg_sleep(300), 'bystander-marker'") }()
		t.Cleanup(func() { terminateBackends(t, f.srcAdmin, "bystander-marker") })
		require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "bystander-marker") == 1 }, 20*time.Second, 200*time.Millisecond)

		before := f.control().version(ctx, "migrateTG")
		_, err = f.control().fenceAt(ctx, "migrateTG", before, 60*time.Second)
		requireMember(t, requireFenceNotClean(t, err), "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "1 unexplained backends present")
		require.Equal(t, 1, activeBackends(t, f.srcAdmin, "bystander-marker"), "and it is left alone")

		// The reference migrator waits for it: the same fence, retried, until the statement ends.
		fenced := make(chan error, 1)
		go func() {
			_, err := f.lib(func(c *mig.Config) { c.GiveUpAfter = 60 * time.Second }).FenceOrGiveUp(ctx, "migrateTG")
			fenced <- err
		}()
		select {
		case err := <-fenced:
			t.Fatalf("the fence completed (%v) while another role's statement was running", err)
		case <-time.After(3 * time.Second):
		}
		cancelBystander()
		terminateBackends(t, f.srcAdmin, "bystander-marker")
		require.NoError(t, <-fenced, "once the statement is over the retried fence is clean")
		require.EqualValues(t, before+1, f.control().version(ctx, "migrateTG"), "the retries wrote nothing")
		require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", before+1))
	})

	t.Run("a session in another database", func(t *testing.T) {
		f := f.with(t)
		_, err := f.srcAdmin.Exec(ctx, "CREATE DATABASE elsewhere")
		require.NoError(t, err)
		t.Cleanup(func() {
			bg := context.WithoutCancel(ctx)
			_, _ = f.srcAdmin.Exec(bg, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'elsewhere'")
			_, err := f.srcAdmin.Exec(bg, "DROP DATABASE IF EXISTS elsewhere WITH (FORCE)")
			require.NoError(t, err, "dropping the other database")
		})
		other, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/elsewhere?sslmode=disable", externalPostgresPassword, f.srcPort))
		require.NoError(t, err)
		t.Cleanup(func() { _ = other.Close(context.Background()) })
		stop, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() { _, _ = other.Exec(stop, "SELECT pg_sleep(300), 'elsewhere-marker'") }()
		require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "elsewhere-marker") == 1 }, 20*time.Second, 200*time.Millisecond)

		resp, err := f.control().fenceWithin(ctx, "migrateTG", 60*time.Second)
		require.NoError(t, err, "a statement in another database is not the pooler's database's work")
		require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", resp.State.Version))
	})
}

// TestRestartedPoolerStillFindsABackendItsPredecessorLeftRunning: the pooler crashes while a
// statement is running; the server notices nothing until the statement tries to answer. The
// pooler that replaces it has no memory of the connection, and the fence still finds the
// backend, because it asks the server and not its own pools.
func TestRestartedPoolerStillFindsABackendItsPredecessorLeftRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)

	busy := appConnWhenRouted(t, f.gatewayPort)
	t.Cleanup(func() { _ = busy.Close(context.Background()) })
	go func() {
		_, _ = busy.Exec(ctx, "INSERT INTO ledger SELECT -3, 7 FROM (SELECT pg_sleep(120), 'restart-marker') s")
	}()
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "restart-marker") == 1 },
		20*time.Second, 200*time.Millisecond, "the statement never started")

	// The pooler crashes. The server notices nothing until the statement tries to answer.
	f.src.kill(t)
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "restart-marker"), "the statement is still running on the server")

	restarted := startUnmanagedPooler(t, f.def, "mig-src-1", "postgres", "mig-src")
	waitBackendReady(t, restarted)
	requireAdmits(t, restarted.grpcPort, "the restarted pooler opens, having read an unfenced row")

	before := f.control().version(ctx, "migrateTG")
	_, err := f.control().fenceAt(ctx, "migrateTG", before, 60*time.Second)
	requireMember(t, requireFenceNotClean(t, err), "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "unexplained backends present")
	requireStillFenced(t, f, "migrateTG", before, restarted.grpcPort)
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "restart-marker"), "the backend was left alone")

	terminateBackends(t, f.srcAdmin, "restart-marker")
	resp, err := f.control().fenceWithin(ctx, "migrateTG", 60*time.Second)
	require.NoError(t, err, "once the backend is gone the same fence is clean")
	require.Equal(t, admFenced, resp.State.State)
	require.Zero(t, rowCount(t, f.srcAdmin, -3))
}
