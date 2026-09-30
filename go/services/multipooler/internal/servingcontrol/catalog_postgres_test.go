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

package servingcontrol

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	qmock "github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/tools/executil"
)

// Test-only adapter intentionally executes each statement and COMMIT once.
// Each admin transaction gets a fresh connection, including after cancellation.
type postgresQueries struct {
	executor.InternalQueryService
	dsn string
}

func (q postgresQueries) BeginAdmin(ctx context.Context) (executor.InternalTx, error) {
	c, err := pgx.Connect(ctx, q.dsn)
	if err != nil {
		return nil, err
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		_ = c.Close(context.Background())
		return nil, err
	}
	return &postgresTx{tx: tx, conn: c}, nil
}

func (q postgresQueries) QueryAdmin(ctx context.Context, query string) (*sqltypes.Result, error) {
	tx, err := q.BeginAdmin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	return tx.Query(ctx, query)
}

type postgresTx struct {
	tx   pgx.Tx
	conn *pgx.Conn
}

func pgResult(ctx context.Context, tx pgx.Tx, query string, args ...any) (*sqltypes.Result, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, f.Name)
	}
	var values [][]any
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return &sqltypes.Result{}, nil
	}
	return qmock.MakeQueryResult(names, values), nil
}

func (tx *postgresTx) Query(ctx context.Context, q string) (*sqltypes.Result, error) {
	return pgResult(ctx, tx.tx, q)
}

func (tx *postgresTx) QueryArgs(ctx context.Context, q string, args ...any) (*sqltypes.Result, error) {
	return pgResult(ctx, tx.tx, q, args...)
}

func (tx *postgresTx) Commit(ctx context.Context) error {
	defer tx.conn.Close(context.Background())
	return tx.tx.Commit(ctx)
}

func (tx *postgresTx) Rollback(ctx context.Context) error {
	defer tx.conn.Close(context.Background())
	return tx.tx.Rollback(ctx)
}

func disposablePostgres(t *testing.T) (string, string, int) {
	t.Helper()
	if testing.Short() {
		t.Skip("requires disposable PostgreSQL processes")
	}
	if _, err := exec.LookPath("initdb"); err != nil {
		t.Skip("initdb not on PATH")
	}
	dir := t.TempDir()
	data := filepath.Join(dir, "primary")
	out, err := executil.Command(t.Context(), "initdb", "-D", data, "-U", "postgres", "--auth=trust").CombinedOutput()
	require.NoError(t, err, "%s", out)
	port := postgresPort(t)
	require.NoError(t, os.WriteFile(filepath.Join(data, "postgresql.auto.conf"), []byte("wal_level='replica'\nmax_wal_senders=10\n"), 0o600))
	runPostgres(t, data, port)
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres sslmode=disable application_name=catalog_confirmation_test", port)
	require.Eventually(t, func() bool {
		c, e := pgx.Connect(t.Context(), dsn)
		if e != nil {
			return false
		}
		_ = c.Close(context.Background())
		return true
	}, 10*time.Second, 20*time.Millisecond)
	return dsn, dir, port
}

func postgresPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return p
}

func runPostgres(t *testing.T, data string, port int) {
	t.Helper()
	log, err := os.Create(filepath.Join(filepath.Dir(data), filepath.Base(data)+".log"))
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), "postgres", "-D", data, "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-c", "unix_socket_directories=")
	cmd.SetStdout(log)
	cmd.SetStderr(log)
	require.NoError(t, cmd.Start())
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = cmd.Stop(ctx)
		_ = log.Close()
	})
}

func TestPostgresUncertainCommitAndRestartConfirmation(t *testing.T) {
	dsn, dir, port := disposablePostgres(t)
	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer observer.Close(context.Background())
	_, err = observer.Exec(t.Context(), "CREATE SCHEMA multigres;"+Schema)
	require.NoError(t, err)
	_, err = observer.Exec(t.Context(), "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (confirmation_standby)'")
	require.NoError(t, err)
	_, err = observer.Exec(t.Context(), "SELECT pg_reload_conf()")
	require.NoError(t, err)
	c, err := New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- c.Update(ctx, func(tx executor.InternalTx, s *pb.MigrationRouting) error {
			if err := c.SetMigrationIdentity(ctx, tx, "attach-source"); err != nil {
				return err
			}
			if _, err := c.RequiredPoolers(ctx, tx, []*pb.ID{{Component: pb.ID_MULTIPOOLER, Cell: "a", Name: "source-process"}}); err != nil {
				return err
			}
			_, err := c.ReserveRequest(ctx, tx, "fence", "hash", "controller:fence", s)
			if err != nil {
				return err
			}
			s.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
			return c.CompleteRequest(ctx, tx, "fence")
		})
	}()
	// COMMIT has locally committed but is waiting for synchronous replication.
	require.Eventually(t, func() bool {
		var waiting bool
		e := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event='SyncRep')").Scan(&waiting)
		return e == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	cancel()
	require.Error(t, <-result)
	// PostgreSQL's synchronous wait can outlive a closed client connection.
	// Cancel the server-side wait, leaving the transaction committed locally.
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)
	var mode int32
	var completed bool
	require.Eventually(t, func() bool {
		err := observer.QueryRow(t.Context(), "SELECT mode,completed FROM multigres.migration_routing JOIN multigres.serving_requests ON request_id=active_request_id").Scan(&mode, &completed)
		return err == nil && completed && mode == int32(pb.MigrationMode_MIGRATION_MODE_FENCED)
	}, 5*time.Second, 20*time.Millisecond)
	// A fresh Catalog models a pooler-only restart. Completed retry and publication
	// must each require another synchronous write; local visibility is insufficient.
	restarted, err := New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	for _, check := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := restarted.ConfirmedRouting(ctx); return err },
		func(ctx context.Context) error {
			_, err := restarted.RequestCompleted(ctx, "fence", "hash")
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		require.Error(t, check(ctx))
		cancel()
		_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
		require.NoError(t, err)
	}
	// Introduce a real physical standby. Confirmation now proves the earlier WAL,
	// including the journal completion that had no received COMMIT acknowledgment.
	standby := filepath.Join(dir, "standby")
	out, err := executil.Command(t.Context(), "pg_basebackup", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-D", standby, "-X", "stream").CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.NoError(t, os.WriteFile(filepath.Join(standby, "standby.signal"), nil, 0o600))
	conninfo := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres application_name=confirmation_standby", port)
	require.NoError(t, os.WriteFile(filepath.Join(standby, "postgresql.auto.conf"), []byte("primary_conninfo='"+conninfo+"'\n"), 0o600))
	standbyPort := postgresPort(t)
	runPostgres(t, standby, standbyPort)
	require.Eventually(t, func() bool {
		var ready bool
		e := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_replication WHERE application_name='confirmation_standby' AND sync_state='sync')").Scan(&ready)
		return e == nil && ready
	}, 10*time.Second, 20*time.Millisecond)
	confirmCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	state, err := restarted.ConfirmedRouting(confirmCtx)
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, state.Mode)
	done, err := restarted.RequestCompleted(confirmCtx, "fence", "hash")
	require.NoError(t, err)
	require.True(t, done)
	replicaDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres sslmode=disable", standbyPort)
	replica, err := pgx.Connect(t.Context(), replicaDSN)
	require.NoError(t, err)
	defer replica.Close(context.Background())
	require.NoError(t, replica.QueryRow(t.Context(), "SELECT completed FROM multigres.serving_requests WHERE request_id='fence'").Scan(&completed))
	require.True(t, completed)

	// A second, asynchronous process can lag even after a remote_apply commit is
	// confirmed by the synchronous cohort. Matching FENCED alone is insufficient.
	laggingDir := filepath.Join(dir, "lagging")
	out, err = executil.Command(t.Context(), "pg_basebackup", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-D", laggingDir, "-X", "stream").CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.NoError(t, os.WriteFile(filepath.Join(laggingDir, "standby.signal"), nil, 0o600))
	laggingInfo := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres application_name=lagging_process", port)
	require.NoError(t, os.WriteFile(filepath.Join(laggingDir, "postgresql.auto.conf"), []byte("primary_conninfo='"+laggingInfo+"'\n"), 0o600))
	laggingPort := postgresPort(t)
	runPostgres(t, laggingDir, laggingPort)
	laggingDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres sslmode=disable", laggingPort)
	var laggingConn *pgx.Conn
	require.Eventually(t, func() bool { laggingConn, err = pgx.Connect(t.Context(), laggingDSN); return err == nil }, 10*time.Second, 20*time.Millisecond)
	defer laggingConn.Close(context.Background())
	_, err = laggingConn.Exec(t.Context(), "SELECT pg_wal_replay_pause()")
	require.NoError(t, err)
	require.NoError(t, restarted.Update(confirmCtx, func(tx executor.InternalTx, s *pb.MigrationRouting) error {
		if _, err := restarted.ReserveRequest(confirmCtx, tx, "replay-next", "next-hash", "controller:fence", s); err != nil {
			return err
		}
		s.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
		return restarted.CompleteRequest(confirmCtx, tx, "replay-next")
	}))
	laggingCatalog, err := New(postgresQueries{dsn: laggingDSN}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	laggingState, err := laggingCatalog.Routing(t.Context())
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, laggingState.Mode)
	require.Equal(t, "fence", laggingState.ActiveRequestId)
	lagCtx, lagCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	_, err = laggingCatalog.AwaitRouting(lagCtx, "replay-next", pb.MigrationMode_MIGRATION_MODE_FENCED)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	lagCancel()
	_, err = laggingConn.Exec(t.Context(), "SELECT pg_wal_replay_resume()")
	require.NoError(t, err)
	replayCtx, replayCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer replayCancel()
	laggingState, err = laggingCatalog.AwaitRouting(replayCtx, "replay-next", pb.MigrationMode_MIGRATION_MODE_FENCED)
	require.NoError(t, err)
	require.Equal(t, "replay-next", laggingState.ActiveRequestId)
	// A standby cannot publish authority through this API until consensus recovery
	// promotes it and restores a writable synchronous policy.
	follower, err := New(postgresQueries{dsn: replicaDSN}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	_, err = follower.ConfirmedRouting(confirmCtx)
	require.Error(t, err)
	// Disposable physical failover preserves the confirmed journal. Production
	// promotion additionally restores consensus and its configured quorum policy;
	// this fixture intentionally promotes to a single-node policy.
	out, err = executil.Command(t.Context(), "pg_ctl", "-D", filepath.Join(dir, "primary"), "stop", "-m", "fast", "-w").CombinedOutput()
	require.NoError(t, err, "%s", out)
	var promoted bool
	require.NoError(t, replica.QueryRow(t.Context(), "SELECT pg_promote(true,5)").Scan(&promoted))
	require.True(t, promoted)
	failoverCtx, cancelFailover := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelFailover()
	state, err = follower.ConfirmedRouting(failoverCtx)
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, state.Mode)
	done, err = follower.RequestCompleted(failoverCtx, "fence", "hash")
	require.NoError(t, err)
	require.True(t, done)
	require.NoError(t, follower.Update(failoverCtx, func(tx executor.InternalTx, _ *pb.MigrationRouting) error {
		if err := follower.RequireMigration(failoverCtx, tx, "attach-source"); err != nil {
			return err
		}
		required, err := follower.RequiredPoolers(failoverCtx, tx, nil)
		if err != nil {
			return err
		}
		require.Len(t, required, 1)
		require.Equal(t, "source-process", required[0].Name)
		return nil
	}))
	require.Error(t, follower.Update(failoverCtx, func(tx executor.InternalTx, _ *pb.MigrationRouting) error {
		return follower.RequireMigration(failoverCtx, tx, "different-migration")
	}))
}
