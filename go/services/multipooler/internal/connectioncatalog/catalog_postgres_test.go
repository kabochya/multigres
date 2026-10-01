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

package connectioncatalog_test

import (
	"bytes"
	"context"
	"errors"
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
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
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

func TestPostgresConnectionDurability(t *testing.T) {
	dsn, dir, port := disposablePostgres(t)
	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer observer.Close(context.Background())
	_, err = observer.Exec(t.Context(), "CREATE SCHEMA multigres")
	require.NoError(t, err)
	key := bytes.Repeat([]byte{1}, 32)
	catalog, err := connectioncatalog.New(postgresQueries{dsn: dsn}, key)
	require.NoError(t, err)
	require.NoError(t, catalog.Initialize(t.Context()))
	require.NoError(t, catalog.Initialize(t.Context()))
	config := &rpc.SourceConnection{Name: "source", Host: "db", Port: 5432, Database: "postgres", Username: "admin", Password: "secret", SslMode: "require"}
	require.Error(t, catalog.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		_, err := catalog.Create(ctx, tx, config)
		require.NoError(t, err)
		return errors.New("abort provisioning")
	}))
	var count int
	require.NoError(t, observer.QueryRow(t.Context(), "SELECT count(*) FROM multigres.connections").Scan(&count))
	require.Zero(t, count)
	_, err = observer.Exec(t.Context(), "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (confirmation_standby)'")
	require.NoError(t, err)
	_, err = observer.Exec(t.Context(), "SELECT pg_reload_conf()")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- catalog.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
			_, err := catalog.Create(ctx, tx, config)
			return err
		})
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event='SyncRep')").Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	cancel()
	require.Error(t, <-done)
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		err := observer.QueryRow(t.Context(), "SELECT count(*) FROM multigres.connections").Scan(&count)
		return err == nil && count == 1
	}, 5*time.Second, 20*time.Millisecond)
	// Locally visible uncertain creation is insufficient for protected bootstrap,
	// including with a new catalog instance after a pooler-only restart.
	restarted, err := connectioncatalog.New(postgresQueries{dsn: dsn}, key)
	require.NoError(t, err)
	confirmCtx, stop := context.WithTimeout(t.Context(), 200*time.Millisecond)
	record, err := restarted.Confirmed(confirmCtx, "source")
	stop()
	require.Error(t, err)
	require.Nil(t, record)
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)
	standby := filepath.Join(dir, "standby")
	out, err := executil.Command(t.Context(), "pg_basebackup", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-D", standby, "-X", "stream").CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.NoError(t, os.WriteFile(filepath.Join(standby, "standby.signal"), nil, 0o600))
	info := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres application_name=confirmation_standby", port)
	require.NoError(t, os.WriteFile(filepath.Join(standby, "postgresql.auto.conf"), []byte("primary_conninfo='"+info+"'\n"), 0o600))
	standbyPort := postgresPort(t)
	runPostgres(t, standby, standbyPort)
	require.Eventually(t, func() bool {
		var ready bool
		err := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_replication WHERE application_name='confirmation_standby' AND sync_state='sync')").Scan(&ready)
		return err == nil && ready
	}, 10*time.Second, 20*time.Millisecond)
	confirmedCtx, confirmedCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer confirmedCancel()
	record, err = restarted.Confirmed(confirmedCtx, "source")
	require.NoError(t, err)
	require.Equal(t, "secret", record.Configuration.Password)
	binding := record.Binding
	require.Error(t, restarted.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		_, err := restarted.Create(ctx, tx, config)
		return err
	}))
	replicaDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres sslmode=disable", standbyPort)
	replica, err := pgx.Connect(t.Context(), replicaDSN)
	require.NoError(t, err)
	defer replica.Close(context.Background())
	var stored string
	require.NoError(t, replica.QueryRow(t.Context(), "SELECT binding FROM multigres.connections WHERE name='source'").Scan(&stored))
	require.Equal(t, binding, stored)
	follower, err := connectioncatalog.New(postgresQueries{dsn: replicaDSN}, key)
	require.NoError(t, err)
	record, err = follower.Confirmed(confirmedCtx, "source")
	require.Error(t, err)
	require.Nil(t, record)
	out, err = executil.Command(t.Context(), "pg_ctl", "-D", filepath.Join(dir, "primary"), "stop", "-m", "fast", "-w").CombinedOutput()
	require.NoError(t, err, "%s", out)
	var promoted bool
	require.NoError(t, replica.QueryRow(t.Context(), "SELECT pg_promote(true,5)").Scan(&promoted))
	require.True(t, promoted)
	// Fixture promotes to single-node synchronous policy. Actual consensus election
	// and quorum restoration are exercised at the integrated controller boundary.
	record, err = follower.Confirmed(confirmedCtx, "source")
	require.NoError(t, err)
	require.Equal(t, binding, record.Binding)
	wrong, err := connectioncatalog.New(postgresQueries{dsn: replicaDSN}, bytes.Repeat([]byte{2}, 32))
	require.NoError(t, err)
	record, err = wrong.Confirmed(confirmedCtx, "source")
	require.Error(t, err)
	require.Nil(t, record)
	// Catalog privileges are private even though encrypted values are stored.
	var publicRead bool
	require.NoError(t, replica.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_class c, aclexplode(c.relacl) a WHERE c.oid='multigres.connections'::regclass AND a.grantee=0 AND a.privilege_type='SELECT')").Scan(&publicRead))
	require.False(t, publicRead)
}
