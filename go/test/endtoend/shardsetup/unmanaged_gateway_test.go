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

package shardsetup

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

const (
	appRole     = "app"
	appPassword = "app-secret"
)

// appDSN connects as the application role through a gateway.
func appDSN(port int) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable&connect_timeout=5", appRole, appPassword, port)
}

// createAppRole creates the application role on an external database and on the default cohort
// with a byte-identical SCRAM verifier: the gateway verifies the client against the tablegroup
// that serves the application and passes its keys through to the backend, and control traffic
// such as NOTIFY reaches the default cohort with the same keys, so one proof must work on both.
func createAppRole(t *testing.T, extAdmin *pgx.Conn, def *ShardSetup) {
	t.Helper()
	ctx := t.Context()
	_, err := extAdmin.Exec(ctx, "CREATE ROLE "+appRole+" LOGIN PASSWORD '"+appPassword+"'")
	require.NoError(t, err)
	verifier, err := RoleVerifier(ctx, extAdmin, appRole)
	require.NoError(t, err)
	// The default cohort is shared by the tests of a package: replace what an earlier one made.
	defAdmin := def.ConnectPrimaryAdmin(t)
	_, err = defAdmin.Exec(ctx, "DROP ROLE IF EXISTS "+appRole)
	require.NoError(t, err)
	require.NoError(t, CreateRoleWithVerifier(ctx, defAdmin, appRole, verifier))
}

// startGatewayServing starts a multigateway that sends the application's SQL of the cluster's
// database to tablegroup, and returns its PostgreSQL port.
func startGatewayServing(t *testing.T, s *ShardSetup, name, tablegroup string) int {
	t.Helper()
	// CreateMultigatewayInstance records the instance on the setup; put the shared one back.
	shared, sharedPort := s.Multigateway, s.MultigatewayPgPort
	t.Cleanup(func() { s.Multigateway, s.MultigatewayPgPort = shared, sharedPort })

	pgPort, httpPort, grpcPort := utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t)
	gw := s.CreateMultigatewayInstance(t, name, pgPort, httpPort, grpcPort)
	gw.ExtraArgs = []string{"--routing-database=" + s.Database, "--routing-tablegroup=" + tablegroup}
	require.NoError(t, gw.Start(s.Context(), t))
	t.Cleanup(func() { gw.TerminateGracefully(t.Logf, 10*time.Second) })
	return pgPort
}

// listenOn opens a direct connection that listens on channel.
func listenOn(t *testing.T, dsn, channel string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	_, err = conn.Exec(t.Context(), "LISTEN "+channel)
	require.NoError(t, err)
	return conn
}

// externalOnlyRole is a role that exists only on the external database. A login that was
// routed to the default cohort instead would fail for it.
const (
	externalOnlyRole     = "extonly"
	externalOnlyPassword = "extonly-secret"
)

func externalOnlyDSN(port int) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable&connect_timeout=5", externalOnlyRole, externalOnlyPassword, port)
}

// notificationsUntil collects the payloads delivered to listener up to and including the one
// that is sentinel.
func notificationsUntil(t *testing.T, listener *pgx.Conn, sentinel string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var got []string
	for {
		n, err := listener.WaitForNotification(ctx)
		require.NoError(t, err, "waiting for %q after %v", sentinel, got)
		got = append(got, n.Payload)
		if n.Payload == sentinel {
			return got
		}
	}
}

// requireNoNotification fails if listener is sent anything within d.
func requireNoNotification(t *testing.T, listener *pgx.Conn, d time.Duration, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	n, err := listener.WaitForNotification(ctx)
	require.Error(t, err, "%s: got %v", msg, n)
}

// TestGatewayServesFromUnmanagedTablegroup sends a database's application traffic through a
// gateway to a tablegroup served by an unmanaged pooler on an external PostgreSQL: the client
// authenticates against that database, its queries run there, and a NOTIFY, which is control
// traffic, is delivered on the default cohort instead, after the transaction that issued it
// commits and not at all if the transaction, or the savepoint, is rolled back.
func TestGatewayServesFromUnmanagedTablegroup(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, "serves-gateway", externalDSN(extPort), externalSystemIdentifier(t, extPort))
	bindTablegroup(t, s, "migrateTG", "serves-gateway")

	extAdmin, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer extAdmin.Close(context.Background())
	createAppRole(t, extAdmin, s)
	_, err = extAdmin.Exec(ctx, "CREATE ROLE "+externalOnlyRole+" LOGIN PASSWORD '"+externalOnlyPassword+"'")
	require.NoError(t, err)
	_, err = extAdmin.Exec(ctx, "CREATE TABLE orders (id int PRIMARY KEY); GRANT ALL ON orders TO "+appRole)
	require.NoError(t, err)

	p := startUnmanagedPooler(t, s, "unmanaged-gateway", "postgres", "migrateTG", "serves-gateway")
	require.Eventually(t, func() bool {
		h, err := p.firstHealth(ctx)
		return err == nil && h.GetBackendReady()
	}, 60*time.Second, 200*time.Millisecond, "unmanaged pooler never reported its backend ready")

	// Witnesses for where a notification lands: a direct listener on each database.
	defaultDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=%s sslmode=disable",
		s.PrimaryPgctld(t).PgPort, TestPostgresPassword, s.Database)
	onDefault := listenOn(t, defaultDSN, "audit_chan")
	onExternal := listenOn(t, externalDSN(extPort), "audit_chan")

	gwPort := startGatewayServing(t, s, "serving-gateway", "migrateTG")
	connect := func(dsn string) *pgx.Conn {
		t.Helper()
		var conn *pgx.Conn
		require.Eventually(t, func() bool {
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return false
			}
			conn = c
			return true
		}, 60*time.Second, 200*time.Millisecond, "could not log in through the gateway")
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	app := connect(appDSN(gwPort))

	// The query runs on the external database.
	got, err := backendPort(ctx, app)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(extPort), got, "application queries must reach the external database")

	// A role that exists only on the external database logs in: the login was verified
	// against the tablegroup that serves the application, not against the default cohort.
	extOnly := connect(externalOnlyDSN(gwPort))
	got, err = backendPort(ctx, extOnly)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(extPort), got)
	_, err = pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable&connect_timeout=5",
		externalOnlyRole, externalOnlyPassword, s.PrimaryPgctld(t).PgPort, s.Database))
	require.Error(t, err, "the role must not exist on the default cohort, or the login above proves nothing")

	// NOTIFY outside a transaction is delivered on the default cohort and never on the external
	// database.
	_, err = app.Exec(ctx, "NOTIFY audit_chan, 'hello'")
	require.NoError(t, err)
	require.Equal(t, []string{"hello"}, notificationsUntil(t, onDefault, "hello"))
	requireNoNotification(t, onExternal, time.Second, "the NOTIFY must not reach the external database")

	// A LISTEN issued through the gateway receives a NOTIFY.
	listener := connect(appDSN(gwPort))
	_, err = listener.Exec(ctx, "LISTEN gateway_chan")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "NOTIFY gateway_chan, 'through-the-gateway'")
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	n, err := listener.WaitForNotification(waitCtx)
	require.NoError(t, err, "a LISTEN through the gateway must receive a NOTIFY")
	require.Equal(t, "gateway_chan", n.Channel)
	require.Equal(t, "through-the-gateway", n.Payload)

	sentinel := func(name string) []string {
		_, err := app.Exec(ctx, "NOTIFY audit_chan, '"+name+"'")
		require.NoError(t, err)
		return notificationsUntil(t, onDefault, name)
	}
	orders := func() int {
		var n int
		require.NoError(t, extAdmin.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n))
		return n
	}

	// A transaction that rolls back neither writes nor notifies.
	_, err = app.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "INSERT INTO orders VALUES (1)")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "NOTIFY audit_chan, 'rolled-back'")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.Zero(t, orders())
	require.Equal(t, []string{"after-rollback"}, sentinel("after-rollback"), "a rolled-back transaction must not notify")

	// One that commits writes on the external database, and notifies on the default cohort
	// only once it has committed.
	_, err = app.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "INSERT INTO orders VALUES (2)")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "NOTIFY audit_chan, 'committed'")
	require.NoError(t, err)
	requireNoNotification(t, onDefault, time.Second, "a NOTIFY must not be delivered before its transaction commits")
	_, err = app.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, 1, orders())
	require.Equal(t, []string{"committed"}, notificationsUntil(t, onDefault, "committed"))

	// A rollback to a savepoint drops what was notified after it, and keeps what came before.
	_, err = app.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "NOTIFY audit_chan, 'kept'")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "SAVEPOINT s1")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "NOTIFY audit_chan, 'dropped'")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "ROLLBACK TO SAVEPOINT s1")
	require.NoError(t, err)
	_, err = app.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, []string{"kept", "after-savepoint"}, sentinel("after-savepoint"))

	// None of it reached the external database's listeners.
	requireNoNotification(t, onExternal, time.Second, "no NOTIFY may reach the external database")
}

// backendPort asks the database a connection reaches which port it listens on, which identifies
// the external database that served the query.
func backendPort(ctx context.Context, conn *pgx.Conn) (string, error) {
	var port string
	err := conn.QueryRow(ctx, "SELECT current_setting('port')").Scan(&port)
	return port, err
}
