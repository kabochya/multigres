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
	"os"
	"strconv"
	"strings"
	"sync"
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

// createAppRole creates the application role on an external source and on the
// default cohort with a byte-identical SCRAM verifier: the gateway verifies the
// client against whichever side serves it and passes its keys through to the
// backend, so one proof must work on both.
func createAppRole(t *testing.T, srcAdmin *pgx.Conn, def *ShardSetup) {
	t.Helper()
	ctx := t.Context()
	_, err := srcAdmin.Exec(ctx, "CREATE ROLE "+appRole+" LOGIN PASSWORD '"+appPassword+"'")
	require.NoError(t, err)
	verifier, err := RoleVerifier(ctx, srcAdmin, appRole)
	require.NoError(t, err)
	require.NoError(t, CreateRoleWithVerifier(ctx, def.ConnectPrimaryAdmin(t), appRole, verifier))
}

// gatewayLogFiles maps the PostgreSQL port of a routed gateway started by the
// helpers below to its log file.
var gatewayLogFiles sync.Map

// gatewayLogSize is the current size of a gateway's log, to be passed to
// gatewayLogSince later to read only what was written in between.
func gatewayLogSize(t *testing.T, port int) int {
	t.Helper()
	path, ok := gatewayLogFiles.Load(port)
	require.True(t, ok, "no log file known for the gateway on port %d", port)
	data, err := os.ReadFile(path.(string))
	require.NoError(t, err)
	return len(data)
}

// requireGatewayLogLine waits until a single line a gateway logged after offset contains
// every one of the given texts: a message and the fields that scope it, say, so that
// something logged earlier, or about another tablegroup, does not satisfy it.
func requireGatewayLogLine(t *testing.T, port, offset int, msg string, texts ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for line := range strings.SplitSeq(gatewayLogSince(t, port, offset), "\n") {
			ok := true
			for _, text := range texts {
				ok = ok && strings.Contains(line, text)
			}
			if ok {
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, msg)
}

// gatewayLogSince returns what a gateway logged after the given size.
func gatewayLogSince(t *testing.T, port, offset int) string {
	t.Helper()
	path, ok := gatewayLogFiles.Load(port)
	require.True(t, ok, "no log file known for the gateway on port %d", port)
	data, err := os.ReadFile(path.(string))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), offset)
	return string(data[offset:])
}

// startRoutedGateway starts a multigateway that follows the application routing
// pointer of the cluster's database, and returns its PostgreSQL port.
func startRoutedGateway(t *testing.T, s *ShardSetup) int {
	t.Helper()
	return startRoutedGatewayNamed(t, s, "routed-gateway", "200ms")
}

// startRoutedGatewayNamed is startRoutedGateway with a name and a routing poll
// interval. A long interval makes a gateway whose routing goes stale.
func startRoutedGatewayNamed(t *testing.T, s *ShardSetup, name, pollInterval string, extraArgs ...string) int {
	t.Helper()
	pgPort, httpPort, grpcPort := utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t)
	gw := s.CreateMultigatewayInstance(t, name, pgPort, httpPort, grpcPort)
	gw.ExtraArgs = []string{
		"--routing-poll-database=" + s.Database, "--routing-poll-interval=" + pollInterval,
		"--buffer-enabled", "--buffer-window", "30s", "--buffer-size", "1000",
		// The timing guard between buffer cycles stays at its default: a rollback
		// right after a cutover must be held like the cutover was.
		"--buffer-max-failover-duration", "60s",
		"--buffer-drain-concurrency", "5",
	}
	gw.ExtraArgs = append(gw.ExtraArgs, extraArgs...)
	require.NoError(t, gw.Start(s.Context(), t))
	gatewayLogFiles.Store(pgPort, gw.LogFile)
	// Only the most recent gateway is tracked by the setup; stop each one.
	t.Cleanup(func() { gw.TerminateGracefully(t.Logf, 10*time.Second) })
	return pgPort
}

// backendPort asks the database a connection reaches which port it listens on,
// which identifies the external database that served the query.
func backendPort(ctx context.Context, conn *pgx.Conn) (string, error) {
	var port string
	err := conn.QueryRow(ctx, "SELECT current_setting('port')").Scan(&port)
	return port, err
}

// TestGatewayFollowsRoutingAcrossACutoverAndRollback: application clients reach
// the external source through a gateway and an unmanaged pooler, a request that
// arrives while the source is fenced is held and then served by the destination, the
// default cohort, once routing moves, and a rollback sends traffic back.
func TestGatewayFollowsRoutingAcrossACutoverAndRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	def := startControlledDefaultCohort(t)

	srcPort := startExternalPostgres(t)
	srcAdmin, err := pgx.Connect(ctx, externalDSN(srcPort))
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcAdmin.Close(context.Background()) })
	createAppRole(t, srcAdmin, def)
	seedBackingConnection(t, def, "routing-src", externalDSN(srcPort), externalSystemIdentifier(t, srcPort))
	setServingRow(t, def, "migrateTG", "routing-src", "UNFENCED", "r-src")
	seedRouting(t, def, "migrateTG")

	src := startUnmanagedPooler(t, def, "routing-src-1", "postgres", "routing-src")
	waitBackendReady(t, src)
	requireAdmits(t, src.grpcPort, "the source must admit while UNFENCED")

	gatewayPort := startRoutedGateway(t, def)
	srcText, dstText := strconv.Itoa(srcPort), strconv.Itoa(def.PrimaryPgctld(t).PgPort)

	// A client reaches the external source through the gateway.
	var client *pgx.Conn
	require.Eventually(t, func() bool {
		c, err := pgx.Connect(ctx, appDSN(gatewayPort))
		if err != nil {
			return false
		}
		if got, err := backendPort(ctx, c); err != nil || got != srcText {
			_ = c.Close(ctx)
			return false
		}
		client = c
		return true
	}, 60*time.Second, 300*time.Millisecond, "the gateway never served from the source")
	defer client.Close(context.Background())

	ctl := newControlClient(t, def.PrimaryMultipooler(t).GrpcPort)

	// Cutover. Fence both sides first.
	_, err = ctl.fence(ctx, "default", "fence-def-1")
	require.NoError(t, err)
	_, err = ctl.fence(ctx, "migrateTG", "fence-src-1")
	require.NoError(t, err)

	// A request arriving now finds the source fenced. It must be held, not failed.
	type result struct {
		port string
		err  error
	}
	held := make(chan result, 1)
	go func() {
		callCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		port, err := backendPort(callCtx, client)
		held <- result{port, err}
	}()
	select {
	case r := <-held:
		t.Fatalf("a request issued while the source was fenced returned %+v instead of waiting", r)
	case <-time.After(2 * time.Second):
	}

	// Route, then unfence the destination: the held request is served there.
	_, err = ctl.route(ctx, "migrateTG", "default", "route-1")
	require.NoError(t, err)
	_, err = ctl.unfence(ctx, "default", "unfence-def-1", "fence-def-1")
	require.NoError(t, err)
	select {
	case r := <-held:
		require.NoError(t, r.err)
		require.Equal(t, dstText, r.port, "the held request must be served by the destination")
	case <-time.After(30 * time.Second):
		t.Fatal("the held request was never released")
	}

	// The same client connection and a fresh one now land on the destination.
	got, err := backendPort(ctx, client)
	require.NoError(t, err)
	require.Equal(t, dstText, got)
	fresh, err := pgx.Connect(ctx, appDSN(gatewayPort))
	require.NoError(t, err)
	defer fresh.Close(context.Background())
	got, err = backendPort(ctx, fresh)
	require.NoError(t, err)
	require.Equal(t, dstText, got)

	// Rollback.
	_, err = ctl.fence(ctx, "default", "fence-def-2")
	require.NoError(t, err)
	_, err = ctl.route(ctx, "default", "migrateTG", "route-2")
	require.NoError(t, err)
	_, err = ctl.unfence(ctx, "migrateTG", "unfence-src-1", "fence-src-1")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, client)
		return err == nil && port == srcText
	}, 30*time.Second, 200*time.Millisecond, "traffic never returned to the source after the rollback")
}
