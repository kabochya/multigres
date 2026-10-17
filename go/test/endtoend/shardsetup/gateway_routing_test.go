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
