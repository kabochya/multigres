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

// makeVerifiersIdentical gives the postgres role on dst exactly the SCRAM
// verifier it has on src. SCRAM key passthrough needs byte-identical verifiers on
// the two sides of a cutover.
func makeVerifiersIdentical(t *testing.T, srcPort, dstPort int) {
	t.Helper()
	ctx := t.Context()
	src, err := pgx.Connect(ctx, externalDSN(srcPort))
	require.NoError(t, err)
	defer src.Close(context.Background())
	dst, err := pgx.Connect(ctx, externalDSN(dstPort))
	require.NoError(t, err)
	defer dst.Close(context.Background())
	verifier, err := RoleVerifier(ctx, src, "postgres")
	require.NoError(t, err)
	_, err = dst.Exec(ctx, "ALTER ROLE postgres PASSWORD '"+verifier+"'")
	require.NoError(t, err)
	got, err := RoleVerifier(ctx, dst, "postgres")
	require.NoError(t, err)
	require.Equal(t, verifier, got)
}

// startRoutedGateway starts a multigateway that follows the application routing
// pointer of the cluster's database, and returns its PostgreSQL port.
func startRoutedGateway(t *testing.T, s *ShardSetup) int {
	t.Helper()
	pgPort, httpPort, grpcPort := utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t)
	gw := s.CreateMultigatewayInstance(t, "routed-gateway", pgPort, httpPort, grpcPort)
	gw.ExtraArgs = []string{
		"--routing-poll-database=" + s.Database, "--routing-poll-interval=200ms",
		"--buffer-enabled", "--buffer-window", "30s", "--buffer-size", "1000",
		"--buffer-max-failover-duration", "60s", "--buffer-min-time-between-failovers", "0s",
		"--buffer-drain-concurrency", "5",
	}
	require.NoError(t, gw.Start(s.Context(), t))
	return pgPort
}

func gatewayDSN(port int) string {
	return fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/postgres?sslmode=disable&connect_timeout=5", externalPostgresPassword, port)
}

// backendPort asks the database a connection reaches which port it listens on,
// which identifies the external database that served the query.
func backendPort(ctx context.Context, conn *pgx.Conn) (string, error) {
	var port string
	err := conn.QueryRow(ctx, "SELECT current_setting('port')").Scan(&port)
	return port, err
}

// TestGatewayFollowsRoutingAcrossACutoverAndRollback: application clients reach
// the external source through a gateway and unmanaged poolers, a request that
// arrives while the source is fenced is held and then served by the destination
// once routing moves, and a rollback sends traffic back.
func TestGatewayFollowsRoutingAcrossACutoverAndRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s, cleanup := NewIsolated(t, WithMultipoolerCount(2))
	defer cleanup()

	srcPort, dstPort := startExternalPostgres(t), startExternalPostgres(t)
	makeVerifiersIdentical(t, srcPort, dstPort)
	seedBackingConnection(t, s, "w6-src", externalDSN(srcPort), externalSystemIdentifier(t, srcPort))
	seedBackingConnection(t, s, "w6-dst", externalDSN(dstPort), externalSystemIdentifier(t, dstPort))
	setServingRow(t, s, "migrateTG", "w6-src", "UNFENCED", "r-src")
	setServingRow(t, s, "destTG", "w6-dst", "FENCED", "r-dst")
	seedRouting(t, s, "migrateTG")

	src := startUnmanagedPooler(t, s, "w6-src-1", "postgres", "w6-src")
	dst := startUnmanagedPooler(t, s, "w6-dst-1", "postgres", "w6-dst", "--table-group=destTG")
	waitBackendReady(t, src)
	waitBackendReady(t, dst)
	requireAdmits(t, src.grpcPort, "the source must admit while UNFENCED")

	gatewayPort := startRoutedGateway(t, s)
	srcText, dstText := strconv.Itoa(srcPort), strconv.Itoa(dstPort)

	// A client reaches the external source through the gateway.
	var client *pgx.Conn
	require.Eventually(t, func() bool {
		c, err := pgx.Connect(ctx, gatewayDSN(gatewayPort))
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

	ctl := newControlClient(t, s.PrimaryMultipooler(t).GrpcPort)

	// Cutover. Fence both sides first.
	_, err := ctl.fence(ctx, "destTG", "fence-dst-1")
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
	_, err = ctl.route(ctx, "migrateTG", "destTG", "route-1")
	require.NoError(t, err)
	_, err = ctl.unfence(ctx, "destTG", "unfence-dst-1", "fence-dst-1")
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
	fresh, err := pgx.Connect(ctx, gatewayDSN(gatewayPort))
	require.NoError(t, err)
	defer fresh.Close(context.Background())
	got, err = backendPort(ctx, fresh)
	require.NoError(t, err)
	require.Equal(t, dstText, got)

	// Rollback.
	_, err = ctl.fence(ctx, "destTG", "fence-dst-2")
	require.NoError(t, err)
	_, err = ctl.route(ctx, "destTG", "migrateTG", "route-2")
	require.NoError(t, err)
	_, err = ctl.unfence(ctx, "migrateTG", "unfence-src-1", "fence-src-1")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, client)
		return err == nil && port == srcText
	}, 30*time.Second, 200*time.Millisecond, "traffic never returned to the source after the rollback")
}
