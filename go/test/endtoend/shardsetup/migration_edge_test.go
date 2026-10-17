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

// PROTOTYPE STUB: further end-to-end scenarios for the unmanaged pooler prototype,
// on the test migrator of migration_harness_test.go. Delete them with it.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rejectsAsAdmissionClosed reports whether a pooler, asked directly, refuses
// application work because its gate is closed.
func rejectsAsAdmissionClosed(t *testing.T, grpcPort int) bool {
	t.Helper()
	client, err := NewMultipoolerClient(grpcPort)
	if err != nil {
		return false
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = client.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	return err != nil && strings.Contains(err.Error(), "application admission is closed")
}

// TestStaleGatewayRequestIsHeldAndServedByTheDestination: a gateway whose routing is stale
// (it polls rarely, and has not seen the cutover) gets a request for the source, which is
// fenced. The gateway holds the request, and serves it from the destination once its next
// poll shows that the traffic moved. The buffer window has to be longer than the routing poll interval for this: a
// closed gate is not a signal to read the routing again, so a stale gateway waits for its
// next poll.
func TestStaleGatewayRequestIsHeldAndServedByTheDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	// Polls every 25 seconds, held requests wait up to 60: it reads the routing once when
	// it starts, which is the source, and sees the cutover only at its next poll.
	staleGateway := startRoutedGatewayNamed(t, f.def, "stale-gateway", "25s", "--buffer-window", "60s", "--statement-timeout", "0")
	logOffset := gatewayLogSize(t, staleGateway)
	stale := appConnWhenRouted(t, staleGateway)
	port, err := backendPort(ctx, stale)
	require.NoError(t, err)
	require.Equal(t, f.srcPortText, port)

	m.Cutover(ctx)
	cutoverDone := time.Now()
	destText := portOf(t, f.def)

	sent := time.Now()
	servedPort, err := backendPort(ctx, stale)
	require.NoError(t, err, "a request through a stale gateway must be held, not failed")
	require.Equal(t, destText, servedPort, "and served by the destination, not by the fenced source")
	held := time.Since(sent)
	require.Greater(t, held, time.Second, "the gateway had not seen the cutover, so the request was held for its next poll")
	// The gateway's routing was stale: the request was held, with the buffer started for
	// the source's tablegroup, and served by the destination once the gateway read the
	// move. The fenced source itself refuses application work, asked directly.
	requireGatewayLogLine(t, staleGateway, logOffset, "the request was not held: the gateway's buffer never started for the source's tablegroup",
		"failover detected, starting buffering", `"tablegroup":"migrateTG"`)
	require.True(t, rejectsAsAdmissionClosed(t, f.src.grpcPort), "the fenced source itself refuses application work")
	t.Logf("the stale gateway's request was held %s (%s after the cutover) and then served by the destination",
		held.Round(time.Millisecond), time.Since(cutoverDone).Round(time.Millisecond))
}
