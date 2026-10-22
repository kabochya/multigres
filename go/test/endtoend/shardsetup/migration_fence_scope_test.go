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

// PROTOTYPE STUB: end-to-end scenarios for what a fence covers. It covers what goes through
// the pooler and nothing else: a direct connection to the source is not fenced and does not
// hold a fence back, and an idle reservation held through the pooler does, until the reserved
// pool's own inactivity timeout reaps it. Delete them with the prototype.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	mig "github.com/multigres/multigres/go/test/migrator"
)

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

// scenarioDirectConnectionsToTheSourceAreNotFenced: a client that reaches the source without
// going through the pooler is outside the fence: the fence neither waits for it nor ends it.
// A direct connection that is running a long statement does not stop the fence, or the whole
// cutover, from completing, and the statement is still running afterwards. Stopping such
// writers is a precondition of a cutover that the workflow meets, not something the fence
// checks.
func scenarioDirectConnectionsToTheSourceAreNotFenced(t *testing.T, f *migrationFixture) {
	ctx := t.Context()
	m := &migrator{f: f}

	direct, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable", appRole, appPassword, f.srcPort))
	require.NoError(t, err)
	t.Cleanup(func() { _ = direct.Close(context.Background()) })
	go func() { _, _ = direct.Exec(ctx, "SELECT pg_sleep(300), 'direct-marker'") }()
	t.Cleanup(func() { terminateBackends(t, f.srcAdmin, "direct-marker") })
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "direct-marker") == 1 }, 20*time.Second, 200*time.Millisecond)

	// The fence is not held back by it, and leaves it running.
	resp, err := f.control().fenceWithin(ctx, "migrateTG", 60*time.Second)
	require.NoError(t, err, "a direct connection that is running a statement does not hold the fence back")
	require.Equal(t, admFenced, resp.Fence.State)
	require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", resp.Fence.Version))
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "direct-marker"), "the fence does not end a client it does not serve")

	// The whole cutover completes with it still running.
	m.Cutover(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	destText := portOf(t, f.def)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == destText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the destination")
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "direct-marker"), "and the statement is still running on the source")
}

// TestAnIdleReservationRefusesOneFenceAndARetryAfterTheInactivityTimeoutSucceeds: an idle
// transaction held through the pooler is a reservation, and a commit reports busy while it is
// held and ends nothing. The reserved pool's own inactivity timeout reaps it, so the commit,
// asked again, succeeds, which is what the reference migrator does within its retry window.
// The timeout is shortened with the existing flag.
func TestAnIdleReservationRefusesOneFenceAndARetryAfterTheInactivityTimeoutSucceeds(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixtureWith(t, migrationOptions{
		drain:         time.Second,
		srcPoolerArgs: []string{"--connpool-user-reserved-inactivity-timeout=10s"},
	})

	idle := appConnWhenRouted(t, f.gatewayPort)
	_, err := idle.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = idle.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
	idleSince := time.Now()

	// One fence attempt, inside the timeout: busy, with the reservation named, and nothing ended.
	before := f.control().version(ctx, "migrateTG")
	_, err = f.control().fenceAt(ctx, "migrateTG", before, 30*time.Second)
	results := requireFenceNotClean(t, err)
	requireMember(t, results, "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "1 reservations held")
	requireStillFenced(t, f, "migrateTG", before, f.src.grpcPort)

	// It names what holds the fence: one reservation, held by a transaction, idle for a
	// while, with its backend and role, and nothing the client sent.
	busy := results["mig-src-1"].GetBusy()
	require.NotNil(t, busy, "a busy member says what holds its fence")
	require.EqualValues(t, 1, busy.GetReservationsHeld())
	require.Zero(t, busy.GetReservationsTruncated())
	require.Len(t, busy.GetReservations(), 1)
	held := busy.GetReservations()[0]
	require.Equal(t, []string{"transaction"}, held.GetReasons())
	require.Positive(t, held.GetIdleFor().AsDuration(), "the session has been idle since its last statement")
	require.GreaterOrEqual(t, held.GetHeldFor().AsDuration(), held.GetIdleFor().AsDuration())
	require.Equal(t, appRole, held.GetUser())
	require.Positive(t, held.GetBackendPid())
	require.False(t, held.GetInUse(), "no request is running on it: it is idle, not busy")
	require.Positive(t, held.GetClientPort(), "the pooler reported the local port of its connection to the backend")
	var shown int32
	require.NoError(t, f.srcAdmin.QueryRow(ctx, "SELECT client_port FROM pg_stat_activity WHERE pid = $1", held.GetBackendPid()).Scan(&shown))
	require.Equal(t, shown, held.GetClientPort(), "it is the client_port the server shows for that process")
	require.NotContains(t, mig.DescribeBusy(busy), "SELECT", "what a client sent is not part of it")
	require.Less(t, time.Since(idleSince), 10*time.Second, "the attempt was made inside the inactivity timeout")

	// The migrator retries the same fence for longer than the timeout: the pooler reaps the idle
	// transaction and the fence converges, at the same version.
	_, err = f.lib(func(c *mig.Config) { c.GiveUpAfter = 30 * time.Second }).FenceOrGiveUp(ctx, "migrateTG")
	require.NoError(t, err)
	st := f.control().fenceRow(ctx, "migrateTG")
	require.Equal(t, admFenced, st.State)
	require.EqualValues(t, before+1, st.Version, "the retries wrote nothing more")

	// The idle session's reservation was reaped: its next statement finds it gone.
	_, err = idle.Exec(ctx, "SELECT 1")
	require.Error(t, err)

	require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", st.Version))
}

// TestTheMigratorCanEndAnIdleReservationByItsPid: the reference migrator's optional policy ends a
// reservation that has been idle for longer than a threshold, on the source, naming its backend
// by pid and client port. The commit then succeeds long before the pooler's own inactivity
// timeout would have reaped the reservation, and the client's next statement finds its
// session gone. Multigres itself ended nothing: it only reported.
func TestTheMigratorCanEndAnIdleReservationByItsPid(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixtureWith(t, migrationOptions{drain: time.Second}) // the inactivity timeout stays at 30s

	idle := appConnWhenRouted(t, f.gatewayPort)
	_, err := idle.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = idle.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
	var pid int32
	require.NoError(t, idle.QueryRow(ctx, "SELECT 1").Scan(new(int)))
	// The backend the gateway reserved is the one the source shows idle in a transaction.
	require.NoError(t, f.srcAdmin.QueryRow(ctx,
		"SELECT pid FROM pg_stat_activity WHERE state = 'idle in transaction' AND usename = $1", appRole).Scan(&pid))

	var logged []string
	started := time.Now()
	resp, err := f.lib(func(c *mig.Config) {
		c.GiveUpAfter = 25 * time.Second
		c.TerminateIdle = &mig.TerminateIdle{After: 2 * time.Second, Source: mig.SQLTerminator{Source: f.srcAdmin}}
		c.Logf = func(format string, args ...any) {
			logged = append(logged, fmt.Sprintf(format, args...))
			t.Logf(format, args...)
		}
	}).FenceOrGiveUp(ctx, "migrateTG")
	require.NoError(t, err, "the fence completes once the idle reservation is ended")
	require.Equal(t, admFenced, resp.Fence.State)
	require.Less(t, time.Since(started), 25*time.Second, "well before the pooler's inactivity timeout of 30s would have reaped it")
	var ended bool
	for _, line := range logged {
		if strings.Contains(line, "ending the backend") && strings.Contains(line, "ended=true") {
			ended = true
		}
	}
	require.True(t, ended, "the migrator logged that it ended the backend: %v", logged)

	// The backend is gone from the source, and the client's next statement finds its session
	// terminated.
	var n int
	require.NoError(t, f.srcAdmin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", pid).Scan(&n))
	require.Zero(t, n, "the backend was ended")
	_, err = idle.Exec(ctx, "SELECT 1")
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "%v", err)
	require.Contains(t, []string{"57P01", "40001"}, pgErr.Code, "the session was terminated: %v", pgErr)
	t.Logf("the client's next statement fails with %s: %s", pgErr.Code, pgErr.Message)

	require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", resp.Fence.Version))
}

// TestASessionWhoseReservationWasReapedGets40001AfterACutoverAndGoesOn: a session holds a temporary
// table on the source and goes quiet; the reserved pool's inactivity timeout (shortened with its
// existing flag) reaps the reservation, which is what lets the fence complete, and the cutover moves
// the traffic. The move hides that loss: the session's next statement goes to the destination, which
// never consults the lost reservation. So it fails with 40001, as it would when a failover or a reap
// takes a reservation, the stale entry is dropped, and the session goes on, served by the
// destination. What else the lost reservation held is the business of the general reservation-loss
// handling, not of the cutover.
func TestASessionWhoseReservationWasReapedGets40001AfterACutoverAndGoesOn(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixtureWith(t, migrationOptions{
		drain:         time.Second,
		srcPoolerArgs: []string{"--connpool-user-reserved-inactivity-timeout=8s"},
	})
	m := &migrator{f: f}

	session := appConnWhenRouted(t, f.gatewayPort)
	_, err := session.Exec(ctx, "CREATE TEMP TABLE scratch (i int)")
	require.NoError(t, err)

	// The fence waits, and the reaping lets it complete; the cutover then goes through.
	m.Cutover(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	destText := portOf(t, f.def)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == destText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the destination")

	// The first statement after the move: the lost reservation is reported, not hidden.
	_, err = session.Exec(ctx, "INSERT INTO scratch VALUES (1)")
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "%v", err)
	require.Equal(t, "40001", pgErr.Code, "%v", pgErr)
	require.Contains(t, pgErr.Message, "temp_table")

	// And the session goes on, on the destination.
	port, err := backendPort(ctx, session)
	require.NoError(t, err, "the session was not ended")
	require.Equal(t, destText, port)
}
