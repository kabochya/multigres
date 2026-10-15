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

// PROTOTYPE STUB: end-to-end scenarios for the unmanaged pooler prototype. They
// run the test migrator of migration_harness_test.go; delete them with it.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func appConn(t *testing.T, port int) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), appDSN(port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// appConnWhenRouted connects as the application role once the gateway has learned
// where application traffic goes: before that its credential lookup has nowhere to
// find the role and fails.
func appConnWhenRouted(t *testing.T, port int) *pgx.Conn {
	t.Helper()
	var conn *pgx.Conn
	require.Eventually(t, func() bool {
		c, err := pgx.Connect(t.Context(), appDSN(port))
		if err != nil {
			return false
		}
		conn = c
		return true
	}, 60*time.Second, 300*time.Millisecond, "the gateway never accepted the application role")
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// openLongTransaction leaves a transaction with an uncommitted write open on
// whichever database the gateway currently routes to.
func openLongTransaction(t *testing.T, gatewayPort int) *pgx.Conn {
	t.Helper()
	conn := appConn(t, gatewayPort)
	_, err := conn.Exec(t.Context(), "BEGIN")
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), "INSERT INTO ledger (id, writer) VALUES (-1, 99)")
	require.NoError(t, err)
	return conn
}

func waitForAcks(t *testing.T, w *workload, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return w.ackedCount() >= n }, 60*time.Second, 100*time.Millisecond,
		"the workload never reached %d acknowledged writes", n)
}

// failOverPrimary kills postgres on a cohort's primary and waits for another to be
// elected, recording it as the cohort's primary. The cohort's multiorch must be
// running.
func failOverPrimary(t *testing.T, s *ShardSetup) (oldName, newName string) {
	t.Helper()
	ctx := t.Context()
	old := s.GetPrimary(t)
	client, err := NewMultipoolerClient(old.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()
	// Keep the old primary down long enough for the failover to run.
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	s.KillPostgres(t, old.Name)
	newName = WaitForNewPrimary(t, s, old.Name, 90*time.Second)
	require.NotEqual(t, old.Name, newName)
	s.PrimaryName = newName
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)
	return old.Name, newName
}

// TestMigrationCutoverAndRollbackUnderLoad runs a full cutover and rollback with
// writers going through the gateway the whole time. It checks that no write is
// lost or duplicated, that nothing lands on a side after its fence was
// acknowledged, that a long transaction is terminated at the drain deadline, and
// that a gateway whose routing is stale is held and then served from the new
// destination.
func TestMigrationCutoverAndRollbackUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	w := startWorkload(t, f.gatewayPort, 4, false)
	waitForAcks(t, w, 50)

	// A transaction that never ends, on the source.
	long := openLongTransaction(t, f.gatewayPort)

	// A second gateway whose routing poll is slower than the whole cutover. It
	// polls once as it starts and not again for staleInterval, so a request it
	// serves before then cannot have been routed by a poll that saw the cutover.
	// Its buffer and statement timeout outlast the interval so the held request is
	// not timed out before the poll that releases it.
	const staleInterval = 90 * time.Second
	staleStarted := time.Now()
	staleGateway := startRoutedGatewayNamed(t, f.def, "stale-gateway", staleInterval.String(),
		"--buffer-window", "150s", "--buffer-max-failover-duration", "180s", "--statement-timeout", "0")
	stale := appConnWhenRouted(t, staleGateway)
	port, err := backendPort(ctx, stale)
	require.NoError(t, err)
	require.Equal(t, f.srcPortText, port)

	m.Cutover(ctx)
	cutoverDone := time.Now()
	require.Less(t, cutoverDone.Sub(staleStarted), staleInterval-10*time.Second,
		"the cutover took too long for the stale gateway to still be stale; the scenario proves nothing")
	destText := portOf(t, f.def)

	// The stale gateway still believes the source owns the traffic. Its request is
	// held, then served by the destination once it learns routing moved.
	port, err = backendPort(ctx, stale)
	servedAfter := time.Since(staleStarted)
	require.NoError(t, err, "a request through a stale gateway must be held, not failed")
	require.Equal(t, destText, port, "and served by the destination")
	require.GreaterOrEqual(t, servedAfter, staleInterval,
		"the request must have been held until the gateway's next routing poll, not served earlier")
	t.Logf("the stale gateway's request was held %s after the cutover and then served by the destination",
		time.Since(cutoverDone).Round(time.Millisecond))

	// The long transaction was terminated at the drain deadline: it cannot commit,
	// and its write is on neither side.
	_, _ = long.Exec(ctx, "COMMIT")
	var n int
	require.NoError(t, f.srcAdmin.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE id = -1").Scan(&n))
	require.Zero(t, n, "the terminated transaction's write must not be on the source")
	require.NoError(t, f.adminConn(f.def).QueryRow(ctx, "SELECT count(*) FROM ledger WHERE id = -1").Scan(&n))
	require.Zero(t, n, "nor on the destination")

	// Traffic runs on the destination for a while, then comes back.
	acked := w.ackedCount()
	waitForAcks(t, w, acked+50)
	m.Rollback(ctx)
	acked = w.ackedCount()
	waitForAcks(t, w, acked+50)

	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs, "no client may see an error across a cutover and a rollback")
	found, err := ledgerIDs(ctx, f.srcAdmin)
	require.NoError(t, err)
	requireExactly(t, "source after rollback", ackedSet, found)
	// The destination was fenced when traffic went back, and must still hold what
	// it held then.
	m.requireDestFrozen(ctx, "at the end of the test")
	t.Logf("%d writes acknowledged across a cutover and a rollback; none lost or duplicated", len(ackedSet))
}

func portOf(t *testing.T, s *ShardSetup) string {
	t.Helper()
	conn := s.ConnectPrimaryAdmin(t)
	var port string
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT current_setting('port')").Scan(&port))
	return port
}

// TestDefaultPrimaryFailoverDuringFencing fails over the default primary while a
// fence is waiting on a pooler, once fencing the source and once fencing the default
// tablegroup, which is the destination and the cohort that holds the coordinator.
// The persisted state must stay FENCING, and the same request, retried against the
// new default primary, must complete.
func TestDefaultPrimaryFailoverDuringFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	for _, tc := range []struct {
		name, fenced string
	}{
		{"fencing the source", "migrateTG"},
		{"fencing the default tablegroup", "default"},
	} {
		t.Run(tc.name, func(t *testing.T) { defaultPrimaryFailoverDuringFencing(t, tc.fenced) })
	}
}

func defaultPrimaryFailoverDuringFencing(t *testing.T, fenced string) {
	ctx := t.Context()
	// A long drain keeps the fence waiting while the default primary fails. Only the
	// side that is fenced gets one: the cutover that precedes fencing the default
	// tablegroup must not wait.
	opts := migrationOptions{sourceDrain: 3 * time.Second, defaultDrain: 3 * time.Second}
	if fenced == "migrateTG" {
		opts.sourceDrain = 40 * time.Second
	} else {
		opts.defaultDrain = 40 * time.Second
	}
	f := newMigrationFixtureWith(t, opts)
	f.def.StartMultiorchs(ctx, t)
	m := &migrator{f: f}
	if fenced == "default" {
		// The default tablegroup carries the application's work only after the
		// cutover, so there is something for its fence to wait for.
		m.Cutover(ctx)
	}

	long := openLongTransaction(t, f.gatewayPort)
	requestID := "fence-" + fenced + "-x"

	ctl := f.control()
	done := make(chan error, 1)
	go func() {
		// The coordinator's own deadline is longer than the drain, so a fence that
		// is not interrupted completes, by terminating the transaction at the
		// deadline: an error can only come from the failover.
		_, err := ctl.fenceWithin(ctx, fenced, requestID, 100*time.Second)
		done <- err
	}()

	// The coordinator has decided: the fence is persisted as FENCING and is waiting.
	require.Eventually(t, func() bool {
		resp, err := f.control().tryState(ctx, fenced)
		if err != nil {
			return false
		}
		st := resp.Tablegroups[0]
		return st.AdmissionState == multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING && st.RequestId == requestID
	}, 20*time.Second, 200*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the fence returned %v while a transaction was still open", err)
	case <-time.After(time.Second):
	}

	// The default primary fails mid fan-out.
	oldPrimary, newPrimary := failOverPrimary(t, f.def)
	require.NotEqual(t, oldPrimary, newPrimary, "the default primary must really have changed")
	select {
	case err := <-done:
		require.Error(t, err, "the call that was in flight must not report success")
	case <-time.After(150 * time.Second):
		t.Fatal("the in-flight fence neither completed nor failed")
	}

	// The new default primary reads the same persisted decision, and answers the
	// control plane although its own tablegroup may be the one being fenced.
	var st *multipoolerservicepb.TablegroupServingState
	require.Eventually(t, func() bool {
		resp, err := f.control().GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: f.def.Database, Tablegroups: []string{fenced}})
		if err != nil {
			return false
		}
		st = resp.Tablegroups[0]
		return true
	}, 60*time.Second, 500*time.Millisecond)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, st.AdmissionState, "an interrupted fence stays FENCING")
	require.Equal(t, requestID, st.RequestId)

	// Recovery: let the transaction go, then retry the same request.
	_ = long.Close(ctx)
	require.Eventually(t, func() bool {
		resp, err := f.control().fence(ctx, fenced, requestID)
		return err == nil && resp.State.AdmissionState == multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	}, 90*time.Second, time.Second, "retrying the same request id against the new default primary must complete the fence")
	st = f.control().state(ctx, fenced).Tablegroups[0]
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.AdmissionState)
	require.Equal(t, requestID, st.RequestId)
	require.False(t, admits(t, f.src.grpcPort), "the source pooler must be closed")
	if fenced == "default" {
		for name, inst := range f.def.Multipoolers {
			require.False(t, admits(t, inst.Multipooler.GrpcPort), "default pooler %s must be closed", name)
		}
	}
}

// TestDefaultPrimaryFailoverAfterCutover fails over the default primary after the
// cutover, with writers running. The default cohort is the destination, so the
// failover moves the coordinator and the routing pointer along with the application's
// data: all three must survive. It must not reopen the source, must not change the
// persisted admission decisions or the routing, must not lose a write, and the new
// primary's coordinator must be able to drive the rollback.
func TestDefaultPrimaryFailoverAfterCutover(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	f.def.StartMultiorchs(ctx, t)
	m := &migrator{f: f}

	// An in-flight write can fail when the primary crashes. Those writers resolve an
	// unknown outcome by retrying the same key.
	w := startWorkload(t, f.gatewayPort, 4, true)
	waitForAcks(t, w, 50)
	m.Cutover(ctx)
	acked := w.ackedCount()
	waitForAcks(t, w, acked+50)

	before := f.control().state(ctx, "migrateTG", "default")

	logOffset := gatewayLogSize(t, f.gatewayPort)
	failOverPrimary(t, f.def)

	// Writes resume on the new default primary.
	acked = w.ackedCount()
	waitForAcks(t, w, acked+50)
	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs, "every write must resolve, by being held or by a retry of its own key")
	t.Logf("%d writes resolved across the default-primary failover, %d of them by retry", len(ackedSet), w.retryCount())

	// The default cohort runs with admission control, but it was not fenced: its
	// failover is an ordinary one. Whatever the gateway held, it held as for a
	// failover; a closed gate reported as a fence (MTF02, or admission_closed) would have
	// exempted the request from the guard against buffering again soon after a failover.
	// The new primary is closed until it has read its admission state, which it says as
	// pending, not as a fence.
	gwLog := gatewayLogSince(t, f.gatewayPort, logOffset)
	require.Contains(t, gwLog, "waiting after a bufferable rejection", "the failover must have held requests")
	require.NotContains(t, gwLog, `"rejected_by":"pooler:MTF02"`, "an unfenced pooler must not report a closed gate")
	require.NotContains(t, gwLog, `"admission_closed":true`, "and no wait is exempt from the timing guard")

	found, err := ledgerIDs(ctx, f.adminConn(f.def))
	require.NoError(t, err)
	requireExactly(t, "default cohort after its failover", ackedSet, found)

	// Admission and routing are exactly as the cutover left them, as the new
	// primary's coordinator reports them.
	after := f.control().state(ctx, "migrateTG", "default")
	require.Equal(t, before.AppTablegroup, after.AppTablegroup)
	require.Equal(t, before.RoutingVersion, after.RoutingVersion)
	for i := range before.Tablegroups {
		require.Equal(t, before.Tablegroups[i].AdmissionState, after.Tablegroups[i].AdmissionState, before.Tablegroups[i].Tablegroup)
		require.Equal(t, before.Tablegroups[i].RequestId, after.Tablegroups[i].RequestId, before.Tablegroups[i].Tablegroup)
	}
	require.False(t, admits(t, f.src.grpcPort), "a default-primary failover must not reopen the source")

	// The coordinator moved with the primary: it can still carry out the rollback.
	// A fence needs every pooler of the cohort to prove its backend quiet, so the
	// old primary, which is coming back as a replica, must be up first.
	waitForAllPostgresReady(t, f.def, 120*time.Second)
	m.Rollback(ctx)
	back, err := ledgerIDs(ctx, f.srcAdmin)
	require.NoError(t, err)
	requireExactly(t, "source after the rollback", ackedSet, back)
}

// waitForAllPostgresReady waits until every multipooler of a cohort reports its
// postgres ready.
func waitForAllPostgresReady(t *testing.T, s *ShardSetup, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, inst := range s.Multipoolers {
			client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				return false
			}
			resp, err := client.Manager.Status(t.Context(), &multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			if err != nil || !resp.GetStatus().GetPostgresReady() {
				return false
			}
		}
		return true
	}, timeout, time.Second, "a pooler's postgres never came back")
}

// rowCount returns how many rows of the ledger have the given id.
func rowCount(t *testing.T, conn *pgx.Conn, id int64) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count(*) FROM ledger WHERE id = $1", id).Scan(&n))
	return n
}

// requireSessionEnded sends a request and requires a FATAL admin_shutdown error, the
// one outcome for a session that held state on the tablegroup that lost the traffic, and
// that the client's connection is closed afterwards. (The driver closes its connection
// on any FATAL error, so this does not show that the gateway did; the error does.)
func requireSessionEnded(t *testing.T, conn *pgx.Conn, label string, request func() error) {
	t.Helper()
	err := request()
	require.Error(t, err, "%s: the request must not succeed", label)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "%s: %v", label, err)
	require.Equal(t, "57P01", pgErr.Code, "%s: the session must be ended, not told to retry", label)
	require.Equal(t, "FATAL", pgErr.Severity, label)
	require.Regexp(t, "closed by a migration fence|application traffic moved while this session held state", pgErr.Message,
		"%s: the pooler's or the gateway's own message, not PostgreSQL's", label)
	require.Eventually(t, conn.IsClosed, 5*time.Second, 50*time.Millisecond, "%s: the client connection must be closed", label)
}

// requireSessionEndedByFence is requireSessionEnded for a session whose reservation a fence
// closed, as the pooler tells it: that message, and no other.
func requireSessionEndedByFence(t *testing.T, conn *pgx.Conn, label string, request func() error) {
	t.Helper()
	err := request()
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "%s: %v", label, err)
	require.Contains(t, pgErr.Message, "closed by a migration fence", "%s: the pooler's message", label)
	require.Equal(t, "57P01", pgErr.Code, label)
	require.Equal(t, "FATAL", pgErr.Severity, label)
	require.Eventually(t, conn.IsClosed, 5*time.Second, 50*time.Millisecond, "%s: the client connection must be closed", label)
}

// TestSessionsHoldingStateAcrossACutoverAreEnded: a session that holds a
// transaction, temp tables or any other reservation on the source when traffic
// moves must not carry on against the destination with half of what it did, or with
// state that is gone. Whatever its next request is, including COMMIT and ROLLBACK,
// the session is ended; nothing it wrote anywhere commits; a session with no state
// on the source is not disturbed. The rollback direction ends sessions the same way.
func TestSessionsHoldingStateAcrossACutoverAreEnded(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	openTxn := func(id int64) *pgx.Conn {
		conn := appConnWhenRouted(t, f.gatewayPort)
		_, err := conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES ($1, 7)", id)
		require.NoError(t, err)
		return conn
	}
	statement := openTxn(-1)
	commit := openTxn(-2)
	rollback := openTxn(-3)
	tempOnly := appConnWhenRouted(t, f.gatewayPort)
	_, err := tempOnly.Exec(ctx, "CREATE TEMP TABLE scratch (i int)")
	require.NoError(t, err)
	stateless := appConnWhenRouted(t, f.gatewayPort)

	// The clients go idle; the fence's drain deadline ends their transactions.
	m.Cutover(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	destText := portOf(t, f.def)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == destText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the destination")

	requireSessionEnded(t, statement, "next statement", func() error {
		_, err := statement.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES (-11, 7)")
		return err
	})
	requireSessionEnded(t, commit, "COMMIT", func() error {
		_, err := commit.Exec(ctx, "COMMIT")
		return err
	})
	requireSessionEnded(t, rollback, "ROLLBACK", func() error {
		_, err := rollback.Exec(ctx, "ROLLBACK")
		return err
	})
	requireSessionEnded(t, tempOnly, "temp-table-only session", func() error {
		_, err := tempOnly.Exec(ctx, "SELECT 1")
		return err
	})

	// A session with no state on the source is served by the destination as usual.
	port, err := backendPort(ctx, stateless)
	require.NoError(t, err)
	require.Equal(t, destText, port)

	destAdmin := f.adminConn(f.def)
	for _, id := range []int64{-1, -2, -3, -11} {
		require.Zero(t, rowCount(t, f.srcAdmin, id), "row %d must not be on the source", id)
		require.Zero(t, rowCount(t, destAdmin, id), "row %d must not be on the destination", id)
	}

	// The rollback direction: a transaction open on the destination when traffic goes
	// back ends the same way.
	onDest := openTxn(-20)
	m.Rollback(ctx)
	probe = appConnWhenRouted(t, f.gatewayPort)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == f.srcPortText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved back to the source")
	requireSessionEnded(t, onDest, "rollback direction", func() error {
		_, err := onDest.Exec(ctx, "COMMIT")
		return err
	})
	require.Zero(t, rowCount(t, f.srcAdmin, -20))
	require.Zero(t, rowCount(t, f.adminConn(f.def), -20))
}

// TestDelayedUnfenceHoldsWritesWithoutErrors: routing moves, and the destination
// opens only some time later. Writes in that gap are held rather than failed or
// sent anywhere, and every one is served once the destination admits.
func TestDelayedUnfenceHoldsWritesWithoutErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	w := startWorkload(t, f.gatewayPort, 4, false)
	waitForAcks(t, w, 50)

	var heldAcks int
	m.betweenRouteAndUnfence = func() {
		// Let the gateway see the new routing and the writers run into the closed
		// destination, then watch that nothing is acknowledged while it is closed.
		time.Sleep(2 * time.Second)
		before := w.ackedCount()
		// New logins are not held back by the closed gate: authentication is not
		// application work. Only the first statement of such a session would wait.
		loginCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		login, err := pgx.Connect(loginCtx, appDSN(f.gatewayPort))
		cancel()
		require.NoError(t, err, "a new login while the gate is closed must not wait for it")
		_ = login.Close(ctx)
		time.Sleep(4 * time.Second)
		heldAcks = w.ackedCount() - before
	}
	m.Cutover(ctx)
	require.Zero(t, heldAcks, "no write may be acknowledged while neither side admits")

	acked := w.ackedCount()
	waitForAcks(t, w, acked+50)
	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs, "writes held across the gap must be served, not failed")
	found, err := ledgerIDs(ctx, f.adminConn(f.def))
	require.NoError(t, err)
	requireExactly(t, "destination", ackedSet, found)
	m.requireSourceFrozen(ctx, "at the end of the test")
}

// activeBackends counts the active client backends of the source whose query text
// carries marker.
func activeBackends(t *testing.T, admin *pgx.Conn, marker string) int {
	t.Helper()
	var n int
	require.NoError(t, admin.QueryRow(t.Context(),
		"SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%"+marker+"%' AND pid <> pg_backend_pid()").Scan(&n))
	return n
}

// requireFenceAborted requires the error of a fence that could not complete and was
// aborted: ABORTED, saying the fence was aborted and containing every text given.
func requireFenceAborted(t *testing.T, err error, contains ...string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, codes.Aborted, status.Code(err), "%v", err)
	require.ErrorContains(t, err, "fence aborted")
	for _, text := range contains {
		require.ErrorContains(t, err, text)
	}
}

// requireServingAfterAbort requires the tablegroup to be UNFENCED under the abort of the given fence
// and its pooler to admit application queries again.
func requireServingAfterAbort(t *testing.T, f *migrationFixture, tablegroup, fenceID string, poolerPort int) {
	t.Helper()
	st := f.control().state(t.Context(), tablegroup).Tablegroups[0]
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.AdmissionState, "an aborted fence leaves normal serving, not a stuck FENCING")
	require.Equal(t, "abort-"+fenceID, st.RequestId)
	requireAdmits(t, poolerPort, "the pooler admits again after the abort")
}

// TestFenceAbortsWhileAStatementIsRunningAndTheSourceServesAgain: a statement that is
// still running when the drain deadline passes is neither cancelled nor terminated:
// the pooler says it is busy and the fence is aborted. The source serves again; a
// request sent during the attempt was held and is served with no client error; the aborted
// fence cannot be retried, and a fresh one succeeds once the statement is over.
func TestFenceAbortsWhileAStatementIsRunningAndTheSourceServesAgain(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	ctl := f.control()

	busy := appConnWhenRouted(t, f.gatewayPort)
	busyDone := make(chan error, 1)
	go func() {
		_, err := busy.Exec(context.Background(), "INSERT INTO ledger SELECT -2, 7 FROM (SELECT pg_sleep(9), 'busy-marker') s")
		busyDone <- err
	}()
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "busy-marker") == 1 },
		20*time.Second, 200*time.Millisecond, "the statement never started")

	// A request that arrives while the fence is draining is held, not failed.
	held := make(chan error, 1)
	go func() {
		time.Sleep(time.Second)
		conn, err := pgx.Connect(ctx, appDSN(f.gatewayPort))
		if err != nil {
			held <- err
			return
		}
		defer conn.Close(ctx)
		_, err = conn.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES (-70, 1)")
		held <- err
	}()

	_, err := ctl.fenceWithin(ctx, "migrateTG", "fence-busy-1", 60*time.Second)
	requireFenceAborted(t, err, "busy", "serving again")
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "busy-marker"), "the running statement was left alone")
	requireServingAfterAbort(t, f, "migrateTG", "fence-busy-1", f.src.grpcPort)

	select {
	case err := <-held:
		require.NoError(t, err, "a request held during an aborted fence is served by the source with no client error")
	case <-time.After(30 * time.Second):
		t.Fatal("the request sent during the attempt was never served")
	}
	require.Equal(t, 1, rowCount(t, f.srcAdmin, -70))

	// A delayed retry of the aborted fence cannot fence again.
	_, err = ctl.fenceWithin(ctx, "migrateTG", "fence-busy-1", 60*time.Second)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.ErrorContains(t, err, "was aborted")
	st := ctl.state(ctx, "migrateTG").Tablegroups[0]
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.AdmissionState)

	// The statement was left alone, and finishes: it commits. A fresh fence then succeeds.
	select {
	case err := <-busyDone:
		require.NoError(t, err, "the statement the fence left alone completes")
	case <-time.After(30 * time.Second):
		t.Fatal("the statement never returned")
	}
	require.Equal(t, 1, rowCount(t, f.srcAdmin, -2))
	resp, err := ctl.fenceWithin(ctx, "migrateTG", "fence-busy-2", 60*time.Second)
	require.NoError(t, err, "a fresh fence succeeds once nothing is running")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.AdmissionState)
}

// TestFenceClosesIdleSessionsAtTheDeadline: an idle-in-transaction session, a session pinned
// by an advisory lock and a session with a temporary table have nothing running at the
// deadline: their sockets are closed and the fence succeeds. Nothing they had done commits,
// and the next request of each is answered with FATAL 57P01 and the pooler's own message.
// The pooler's idle pooled connections are not touched.
func TestFenceClosesIdleSessionsAtTheDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)

	inTxn := appConnWhenRouted(t, f.gatewayPort)
	_, err := inTxn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = inTxn.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES (-1, 7)")
	require.NoError(t, err)
	pinned := appConnWhenRouted(t, f.gatewayPort)
	_, err = pinned.Exec(ctx, "SELECT pg_advisory_lock(4401)")
	require.NoError(t, err)
	temp := appConnWhenRouted(t, f.gatewayPort)
	_, err = temp.Exec(ctx, "CREATE TEMP TABLE scratch (a int)")
	require.NoError(t, err)

	// Idle pooled connections, from concurrent single queries, that must survive.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			conn := appConnWhenRouted(t, f.gatewayPort)
			for range 5 {
				_, _ = conn.Exec(ctx, "SELECT pg_sleep(0.05)")
			}
		})
	}
	wg.Wait()
	backends := func(where string) map[int]bool {
		rows, err := f.srcAdmin.Query(ctx, "SELECT pid FROM pg_stat_activity WHERE usename = $1 AND backend_type = 'client backend' AND "+where, appRole)
		require.NoError(t, err)
		defer rows.Close()
		pids := map[int]bool{}
		for rows.Next() {
			var pid int
			require.NoError(t, rows.Scan(&pid))
			pids[pid] = true
		}
		require.NoError(t, rows.Err())
		return pids
	}
	idlePooled := backends("state = 'idle'")
	require.NotEmpty(t, idlePooled)
	reserved := backends("state IN ('idle in transaction') OR query LIKE '%pg_advisory_lock(4401)%' OR query LIKE '%CREATE TEMP TABLE scratch%'")
	require.Len(t, reserved, 3)

	resp, err := f.control().fenceWithin(ctx, "migrateTG", "fence-idle-1", 60*time.Second)
	require.NoError(t, err, "nothing is running, so the fence succeeds")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.AdmissionState)

	after := backends("true")
	for pid := range reserved {
		require.False(t, after[pid], "the backend of an idle session (%d) was closed", pid)
	}
	require.Zero(t, rowCount(t, f.srcAdmin, -1), "the open transaction aborted")

	for name, conn := range map[string]*pgx.Conn{"idle in transaction": inTxn, "pinned by an advisory lock": pinned, "temporary table": temp} {
		err := func() error { _, err := conn.Exec(ctx, "SELECT 1"); return err }()
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "%s: %v", name, err)
		require.Equal(t, "57P01", pgErr.Code, name)
		require.Equal(t, "FATAL", pgErr.Severity, name)
		require.Contains(t, pgErr.Message, "closed by a migration fence", "%s: the pooler's own message, not PostgreSQL's", name)
	}
}

// TestFenceAbortsWhileAnAbandonedBackendIsStillRunning: a statement whose cancel did not take
// effect (it swallows the cancellation) is abandoned when the pooler closes its connection,
// and keeps running on the server. The pooler no longer has anything in flight, but it
// cannot rule that backend out: the fence is aborted. Once the backend is gone a fence succeeds.
func TestFenceAbortsWhileAnAbandonedBackendIsStillRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)

	conn, err := connectSendingCancelRequests(ctx, appDSN(f.gatewayPort))
	require.NoError(t, err)
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
  INSERT INTO ledger (id, writer) VALUES (-60, 7);
END $$`)
		stmtDone <- err
	}()
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

	_, err = f.control().fenceWithin(ctx, "migrateTG", "fence-abandoned-1", 60*time.Second)
	requireFenceAborted(t, err, "1 abandoned backends present")
	requireServingAfterAbort(t, f, "migrateTG", "fence-abandoned-1", f.src.grpcPort)
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "ignores-cancel-marker"), "the backend was left alone")

	// The backend is gone: a fence succeeds.
	_, err = f.srcAdmin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%ignores-cancel-marker%' AND pid <> pg_backend_pid()")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "ignores-cancel-marker") == 0 }, 20*time.Second, 200*time.Millisecond)
	resp, err := f.control().fenceWithin(ctx, "migrateTG", "fence-abandoned-2", 60*time.Second)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.AdmissionState)
	require.Zero(t, rowCount(t, f.srcAdmin, -60))
}

// TestFenceAbortsWhenAMemberIsUnreachableAndTheOthersReopen: a pooler of the tablegroup that
// cannot answer (it is stopped, but still registered) makes the fence abort, and the member
// that did fence reopens. The abort cannot reach the stopped member either, so the
// tablegroup reads UNFENCING and the error says how to finish; once the member is back,
// retrying the abort's request id reopens it too, and a fresh fence succeeds.
func TestFenceAbortsWhenAMemberIsUnreachableAndTheOthersReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	second := startUnmanagedPooler(t, f.def, "mig-src-2", "postgres", "mig-src", "--admission-drain-timeout=3s")
	waitBackendReady(t, second)
	requireAdmits(t, second.grpcPort, "the second source pooler must admit")
	second.suspend()

	_, err := f.control().fenceWithin(ctx, "migrateTG", "fence-member-1", 20*time.Second)
	requireFenceAborted(t, err, "mig-src-2", "reopening is incomplete", `request_id="abort-fence-member-1"`)
	requireAdmits(t, f.src.grpcPort, "the member that did fence reopened")
	st := f.control().state(ctx, "migrateTG").Tablegroups[0]
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING, st.AdmissionState, "the abort could not reach every member")

	second.resume()
	resp, err := f.control().unfence(ctx, "migrateTG", "abort-fence-member-1", "fence-member-1")
	require.NoError(t, err, "retrying the abort's request id finishes it")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, resp.State.AdmissionState)
	requireAdmits(t, second.grpcPort, "and the member that came back admits too")

	fresh, err := f.control().fenceWithin(ctx, "migrateTG", "fence-member-2", 60*time.Second)
	require.NoError(t, err, "once every member answers, a fresh fence succeeds")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, fresh.State.AdmissionState)
}

// TestRollbackFenceOfDefaultAbortsUnderBusyLoadAndKeepsServing: after a cutover, a statement
// still running on the default cohort makes the rollback's fence of default abort. The
// default cohort keeps serving, the migrator's retry succeeds once the statement is over.
func TestRollbackFenceOfDefaultAbortsUnderBusyLoadAndKeepsServing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}
	m.Cutover(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == portOf(t, f.def)
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the default cohort")

	defAdmin := f.adminConn(f.def)
	busy := appConnWhenRouted(t, f.gatewayPort)
	busyDone := make(chan error, 1)
	go func() {
		_, err := busy.Exec(context.Background(), "SELECT pg_sleep(9), 'default-busy-marker'")
		busyDone <- err
	}()
	require.Eventually(t, func() bool { return activeBackends(t, defAdmin, "default-busy-marker") == 1 },
		20*time.Second, 200*time.Millisecond, "the statement never started")

	_, err := f.control().fenceWithin(ctx, "default", "fence-default-busy", 60*time.Second)
	requireFenceAborted(t, err, "busy")
	st := f.control().state(ctx, "default").Tablegroups[0]
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.AdmissionState, "the default cohort stays serving")
	var one int
	require.NoError(t, probe.QueryRow(ctx, "SELECT 1").Scan(&one), "and answers application queries")

	select {
	case err := <-busyDone:
		require.NoError(t, err, "the statement the fence left alone completes")
	case <-time.After(30 * time.Second):
		t.Fatal("the statement never returned")
	}
	resp, err := f.control().fenceWithin(ctx, "default", "fence-default-2", 60*time.Second)
	require.NoError(t, err, "the retry succeeds once nothing is running")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.AdmissionState)
}

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

// TestACancelRequestStopsTheStatementOnTheServer is the control of the cancel scenario
// below: the same client sends a real cancel request, which reaches the pooler through
// the gateway; the pooler cancels the backend, and the statement stops with the cancel
// error. So a statement that outlives the cancel in the scenario below does so because
// it swallows the cancellation, not because no cancel was sent.
func TestACancelRequestStopsTheStatementOnTheServer(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	conn, err := connectSendingCancelRequests(ctx, appDSN(f.gatewayPort))
	require.NoError(t, err)
	stmtCtx, cancelStmt := context.WithCancel(ctx)
	stmtDone := make(chan error, 1)
	go func() {
		_, err := conn.Exec(stmtCtx, "SELECT pg_sleep(300), 'control-cancel-marker'")
		stmtDone <- err
	}()
	running := func() int {
		var n int
		require.NoError(t, f.srcAdmin.QueryRow(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%control-cancel-marker%' AND pid <> pg_backend_pid()").Scan(&n))
		return n
	}
	require.Eventually(t, func() bool { return running() == 1 }, 20*time.Second, 200*time.Millisecond, "the statement never started")

	cancelStmt()
	select {
	case err := <-stmtDone:
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "the client must get the server's cancel error, not a closed socket: %v", err)
		require.Equal(t, "57014", pgErr.Code)
	case <-time.After(8 * time.Second):
		t.Fatal("the cancelled statement did not return")
	}
	require.Eventually(t, func() bool { return running() == 0 }, 10*time.Second, 200*time.Millisecond, "the cancel must have stopped the statement on the server")
}

// TestDirectClientsOfTheSourceDoNotBlockItsFence: a fence ends the work of the clients
// the pooler serves, not of clients that reach the source without going through
// Multigres: stopping those is the migrator's job, which the migration design says in so
// many words. A direct client that is mid-statement does not hold the fence back, and is
// not terminated by it.
func TestDirectClientsOfTheSourceDoNotBlockItsFence(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)

	bypass, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable", appRole, appPassword, f.srcPort))
	require.NoError(t, err)
	bypassCtx, stopBypass := context.WithCancel(ctx)
	defer stopBypass()
	go func() { _, _ = bypass.Exec(bypassCtx, "SELECT pg_sleep(300), 'bypass-marker'") }()
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "bypass-marker") == 1 }, 20*time.Second, 200*time.Millisecond)

	resp, err := f.control().fenceWithin(ctx, "migrateTG", "fence-direct-1", 60*time.Second)
	require.NoError(t, err, "a direct client must not block the fence")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.AdmissionState)
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "bypass-marker"), "the fence does not end a client it does not serve")
}

// TestRollbackWithADefaultReplicaDown: the default cohort's backends are down for a
// replica when the rollback fences it. A replica cannot be written to, so a
// backend that cannot be asked does not hold the fence back: the fence is
// acknowledged by all three, the rollback completes, and when the replica returns it
// comes back closed, not open, since its decision is the fence.
func TestRollbackWithADefaultReplicaDown(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	w := startWorkload(t, f.gatewayPort, 2, false)
	waitForAcks(t, w, 20)
	m.Cutover(ctx)
	waitForAcks(t, w, w.ackedCount()+20)

	primary := f.def.GetPrimary(t).Name
	var replica string
	for name := range f.def.Multipoolers {
		if name != primary {
			replica = name
			break
		}
	}
	require.NotEmpty(t, replica)
	resume := f.def.StopPostgres(t, replica, "fast")
	defer resume()

	m.Rollback(ctx)
	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs)
	back, err := ledgerIDs(ctx, f.srcAdmin)
	require.NoError(t, err)
	requireExactly(t, "source after a rollback with a default replica down", ackedSet, back)

	// The replica comes back fenced.
	resume()
	waitForAllPostgresReady(t, f.def, 120*time.Second)
	for name, inst := range f.def.Multipoolers {
		requireStaysClosed(t, inst.Multipooler.GrpcPort, 3*time.Second, "default pooler "+name+" must stay fenced after the rollback")
	}
}
