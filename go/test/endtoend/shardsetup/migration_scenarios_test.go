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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

// scenarioCutoverAndRollbackUnderLoad runs a full cutover and rollback with
// writers going through the gateway the whole time. It checks that no write is
// lost or duplicated, that nothing lands on a side after its fence was
// acknowledged, that a transaction that commits during the drain does not make the
// fence busy and its COMMIT succeeds, that the writers were really held, and that a rollback
// inside the buffer's timing guard holds the writers too.
func scenarioCutoverAndRollbackUnderLoad(t *testing.T, f *migrationFixture) {
	ctx := t.Context()
	// The data movement takes half a second, in which both sides are fenced and the writers
	// can only wait.
	m := &migrator{f: f, catchUpDelay: 500 * time.Millisecond}

	w := startWorkload(t, f.gatewayPort, 4, false)
	waitForAcks(t, w, 50)

	// A transaction that is open when the cutover starts and commits inside the fence's
	// drain: it commits once the source's gate is observed closed, which an existing
	// reservation passes, and the COMMIT must not fail.
	long := openLongTransaction(t, f.gatewayPort)
	committed := make(chan error, 1)
	duringDrain := make(chan bool, 1)
	go func() {
		for end := time.Now().Add(30 * time.Second); admits(t, f.src.grpcPort) && time.Now().Before(end); {
			time.Sleep(20 * time.Millisecond)
		}
		duringDrain <- !admits(t, f.src.grpcPort)
		_, err := long.Exec(ctx, "COMMIT")
		committed <- err
	}()

	cutoverStarted := time.Now()
	m.Cutover(ctx)
	require.True(t, <-duringDrain, "the COMMIT was sent while the source's gate was closed")
	require.NoError(t, <-committed, "a COMMIT inside the drain passes the closed gate and succeeds")

	// The transaction committed on the source before its fence was acknowledged, so the
	// catch-up carried it to the destination.
	require.Equal(t, 1, rowCount(t, f.srcAdmin, -1), "the transaction committed during the drain")
	require.Equal(t, 1, rowCount(t, f.adminConn(f.def), -1), "and the catch-up copied it")

	// Traffic runs on the destination for a while, then comes back.
	acked := w.ackedCount()
	waitForAcks(t, w, acked+50)
	// The rollback starts well inside the buffer's timing guard (one minute between
	// failovers by default): each of its fences is a second buffer cycle for its tablegroup
	// within the guard, and must hold the writers rather than let them fail.
	require.Less(t, time.Since(cutoverStarted), 45*time.Second,
		"the rollback must start well inside the one minute timing guard, or this proves nothing")
	m.Rollback(ctx)
	acked = w.ackedCount()
	waitForAcks(t, w, acked+50)

	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs, "no client may see an error across a cutover and a rollback")
	require.Greater(t, w.longestWrite(), 400*time.Millisecond, "the writers were really held while neither side admitted")
	require.Less(t, w.longestWrite(), 25*time.Second, "and the pause is bounded by the drain and the catch-up")
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
// The fence is persisted before the members are asked, so it survives the failover, and
// nothing undoes it: the migrator, which is waiting on the call, gets the error, reads
// GetServingState, and unfences the tablegroup at the version it read, through the new
// default primary. Then a fence at a fresh version works, and the delayed retry of the
// first, which names the version before, is rejected.
func TestDefaultPrimaryFailoverDuringFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	slowScenario(t)
	runInParallel(t)
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
	// A drain that keeps the fence waiting on the open transaction while the default
	// primary fails. The cutover that precedes fencing the default tablegroup has nothing
	// open to wait for, so it is not slowed by it.
	f := newMigrationFixtureWith(t, replicated(migrationOptions{drain: 6 * time.Second}))
	f.def.StartMultiorchs(ctx, t)
	m := &migrator{f: f}
	if fenced == "default" {
		// The default tablegroup carries the application's work only after the
		// cutover, so there is something for its fence to wait for.
		m.Cutover(ctx)
	}

	long := openLongTransaction(t, f.gatewayPort)
	ctl := f.control()
	before := ctl.version(ctx, fenced)
	done := make(chan error, 1)
	go func() {
		_, err := ctl.fenceAt(ctx, fenced, before, 100*time.Second)
		done <- err
	}()

	// The coordinator has decided: the fence is persisted, and the call is waiting on the
	// open transaction.
	require.Eventually(t, func() bool {
		resp, err := f.control().tryState(ctx, fenced)
		if err != nil {
			return false
		}
		st := resp.Tablegroups[0]
		return st.State == admFencing && st.Version == before+1
	}, 20*time.Second, 100*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the fence returned %v while a transaction was still open", err)
	case <-time.After(time.Second):
	}

	// The default primary fails mid fan-out. The call in flight may complete (its members
	// answer without the failed server) or fail; what must hold is what is persisted.
	oldPrimary, newPrimary := failOverPrimary(t, f.def)
	require.NotEqual(t, oldPrimary, newPrimary, "the default primary must really have changed")
	select {
	case <-done:
	case <-time.After(150 * time.Second):
		t.Fatal("the in-flight fence neither completed nor failed")
	}

	// The new default primary reads the same persisted row, and answers the control plane
	// although its own tablegroup may be the one being fenced. Nothing undoes the fence.
	_ = long.Close(ctx)
	var st *multipoolerservicepb.TablegroupServingState
	require.Eventually(t, func() bool {
		resp, err := f.control().tryState(ctx, fenced)
		if err != nil {
			return false
		}
		st = resp.Tablegroups[0]
		return true
	}, 60*time.Second, 500*time.Millisecond)
	require.Equal(t, admFencing, st.State, "a fence that was committed survives the failover of its coordinator, as FENCING: its members were not all confirmed clean")
	require.EqualValues(t, before+1, st.Version)

	// A flip refuses it, whichever way the migrator reached this state: the incomplete fence
	// is not a completed one.
	app := f.control().state(ctx).AppTablegroup
	other := "default"
	if app == "default" {
		other = "migrateTG"
	}
	_, err := f.control().flip(ctx, app, other)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a flip must refuse a fence that is not complete: %v", err)
	require.Equal(t, app, f.control().state(ctx).AppTablegroup, "the traffic did not move")

	// The migrator resumes the same fence through the new default primary: the transaction
	// that held it is gone, every member is clean, and it completes at the version it has.
	require.Eventually(t, func() bool {
		resp, err := f.control().fenceAt(ctx, fenced, st.Version, 60*time.Second)
		return err == nil && resp.State.State == admFenced && resp.State.Version == st.Version
	}, 90*time.Second, time.Second, "the resumed fence completes")

	// The migrator's policy on any error: read the state; the tablegroup is fenced and the
	// cutover has not flipped, so unfence it at the version read, and fence later at a fresh one.
	require.Eventually(t, func() bool {
		_, err := f.control().unfenceAt(ctx, fenced, st.Version)
		return err == nil
	}, 90*time.Second, time.Second, "the migrator unfences the fence it found, through the new default primary")
	st = f.control().state(ctx, fenced).Tablegroups[0]
	require.Equal(t, admUnfenced, st.State)
	require.EqualValues(t, before+2, st.Version)
	if fenced == "default" {
		for name, inst := range f.def.Multipoolers {
			requireAdmits(t, inst.Multipooler.GrpcPort, "default pooler "+name+" reopens once the migrator unfences")
		}
		require.False(t, admits(t, f.src.grpcPort), "the source's fence was complete before the failover and stays")
	} else {
		requireAdmits(t, f.src.grpcPort, "the source pooler reopens once the migrator unfences")
	}

	// The delayed retry of the fence, naming the version before it, is rejected; a fence at
	// the current version works.
	_, err = f.control().fenceAt(ctx, fenced, before, 30*time.Second)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Eventually(t, func() bool {
		resp, err := f.control().fenceAt(ctx, fenced, before+2, 30*time.Second)
		return err == nil && resp.State.State == admFenced
	}, 90*time.Second, time.Second, "a fence naming the current version must complete")
	st = f.control().state(ctx, fenced).Tablegroups[0]
	require.Equal(t, admFenced, st.State)
	require.EqualValues(t, before+3, st.Version)
	if fenced == "migrateTG" {
		require.False(t, admits(t, f.src.grpcPort), "the source pooler must be closed by the new fence")
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
	f := newMigrationFixtureWith(t, replicated(migrationOptions{drain: 3 * time.Second}))
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

	// The default cohort was not fenced: its failover is an ordinary one, and what the
	// gateway held, it held for the failover. The new primary is closed until it has read
	// its admission state, which is the same closed gate a fence makes.
	requireGatewayLogLine(t, f.gatewayPort, logOffset, "the failover must have held requests for the default tablegroup",
		"failover detected, starting buffering", `"tablegroup":"default"`)

	found, err := ledgerIDs(ctx, f.adminConn(f.def))
	require.NoError(t, err)
	requireExactly(t, "default cohort after its failover", ackedSet, found)

	// Admission and routing are exactly as the cutover left them, as the new
	// primary's coordinator reports them.
	after := f.control().state(ctx, "migrateTG", "default")
	require.Equal(t, before.AppTablegroup, after.AppTablegroup)
	require.Equal(t, before.RoutingVersion, after.RoutingVersion)
	for i := range before.Tablegroups {
		require.Equal(t, before.Tablegroups[i].State, after.Tablegroups[i].State, before.Tablegroups[i].Tablegroup)
		require.Equal(t, before.Tablegroups[i].Version, after.Tablegroups[i].Version, before.Tablegroups[i].Tablegroup)
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

// scenarioSessionsHoldingState: a session that holds a transaction, a savepoint, a WITH HOLD
// cursor, temp tables, a session advisory lock or any other reservation on the source
// makes the fence busy, whatever it is doing, and the fence never ends the session's state:
// every session carries on as if nothing had happened, and what it does commits. Once the
// clients have let their state go the cutover goes through with no client error, and
// sessions with no state, even ones that only prepared statements, are served by the
// destination without noticing. The rollback direction is the same: a transaction open on
// the destination holds its fence busy.
func scenarioSessionsHoldingState(t *testing.T, f *migrationFixture) {
	ctx := t.Context()
	m := &migrator{f: f}

	inTxn := func(setup ...string) *pgx.Conn {
		conn := appConnWhenRouted(t, f.gatewayPort)
		_, err := conn.Exec(ctx, "BEGIN")
		require.NoError(t, err)
		for _, sql := range setup {
			_, err = conn.Exec(ctx, sql)
			require.NoError(t, err, sql)
		}
		return conn
	}
	var one int
	extendedTxn := inTxn()
	require.NoError(t, extendedTxn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&one)) // Parse, Bind, Execute
	committing := inTxn("INSERT INTO ledger (id, writer) VALUES (-1, 7)")
	rolledBack := inTxn("INSERT INTO ledger (id, writer) VALUES (-2, 7)")
	savepoint := inTxn("SAVEPOINT s1", "INSERT INTO ledger (id, writer) VALUES (-3, 7)")
	holdCursor := appConnWhenRouted(t, f.gatewayPort)
	_, err := holdCursor.Exec(ctx, "DECLARE held CURSOR WITH HOLD FOR SELECT id FROM ledger")
	require.NoError(t, err)
	tempOnly := appConnWhenRouted(t, f.gatewayPort)
	_, err = tempOnly.Exec(ctx, "CREATE TEMP TABLE scratch (i int)")
	require.NoError(t, err)
	locker := appConnWhenRouted(t, f.gatewayPort)
	_, err = locker.Exec(ctx, "SELECT pg_advisory_lock(4401)")
	require.NoError(t, err)
	// Sessions with no state on the source, ones that only prepared statements among them.
	stateless := appConnWhenRouted(t, f.gatewayPort)
	prepared := appConnWhenRouted(t, f.gatewayPort)
	sd, err := prepared.Prepare(ctx, "count_rows", "SELECT count(*) FROM ledger WHERE id >= $1")
	require.NoError(t, err)
	require.Len(t, sd.Fields, 1, "Describe of a prepared statement")
	require.NoError(t, prepared.QueryRow(ctx, "count_rows", int64(0)).Scan(&one))

	// The fence is busy, whatever the sessions are doing. It never undoes itself: the row stays
	// fenced and the source's gate closed, and nothing of the sessions' is ended.
	before := f.control().version(ctx, "migrateTG")
	_, err = f.control().fenceAt(ctx, "migrateTG", before, 60*time.Second)
	results := requireFenceNotClean(t, err)
	requireMember(t, results, "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "7 reservations held")
	requireStillFenced(t, f, "migrateTG", before, f.src.grpcPort)

	// Nothing was ended: every session carries on, through the closed gate, and what it does
	// commits.
	require.NoError(t, extendedTxn.QueryRow(ctx, "SELECT $1::int", 2).Scan(&one), "extended protocol in a transaction")
	_, err = savepoint.Exec(ctx, "ROLLBACK TO SAVEPOINT s1")
	require.NoError(t, err, "ROLLBACK TO SAVEPOINT")
	_, err = savepoint.Exec(ctx, "RELEASE SAVEPOINT s1")
	require.NoError(t, err, "RELEASE SAVEPOINT")
	_, err = holdCursor.Exec(ctx, "FETCH 1 FROM held")
	require.NoError(t, err, "FETCH from a WITH HOLD cursor")
	_, err = tempOnly.Exec(ctx, "INSERT INTO scratch VALUES (1)")
	require.NoError(t, err, "the temporary table")
	var unlocked bool
	require.NoError(t, locker.QueryRow(ctx, "SELECT pg_advisory_unlock(4401)").Scan(&unlocked))
	require.True(t, unlocked, "the session still holds its lock")
	_, err = committing.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, 1, rowCount(t, f.srcAdmin, -1), "the transaction that was deferred for commits on the source")
	_, err = rolledBack.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.Zero(t, rowCount(t, f.srcAdmin, -2))

	// The clients let their state go, and the cutover goes through: its fence is the same
	// one, retried, and is clean now.
	for _, c := range []*pgx.Conn{extendedTxn, savepoint, holdCursor, tempOnly, locker} {
		require.NoError(t, c.Close(ctx))
	}
	require.Eventually(t, func() bool { return countHeldBackends(t, f.srcAdmin) == 0 }, 20*time.Second, 200*time.Millisecond, "the source's application backends never settled")
	// The busy fence left in place on the source is this migrator's own: the cutover resumes it.
	m.resume = true
	m.Cutover(ctx)
	m.resume = false
	probe := appConnWhenRouted(t, f.gatewayPort)
	destText := portOf(t, f.def)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == destText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the destination")

	// Sessions with no state on the source are served by the destination as usual,
	// prepared statements included.
	port, err := backendPort(ctx, stateless)
	require.NoError(t, err)
	require.Equal(t, destText, port)
	require.NoError(t, prepared.QueryRow(ctx, "count_rows", int64(0)).Scan(&one), "Bind and Execute of a prepared statement across the flip")
	sd, err = prepared.Prepare(ctx, "count_again", "SELECT id, writer FROM ledger")
	require.NoError(t, err, "Parse and Describe across the flip")
	require.Len(t, sd.Fields, 2)
	port, err = backendPort(ctx, prepared)
	require.NoError(t, err)
	require.Equal(t, destText, port, "the session is served by the destination")
	require.Equal(t, 1, rowCount(t, f.srcAdmin, -1), "the committed row stayed on the source")

	// The rollback direction: a transaction open on the destination holds the fence busy, and
	// the client's COMMIT or ROLLBACK reports the truth.
	onDest := inTxn("INSERT INTO ledger (id, writer) VALUES (-20, 7)")
	before = f.control().version(ctx, "default")
	_, err = f.control().fenceAt(ctx, "default", before, 60*time.Second)
	requireSomeMemberBusy(t, requireFenceNotClean(t, err), "1 reservations held")
	requireStillFenced(t, f, "default", before, f.def.GetPrimary(t).Multipooler.GrpcPort)
	_, err = onDest.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.Zero(t, rowCount(t, f.adminConn(f.def), -20))
	// Likewise the busy fence on the destination.
	m.resume = true
	m.Rollback(ctx)
	m.resume = false
	probe = appConnWhenRouted(t, f.gatewayPort)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == f.srcPortText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved back to the source")
}

// countHeldBackends counts the application's backends on the source that are not idle or
// that hold a lock, as a client that disconnected leaves them until the pooler lets go.
func countHeldBackends(t *testing.T, admin *pgx.Conn) int {
	t.Helper()
	var n int
	require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity
WHERE usename = $1 AND backend_type = 'client backend' AND pid <> pg_backend_pid()
  AND (state <> 'idle' OR EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = pg_stat_activity.pid AND l.locktype = 'advisory'))`, appRole).Scan(&n))
	return n
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

// requireFenceNotClean requires the error of a fence that some member did not complete,
// and returns what each member answered: ABORTED, with the per-member results as the details
// of the error. A fence that cannot complete never undoes itself.
func requireFenceNotClean(t *testing.T, err error) map[string]*multipoolerservicepb.MemberFenceResult {
	t.Helper()
	require.Error(t, err)
	return memberResults(t, err)
}

// requireMember requires a member's answer to a fence: its status, and that its detail
// contains every text given.
func requireMember(t *testing.T, results map[string]*multipoolerservicepb.MemberFenceResult, member string, want multipoolerservicepb.MemberFenceResult_Status, contains ...string) {
	t.Helper()
	got, ok := results[member]
	require.True(t, ok, "no result for member %s in %v", member, results)
	require.Equal(t, want, got.Status, "%s: %s", member, got.Detail)
	for _, text := range contains {
		require.Contains(t, got.Detail, text, member)
	}
}

// requireSomeMemberBusy requires that a member answered busy with the given text, whichever
// it is, and returns its name.
func requireSomeMemberBusy(t *testing.T, results map[string]*multipoolerservicepb.MemberFenceResult, contains string) string {
	t.Helper()
	for name, m := range results {
		if m.Status == multipoolerservicepb.MemberFenceResult_STATUS_BUSY && strings.Contains(m.Detail, contains) {
			return name
		}
	}
	t.Fatalf("no member answered busy with %q: %v", contains, results)
	return ""
}

// requireStillFenced requires that a fence that was not clean left the tablegroup FENCING, at
// the version the fence has, and its pooler's gate closed, however long the migrator takes to
// decide.
func requireStillFenced(t *testing.T, f *migrationFixture, tablegroup string, versionBefore int64, poolerPort int) {
	t.Helper()
	st := f.control().state(t.Context(), tablegroup).Tablegroups[0]
	require.Equal(t, admFencing, st.State, "a fence that could not complete stays FENCING")
	require.EqualValues(t, versionBefore+1, st.Version, "and wrote nothing more")
	require.False(t, admits(t, poolerPort), "the pooler's gate stays closed")
}

// giveUp is the migrator giving up on a fence it made at versionBefore: it unfences at the
// version the fence has, until every member was reached, and requires the tablegroup to be
// serving again at the version after.
func giveUp(t *testing.T, f *migrationFixture, tablegroup string, versionBefore int64, poolerPort int) {
	t.Helper()
	require.NoError(t, f.lib().UnfenceUntilReached(t.Context(), tablegroup, versionBefore+1))
	st := f.control().state(t.Context(), tablegroup).Tablegroups[0]
	require.Equal(t, admUnfenced, st.State)
	require.EqualValues(t, versionBefore+2, st.Version, "the fence and the unfence each moved the version once")
	requireAdmits(t, poolerPort, "the pooler admits again after the unfence")
}

// scenarioDirectClientsOfTheSource: a client that reaches the source without going through
// Multigres is not the pooler's to end, but its work is not safe to ignore either: the
// fence cannot tell it from a statement the pooler lost track of, so a direct client that
// is running something makes the fence busy, and is not terminated by it. A direct client that
// is idle does not, and neither is it held up. Other clients of a source have to be stopped
// before a cutover.
func scenarioDirectClientsOfTheSource(t *testing.T, f *migrationFixture) {
	ctx := t.Context()
	ctl := f.control()

	direct, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable", appRole, appPassword, f.srcPort))
	require.NoError(t, err)
	defer func() { _ = direct.Close(context.Background()) }()
	_, err = direct.Exec(ctx, "SELECT 1")
	require.NoError(t, err)

	// Idle: the fence goes through, and the session is left alone.
	resp, err := ctl.fenceWithin(ctx, "migrateTG", 60*time.Second)
	require.NoError(t, err, "an idle direct client does not hold the fence back")
	require.NoError(t, f.lib().UnfenceUntilReached(ctx, "migrateTG", resp.State.Version))
	_, err = direct.Exec(ctx, "SELECT 1")
	require.NoError(t, err, "the fence does not end a client it does not serve")
	requireAdmits(t, f.src.grpcPort, "the source serves again after the unfence")

	// Running a statement: the fence is busy and leaves it running.
	stopDirect, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _, _ = direct.Exec(stopDirect, "SELECT pg_sleep(300), 'bypass-marker'") }()
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "bypass-marker") == 1 }, 20*time.Second, 200*time.Millisecond)

	before := ctl.version(ctx, "migrateTG")
	_, err = ctl.fenceAt(ctx, "migrateTG", before, 60*time.Second)
	requireMember(t, requireFenceNotClean(t, err), "mig-src-1", multipoolerservicepb.MemberFenceResult_STATUS_BUSY, "1 unexplained backends present")
	requireStillFenced(t, f, "migrateTG", before, f.src.grpcPort)
	require.Equal(t, 1, activeBackends(t, f.srcAdmin, "bypass-marker"), "the fence does not end a client it does not serve")
	giveUp(t, f, "migrateTG", before, f.src.grpcPort)

	// The shared cluster is reset after this scenario, and its reset waits for the source's
	// application backends to settle: end the statement here and wait for it to be gone.
	_, err = f.srcAdmin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%bypass-marker%' AND pid <> pg_backend_pid()")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return activeBackends(t, f.srcAdmin, "bypass-marker") == 0 }, 20*time.Second, 200*time.Millisecond)
}
