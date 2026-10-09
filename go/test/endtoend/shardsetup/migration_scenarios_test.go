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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

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

	// A second gateway whose routing poll is slower than the whole cutover.
	staleGateway := startRoutedGatewayNamed(t, f.def, "stale-gateway", "20s")
	stale := appConnWhenRouted(t, staleGateway)
	port, err := backendPort(ctx, stale)
	require.NoError(t, err)
	require.Equal(t, f.srcPortText, port)

	m.Cutover(ctx)
	cutoverDone := time.Now()
	destText := portOf(t, f.dest)

	// The stale gateway still believes the source owns the traffic. Its request is
	// held, then served by the destination once it learns routing moved.
	port, err = backendPort(ctx, stale)
	require.NoError(t, err, "a request through a stale gateway must be held, not failed")
	require.Equal(t, destText, port, "and served by the destination")
	t.Logf("the stale gateway's request was served by the destination %s after the cutover", time.Since(cutoverDone).Round(time.Millisecond))

	// The long transaction was terminated at the drain deadline: it cannot commit,
	// and its write is on neither side.
	_, _ = long.Exec(ctx, "COMMIT")
	var n int
	require.NoError(t, f.srcAdmin.QueryRow(ctx, "SELECT count(*) FROM ledger WHERE id = -1").Scan(&n))
	require.Zero(t, n, "the terminated transaction's write must not be on the source")
	require.NoError(t, f.adminConn(f.dest).QueryRow(ctx, "SELECT count(*) FROM ledger WHERE id = -1").Scan(&n))
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
// fence is waiting on a pooler. The persisted state must stay FENCING, and the
// same request, retried against the new default primary, must complete.
func TestDefaultPrimaryFailoverDuringFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	// A long drain keeps the fence waiting while the default primary fails.
	f := newMigrationFixture(t, 40*time.Second)
	f.def.StartMultiorchs(ctx, t)

	long := openLongTransaction(t, f.gatewayPort)

	ctl := f.control()
	done := make(chan error, 1)
	go func() {
		_, err := ctl.fence(ctx, "migrateTG", "fence-src-1")
		done <- err
	}()

	// The coordinator has decided: the fence is persisted as FENCING and is waiting.
	require.Eventually(t, func() bool {
		st := f.control().state(ctx, "migrateTG").Tablegroups[0]
		return st.AdmissionState == multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING && st.RequestId == "fence-src-1"
	}, 20*time.Second, 200*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the fence returned %v while a transaction was still open", err)
	case <-time.After(time.Second):
	}

	// The default primary fails mid fan-out.
	failOverPrimary(t, f.def)
	require.Error(t, <-done, "the call that was in flight must not report success")

	// The new default primary reads the same persisted decision.
	var st *multipoolerservicepb.TablegroupServingState
	require.Eventually(t, func() bool {
		resp, err := f.control().GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{Database: f.def.Database, Tablegroups: []string{"migrateTG"}})
		if err != nil {
			return false
		}
		st = resp.Tablegroups[0]
		return true
	}, 60*time.Second, 500*time.Millisecond)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, st.AdmissionState, "an interrupted fence stays FENCING")
	require.Equal(t, "fence-src-1", st.RequestId)

	// Recovery: let the transaction go, then retry the same request.
	_ = long.Close(ctx)
	require.Eventually(t, func() bool {
		resp, err := f.control().fence(ctx, "migrateTG", "fence-src-1")
		return err == nil && resp.State.AdmissionState == multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	}, 90*time.Second, time.Second, "retrying the same request id against the new default primary must complete the fence")
	for _, p := range f.srcPoolers {
		require.False(t, admits(t, p.grpcPort), "both source poolers must be closed")
	}
	st = f.control().state(ctx, "migrateTG").Tablegroups[0]
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.AdmissionState)
	require.Equal(t, "fence-src-1", st.RequestId)
}

// TestDestFailoverAfterCutover fails over the destination cohort's primary after
// the cutover, with writers running. The failover must not reopen the source, must
// not change the persisted admission decisions, and must not lose a write.
func TestDestFailoverAfterCutover(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	f.dest.StartMultiorchs(ctx, t)
	m := &migrator{f: f}

	// An in-flight write can fail when the destination's primary crashes. Those
	// writers resolve an unknown outcome by retrying the same key.
	w := startWorkload(t, f.gatewayPort, 4, true)
	waitForAcks(t, w, 50)
	m.Cutover(ctx)
	acked := w.ackedCount()
	waitForAcks(t, w, acked+50)

	before := f.control().state(ctx, "migrateTG", "destTG")

	failOverPrimary(t, f.dest)

	// Writes resume on the new destination primary.
	acked = w.ackedCount()
	waitForAcks(t, w, acked+50)
	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs, "every write must resolve, by being held or by a retry of its own key")
	t.Logf("%d writes resolved across the destination failover, %d of them by retry", len(ackedSet), w.retryCount())

	found, err := ledgerIDs(ctx, f.adminConn(f.dest))
	require.NoError(t, err)
	requireExactly(t, "destination after its failover", ackedSet, found)

	// Admission and routing are exactly as the cutover left them.
	after := f.control().state(ctx, "migrateTG", "destTG")
	require.Equal(t, before.AppTablegroup, after.AppTablegroup)
	require.Equal(t, before.RoutingVersion, after.RoutingVersion)
	for i := range before.Tablegroups {
		require.Equal(t, before.Tablegroups[i].AdmissionState, after.Tablegroups[i].AdmissionState, before.Tablegroups[i].Tablegroup)
		require.Equal(t, before.Tablegroups[i].RequestId, after.Tablegroups[i].RequestId, before.Tablegroups[i].Tablegroup)
	}
	for _, p := range f.srcPoolers {
		require.False(t, admits(t, p.grpcPort), "a destination failover must not reopen the source")
	}
}
