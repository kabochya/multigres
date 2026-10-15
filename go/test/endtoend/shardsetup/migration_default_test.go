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

// PROTOTYPE STUB: end-to-end scenarios for the default tablegroup as the destination
// of a migration. The default cohort holds the cluster metadata and answers the
// control plane, so fencing it must leave the control plane working. Delete them
// with the test migrator.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// requireNotification waits for a NOTIFY on conn's LISTEN channels.
func requireNotification(t *testing.T, conn *pgx.Conn, channel, payload, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	require.NoError(t, err, msg)
	require.Equal(t, channel, n.Channel, msg)
	require.Equal(t, payload, n.Payload, msg)
}

// firstHealthOf reads one message from a pooler's health stream.
func firstHealthOf(ctx context.Context, grpcPort int) (*multipoolerservicepb.StreamPoolerHealthResponse, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	stream, err := multipoolerservicepb.NewMultipoolerServiceClient(conn).StreamPoolerHealth(ctx, &multipoolerservicepb.StreamPoolerHealthRequest{})
	if err != nil {
		return nil, err
	}
	return stream.Recv()
}

// TestFencedDefaultStillServesTheControlPlane fences the default tablegroup, which
// stays fenced through the whole synchronization phase of a migration, and checks
// that everything that is not application work still gets through its gate: the
// serving-control RPCs, credential lookups for a new login, health streams, the
// routing poll, the manager RPCs (the consensus status among them), the heartbeat and
// the manager's own administrative queries. NOTIFY is control traffic too and reaches a LISTENer, in
// both windows: while the source still carries the application, and after routing
// has moved to the default tablegroup but it is not unfenced yet.
func TestFencedDefaultStillServesTheControlPlane(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	primary := f.def.GetPrimary(t).Multipooler.GrpcPort
	ctl := f.control()

	// A client that listens on the default cohort, as the application's LISTEN does.
	listener := appConnWhenRouted(t, f.gatewayPort)
	_, err := listener.Exec(ctx, "LISTEN audit_chan")
	require.NoError(t, err)
	notify := func(payload string) error {
		_, err := appConn(t, f.gatewayPort).Exec(ctx, fmt.Sprintf("NOTIFY audit_chan, '%s'", payload))
		return err
	}
	require.NoError(t, notify("before"))
	requireNotification(t, listener, "audit_chan", "before", "NOTIFY reaches a LISTENer while nothing is fenced")

	// The sync phase: the destination is fenced, the source still serves.
	resp, err := ctl.fence(ctx, "default", "fence-def-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 3)
	for name, inst := range f.def.Multipoolers {
		require.False(t, admits(t, inst.Multipooler.GrpcPort), "default pooler %s must refuse application work", name)
	}

	// The serving-control RPCs, on the fenced default primary.
	st := ctl.state(ctx, "migrateTG", "default")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Tablegroups[1].AdmissionState)
	backing, err := ctl.GetBackingConnection(ctx, &multipoolerservicepb.GetBackingConnectionRequest{Database: f.def.Database, ConnectionName: "mig-src"})
	require.NoError(t, err, "GetBackingConnection")
	require.Equal(t, "mig-src", backing.GetConnection().GetName())
	refreshed, err := refreshAdmission(ctx, primary, f.def.Database, "default", "FENCED", "fence-def-1")
	require.NoError(t, err, "RefreshAdmission")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, refreshed.AppliedState)

	// Health streams report the closed gate and keep streaming.
	health, err := firstHealthOf(ctx, primary)
	require.NoError(t, err, "health stream")
	require.True(t, health.GetAdmissionClosed())

	// Manager and consensus RPCs, which the orchestrator uses.
	client, err := NewMultipoolerClient(primary)
	require.NoError(t, err)
	defer client.Close()
	status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "manager Status")
	require.NotNil(t, status.GetConsensusStatus(), "the consensus status, which the orchestrator reads, is served too")

	// The heartbeat, written by the manager's own administrative queries, keeps
	// advancing.
	heartbeat := func() int64 {
		var ts int64
		require.NoError(t, f.adminConn(f.def).QueryRow(ctx, "SELECT max(ts) FROM multigres.heartbeat").Scan(&ts))
		return ts
	}
	first := heartbeat()
	require.Eventually(t, func() bool { return heartbeat() > first }, 20*time.Second, 500*time.Millisecond, "the heartbeat stopped while the default tablegroup was fenced")

	// Application traffic still runs on the source, new logins included, and NOTIFY
	// still reaches the LISTENer on the fenced default cohort.
	login, err := pgx.Connect(ctx, appDSN(f.gatewayPort))
	require.NoError(t, err, "a new login while the default tablegroup is fenced")
	port, err := backendPort(ctx, login)
	require.NoError(t, err)
	require.Equal(t, f.srcPortText, port, "application traffic is still on the source")
	_ = login.Close(ctx)
	require.NoError(t, notify("sync-phase"))
	requireNotification(t, listener, "audit_chan", "sync-phase", "NOTIFY must reach a LISTENer while the default tablegroup is fenced")

	// The cutover's next steps run against the fenced default primary too.
	_, err = ctl.fence(ctx, "migrateTG", "fence-src-1")
	require.NoError(t, err)
	logOffset := gatewayLogSize(t, f.gatewayPort)
	_, err = ctl.route(ctx, "migrateTG", "default", "route-1")
	require.NoError(t, err, "UpdateMigrationRouting with the default tablegroup as the destination")

	// The routing poll reads the new pointer from the fenced default primary: the
	// gateway learns it before anything is unfenced.
	requireGatewayLogLine(t, f.gatewayPort, logOffset, "the gateway never read the new routing from the fenced default primary",
		"application routing changed", `"tablegroup":"default"`)

	// Window two: application traffic is routed to the fenced default tablegroup and
	// held, but a NOTIFY is not application traffic.
	writer := appConn(t, f.gatewayPort)
	held := make(chan error, 1)
	go func() {
		callCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
		defer cancel()
		_, err := writer.Exec(callCtx, "INSERT INTO ledger (id, writer) VALUES (-7, 1)")
		held <- err
	}()
	testpoll.Never(t, func() bool {
		select {
		case <-held:
			return true // it returned: it was not held
		default:
		}
		return rowCount(t, f.adminConn(f.def), -7) != 0
	}, 3*time.Second, 200*time.Millisecond, "an application write must be held while its destination is fenced, and not have run")
	require.NoError(t, notify("route-window"))
	requireNotification(t, listener, "audit_chan", "route-window", "NOTIFY must not be held with the application's traffic")
	loginCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	login, err = pgx.Connect(loginCtx, appDSN(f.gatewayPort))
	cancel()
	require.NoError(t, err, "a login whose credential lookup goes to the fenced default tablegroup")
	_ = login.Close(ctx)

	// Unfencing releases the held write, served by the default cohort.
	_, err = ctl.unfence(ctx, "default", "unfence-def-1", "fence-def-1")
	require.NoError(t, err)
	require.NoError(t, <-held)
	assert.Equal(t, 1, rowCount(t, f.adminConn(f.def), -7))
}

// TestControlTrafficStaysOnDefaultAcrossACutoverIntoItAndBack: once the default
// tablegroup has been the application tablegroup, control traffic must still be
// treated as control. After the rollback NOTIFY still reaches the default cohort's
// LISTENers rather than following the application back to the source; a session
// whose only reservation there was made by control traffic is left alone by the
// move while one that holds application state is ended.
func TestControlTrafficStaysOnDefaultAcrossACutoverIntoItAndBack(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	listener := appConnWhenRouted(t, f.gatewayPort)
	_, err := listener.Exec(ctx, "LISTEN stay_chan")
	require.NoError(t, err)

	m.Cutover(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == portOf(t, f.def)
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the default cohort")

	// Two sessions on the default tablegroup, which now carries the application:
	// one that has only sent a NOTIFY in its transaction, and one that has written.
	controlOnly := appConnWhenRouted(t, f.gatewayPort)
	_, err = controlOnly.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = controlOnly.Exec(ctx, "NOTIFY stay_chan, 'in-transaction'")
	require.NoError(t, err)
	application := appConnWhenRouted(t, f.gatewayPort)
	_, err = application.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = application.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES (-30, 3)")
	require.NoError(t, err)

	m.Rollback(ctx)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == f.srcPortText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved back to the source")

	// The session with application state on the default tablegroup, which lost the
	// traffic, is ended. The one with only a control reservation is not: whatever
	// else the fence did to its backend, the gateway does not close the connection.
	requireSessionEnded(t, application, "application state on the default tablegroup", func() error {
		_, err := application.Exec(ctx, "COMMIT")
		return err
	})
	_, _ = controlOnly.Exec(ctx, "ROLLBACK")
	require.False(t, controlOnly.IsClosed(), "a control-only reservation must not end the session at a routing move")
	port, err := backendPort(ctx, controlOnly)
	require.NoError(t, err, "the control-only session carries on, and is served by the source now")
	require.Equal(t, f.srcPortText, port)

	// Control traffic did not follow the application back to the source: NOTIFY
	// still lands on the default cohort, where the LISTENer is, though that
	// tablegroup is fenced.
	sender := appConnWhenRouted(t, f.gatewayPort)
	_, err = sender.Exec(ctx, "NOTIFY stay_chan, 'after-rollback'")
	require.NoError(t, err)
	requireNotification(t, listener, "stay_chan", "after-rollback", "NOTIFY must stay on the default tablegroup after the rollback")

	// And a new login is answered from the application's tablegroup, as ever.
	login, err := pgx.Connect(ctx, appDSN(f.gatewayPort))
	require.NoError(t, err)
	_ = login.Close(ctx)
	assert.Zero(t, rowCount(t, f.srcAdmin, -30), "the ended session's write is not on the source")
}

// notificationsUntilSentinel reads the notifications a listener receives, in order,
// until the one named sentinel, which the caller sends after whatever it is testing.
// Notifications are delivered in order, so what comes before the sentinel is all there
// was: nothing missing is hiding behind a timeout.
func notificationsUntilSentinel(t *testing.T, listener *pgx.Conn, sentinel string) []string {
	t.Helper()
	var got []string
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		n, err := listener.WaitForNotification(ctx)
		cancel()
		require.NoError(t, err, "the sentinel %q never arrived; got %v", sentinel, got)
		if n.Payload == sentinel {
			return got
		}
		got = append(got, n.Payload)
	}
}

// notifyScenario is the setup of the NOTIFY scenarios: a listener on the default
// cohort and a way to send a sentinel and read what preceded it.
type notifyScenario struct {
	t        *testing.T
	f        *migrationFixture
	listener *pgx.Conn
	n        int
}

func newNotifyScenario(t *testing.T, f *migrationFixture) *notifyScenario {
	t.Helper()
	listener := appConnWhenRouted(t, f.gatewayPort)
	_, err := listener.Exec(t.Context(), "LISTEN txn_chan")
	require.NoError(t, err)
	return &notifyScenario{t: t, f: f, listener: listener}
}

// delivered sends a sentinel from a fresh session and returns what was delivered
// before it.
func (s *notifyScenario) delivered() []string {
	s.t.Helper()
	s.n++
	sentinel := fmt.Sprintf("sentinel-%d", s.n)
	_, err := appConn(s.t, s.f.gatewayPort).Exec(s.t.Context(), "NOTIFY txn_chan, '"+sentinel+"'")
	require.NoError(s.t, err)
	return notificationsUntilSentinel(s.t, s.listener, sentinel)
}

// transaction runs statements on one session and returns it for the caller to finish.
func (s *notifyScenario) transaction(begin string, statements ...string) *pgx.Conn {
	s.t.Helper()
	conn := appConnWhenRouted(s.t, s.f.gatewayPort)
	_, err := conn.Exec(s.t.Context(), begin)
	require.NoError(s.t, err)
	for _, sql := range statements {
		_, err = conn.Exec(s.t.Context(), sql)
		require.NoError(s.t, err, sql)
	}
	return conn
}

// runNotifyChecks is what a NOTIFY in a transaction must do while the application
// tablegroup is not the default one, in whatever state the default tablegroup is.
func (s *notifyScenario) runNotifyChecks(label string, idBase int64) {
	t := s.t
	ctx := t.Context()
	notify := func(payload string) string { return "NOTIFY txn_chan, '" + payload + "'" }
	insert := func(id int64) string { return fmt.Sprintf("INSERT INTO ledger (id, writer) VALUES (%d, 1)", idBase+id) }
	srcRows := func(id int64) int { return rowCount(t, s.f.srcAdmin, idBase+id) }
	s.delivered() // start from a quiet channel

	// Delivered after the commit, not when it was issued; the data is on the source.
	c := s.transaction("BEGIN", insert(1), notify(label+"-committed"))
	_, err := c.Exec(ctx, "COMMIT")
	require.NoError(t, err, "COMMIT of a transaction that ran NOTIFY while the default tablegroup is "+label)
	require.Equal(t, []string{label + "-committed"}, s.delivered())
	require.Equal(t, 1, srcRows(1), "the data committed on the source")

	// A rollback discards it.
	c = s.transaction("BEGIN", insert(2), notify(label+"-rolled-back"))
	_, err = c.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.Empty(t, s.delivered())
	require.Zero(t, srcRows(2))

	// A rollback to a savepoint discards only the notifications after it.
	c = s.transaction("BEGIN", notify("a"), "SAVEPOINT sp", notify("b"), "ROLLBACK TO sp", notify("c"))
	_, err = c.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "c"}, s.delivered())

	// Order is kept and a repeat of a channel and payload is delivered once.
	c = s.transaction("BEGIN", notify("x"), notify("y"), notify("x"))
	_, err = c.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, []string{"x", "y"}, s.delivered())

	// The transaction's options stay on its own transaction: with the NOTIFY first, the
	// BEGIN's isolation level is still the source transaction's.
	c = s.transaction("BEGIN ISOLATION LEVEL SERIALIZABLE", notify("ser"))
	var isolation string
	require.NoError(t, c.QueryRow(ctx, "SELECT current_setting('transaction_isolation')").Scan(&isolation))
	require.Equal(t, "serializable", isolation, "the BEGIN options must reach the source transaction, not be spent on the NOTIFY")
	_, err = c.Exec(ctx, insert(3))
	require.NoError(t, err)
	_, err = c.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, []string{"ser"}, s.delivered())
	require.Equal(t, 1, srcRows(3))

	c = s.transaction("BEGIN READ ONLY", notify("ro"))
	_, err = c.Exec(ctx, insert(4))
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr) && pgErr.Code == "25006", "a read-only transaction stays read-only on the source: %v", err)
	_, err = c.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.Zero(t, srcRows(4))
	s.delivered()
}

// TestNotifyInATransactionIsSentAfterTheCommit: while the application is not on the
// default tablegroup, a NOTIFY inside a transaction is sent to the default cohort
// after the transaction commits, so that the transaction stays on the application's
// tablegroup. It is delivered when the default tablegroup admits, when it is fenced
// for the synchronization phase of a migration, and after a rollback leaves it fenced.
func TestNotifyInATransactionIsSentAfterTheCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	s := newNotifyScenario(t, f)
	m := &migrator{f: f}

	s.runNotifyChecks("unfenced", 1000)

	// The synchronization phase: the default tablegroup is fenced, and a transaction
	// that ran NOTIFY must neither wait for it nor fail.
	_, err := f.control().fence(ctx, "default", "fence-notify-1")
	require.NoError(t, err)
	started := time.Now()
	s.runNotifyChecks("fenced", 2000)
	t.Logf("the NOTIFY checks took %s with the default tablegroup fenced", time.Since(started).Round(time.Second))
	_, err = f.control().unfence(ctx, "default", "unfence-notify-1", "fence-notify-1")
	require.NoError(t, err)

	// After a cutover and its rollback the default tablegroup stays fenced, and nothing
	// unfences it: a NOTIFY must still go through.
	m.Cutover(ctx)
	m.Rollback(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == f.srcPortText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved back to the source")
	s.runNotifyChecks("rolled-back", 3000)
}

// TestNotifyInATransactionOnTheDefaultTablegroupIsAtomic: once the application is on
// the default tablegroup, a NOTIFY inside a transaction is sent to the transaction's
// own backend and delivered by its commit: a rollback delivers nothing, a commit
// delivers it.
func TestNotifyInATransactionOnTheDefaultTablegroupIsAtomic(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}
	s := newNotifyScenario(t, f)
	m.Cutover(ctx)
	probe := appConnWhenRouted(t, f.gatewayPort)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == portOf(t, f.def)
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the default cohort")
	s.delivered()

	c := s.transaction("BEGIN", "INSERT INTO ledger (id, writer) VALUES (9001, 1)", "NOTIFY txn_chan, 'rolled-back'")
	_, err := c.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.Empty(t, s.delivered(), "a rollback must deliver nothing: the NOTIFY was part of the transaction")
	require.Zero(t, rowCount(t, f.adminConn(f.def), 9001))

	c = s.transaction("BEGIN", "INSERT INTO ledger (id, writer) VALUES (9002, 1)", "NOTIFY txn_chan, 'committed'")
	_, err = c.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	require.Equal(t, []string{"committed"}, s.delivered())
	require.Equal(t, 1, rowCount(t, f.adminConn(f.def), 9002))
}

// TestNotifyThatCannotBeDeliveredNeverFailsACommit: with the default cohort
// unreachable, a transaction on the application's tablegroup that ran NOTIFY still
// commits, once, and the client is warned that the notification was dropped.
func TestNotifyThatCannotBeDeliveredNeverFailsACommit(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)

	var mu sync.Mutex
	var notices []string
	cfg, err := pgx.ParseConfig(appDSN(f.gatewayPort))
	require.NoError(t, err)
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		defer mu.Unlock()
		notices = append(notices, n.Message)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	for name, inst := range f.def.Multipoolers {
		t.Logf("stopping default pooler %s", name)
		inst.Multipooler.TerminateGracefully(t.Logf, 20*time.Second)
	}

	for _, sql := range []string{"BEGIN", "INSERT INTO ledger (id, writer) VALUES (9101, 1)", "NOTIFY txn_chan, 'lost'"} {
		_, err := conn.Exec(ctx, sql)
		require.NoError(t, err, sql)
	}
	started := time.Now()
	_, err = conn.Exec(ctx, "COMMIT")
	require.NoError(t, err, "a notification must never turn a data commit into an error")
	require.Less(t, time.Since(started), 15*time.Second, "the commit must not wait out the buffer window for the default cohort")
	require.Equal(t, 1, rowCount(t, f.srcAdmin, 9101), "the data committed on the source exactly once")
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, notices)
	require.Contains(t, strings.Join(notices, "|"), "NOTIFY could not be delivered")
}

// TestManyNotifiesInATransactionAreAllDelivered: the notifications of a transaction that ran
// more NOTIFYs than one statement can carry are all delivered, in order; a notification
// PostgreSQL would refuse is refused when it is issued, not dropped after the commit
// together with every other.
func TestManyNotifiesInATransactionAreAllDelivered(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)

	// A listener on the default primary itself: the gateway keeps few notifications for
	// a session that is not reading, and this test is about what reaches the default cohort.
	direct := f.adminConn(f.def)
	_, err := direct.Exec(ctx, "LISTEN txn_chan")
	require.NoError(t, err)

	const count = 1800 // more than the 1664 entries of one select list
	want := make([]string, count)
	for i := range count {
		want[i] = fmt.Sprintf("n%04d", i)
	}
	conn := appConnWhenRouted(t, f.gatewayPort)
	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	for i := range count {
		_, err := conn.Exec(ctx, "NOTIFY txn_chan, '"+want[i]+"'")
		require.NoError(t, err)
	}
	// A notification PostgreSQL would refuse is refused when it is issued, and fails the
	// transaction as it does there.
	_, err = conn.Exec(ctx, "NOTIFY txn_chan, '"+strings.Repeat("x", 8000)+"'")
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "an oversized payload is refused when it is issued: %v", err)
	require.Equal(t, "22023", pgErr.Code)
	_, err = conn.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)

	conn = appConnWhenRouted(t, f.gatewayPort)
	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	for i := range count {
		_, err := conn.Exec(ctx, "NOTIFY txn_chan, '"+want[i]+"'")
		require.NoError(t, err)
	}
	_, err = conn.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	_, err = appConn(t, f.gatewayPort).Exec(ctx, "NOTIFY txn_chan, 'sentinel-many'")
	require.NoError(t, err)

	got := notificationsUntilSentinel(t, direct, "sentinel-many")
	require.Equal(t, want, got, "every notification of the committed transaction, in the order it was issued, and none of the rolled back one")
}
