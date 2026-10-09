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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// PROTOTYPE STUB: the migration test harness. It plays the part of the migration
// controller (the "migrator") that the design leaves to other work: it runs the
// documented RPC sequences against the default primary, and replaces data
// replication with a COPY of a small table while both sides are fenced.

const (
	appRole     = "app"
	appPassword = "app-secret"
)

// appDSN connects as the application role through a gateway.
func appDSN(port int) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable&connect_timeout=5", appRole, appPassword, port)
}

// migrationFixture is a cluster for migration scenarios: a default cohort that
// holds metadata, a managed destTG cohort, an external source served by two
// unmanaged poolers as migrateTG, and a gateway that follows routing.
type migrationFixture struct {
	t           *testing.T
	def         *ShardSetup
	dest        *ShardSetup
	srcPort     int
	srcPoolers  []*unmanagedPooler
	gatewayPort int
	srcAdmin    *pgx.Conn
	srcPortText string
}

// migrationOptions varies the fixture.
type migrationOptions struct {
	// sourceDrain is the unmanaged poolers' fence drain deadline.
	sourceDrain time.Duration
	// limitedBackingRole makes the poolers' backing connection a role that is
	// neither a superuser nor a member of the application role, so it cannot
	// terminate the application's backends.
	limitedBackingRole bool
}

// limitedBackingRole is the non-superuser role of migrationOptions.
const (
	limitedRole     = "appbackend"
	limitedPassword = "backend-secret"
)

// newMigrationFixture builds the cluster. sourceDrain is the unmanaged poolers'
// fence drain deadline.
func newMigrationFixture(t *testing.T, sourceDrain time.Duration) *migrationFixture {
	t.Helper()
	return newMigrationFixtureWith(t, migrationOptions{sourceDrain: sourceDrain})
}

func newMigrationFixtureWith(t *testing.T, opts migrationOptions) *migrationFixture {
	t.Helper()
	ctx := t.Context()
	sourceDrain := opts.sourceDrain

	def, cleanupDef := NewIsolated(t, WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"))
	t.Cleanup(cleanupDef)
	// The destination cohort's bootstrap queries its poolers, so they must admit:
	// start it UNFENCED. The cutover fences it before anything else.
	setServingRow(t, def, "destTG", "", "UNFENCED", "r-dst0")
	dest, cleanupDest := NewIsolated(t,
		WithParentCluster(def), WithTableGroup("destTG"), WithNamePrefix("dest"),
		WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"),
		WithMultipoolerExtraArgs("--admission-control", "--admission-drain-timeout=3s"))
	t.Cleanup(cleanupDest)

	f := &migrationFixture{t: t, def: def, dest: dest}
	f.srcPort = startExternalPostgres(t)
	f.srcPortText = strconv.Itoa(f.srcPort)
	var err error
	f.srcAdmin, err = pgx.Connect(ctx, externalDSN(f.srcPort))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.srcAdmin.Close(context.Background()) })

	// The same application role on both sides, with a byte-identical SCRAM
	// verifier: the gateway verifies the client against whichever side serves it
	// and passes its keys through to the backend.
	_, err = f.srcAdmin.Exec(ctx, "CREATE ROLE "+appRole+" LOGIN PASSWORD '"+appPassword+"'")
	require.NoError(t, err)
	verifier, err := RoleVerifier(ctx, f.srcAdmin, appRole)
	require.NoError(t, err)
	destAdmin := f.adminConn(dest)
	require.NoError(t, CreateRoleWithVerifier(ctx, destAdmin, appRole, verifier))
	for _, conn := range []*pgx.Conn{f.srcAdmin, destAdmin} {
		_, err = conn.Exec(ctx, "CREATE TABLE ledger (id bigint PRIMARY KEY, writer int NOT NULL)")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "GRANT ALL ON ledger TO "+appRole)
		require.NoError(t, err)
	}

	backingDSN := externalDSN(f.srcPort)
	if opts.limitedBackingRole {
		_, err = f.srcAdmin.Exec(ctx, "CREATE ROLE "+limitedRole+" LOGIN PASSWORD '"+limitedPassword+"'")
		require.NoError(t, err)
		_, err = f.srcAdmin.Exec(ctx, "GRANT CONNECT ON DATABASE postgres TO "+limitedRole)
		require.NoError(t, err)
		// Credential lookup reads the role verifiers, which the backing role needs
		// to be able to see; that is the only privilege beyond connecting.
		_, err = f.srcAdmin.Exec(ctx, "GRANT SELECT ON pg_authid TO "+limitedRole)
		require.NoError(t, err)
		backingDSN = fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable", limitedRole, limitedPassword, f.srcPort)
	}
	seedBackingConnection(t, def, "mig-src", backingDSN, externalSystemIdentifier(t, f.srcPort))
	setServingRow(t, def, "migrateTG", "mig-src", "UNFENCED", "r-src0")
	seedRouting(t, def, "migrateTG")
	for _, name := range []string{"mig-src-1", "mig-src-2"} {
		p := startUnmanagedPooler(t, def, name, "postgres", "mig-src", "--admission-drain-timeout="+sourceDrain.String())
		f.srcPoolers = append(f.srcPoolers, p)
	}
	for _, p := range f.srcPoolers {
		waitBackendReady(t, p)
		if !opts.limitedBackingRole {
			// The probe connects as the backing role; with a limited one the
			// gateway login below is the proof that the source admits.
			requireAdmits(t, p.grpcPort, "the source must admit while UNFENCED")
		}
	}
	f.gatewayPort = startRoutedGateway(t, def)
	require.Eventually(t, func() bool {
		c, err := pgx.Connect(ctx, appDSN(f.gatewayPort))
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		port, err := backendPort(ctx, c)
		return err == nil && port == f.srcPortText
	}, 60*time.Second, 300*time.Millisecond, "the gateway never served the application role from the source")
	return f
}

// adminConn opens an administrative connection to a cohort's current primary,
// as recorded in its PrimaryName (a test that fails a primary over updates it).
func (f *migrationFixture) adminConn(s *ShardSetup) *pgx.Conn {
	f.t.Helper()
	return s.ConnectPrimaryAdmin(f.t)
}

// control returns a client for the serving-control RPCs of the default cohort's
// current primary, so it follows a default-primary failover once the test has
// recorded the new primary in def.PrimaryName.
func (f *migrationFixture) control() *controlClient {
	f.t.Helper()
	return newControlClient(f.t, f.def.GetPrimary(f.t).Multipooler.GrpcPort)
}

// ledgerIDs returns the application rows on a database.
func ledgerIDs(ctx context.Context, conn *pgx.Conn) (map[int64]bool, error) {
	rows, err := conn.Query(ctx, "SELECT id FROM ledger WHERE id >= 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

func countLedger(t *testing.T, conn *pgx.Conn) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT count(*) FROM ledger WHERE id >= 0").Scan(&n))
	return n
}

// workload is a set of writers that insert unique, monotonically increasing keys
// through a gateway and remember which inserts were acknowledged.
//
// With retry set, a writer treats an error as "outcome unknown", the way a careful
// application treats a connection that broke mid-statement: it reconnects and
// repeats the same key, counting a unique violation as proof the first attempt
// committed. That is the only correct behavior when the database itself crashes
// under an in-flight write; without retry any error ends the writer.
type workload struct {
	mu      sync.Mutex
	acked   map[int64]bool
	errs    []error
	retried int
	stop    chan struct{}
	wg      sync.WaitGroup
	retry   bool
	port    int
}

func startWorkload(t *testing.T, gatewayPort, writers int, retry bool) *workload {
	t.Helper()
	w := &workload{acked: map[int64]bool{}, stop: make(chan struct{}), retry: retry, port: gatewayPort}
	for writer := range writers {
		conn, err := pgx.Connect(t.Context(), appDSN(gatewayPort))
		require.NoError(t, err)
		w.wg.Go(func() { w.run(writer, conn) })
	}
	return w
}

func (w *workload) run(writer int, conn *pgx.Conn) {
	defer func() { conn.Close(context.Background()) }()
	for seq := int64(0); ; seq++ {
		select {
		case <-w.stop:
			return
		default:
		}
		id := int64(writer)*1_000_000 + seq
		deadline := time.Now().Add(90 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			_, err := conn.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES ($1, $2)", id, writer)
			cancel()
			var pgErr *pgconn.PgError
			committedBefore := errors.As(err, &pgErr) && pgErr.Code == "23505"
			if err == nil || (w.retry && committedBefore) {
				w.mu.Lock()
				w.acked[id] = true
				w.mu.Unlock()
				break
			}
			w.mu.Lock()
			w.retried++
			w.mu.Unlock()
			if !w.retry || time.Now().After(deadline) {
				w.mu.Lock()
				w.errs = append(w.errs, fmt.Errorf("writer %d id %d: %w", writer, id, err))
				w.mu.Unlock()
				return
			}
			// Outcome unknown: reconnect and repeat the same key.
			conn.Close(context.Background())
			for {
				c, cerr := pgx.Connect(context.Background(), appDSN(w.port))
				if cerr == nil {
					conn = c
					break
				}
				if time.Now().After(deadline) {
					w.mu.Lock()
					w.errs = append(w.errs, fmt.Errorf("writer %d id %d: reconnect: %w", writer, id, cerr))
					w.mu.Unlock()
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stopAndCollect stops the writers and returns what they were told succeeded and
// any error they saw.
func (w *workload) stopAndCollect() (map[int64]bool, []error) {
	close(w.stop)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	acked := make(map[int64]bool, len(w.acked))
	for id := range w.acked {
		acked[id] = true
	}
	return acked, append([]error(nil), w.errs...)
}

func (w *workload) retryCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.retried
}

func (w *workload) ackedCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.acked)
}

// requireExactly asserts a database holds exactly the acknowledged set: no lost
// write and no duplicate or phantom row.
func requireExactly(t *testing.T, label string, acked, found map[int64]bool) {
	t.Helper()
	var lost, extra []int64
	for id := range acked {
		if !found[id] {
			lost = append(lost, id)
		}
	}
	for id := range found {
		if !acked[id] {
			extra = append(extra, id)
		}
	}
	require.Empty(t, lost, "%s: acknowledged writes that are missing (%d acked, %d found)", label, len(acked), len(found))
	require.Empty(t, extra, "%s: rows that were never acknowledged", label)
}

// migrator runs the cutover and rollback sequences of the design against the
// default primary. Catch-up is a COPY of the ledger while both sides are fenced.
type migrator struct {
	f *migrationFixture
	n int
	// fence request ids of the last cutover, which the rollback must name to
	// unfence what the cutover fenced.
	srcFenceID, destFenceID string
	// betweenRouteAndUnfence, when set, runs after routing moved and before the
	// destination is unfenced.
	betweenRouteAndUnfence func()
	// srcFrozen and destFrozen are the application rows of a side at the moment
	// its fence was acknowledged. A fenced side must hold exactly these until it
	// is unfenced or overwritten by a catch-up, however long that takes.
	srcFrozen, destFrozen map[int64]bool
}

// snapshot returns the application rows currently on a database.
func (m *migrator) snapshot(ctx context.Context, conn *pgx.Conn) map[int64]bool {
	m.f.t.Helper()
	ids, err := ledgerIDs(ctx, conn)
	require.NoError(m.f.t, err)
	return ids
}

// requireSourceFrozen asserts that nothing reached the source since its fence was
// acknowledged: the same rows, not merely as many.
func (m *migrator) requireSourceFrozen(ctx context.Context, when string) {
	m.f.t.Helper()
	if m.srcFrozen == nil {
		return
	}
	requireExactly(m.f.t, "source "+when+": it must hold exactly what it held at its fence", m.srcFrozen, m.snapshot(ctx, m.f.srcAdmin))
}

// requireDestFrozen is requireSourceFrozen for the destination.
func (m *migrator) requireDestFrozen(ctx context.Context, when string) {
	m.f.t.Helper()
	if m.destFrozen == nil {
		return
	}
	requireExactly(m.f.t, "destination "+when+": it must hold exactly what it held at its fence", m.destFrozen, m.snapshot(ctx, m.f.adminConn(m.f.dest)))
}

func (m *migrator) id(step string) string {
	m.n++
	return fmt.Sprintf("%s-%d", step, m.n)
}

func (m *migrator) copyLedger(ctx context.Context, from, to *pgx.Conn) {
	t := m.f.t
	t.Helper()
	_, err := to.Exec(ctx, "TRUNCATE ledger")
	require.NoError(t, err)
	rows, err := from.Query(ctx, "SELECT id, writer FROM ledger")
	require.NoError(t, err)
	var data [][]any
	for rows.Next() {
		var id int64
		var writer int32
		require.NoError(t, rows.Scan(&id, &writer))
		data = append(data, []any{id, writer})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	_, err = to.CopyFrom(ctx, pgx.Identifier{"ledger"}, []string{"id", "writer"}, pgx.CopyFromRows(data))
	require.NoError(t, err)
}

// Cutover moves application traffic from the source to the destination.
func (m *migrator) Cutover(ctx context.Context) {
	f, t := m.f, m.f.t
	t.Helper()
	ctl := f.control()

	// Preflight.
	state := ctl.state(ctx, "migrateTG", "destTG")
	require.Equal(t, "migrateTG", state.AppTablegroup, "the source must own the traffic")

	m.destFenceID, m.srcFenceID = m.id("fence-dest"), m.id("fence-src")
	_, err := ctl.fence(ctx, "destTG", m.destFenceID)
	require.NoError(t, err)
	_, err = ctl.fence(ctx, "migrateTG", m.srcFenceID)
	require.NoError(t, err)

	// Both sides are fenced and acknowledged: nothing may write to the source from
	// here on. Catch up, then route.
	srcBefore := countLedger(t, f.srcAdmin)
	m.srcFrozen = m.snapshot(ctx, f.srcAdmin)
	destAdmin := f.adminConn(f.dest)
	m.copyLedger(ctx, f.srcAdmin, destAdmin)
	require.Equal(t, srcBefore, countLedger(t, destAdmin), "catch-up must copy every row")

	_, err = f.control().route(ctx, "migrateTG", "destTG", m.id("route"))
	require.NoError(t, err)
	m.requireSourceFrozen(ctx, "after routing moved")
	if m.betweenRouteAndUnfence != nil {
		m.betweenRouteAndUnfence()
	}
	_, err = f.control().unfence(ctx, "destTG", m.id("unfence-dest"), m.destFenceID)
	require.NoError(t, err)

	// The source stays fenced from here on, and the rollback overwrites it, so
	// this is checked again where the rollback begins.
	m.requireSourceFrozen(ctx, "after the destination was unfenced")
}

// Rollback moves application traffic back to the source.
func (m *migrator) Rollback(ctx context.Context) {
	f, t := m.f, m.f.t
	t.Helper()
	ctl := f.control()
	state := ctl.state(ctx, "migrateTG", "destTG")
	require.Equal(t, "destTG", state.AppTablegroup, "the destination must own the traffic")

	// Whatever reached the fenced source while the destination served would be
	// erased by the catch-up below, so look before it runs.
	m.requireSourceFrozen(ctx, "before the rollback's catch-up")

	destFence := m.id("fence-dest")
	_, err := ctl.fence(ctx, "destTG", destFence)
	require.NoError(t, err)

	destAdmin := f.adminConn(f.dest)
	destBefore := countLedger(t, destAdmin)
	m.destFrozen = m.snapshot(ctx, destAdmin)
	m.copyLedger(ctx, destAdmin, f.srcAdmin)
	m.srcFrozen = nil // the catch-up replaced the source's rows
	require.Equal(t, destBefore, countLedger(t, f.srcAdmin), "catch-up must copy every row")

	_, err = f.control().route(ctx, "destTG", "migrateTG", m.id("route"))
	require.NoError(t, err)
	_, err = f.control().unfence(ctx, "migrateTG", m.id("unfence-src"), m.srcFenceID)
	require.NoError(t, err)
	m.destFenceID = destFence

	m.requireDestFrozen(ctx, "after the rollback moved traffic back")
}
