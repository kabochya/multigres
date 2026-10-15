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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/protometadata"
)

// TestRollbackWithinTheTimingGuardUnderLoadHasNoClientErrors: writers that never
// retry run through a cutover and a rollback that starts well inside the buffer's
// timing guard (one minute between failovers by default). Every fence of the
// rollback is a second buffer cycle for its tablegroup within the guard, and each
// must hold the writers rather than let them fail.
func TestRollbackWithinTheTimingGuardUnderLoadHasNoClientErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	w := startWorkload(t, f.gatewayPort, 4, false)
	waitForAcks(t, w, 50)

	cutoverStarted := time.Now()
	m.Cutover(ctx)
	waitForAcks(t, w, w.ackedCount()+20)
	rollbackStarted := time.Now()
	require.Less(t, rollbackStarted.Sub(cutoverStarted), 45*time.Second,
		"the rollback must start well inside the one minute timing guard, or the scenario proves nothing")
	m.Rollback(ctx)
	waitForAcks(t, w, w.ackedCount()+50)

	ackedSet, errs := w.stopAndCollect()
	require.Empty(t, errs, "no writer may see an error across a cutover and a rollback inside the timing guard")
	found, err := ledgerIDs(ctx, f.srcAdmin)
	require.NoError(t, err)
	requireExactly(t, "source after rollback", ackedSet, found)
	m.requireDestFrozen(ctx, "at the end of the test")
	t.Logf("%d writes acknowledged; the rollback started %s after the cutover", len(ackedSet),
		rollbackStarted.Sub(cutoverStarted).Round(time.Second))
}

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

// TestRequestSentWhileTheFenceDrainsIsRejectedByThePoolerAndHeld: a pooler closes
// its gate when its fence starts, but tells gateways it is closed only once the
// fence is acknowledged. A request that a gateway sends in between reaches a
// fenced pooler on a health view that says it is open, so the rejection comes from
// the pooler itself (MTF02), not from the gateway's selection. The gateway must
// hold that request, not fail it, and serve it from the destination once traffic
// has moved.
func TestRequestSentWhileTheFenceDrainsIsRejectedByThePoolerAndHeld(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	// A drain deadline far longer than the request needs to be sent in, and a health
	// report of the closing gate held back, so that for those seconds gateways still see
	// the pooler as admitting: only then does the pooler's own rejection, not the
	// gateway's selection, hold the request.
	f := newMigrationFixtureWith(t, migrationOptions{
		sourceDrain: 10 * time.Second,
		sourceArgs:  []string{"--admission-health-delay=8s"},
	})
	m := &migrator{f: f}
	logOffset := gatewayLogSize(t, f.gatewayPort)

	// A transaction that keeps the source's drain open until the deadline.
	_ = openLongTransaction(t, f.gatewayPort)
	client := appConnWhenRouted(t, f.gatewayPort)
	_, err := client.Exec(ctx, "SELECT 1")
	require.NoError(t, err)

	type outcome struct {
		err        error
		sent, done time.Time
		fenceSeen  bool
	}
	out := make(chan outcome, 1)
	go func() {
		var o outcome
		deadline := time.Now().Add(60 * time.Second)
		for !o.fenceSeen && time.Now().Before(deadline) {
			o.fenceSeen = rejectsAsAdmissionClosed(t, f.src.grpcPort)
			if !o.fenceSeen {
				time.Sleep(50 * time.Millisecond)
			}
		}
		if !o.fenceSeen {
			out <- o
			return
		}
		// The source pooler's gate is closed and its fence is draining: gateways
		// still see it as open.
		o.sent = time.Now()
		_, o.err = client.Exec(ctx, "INSERT INTO ledger (id, writer) VALUES (-50, 5)")
		o.done = time.Now()
		out <- o
	}()

	m.Cutover(ctx)
	o := <-out
	require.True(t, o.fenceSeen, "the source pooler never closed its gate")
	require.NoError(t, o.err, "a request that reached a fenced pooler must be held, not failed")
	assert.Greater(t, o.done.Sub(o.sent), 5*time.Second,
		"the request must have been held through the rest of the drain and the cutover")
	requireGatewayLogLine(t, f.gatewayPort, logOffset,
		"the gateway never waited on a rejection from the pooler itself: the request was held by its own selection, which proves nothing about this path",
		`"rejected_by":"pooler:MTF02"`, `"tablegroup":"migrateTG"`)
	assert.Zero(t, rowCount(t, f.srcAdmin, -50), "it must not have run on the fenced source")
	assert.Equal(t, 1, rowCount(t, f.adminConn(f.def), -50), "it must have run on the destination")
	t.Logf("a request sent during the drain was held %s and served by the destination", o.done.Sub(o.sent).Round(time.Millisecond))
}

// gatedReader feeds a COPY FROM STDIN: its first rows at once, the rest only after
// release closes.
type gatedReader struct {
	first, rest []byte
	release     <-chan struct{}
	stage       int
}

func (r *gatedReader) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		r.stage = 1
		return copy(p, r.first), nil
	case 1:
		select {
		case <-r.release:
		case <-time.After(90 * time.Second):
			return 0, errors.New("gated reader: never released")
		}
		r.stage = 2
		return copy(p, r.rest), nil
	default:
		return 0, io.EOF
	}
}

// stallingWriter consumes a COPY TO STDOUT stream until its first write, then stops
// reading until release closes, as a client that has gone quiet does.
type stallingWriter struct {
	mu      sync.Mutex
	bytes   int
	writes  int
	release <-chan struct{}
}

func (w *stallingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	first := w.writes == 0
	w.bytes += len(p)
	w.writes++
	w.mu.Unlock()
	if !first {
		select {
		case <-w.release:
		case <-time.After(90 * time.Second):
			return 0, errors.New("stalling writer: never released")
		}
	}
	return len(p), nil
}

func (w *stallingWriter) progress() (bytes, writes int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytes, w.writes
}

// TestSessionKindsAcrossACutover: sessions of every kind that carry state on the
// source are ended at their next request when traffic moves; sessions that carry
// none, including ones that only prepared statements, are served by the
// destination without noticing.
func TestSessionKindsAcrossACutover(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	// Sessions that will hold state on the source.
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
	extendedTxn := inTxn()
	var one int
	require.NoError(t, extendedTxn.QueryRow(ctx, "SELECT $1::int", 1).Scan(&one)) // Parse, Bind, Execute
	describing := inTxn("INSERT INTO ledger (id, writer) VALUES (-31, 7)")
	savepoint := inTxn("SAVEPOINT s1", "INSERT INTO ledger (id, writer) VALUES (-32, 7)")
	released := inTxn("SAVEPOINT s1", "INSERT INTO ledger (id, writer) VALUES (-33, 7)")
	holdCursor := appConnWhenRouted(t, f.gatewayPort)
	_, err := holdCursor.Exec(ctx, "DECLARE held CURSOR WITH HOLD FOR SELECT id FROM ledger")
	require.NoError(t, err)

	// Sessions that hold none: only prepared statements, which the destination
	// prepares again on demand.
	prepared := appConnWhenRouted(t, f.gatewayPort)
	sd, err := prepared.Prepare(ctx, "count_rows", "SELECT count(*) FROM ledger WHERE id >= $1")
	require.NoError(t, err)
	require.Len(t, sd.Fields, 1, "Describe of a prepared statement")
	require.NoError(t, prepared.QueryRow(ctx, "count_rows", int64(0)).Scan(&one))

	m.Cutover(ctx)

	probe := appConnWhenRouted(t, f.gatewayPort)
	destText := portOf(t, f.def)
	require.Eventually(t, func() bool {
		port, err := backendPort(ctx, probe)
		return err == nil && port == destText
	}, 30*time.Second, 200*time.Millisecond, "the gateway never moved to the destination")

	requireSessionEnded(t, extendedTxn, "extended protocol in a transaction", func() error {
		return extendedTxn.QueryRow(ctx, "SELECT $1::int", 2).Scan(&one)
	})
	requireSessionEnded(t, describing, "Describe in a transaction", func() error {
		_, err := describing.Prepare(ctx, "d1", "SELECT id FROM ledger")
		return err
	})
	requireSessionEnded(t, savepoint, "ROLLBACK TO SAVEPOINT", func() error {
		_, err := savepoint.Exec(ctx, "ROLLBACK TO SAVEPOINT s1")
		return err
	})
	requireSessionEnded(t, released, "RELEASE SAVEPOINT", func() error {
		_, err := released.Exec(ctx, "RELEASE SAVEPOINT s1")
		return err
	})
	requireSessionEnded(t, holdCursor, "FETCH from a WITH HOLD cursor", func() error {
		_, err := holdCursor.Exec(ctx, "FETCH 1 FROM held")
		return err
	})

	// Prepared statements alone are no state on the source: the same statement
	// runs on the destination, and a new one can be prepared and described there.
	require.NoError(t, prepared.QueryRow(ctx, "count_rows", int64(0)).Scan(&one), "Bind and Execute of a prepared statement across the flip")
	sd, err = prepared.Prepare(ctx, "count_again", "SELECT id, writer FROM ledger")
	require.NoError(t, err, "Parse and Describe across the flip")
	require.Len(t, sd.Fields, 2)
	port, err := backendPort(ctx, prepared)
	require.NoError(t, err)
	require.Equal(t, destText, port, "the session is served by the destination")

	destAdmin := f.adminConn(f.def)
	for _, id := range []int64{-31, -32, -33} {
		require.Zero(t, rowCount(t, f.srcAdmin, id), "row %d must not be on the source", id)
		require.Zero(t, rowCount(t, destAdmin, id), "row %d must not be on the destination", id)
	}
}

// TestStreamingCopyMakesTheFenceAbortInsteadOfHanging: a COPY that a client is still
// streaming, in or out, is a request in flight at the drain deadline, and a COPY blocked
// on a client that has gone quiet cannot finish by itself. The fence does not wait for it
// and does not end it: it reports busy and is aborted. The source serves again, and the
// COPY goes on undisturbed: once its client moves again it completes.
func TestStreamingCopyMakesTheFenceAbortInsteadOfHanging(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	for _, direction := range []string{"COPY FROM", "COPY TO"} {
		t.Run(direction, func(t *testing.T) {
			ctx := t.Context()
			f := newMigrationFixture(t, 3*time.Second)

			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseClient := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseClient()

			conn := appConnWhenRouted(t, f.gatewayPort)
			done := make(chan error, 1)
			if direction == "COPY FROM" {
				go func() {
					_, err := conn.PgConn().CopyFrom(ctx, &gatedReader{
						first: []byte("7001\t1\n7002\t1\n"), rest: []byte("7003\t1\n"), release: release,
					}, "COPY ledger (id, writer) FROM STDIN")
					done <- err
				}()
				require.Eventually(t, func() bool {
					var n int
					require.NoError(t, f.srcAdmin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'COPY ledger%FROM STDIN' AND state = 'active' AND pid <> pg_backend_pid()").Scan(&n))
					return n == 1
				}, 30*time.Second, 100*time.Millisecond, "the COPY FROM never started")
			} else {
				// A client that has stopped reading: the COPY TO is blocked on it.
				sink := &stallingWriter{release: release}
				go func() {
					_, err := conn.PgConn().CopyTo(ctx, sink,
						"COPY (SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g) TO STDOUT")
					done <- err
				}()
				require.Eventually(t, func() bool { _, writes := sink.progress(); return writes >= 2 },
					30*time.Second, 50*time.Millisecond, "the COPY TO never started streaming")
			}

			started := time.Now()
			_, err := f.control().fenceWithin(ctx, "migrateTG", "fence-copy", 60*time.Second)
			requireFenceAborted(t, err, "busy")
			require.Less(t, time.Since(started), 30*time.Second, "the fence did not hang on the COPY")
			requireServingAfterAbort(t, f, "migrateTG", "fence-copy", f.src.grpcPort)

			// The COPY was left alone: it completes when its client moves again.
			releaseClient()
			select {
			case err := <-done:
				require.NoError(t, err, "the COPY goes on undisturbed after the aborted fence")
			case <-time.After(60 * time.Second):
				t.Fatal("the COPY never completed")
			}
			if direction == "COPY FROM" {
				require.Equal(t, 1, rowCount(t, f.srcAdmin, 7001), "the COPY FROM committed")
			}
		})
	}
}

// TestSchemaUpgradeOfAnOldTableToleratesConcurrentPoolers: two poolers that start
// together on a table that predates request_history both see the column missing.
// Neither may fail.
func TestSchemaUpgradeOfAnOldTableToleratesConcurrentPoolers(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	port := startExternalPostgres(t)
	conn, err := pgx.Connect(ctx, externalDSN(port))
	require.NoError(t, err)
	defer conn.Close(context.Background())
	ensure := func(c *pgx.Conn) error {
		return protometadata.EnsureSchema(ctx, func(ctx context.Context, sql string) error {
			_, err := c.Exec(ctx, sql)
			return err
		})
	}
	_, err = conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS multigres") // a managed cluster has it already
	require.NoError(t, err)
	require.NoError(t, ensure(conn))

	const rounds, concurrent = 5, 8
	for round := range rounds {
		_, err = conn.Exec(ctx, "ALTER TABLE multigres.proto_tablegroup_serving DROP COLUMN IF EXISTS request_history")
		require.NoError(t, err)

		conns := make([]*pgx.Conn, concurrent)
		for i := range conns {
			conns[i], err = pgx.Connect(ctx, externalDSN(port))
			require.NoError(t, err)
		}
		start := make(chan struct{})
		errs := make([]error, concurrent)
		var wg sync.WaitGroup
		for i := range conns {
			wg.Go(func() {
				<-start
				errs[i] = ensure(conns[i])
			})
		}
		close(start)
		wg.Wait()
		for i, c := range conns {
			require.NoError(t, errs[i], "round %d: concurrent upgrade %d", round, i)
			_ = c.Close(context.Background())
		}
		var present bool
		require.NoError(t, conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
WHERE table_schema = 'multigres' AND table_name = 'proto_tablegroup_serving' AND column_name = 'request_history')`).Scan(&present))
		require.True(t, present, "round %d: the column must be there afterwards", round)
	}
}

// TestLargeCopyToSpansSeveralStreamChunks: a COPY TO whose result spans many stream
// chunks completes with every row, through a gateway and an unmanaged pooler.
func TestLargeCopyToSpansSeveralStreamChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	conn := appConnWhenRouted(t, f.gatewayPort)

	const rows = 200_000
	for c := range 6 {
		var out bytes.Buffer
		tag, err := conn.PgConn().CopyTo(ctx, &out,
			"COPY (SELECT g, repeat('y', 100) FROM generate_series(1, 200000) g) TO STDOUT")
		require.NoError(t, err, "large copy out %d", c)
		require.EqualValues(t, rows, tag.RowsAffected())
		require.Equal(t, rows, bytes.Count(out.Bytes(), []byte("\n")))
		require.Greater(t, out.Len(), 10<<20, "the result must span many chunks")
	}
}

// TestIdleSessionWithALockIsEndedWhenAFenceClosedItWhateverTheGatewayKnows: a fence
// closes the idle reservations still open at its drain deadline, before routing moves.
// A session that holds a session advisory lock then sends its next request through a
// gateway that has not polled the new routing, so it knows of no move and sends it
// to the old pooler as a request on its reservation. It must be answered with the
// error that ends the session, not one it could retry from, which would let it carry
// on believing it still holds the lock. The same holds after a rollback, when the
// pooler it was closed on admits again.
func TestIdleSessionWithALockIsEndedWhenAFenceClosedItWhateverTheGatewayKnows(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	f := newMigrationFixture(t, 3*time.Second)
	m := &migrator{f: f}

	// A gateway that polls routing once at start and not again for the whole test.
	staleGateway := startRoutedGatewayNamed(t, f.def, "stale-lock-gateway", "300s", "--statement-timeout", "0")
	lockSession := func(key int) *pgx.Conn {
		conn := appConnWhenRouted(t, staleGateway)
		_, err := conn.Exec(ctx, fmt.Sprintf("SELECT pg_advisory_lock(%d)", key))
		require.NoError(t, err)
		return conn
	}
	cutoverSession, rollbackSession := lockSession(4201), lockSession(4202)
	staleOffset := gatewayLogSize(t, staleGateway)

	m.Cutover(ctx)
	require.NotContains(t, gatewayLogSince(t, staleGateway, staleOffset), "application routing changed",
		"the gateway must not have seen the move, or the session's request would not be the one the pooler answers for the killed reservation")
	requireSessionEndedByFence(t, cutoverSession, "a request after the fence closed the reservation", func() error {
		_, err := cutoverSession.Exec(ctx, "SELECT 1")
		return err
	})

	// Back to the source, which admits again: the session that was killed there during
	// the cutover is still ended, not told to retry.
	m.Rollback(ctx)
	require.NotContains(t, gatewayLogSince(t, staleGateway, staleOffset), "application routing changed", "and still has not")
	requireSessionEndedByFence(t, rollbackSession, "a request after cutover and rollback", func() error {
		_, err := rollbackSession.Exec(ctx, "SELECT 1")
		return err
	})
}

// TestPostgresRestartWithAStatementInFlightReturnsAnErrorAtOnce is the guard for an ordinary
// cluster, with no admission control and no unmanaged pooler: the pooler must not hold
// a client's request on a connection while it makes sure of a backend that PostgreSQL's
// own restart has taken away. The client gets its error promptly.
func TestPostgresRestartWithAStatementInFlightReturnsAnErrorAtOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	setup, cleanup := NewIsolated(t,
		WithMultipoolerCount(2),
		WithMultigateway(),
		WithDatabase("postgres"),
		WithCellName("test-cell"),
	)
	defer cleanup()
	setup.WaitForMultigatewayQueryServing(t)

	conn, err := pgx.Connect(ctx, GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	stmtDone := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), "SELECT pg_sleep(300)")
		stmtDone <- err
	}()
	time.Sleep(time.Second) // the statement is running on the primary

	primary := setup.GetPrimary(t)
	started := time.Now()
	setup.KillPostgres(t, primary.Name)
	select {
	case err := <-stmtDone:
		require.Error(t, err)
		t.Logf("the client got its error %s after the restart: %v", time.Since(started).Round(100*time.Millisecond), err)
		require.Less(t, time.Since(started), 20*time.Second, "the request must not hang on the lost backend")
	case <-time.After(60 * time.Second):
		t.Fatal("the client's statement hung after PostgreSQL went away")
	}
}

// TestAPoolerThatHasNotReadItsAdmissionStateSaysSoAsPending: a controlled pooler is closed
// until it has read an admission state. It says so in its health as admission_pending, which
// is not admission_closed, the applied decision to fence, and it clears once the state is read.
func TestAPoolerThatHasNotReadItsAdmissionStateSaysSoAsPending(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	p, _ := startAdmissionPooler(t, s, "unmanaged-pending-1", "reports-pending")
	require.Eventually(t, func() bool {
		h, err := p.firstHealth(ctx)
		return err == nil && h.GetBackendReady() && h.GetAdmissionPending() && !h.GetAdmissionClosed()
	}, 60*time.Second, 200*time.Millisecond, "a pooler with no admission state must report pending, not closed")

	setServingRow(t, s, "migrateTG", "reports-pending", "UNFENCED", "r0")
	requireAdmits(t, p.grpcPort, "pooler never opened")
	h, err := p.firstHealth(ctx)
	require.NoError(t, err)
	require.False(t, h.GetAdmissionPending())
	require.False(t, h.GetAdmissionClosed())

	setServingRow(t, s, "migrateTG", "reports-pending", "FENCING", "fence-1")
	_, err = refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "FENCING", "fence-1")
	require.NoError(t, err)
	h, err = p.firstHealth(ctx)
	require.NoError(t, err)
	require.True(t, h.GetAdmissionClosed(), "an applied fence is admission_closed")
	require.False(t, h.GetAdmissionPending())
}
