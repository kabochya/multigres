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
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/protometadata"
)

// These tests run the statements of the fence and routing side, as the pooler runs them, on a
// plain PostgreSQL with the catalog tables of the default shard, and interleave them with
// connections of their own where the point is what happens when two of them meet.

const (
	sqlOpA = "0a0a0a0a-0000-4000-8000-00000000000a"
	sqlOpB = "0b0b0b0b-0000-4000-8000-00000000000b"
)

// routingSQLDB is a PostgreSQL with the catalog's tablegroup, shard and tablegroup_table tables, a
// default, a src and a dst tablegroup with one shard each, and the routing schema, with src as the
// home tablegroup.
type routingSQLDB struct {
	t    *testing.T
	port int
	// admin is the connection the helpers that read use, so that a test that runs many rounds does
	// not open a connection for each.
	admin *pgx.Conn
}

func newRoutingSQLDB(t *testing.T) *routingSQLDB {
	t.Helper()
	if testing.Short() {
		t.Skip("requires PostgreSQL")
	}
	db := &routingSQLDB{t: t, port: startExternalPostgres(t)}
	ctx := t.Context()
	conn := db.connect()
	db.admin = conn
	for _, stmt := range []string{
		"CREATE SCHEMA multigres",
		`CREATE TABLE multigres.tablegroup (oid BIGSERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, type TEXT NOT NULL)`,
		`CREATE TABLE multigres.tablegroup_table (oid BIGSERIAL PRIMARY KEY, tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
  name TEXT NOT NULL, UNIQUE (tablegroup_oid, name))`,
		`CREATE TABLE multigres.shard (oid BIGSERIAL PRIMARY KEY, tablegroup_oid BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
  shard_name TEXT NOT NULL, key_range_start BYTEA NULL, key_range_end BYTEA NULL, UNIQUE (tablegroup_oid, shard_name))`,
		"INSERT INTO multigres.tablegroup (name, type) VALUES ('default', 'unsharded'), ('src', 'unsharded'), ('dst', 'unsharded'), ('other', 'unsharded')",
		"INSERT INTO multigres.shard (tablegroup_oid, shard_name) SELECT oid, '0-inf' FROM multigres.tablegroup",
	} {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	require.NoError(t, protometadata.EnsureRoutingSchema(ctx, func(ctx context.Context, sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}))
	// The schema is created again by every pooler that starts: it must be repeatable.
	require.NoError(t, protometadata.EnsureRoutingSchema(ctx, func(ctx context.Context, sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}))
	db.reset()
	return db
}

func (db *routingSQLDB) connect() *pgx.Conn {
	db.t.Helper()
	conn, err := pgx.Connect(db.t.Context(), externalDSN(db.port))
	require.NoError(db.t, err)
	db.t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// reset empties the fence and routing state and makes src the home tablegroup at version zero.
func (db *routingSQLDB) reset() {
	db.t.Helper()
	conn := db.admin
	_, err := conn.Exec(db.t.Context(), "TRUNCATE multigres.routing_rule, multigres.admission_fence, multigres.operation")
	require.NoError(db.t, err)
	_, err = conn.Exec(db.t.Context(), `UPDATE multigres.catalog_state
SET home_tablegroup = (SELECT oid FROM multigres.tablegroup WHERE name = 'src'), routing_version = 0`)
	require.NoError(db.t, err)
}

// rows runs a statement and returns its rows as text, nil rows meaning the statement wrote
// nothing.
func rowsOf(ctx context.Context, conn interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, sql string, args ...any,
) ([][]string, error) {
	rs, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out [][]string
	for rs.Next() {
		vals, err := rs.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			switch v := v.(type) {
			case string:
				row[i] = v
			case int32:
				row[i] = itoa(int64(v))
			case int64:
				row[i] = itoa(v)
			default:
				row[i] = "?"
			}
		}
		out = append(out, row)
	}
	return out, rs.Err()
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	neg := n < 0
	if neg {
		n = -n
	}
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

func (db *routingSQLDB) scalar(sql string, args ...any) string {
	db.t.Helper()
	rows, err := rowsOf(db.t.Context(), db.admin, sql, args...)
	require.NoError(db.t, err, sql)
	require.Len(db.t, rows, 1, sql)
	return rows[0][0]
}

// fence writes a fence of the operation on the tablegroup through the statements of the pooler:
// the insert when it has none, and the compare-and-set that completes it.
func (db *routingSQLDB) fence(conn *pgx.Conn, op, tablegroup string) {
	db.t.Helper()
	ctx := db.t.Context()
	rows, err := rowsOf(ctx, conn, protometadata.InsertFenceSQL, op, tablegroup, "FENCING", int64(1))
	require.NoError(db.t, err)
	require.Len(db.t, rows, 1, "the fence of %s was created", tablegroup)
	rows, err = rowsOf(ctx, conn, protometadata.UpdateFenceSQL, op, "FENCED", int64(1),
		db.scalarInt("SELECT fence_id FROM multigres.admission_fence f JOIN multigres.tablegroup t ON t.oid = f.tablegroup WHERE t.name = $1", tablegroup),
		op, "FENCING", int64(1))
	require.NoError(db.t, err)
	require.Len(db.t, rows, 1, "the fence of %s was completed", tablegroup)
}

func (db *routingSQLDB) scalarInt(sql string, args ...any) int64 {
	db.t.Helper()
	var n int64
	require.NoError(db.t, db.admin.QueryRow(db.t.Context(), sql, args...).Scan(&n), sql)
	return n
}

func (db *routingSQLDB) switchOf(conn interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, op string, expected int64,
) ([][]string, error) {
	return rowsOf(db.t.Context(), conn, protometadata.SwitchRoutingSQL, op, "src", "dst", "0-inf", expected, "default")
}

// cleanup is the two statements of a cleanup, as the pooler runs them: it marks the operation
// cleaned up unless something blocks it, and then deletes the fences there are.
func (db *routingSQLDB) cleanup(conn interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, op string, routingVersion int64,
) (deleted int, blocked bool, err error) {
	ctx := db.t.Context()
	rows, err := rowsOf(ctx, conn, protometadata.CleanupMarkSQL, op, routingVersion)
	if err != nil {
		return 0, false, err
	}
	if len(rows) != 1 {
		return 0, false, errors.New("the first statement of the cleanup returned no answer")
	}
	if rows[0][0] == "true" {
		return 0, true, nil
	}
	rows, err = rowsOf(ctx, conn, protometadata.CleanupDeleteSQL, op)
	return len(rows), false, err
}

type cleanupResult struct {
	deleted int
	blocked bool
	err     error
}

// cleanupInTheBackground runs a cleanup on a connection of its own.
func (db *routingSQLDB) cleanupInTheBackground(op string, routingVersion int64) <-chan cleanupResult {
	done := make(chan cleanupResult, 1)
	conn := db.connect()
	go func() {
		defer conn.Close(context.Background())
		deleted, blocked, err := db.cleanup(conn, op, routingVersion)
		done <- cleanupResult{deleted, blocked, err}
	}()
	return done
}

func (db *routingSQLDB) operationState(op string) string {
	return db.scalar("SELECT state || ' ' || version FROM multigres.operation WHERE operation_id = $1::uuid", op)
}

// TestRoutingStatementsRunAnOperationThroughItsLifecycle: the statements, on a real PostgreSQL, take
// an operation through fence, switch, revert, switch again, fold and cleanup, each only from the state
// it needs, and an operation that was cleaned up cannot fence again.
func TestRoutingStatementsRunAnOperationThroughItsLifecycle(t *testing.T) {
	db := newRoutingSQLDB(t)
	ctx := t.Context()
	conn := db.connect()

	// A switch needs both fences FENCED for the operation.
	rows, err := db.switchOf(conn, sqlOpA, 0)
	require.NoError(t, err)
	require.Empty(t, rows, "no fence, nothing is written")
	db.fence(conn, sqlOpA, "src")
	rows, err = db.switchOf(conn, sqlOpA, 0)
	require.NoError(t, err)
	require.Empty(t, rows, "one side FENCED is not enough")
	db.fence(conn, sqlOpA, "dst")
	require.Equal(t, "PREPARE 0", db.operationState(sqlOpA))

	rows, err = db.switchOf(conn, sqlOpA, 7)
	require.NoError(t, err)
	require.Empty(t, rows, "the routing version is the one the caller read")
	rows, err = db.switchOf(conn, sqlOpA, 0)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"1"}}, rows)
	require.Equal(t, "SWITCHED 1", db.operationState(sqlOpA))
	rows, err = db.switchOf(conn, sqlOpA, 1)
	require.NoError(t, err)
	require.Empty(t, rows, "a second rule of the operation is refused")

	// One rule at a time, in the table and not only in the statement.
	_, err = conn.Exec(ctx, `INSERT INTO multigres.routing_rule (operation_id, from_tablegroup, to_tablegroup, to_shard, created_version)
SELECT $1::uuid, f.oid, t.oid, s.oid, 9 FROM multigres.tablegroup f, multigres.tablegroup t, multigres.shard s
WHERE f.name = 'other' AND t.name = 'dst' AND s.tablegroup_oid = t.oid`, sqlOpB)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23505", pgErr.Code)
	require.Equal(t, "routing_rule_single", pgErr.ConstraintName)

	// A revert needs the operation SWITCHED and both fences FENCED.
	rows, err = rowsOf(ctx, conn, protometadata.RevertRoutingSQL, sqlOpA, int64(1))
	require.NoError(t, err)
	require.Equal(t, [][]string{{"2"}}, rows)
	require.Equal(t, "REVERTED 2", db.operationState(sqlOpA))
	require.Equal(t, "src", db.scalar("SELECT t.name FROM multigres.catalog_state c JOIN multigres.tablegroup t ON t.oid = c.home_tablegroup"))
	rows, err = rowsOf(ctx, conn, protometadata.RevertRoutingSQL, sqlOpA, int64(2))
	require.NoError(t, err)
	require.Empty(t, rows, "a revert of a reverted operation writes nothing")
	rows, err = rowsOf(ctx, conn, protometadata.FoldRoutingSQL, sqlOpA, int64(2))
	require.NoError(t, err)
	require.Empty(t, rows, "and neither does a fold: there is no rule")

	// Switched again, then folded: the destination is the home, and the rule is gone.
	rows, err = db.switchOf(conn, sqlOpA, 2)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"3"}}, rows)
	rows, err = rowsOf(ctx, conn, protometadata.FoldRoutingSQL, sqlOpA, int64(3))
	require.NoError(t, err)
	require.Equal(t, [][]string{{"4"}}, rows)
	require.Equal(t, "FOLDED 4", db.operationState(sqlOpA))
	require.Equal(t, "dst", db.scalar("SELECT t.name FROM multigres.catalog_state c JOIN multigres.tablegroup t ON t.oid = c.home_tablegroup"))
	require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.routing_rule"))
	rows, err = rowsOf(ctx, conn, protometadata.RevertRoutingSQL, sqlOpA, int64(3))
	require.NoError(t, err)
	require.Empty(t, rows, "a revert at the version a fold produced writes nothing")

	require.Contains(t, db.scalar(protometadata.SnapshotSQL), sqlOpA, "the operation is in the snapshot while it is live")

	// Cleanup deletes the fences, FENCED ones included, and ends the operation.
	deleted, blocked, err := db.cleanup(conn, sqlOpA, 4)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	require.False(t, blocked)
	require.Equal(t, "CLEANED 4", db.operationState(sqlOpA))
	require.NotContains(t, db.scalar(protometadata.SnapshotSQL), sqlOpA, "and a cleaned up operation is not, so that the snapshot every pooler reads does not grow with them")
	require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence"))
	deleted, blocked, err = db.cleanup(conn, sqlOpA, 4)
	require.NoError(t, err)
	require.Zero(t, deleted, "a cleanup that is repeated deletes nothing and does not fail")
	require.False(t, blocked)

	// The StartFence that began the operation, replayed after the cleanup, creates nothing.
	rows, err = rowsOf(ctx, conn, protometadata.InsertFenceSQL, sqlOpA, "src", "FENCING", int64(1))
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence"))
	require.Equal(t, "CLEANED 4", db.operationState(sqlOpA), "and the operation stays cleaned up")
	// Another operation is not affected.
	rows, err = rowsOf(ctx, conn, protometadata.InsertFenceSQL, sqlOpB, "src", "FENCING", int64(1))
	require.NoError(t, err)
	require.Len(t, rows, 1)
}

// TestCleanupWaitsForASwitchAndDeletesNothingUnderItsRule: the cleanup reads that the operation has no
// rule, a switch of the same operation takes the locks and commits, and the cleanup, which was
// waiting for the lock the switch held, must not go on to delete the fences the rule depends on. It
// is gated on the routing version it read, which the switch bumped.
func TestCleanupWaitsForASwitchAndDeletesNothingUnderItsRule(t *testing.T) {
	db := newRoutingSQLDB(t)
	ctx := t.Context()
	setup := db.connect()
	db.fence(setup, sqlOpA, "src")
	db.fence(setup, sqlOpA, "dst")

	// The switch, in a transaction that is not committed yet: it holds the catalog_state row and the
	// shares of both fences.
	switcher := db.connect()
	tx, err := switcher.Begin(ctx)
	require.NoError(t, err)
	rows, err := db.switchOf(tx, sqlOpA, 0)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"1"}}, rows)

	// The cleanup read the routing version 0, with no rule, before the switch committed.
	done := db.cleanupInTheBackground(sqlOpA, 0)
	select {
	case r := <-done:
		t.Fatalf("the cleanup did not wait for the switch: %+v", r)
	case <-time.After(700 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))

	r := <-done
	require.NoError(t, r.err)
	require.True(t, r.blocked, "nothing was deleted, and it says why")
	require.Zero(t, r.deleted)
	require.EqualValues(t, 2, db.scalarInt("SELECT count(*) FROM multigres.admission_fence WHERE state = 'FENCED'"), "the fences are under the rule still")
	require.EqualValues(t, 1, db.scalarInt("SELECT count(*) FROM multigres.routing_rule"))
	require.Equal(t, "SWITCHED 1", db.operationState(sqlOpA), "and the operation is not cleaned up")

	// The revert, which needs those fences, still works.
	rows, err = rowsOf(ctx, db.connect(), protometadata.RevertRoutingSQL, sqlOpA, int64(1))
	require.NoError(t, err)
	require.Equal(t, [][]string{{"2"}}, rows)
}

// TestRoutingChangesAreSerializedAndNeverDeadlock: a revert and a fold of the same operation, a
// switch and a cleanup, run at the same time, take their locks in one order, so one of them wins and the
// other is refused, and none is a deadlock.
func TestRoutingChangesAreSerializedAndNeverDeadlock(t *testing.T) {
	db := newRoutingSQLDB(t)
	ctx := t.Context()
	const rounds = 15
	for round := range rounds {
		db.reset()
		setup := db.admin
		db.fence(setup, sqlOpA, "src")
		db.fence(setup, sqlOpA, "dst")
		rows, err := db.switchOf(setup, sqlOpA, 0)
		require.NoError(t, err)
		require.Equal(t, [][]string{{"1"}}, rows)

		var wg sync.WaitGroup
		results := make([]struct {
			rows [][]string
			err  error
		}, 3)
		run := func(i int, sql string, args ...any) {
			wg.Go(func() {
				conn, err := pgx.Connect(ctx, externalDSN(db.port))
				if err != nil {
					results[i].err = err
					return
				}
				defer conn.Close(context.Background())
				results[i].rows, results[i].err = rowsOf(ctx, conn, sql, args...)
			})
		}
		run(0, protometadata.RevertRoutingSQL, sqlOpA, int64(1))
		run(1, protometadata.FoldRoutingSQL, sqlOpA, int64(1))
		cleanupDone := db.cleanupInTheBackground(sqlOpA, 1)
		run(2, protometadata.RevertRoutingSQL, sqlOpA, int64(1))
		wg.Wait()
		cleanup := <-cleanupDone

		wrote := 0
		for i, r := range results {
			var pgErr *pgconn.PgError
			if errors.As(r.err, &pgErr) {
				require.NotEqual(t, "40P01", pgErr.Code, "round %d: call %d was a deadlock: %v", round, i, r.err)
			}
			require.NoError(t, r.err, "round %d call %d", round, i)
			if len(r.rows) == 1 {
				wrote++
			}
		}
		// The cleanup read the routing version 1 with the rule there: it is blocked, or, if the revert
		// or the fold won, the version has moved on, and it is blocked by that.
		require.NoError(t, cleanup.err, "round %d", round)
		require.True(t, cleanup.blocked, "round %d: nothing is cleaned up under the rule", round)
		require.Equal(t, 1, wrote, "round %d: of a revert, a fold and a second revert of one rule exactly one commits", round)
		require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.routing_rule"))
		require.EqualValues(t, 2, db.scalarInt("SELECT routing_version FROM multigres.catalog_state"))
	}
}

// TestFenceWritesRacingACleanupLeaveNothingBehind: a fence written while a cleanup runs is either
// refused, or committed before the cleanup marks the operation and then deleted by it. Whatever the
// interleaving, no operation is left cleaned up with a fence that nobody can change.
func TestFenceWritesRacingACleanupLeaveNothingBehind(t *testing.T) {
	// holdThenCleanup runs write in a transaction that is not committed, starts a cleanup that has to
	// wait for it, and commits.
	holdThenCleanup := func(t *testing.T, db *routingSQLDB, write func(tx pgx.Tx)) cleanupResult {
		t.Helper()
		ctx := t.Context()
		tx, err := db.connect().Begin(ctx)
		require.NoError(t, err)
		write(tx)
		done := db.cleanupInTheBackground(sqlOpA, 0)
		select {
		case r := <-done:
			t.Fatalf("the cleanup did not wait for the fence write: %+v", r)
		case <-time.After(700 * time.Millisecond):
		}
		require.NoError(t, tx.Commit(ctx))
		return <-done
	}
	fenceID := func(db *routingSQLDB, tablegroup string) int64 {
		return db.scalarInt("SELECT fence_id FROM multigres.admission_fence f JOIN multigres.tablegroup t ON t.oid = f.tablegroup WHERE t.name = $1", tablegroup)
	}

	t.Run("a StartFence of an UNFENCED fence", func(t *testing.T) {
		db := newRoutingSQLDB(t)
		ctx := t.Context()
		conn := db.connect()
		db.fence(conn, sqlOpA, "src")
		rows, err := rowsOf(ctx, conn, protometadata.UpdateFenceSQL, sqlOpA, "UNFENCED", int64(2), fenceID(db, "src"), sqlOpA, "FENCED", int64(1))
		require.NoError(t, err)
		require.Len(t, rows, 1)

		r := holdThenCleanup(t, db, func(tx pgx.Tx) {
			rows, err := rowsOf(ctx, tx, protometadata.UpdateFenceSQL, sqlOpA, "FENCING", int64(3), fenceID(db, "src"), sqlOpA, "UNFENCED", int64(2))
			require.NoError(t, err)
			require.Len(t, rows, 1, "the start passed its checks, the operation was not cleaned up yet")
		})
		require.NoError(t, r.err)
		require.False(t, r.blocked)
		require.Equal(t, 1, r.deleted, "the FENCING fence the start committed is deleted with the operation")
		require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence"))
		require.Equal(t, "CLEANED 0", db.operationState(sqlOpA))
	})

	t.Run("the first fence of an operation", func(t *testing.T) {
		db := newRoutingSQLDB(t)
		ctx := t.Context()
		r := holdThenCleanup(t, db, func(tx pgx.Tx) {
			rows, err := rowsOf(ctx, tx, protometadata.InsertFenceSQL, sqlOpA, "src", "FENCING", int64(1))
			require.NoError(t, err)
			require.Len(t, rows, 1)
		})
		require.NoError(t, r.err)
		require.False(t, r.blocked)
		require.Equal(t, 1, r.deleted)
		require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence"))
		require.Equal(t, "CLEANED 0", db.operationState(sqlOpA))
	})

	t.Run("a CommitFence of a FENCING fence", func(t *testing.T) {
		db := newRoutingSQLDB(t)
		ctx := t.Context()
		conn := db.connect()
		rows, err := rowsOf(ctx, conn, protometadata.InsertFenceSQL, sqlOpA, "src", "FENCING", int64(1))
		require.NoError(t, err)
		require.Len(t, rows, 1)

		// The cleanup reads the fence FENCING, which is what is committed while the commit is not: it
		// is blocked at once, without waiting, and the commit that follows is not lost.
		tx, err := db.connect().Begin(ctx)
		require.NoError(t, err)
		rows, err = rowsOf(ctx, tx, protometadata.UpdateFenceSQL, sqlOpA, "FENCED", int64(1), fenceID(db, "src"), sqlOpA, "FENCING", int64(1))
		require.NoError(t, err)
		require.Len(t, rows, 1)
		deleted, blocked, err := db.cleanup(conn, sqlOpA, 0)
		require.NoError(t, err)
		require.True(t, blocked, "it read a FENCING fence, and the operation is not cleaned up")
		require.Zero(t, deleted)
		require.NoError(t, tx.Commit(ctx))
		require.Equal(t, "FENCED", db.scalar("SELECT state FROM multigres.admission_fence"))
		require.Equal(t, "PREPARE 0", db.operationState(sqlOpA))
		deleted, blocked, err = db.cleanup(conn, sqlOpA, 0)
		require.NoError(t, err)
		require.False(t, blocked)
		require.Equal(t, 1, deleted, "and the cleanup, asked again, deletes it")
	})

	// The race the other cases cannot show: a write that has read that the operation is live, and is
	// made to wait, while a whole cleanup runs, and writes after it. The insert of a fence waits for the
	// uncommitted insert of another operation's fence of the same scope, and writes when that one rolls
	// back. It has the operation's row by then, so the cleanup waits for it; without that, the cleanup
	// would finish first and the fence would be written under an operation that was cleaned up.
	t.Run("a fence insert that waits while the operation is cleaned up", func(t *testing.T) {
		db := newRoutingSQLDB(t)
		ctx := t.Context()
		blocker, err := db.connect().Begin(ctx)
		require.NoError(t, err)
		rows, err := rowsOf(ctx, blocker, protometadata.InsertFenceSQL, sqlOpB, "src", "FENCING", int64(1))
		require.NoError(t, err)
		require.Len(t, rows, 1)

		writer := make(chan error, 1)
		writerConn := db.connect()
		go func() {
			_, err := rowsOf(ctx, writerConn, protometadata.InsertFenceSQL, sqlOpA, "src", "FENCING", int64(1))
			writer <- err
		}()
		time.Sleep(400 * time.Millisecond) // the insert is waiting for the blocker's
		done := db.cleanupInTheBackground(sqlOpA, 0)
		time.Sleep(400 * time.Millisecond)
		require.NoError(t, blocker.Rollback(ctx))

		require.NoError(t, <-writer)
		r := <-done
		require.NoError(t, r.err)
		require.False(t, r.blocked)
		require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence WHERE operation_id = $1::uuid", sqlOpA),
			"no fence is left under an operation that was cleaned up")
		require.Equal(t, "CLEANED 0", db.operationState(sqlOpA))
	})

	t.Run("a cleanup that stopped between its statements is finished by asking again", func(t *testing.T) {
		db := newRoutingSQLDB(t)
		ctx := t.Context()
		conn := db.connect()
		db.fence(conn, sqlOpA, "src")
		rows, err := rowsOf(ctx, conn, protometadata.CleanupMarkSQL, sqlOpA, int64(0))
		require.NoError(t, err)
		require.Equal(t, [][]string{{"false"}}, rows)
		require.EqualValues(t, 1, db.scalarInt("SELECT count(*) FROM multigres.admission_fence"), "the second statement did not run")
		// Even a fence that is FENCING, which the first statement would block, is deleted: the operation is
		// cleaned up, and nothing can write another.
		_, err = conn.Exec(ctx, "UPDATE multigres.admission_fence SET state = 'FENCING'")
		require.NoError(t, err)
		deleted, blocked, err := db.cleanup(conn, sqlOpA, 99)
		require.NoError(t, err)
		require.False(t, blocked)
		require.Equal(t, 1, deleted)
		require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence"))
	})

	t.Run("the writers refuse an operation that was cleaned up", func(t *testing.T) {
		db := newRoutingSQLDB(t)
		ctx := t.Context()
		conn := db.connect()
		db.fence(conn, sqlOpA, "src")
		id := fenceID(db, "src")
		_, _, err := db.cleanup(conn, sqlOpA, 0)
		require.NoError(t, err)
		rows, err := rowsOf(ctx, conn, protometadata.InsertFenceSQL, sqlOpA, "dst", "FENCING", int64(1))
		require.NoError(t, err)
		require.Empty(t, rows)
		// A takeover by a cleaned-up operation of the fence of another one.
		db.fence(conn, sqlOpB, "dst")
		rows, err = rowsOf(ctx, conn, protometadata.UpdateFenceSQL, sqlOpA, "FENCING", int64(2), fenceID(db, "dst"), sqlOpB, "FENCED", int64(1))
		require.NoError(t, err)
		require.Empty(t, rows)
		_ = id
		require.Zero(t, db.scalarInt("SELECT count(*) FROM multigres.admission_fence WHERE operation_id = $1::uuid", sqlOpA))
		cleaned, err := rowsOf(ctx, conn, protometadata.OperationCleanedSQL, sqlOpA)
		require.NoError(t, err)
		require.Len(t, cleaned, 1)
	})
}

// TestReplyOfARetryIsTheReplyOfTheFirstCall: the operation row records the version and the tablegroup
// that its last switch, revert or fold produced, so that a retry can answer them, whatever happened
// to the routing since.
func TestReplyOfARetryIsTheReplyOfTheFirstCall(t *testing.T) {
	db := newRoutingSQLDB(t)
	conn := db.connect()
	db.fence(conn, sqlOpA, "src")
	db.fence(conn, sqlOpA, "dst")
	rows, err := db.switchOf(conn, sqlOpA, 0)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"1"}}, rows)
	require.Equal(t, "dst", db.scalar("SELECT effective FROM multigres.operation WHERE operation_id = $1::uuid", sqlOpA))
	rows, err = rowsOf(t.Context(), conn, protometadata.RevertRoutingSQL, sqlOpA, int64(1))
	require.NoError(t, err)
	require.Equal(t, [][]string{{"2"}}, rows)
	require.Equal(t, "src", db.scalar("SELECT effective FROM multigres.operation WHERE operation_id = $1::uuid", sqlOpA))
	rows, err = db.switchOf(conn, sqlOpA, 2)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"3"}}, rows)
	rows, err = rowsOf(t.Context(), conn, protometadata.FoldRoutingSQL, sqlOpA, int64(3))
	require.NoError(t, err)
	require.Equal(t, [][]string{{"4"}}, rows)
	require.Equal(t, "dst", db.scalar("SELECT effective FROM multigres.operation WHERE operation_id = $1::uuid", sqlOpA))
	require.Equal(t, "FOLDED 4", db.operationState(sqlOpA))
}
