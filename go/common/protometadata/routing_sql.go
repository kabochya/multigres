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

package protometadata

// The statements that read and write the fence and routing side, in one place so that the tests
// that need a real PostgreSQL can run exactly what the pooler runs.
//
// Every write is one statement, because the admin pool does not hold a transaction across calls.
// A statement that reads and locks rows and writes in data-modifying CTEs commits all of it or
// nothing, and sees one snapshot of what it reads, which is the transaction the contract asks for.
//
// Every statement that changes the routing, or deletes what the routing depends on, takes the
// catalog_state row FOR UPDATE before it locks anything else: that row is the one lock they all
// share, so that two of them run one after the other, in the same order, and cannot deadlock. The
// locking read returns the latest version of the row, which is what its routing_version is compared
// with, and every change of a rule bumps that version: a statement whose snapshot is older than a
// change that committed while it waited therefore sees the new version, and writes nothing. The
// other locks name the catalog_state lock in a condition (csLocked), so that the first row the
// statement locks is the catalog_state's, whatever order the planner reads the tables in.
//
// Named parameters of each statement are in the comment above it.

// csLocked is a condition that is always true and that evaluates the cs CTE, the catalog_state row
// locked FOR UPDATE, before the rows of the table it is a condition of are locked.
const csLocked = "(SELECT count(*) FROM cs) >= 0"

// opLocked is the same for the opx CTE, the operation's row locked FOR UPDATE: a statement that
// locks the fences takes the operation's row first, as the statements that write a fence do, so
// that the two cannot wait for each other.
const opLocked = "(SELECT count(*) FROM opx) >= 0"

// SnapshotSQL reads everything in one statement, so that what it returns is one snapshot. The
// operations that were cleaned up are left out: they are finished for good and only ever looked up
// by id (OperationCleanedSQL), so what every pooler reads of the fences does not grow with them.
const SnapshotSQL = `SELECT json_build_object(
  'routing_version', (SELECT routing_version FROM multigres.catalog_state),
  'home', (SELECT tg.name FROM multigres.catalog_state cs JOIN multigres.tablegroup tg ON tg.oid = cs.home_tablegroup),
  'rules', COALESCE((SELECT json_agg(json_build_object(
      'operation_id', rr.operation_id, 'from', f.name, 'to', t.name, 'to_shard', s.shard_name,
      'created_version', rr.created_version) ORDER BY rr.rule_id)
    FROM multigres.routing_rule rr
    JOIN multigres.tablegroup f ON f.oid = rr.from_tablegroup
    JOIN multigres.tablegroup t ON t.oid = rr.to_tablegroup
    JOIN multigres.shard s ON s.oid = rr.to_shard), '[]'::json),
  'fences', COALESCE((SELECT json_agg(json_build_object(
      'id', af.fence_id, 'operation_id', af.operation_id, 'tablegroup', tg.name, 'state', af.state,
      'version', af.version, 'updated_at', extract(epoch FROM af.updated_at)) ORDER BY af.fence_id)
    FROM multigres.admission_fence af
    JOIN multigres.tablegroup tg ON tg.oid = af.tablegroup
    WHERE af.shard_id IS NULL AND af.table_id IS NULL), '[]'::json),
  'operations', COALESCE((SELECT json_agg(json_build_object(
      'operation_id', o.operation_id, 'state', o.state, 'version', o.version, 'effective', o.effective)
      ORDER BY o.operation_id)
    FROM multigres.operation o WHERE o.state <> 'CLEANED'), '[]'::json)
)::text`

// OperationCleanedSQL returns a row if the operation $1 was cleaned up.
const OperationCleanedSQL = `SELECT 1 FROM multigres.operation WHERE operation_id = $1::uuid AND state = 'CLEANED'`

// The two statements that write a fence first take the operation's row: they insert it if the
// operation is new, and otherwise update it, which locks it and reads its latest committed state, and
// they write the fence only if that row was not CLEANED. A cleanup that is running therefore either
// finished first, and the write is refused, or waits for the write's transaction and then deletes
// what the write left. The row is left behind, as PREPARE, when the fence write then loses (another
// operation holds the scope, the compare-and-set lost): it is cleaned up with the operation.

// InsertFenceSQL creates the first fence of a scope: $1 the operation, $2 the tablegroup name, $3
// the state, $4 the version. The unique index on the scope makes a concurrent insert lose. It
// returns a row when it created the fence.
const InsertFenceSQL = `WITH op AS (
  INSERT INTO multigres.operation (operation_id, state, version) VALUES ($1::uuid, 'PREPARE', 0)
  ON CONFLICT (operation_id) DO UPDATE SET updated_at = now() WHERE multigres.operation.state <> 'CLEANED'
  RETURNING 1
)
INSERT INTO multigres.admission_fence (operation_id, tablegroup, state, version)
SELECT $1::uuid, tg.oid, $3, $4::bigint FROM multigres.tablegroup tg
WHERE tg.name = $2 AND EXISTS (SELECT 1 FROM op)
ON CONFLICT DO NOTHING
RETURNING 1`

// UpdateFenceSQL is the compare-and-set of a fence: $1 the operation that holds it after the write
// (another one than before when it takes the scope over), $2 the state, $3 the version, and what
// the fence has to be still: $4 its id, $5 its operation, $6 its state, $7 its version. It returns
// a row when it wrote.
const UpdateFenceSQL = `WITH op AS (
  INSERT INTO multigres.operation (operation_id, state, version) VALUES ($1::uuid, 'PREPARE', 0)
  ON CONFLICT (operation_id) DO UPDATE SET updated_at = now() WHERE multigres.operation.state <> 'CLEANED'
  RETURNING 1
)
UPDATE multigres.admission_fence
SET operation_id = $1::uuid, state = $2, version = $3::bigint, updated_at = now()
WHERE fence_id = $4::bigint AND operation_id = $5::uuid AND state = $6 AND version = $7::bigint
  AND EXISTS (SELECT 1 FROM op)
RETURNING 1`

// SwitchRoutingSQL adds the rule of an operation: $1 the operation, $2 and $3 the names of the
// source and the destination tablegroup, $4 the name of the destination's shard, $5 the
// routing_version the caller read, $6 the name of the default tablegroup, which cannot be routed
// away. It writes only if the routing is at $5, the source is the home tablegroup, no rule exists,
// the operation has not been cleaned up or folded, and the fences of both tablegroups are FENCED
// for the operation, locked so that neither can change under the write. It returns the new
// routing_version when it wrote.
const SwitchRoutingSQL = `WITH cs AS MATERIALIZED (
  SELECT routing_version, home_tablegroup FROM multigres.catalog_state FOR UPDATE
), tgs AS (
  SELECT f.oid AS from_oid, t.oid AS to_oid, s.oid AS shard_oid
  FROM multigres.tablegroup f, multigres.tablegroup t, multigres.shard s
  WHERE f.name = $2 AND t.name = $3 AND s.tablegroup_oid = t.oid AND s.shard_name = $4
), opx AS MATERIALIZED (
  SELECT state FROM multigres.operation WHERE operation_id = $1::uuid AND ` + csLocked + ` FOR UPDATE
), fences AS (
  SELECT af.fence_id FROM multigres.admission_fence af, tgs
  WHERE af.operation_id = $1::uuid AND af.state = 'FENCED' AND af.tablegroup IN (tgs.from_oid, tgs.to_oid)
    AND af.shard_id IS NULL AND af.table_id IS NULL AND ` + csLocked + ` AND ` + opLocked + `
  FOR SHARE OF af
), ok AS (
  SELECT true AS ok FROM cs, tgs
  WHERE cs.routing_version = $5::bigint AND cs.home_tablegroup = tgs.from_oid
    AND tgs.from_oid <> (SELECT oid FROM multigres.tablegroup WHERE name = $6)
    AND NOT EXISTS (SELECT 1 FROM multigres.routing_rule)
    AND (SELECT count(*) FROM fences) = 2
    AND (SELECT state FROM opx) IN ('PREPARE', 'REVERTED')
), bumped AS (
  UPDATE multigres.catalog_state SET routing_version = routing_version + 1
  WHERE EXISTS (SELECT 1 FROM ok) RETURNING routing_version
), ins AS (
  INSERT INTO multigres.routing_rule (operation_id, from_tablegroup, to_tablegroup, to_shard, created_version)
  SELECT $1::uuid, from_oid, to_oid, shard_oid, (SELECT routing_version FROM bumped) FROM tgs
  WHERE EXISTS (SELECT 1 FROM ok) RETURNING 1
), opup AS (
  UPDATE multigres.operation SET state = 'SWITCHED', version = (SELECT routing_version FROM bumped), effective = $3, updated_at = now()
  WHERE operation_id = $1::uuid AND EXISTS (SELECT 1 FROM ok) RETURNING 1
)
SELECT routing_version::text FROM bumped`

// RevertRoutingSQL deletes the rule of an operation: $1 the operation, $2 the routing_version the
// caller read. It writes only if the routing is at $2, the operation is SWITCHED, and the fences of
// the rule's two tablegroups are FENCED for it. It returns the new routing_version when it wrote.
const RevertRoutingSQL = `WITH cs AS MATERIALIZED (
  SELECT routing_version FROM multigres.catalog_state FOR UPDATE
), rule AS (
  SELECT from_tablegroup, to_tablegroup FROM multigres.routing_rule
  WHERE operation_id = $1::uuid AND ` + csLocked + ` FOR UPDATE
), opx AS MATERIALIZED (
  SELECT state FROM multigres.operation WHERE operation_id = $1::uuid AND ` + csLocked + ` FOR UPDATE
), fences AS (
  SELECT af.fence_id FROM multigres.admission_fence af, rule
  WHERE af.operation_id = $1::uuid AND af.state = 'FENCED' AND af.tablegroup IN (rule.from_tablegroup, rule.to_tablegroup)
    AND af.shard_id IS NULL AND af.table_id IS NULL AND ` + csLocked + ` AND ` + opLocked + `
  FOR SHARE OF af
), ok AS (
  SELECT true AS ok FROM cs, rule
  WHERE cs.routing_version = $2::bigint AND (SELECT count(*) FROM fences) = 2
    AND (SELECT state FROM opx) = 'SWITCHED'
), bumped AS (
  UPDATE multigres.catalog_state SET routing_version = routing_version + 1
  WHERE EXISTS (SELECT 1 FROM ok) RETURNING routing_version
), del AS (
  DELETE FROM multigres.routing_rule WHERE operation_id = $1::uuid AND EXISTS (SELECT 1 FROM ok) RETURNING 1
), opup AS (
  UPDATE multigres.operation SET state = 'REVERTED', version = (SELECT routing_version FROM bumped),
    effective = (SELECT name FROM multigres.tablegroup WHERE oid = (SELECT from_tablegroup FROM rule)), updated_at = now()
  WHERE operation_id = $1::uuid AND EXISTS (SELECT 1 FROM ok) RETURNING 1
)
SELECT routing_version::text FROM bumped`

// FoldRoutingSQL makes the destination of the rule of an operation the home tablegroup and deletes
// the rule: $1 the operation, $2 the routing_version the caller read. It writes only if the routing
// is at $2, the operation is SWITCHED, and the rule applies to the home tablegroup. It returns the new
// routing_version when it wrote.
const FoldRoutingSQL = `WITH cs AS MATERIALIZED (
  SELECT routing_version, home_tablegroup FROM multigres.catalog_state FOR UPDATE
), rule AS (
  SELECT from_tablegroup, to_tablegroup FROM multigres.routing_rule
  WHERE operation_id = $1::uuid AND ` + csLocked + ` FOR UPDATE
), opx AS MATERIALIZED (
  SELECT state FROM multigres.operation WHERE operation_id = $1::uuid AND ` + csLocked + ` FOR UPDATE
), ok AS (
  SELECT true AS ok FROM cs, rule
  WHERE cs.routing_version = $2::bigint AND cs.home_tablegroup = rule.from_tablegroup
    AND (SELECT state FROM opx) = 'SWITCHED'
), bumped AS (
  UPDATE multigres.catalog_state
  SET routing_version = routing_version + 1, home_tablegroup = (SELECT to_tablegroup FROM rule)
  WHERE EXISTS (SELECT 1 FROM ok) RETURNING routing_version
), del AS (
  DELETE FROM multigres.routing_rule WHERE operation_id = $1::uuid AND EXISTS (SELECT 1 FROM ok) RETURNING 1
), opup AS (
  UPDATE multigres.operation SET state = 'FOLDED', version = (SELECT routing_version FROM bumped),
    effective = (SELECT name FROM multigres.tablegroup WHERE oid = (SELECT to_tablegroup FROM rule)), updated_at = now()
  WHERE operation_id = $1::uuid AND EXISTS (SELECT 1 FROM ok) RETURNING 1
)
SELECT routing_version::text FROM bumped`

// CleanupMarkSQL is the first of the two statements of a cleanup. It marks the operation CLEANED, for
// good: $1 the operation, $2 the routing_version the caller read when it saw that the operation had
// no rule. It marks only if the routing is still at $2, so that a switch that committed meanwhile,
// which bumps it, is seen and nothing is cleaned up under its rule, and only if the operation has no
// rule and no fence that is FENCING, or if it was marked already, which is how a cleanup that
// stopped between its two statements is finished. It returns whether something blocked it.
//
// Once it commits no statement writes a fence of the operation (see InsertFenceSQL), and one that
// was running while it waited has committed first, so the second statement sees every fence there
// is.
const CleanupMarkSQL = `WITH cs AS MATERIALIZED (
  SELECT routing_version FROM multigres.catalog_state FOR UPDATE
), blocked AS (
  SELECT 1 FROM multigres.routing_rule WHERE operation_id = $1::uuid
  UNION ALL
  SELECT 1 FROM multigres.admission_fence WHERE operation_id = $1::uuid AND state = 'FENCING'
), gate AS (
  SELECT (
    EXISTS (SELECT 1 FROM multigres.operation WHERE operation_id = $1::uuid AND state = 'CLEANED')
    OR (COALESCE((SELECT routing_version FROM cs), 0) = $2::bigint AND NOT EXISTS (SELECT 1 FROM blocked))
  ) AS ok
), opw AS (
  INSERT INTO multigres.operation (operation_id, state, version)
  SELECT $1::uuid, 'CLEANED', COALESCE((SELECT routing_version FROM cs), 0) WHERE (SELECT ok FROM gate)
  ON CONFLICT (operation_id) DO UPDATE SET state = 'CLEANED', updated_at = now()
  RETURNING 1
)
SELECT (NOT (SELECT ok FROM gate))::text`

// CleanupDeleteSQL is the second statement of a cleanup, run after CleanupMarkSQL committed, with a
// snapshot of its own: it deletes every fence of the operation $1, FENCING ones too, since nothing
// can write one now. It returns a row for each fence it deleted.
const CleanupDeleteSQL = `DELETE FROM multigres.admission_fence WHERE operation_id = $1::uuid RETURNING 1`
