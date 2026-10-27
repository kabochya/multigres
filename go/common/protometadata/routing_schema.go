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

import (
	"context"
	"fmt"
)

// ensureRoutingSchemaSQL creates the fence and routing side. It is one statement, taking a
// transaction-level advisory lock first, for the reasons given with the connection side. It is
// idempotent, so any caller, the default primary serving a write or a test seeding fixtures, can
// run it first. It needs the multigres.tablegroup, multigres.shard and multigres.tablegroup_table
// tables, which the default primary creates when its shard is bootstrapped; the columns that name
// a tablegroup, a shard or a table hold their oid.
const ensureRoutingSchemaSQL = `DO $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('multigres.proto_routing_metadata'));

  -- One row: the home tablegroup, which receives the database's traffic when no routing
  -- rule applies, and the routing_version, which goes up by one every time a rule is
  -- switched, reverted or folded. PROTOTYPE STUB: the real catalog moves the membership of
  -- a tablegroup instead of a home pointer.
  CREATE TABLE IF NOT EXISTS multigres.catalog_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    routing_version BIGINT NOT NULL DEFAULT 0,
    home_tablegroup BIGINT NOT NULL REFERENCES multigres.tablegroup(oid)
  );
  INSERT INTO multigres.catalog_state (home_tablegroup)
    SELECT oid FROM multigres.tablegroup WHERE name = 'default'
    ON CONFLICT DO NOTHING;

  -- The admission fence of a scope. A scope serves application work only while it has no fence
  -- or the fence is UNFENCED; FENCING is a fence whose members are not all confirmed clean,
  -- FENCED one that is. version goes up by one every time the scope is fenced or unfenced (not
  -- when a fence completes), so a compare-and-set on it cannot be replayed. operation_id has no
  -- foreign key: the operation is not in this catalog yet. shard_id and table_id are for the
  -- shard and table scopes, which are not implemented: they must be NULL.
  CREATE TABLE IF NOT EXISTS multigres.admission_fence (
    fence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_id UUID NOT NULL,
    tablegroup BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
    shard_id BIGINT NULL REFERENCES multigres.shard(oid),
    table_id BIGINT NULL REFERENCES multigres.tablegroup_table(oid),
    state TEXT NOT NULL CHECK (state IN ('UNFENCED', 'FENCING', 'FENCED')),
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT admission_fence_tablegroup_scope_only CHECK (shard_id IS NULL AND table_id IS NULL)
  );
  -- At most one operation holds a scope. The index is on the whole scope, with NULL as zero
  -- (no oid is zero), so that it keeps its meaning when the shard and table scopes arrive. An
  -- operation takes a scope over from another by updating the row of the other while it is
  -- UNFENCED, in the statement that checks that it is.
  CREATE UNIQUE INDEX IF NOT EXISTS admission_fence_scope
    ON multigres.admission_fence (tablegroup, COALESCE(shard_id, 0), COALESCE(table_id, 0));

  -- The operations, one row per migration, the RFC's operation table without its foreign keys.
  -- state is where the operation is: PREPARE from its first fence, SWITCHED when its rule is added,
  -- REVERTED or FOLDED when the rule is deleted, and CLEANED once its fences are deleted, for
  -- good: a call that names a cleaned operation is refused, so a delayed one cannot fence a scope
  -- again after the cleanup (a scope with no fence starts again at version zero). version is the
  -- routing_version the last switch, revert or fold produced, which is what recognizes the retry
  -- of that very call and no other.
  CREATE TABLE IF NOT EXISTS multigres.operation (
    operation_id UUID PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('PREPARE', 'SWITCHED', 'REVERTED', 'FOLDED', 'CLEANED')),
    version BIGINT NOT NULL DEFAULT 0,
    -- the tablegroup that receives the traffic after that last change (the destination of a switch
    -- and of a fold, the home tablegroup after a revert), so that a retry answers what the first
    -- call did.
    effective TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );

  -- The routing rules. Only the tablegroup scope exists: the rule covers the whole keyspace of
  -- from_tablegroup, so the range is NULL to NULL. At most one rule applies at a time: the
  -- constant-key unique index below holds the table to one row.
  CREATE TABLE IF NOT EXISTS multigres.routing_rule (
    rule_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_id UUID NOT NULL UNIQUE,
    scope TEXT NOT NULL DEFAULT 'tablegroup' CHECK (scope = 'tablegroup'),
    from_tablegroup BIGINT NOT NULL UNIQUE REFERENCES multigres.tablegroup(oid),
    range_start BYTEA NULL,
    range_end BYTEA NULL,
    to_tablegroup BIGINT NOT NULL REFERENCES multigres.tablegroup(oid),
    to_shard BIGINT NOT NULL REFERENCES multigres.shard(oid),
    created_version BIGINT NOT NULL,
    CONSTRAINT routing_rule_full_range CHECK (range_start IS NULL AND range_end IS NULL),
    CONSTRAINT routing_rule_distinct CHECK (from_tablegroup <> to_tablegroup)
  );
  CREATE UNIQUE INDEX IF NOT EXISTS routing_rule_single ON multigres.routing_rule ((true));
END
$$`

// Known limits of the prototype: a database whose fences predate the operation table has no row for
// their operations, and a switch of such an operation is refused as if the state changed
// concurrently; and the routing_rule_single index cannot be created in a database whose old
// prototype tables hold more than one rule, which fails the whole schema statement. Neither is
// migrated: the tables are throwaway.

// EnsureRoutingSchema creates the catalog state, the admission fences, the operations and the routing rules if
// they do not exist. exec runs one statement with administrative privileges on the default
// primary.
func EnsureRoutingSchema(ctx context.Context, exec func(ctx context.Context, sql string) error) error {
	if err := exec(ctx, ensureRoutingSchemaSQL); err != nil {
		return fmt.Errorf("ensure routing metadata schema: %w", err)
	}
	return nil
}
