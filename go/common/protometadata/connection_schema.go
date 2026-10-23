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

// ensureConnectionSchemaSQL creates the connection side. It is one statement, taking a
// transaction-level advisory lock first: poolers that start together would otherwise race in
// CREATE TABLE IF NOT EXISTS (two sessions can both find the table missing, and the second
// fails on the catalog's unique index), and an exec that may run each statement of a list on a
// different connection could not hold a lock across them. It is idempotent, so any caller, the
// default primary serving a read or a test seeding fixtures, can run it first. It needs the
// multigres.tablegroup table, which the default primary creates when its shard is bootstrapped.
const ensureConnectionSchemaSQL = `DO $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('multigres.proto_connection_metadata'));

  -- Plaintext, insert-only connection catalog. Never UPDATE a row.
  CREATE TABLE IF NOT EXISTS multigres.connection (
    name TEXT PRIMARY KEY,
    dsn TEXT NOT NULL,
    expected_system_identifier TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );

  -- The connection a tablegroup is bound to. NULL means the tablegroup is served by its
  -- managed cohort.
  ALTER TABLE multigres.tablegroup
    ADD COLUMN IF NOT EXISTS backing_connection TEXT NULL REFERENCES multigres.connection(name);
END
$$`

// EnsureConnectionSchema creates the connection table, and the tablegroup column that binds a
// tablegroup to a connection, if they do not exist. exec runs one statement with
// administrative privileges on the default primary.
func EnsureConnectionSchema(ctx context.Context, exec func(ctx context.Context, sql string) error) error {
	if err := exec(ctx, ensureConnectionSchemaSQL); err != nil {
		return fmt.Errorf("ensure connection metadata schema: %w", err)
	}
	return nil
}
