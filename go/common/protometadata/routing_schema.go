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

// ensureSchemaSQL creates the prototype tables. It is one statement, taking a
// transaction-level advisory lock first: poolers that start together would otherwise race
// in CREATE TABLE IF NOT EXISTS (two sessions can both find the table missing, and the
// second fails on the catalog's unique index), and an exec that may run each statement of
// a list on a different connection could not hold a lock across them. It is idempotent, so
// any caller, the default primary serving a read or a test seeding fixtures, can run it
// first.
const ensureSchemaSQL = `DO $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('multigres.proto_metadata'));

  -- Per-tablegroup backing connection and admission. A tablegroup serves application
  -- work only while state is UNFENCED; FENCING is a fence whose members are not all
  -- confirmed clean, FENCED one that is. version goes up by one every time the
  -- tablegroup is fenced or unfenced (not when a fence completes), so a compare-and-set
  -- on it cannot be replayed.
  CREATE TABLE IF NOT EXISTS multigres.proto_tablegroup_serving (
    database TEXT NOT NULL,
    tablegroup TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'UNFENCED' CHECK (state IN ('UNFENCED', 'FENCING', 'FENCED')),
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (database, tablegroup)
  );

  -- Whole-database application tablegroup pointer that gateways poll.
  CREATE TABLE IF NOT EXISTS multigres.proto_routing (
    database TEXT PRIMARY KEY,
    app_tablegroup TEXT NOT NULL,
    version BIGINT NOT NULL
  );
END
$$`

// EnsureSchema creates the prototype tables if they do not exist. exec runs one
// statement with administrative privileges on the default primary.
func EnsureSchema(ctx context.Context, exec func(ctx context.Context, sql string) error) error {
	if err := exec(ctx, ensureSchemaSQL); err != nil {
		return fmt.Errorf("ensure prototype metadata schema: %w", err)
	}
	return nil
}
