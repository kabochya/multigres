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

// Package protometadata holds the throwaway metadata tables the unmanaged
// pooler prototype keeps on the default primary.
//
// PROTOTYPE STUB: delete this package when the real tablegroup and connection
// catalog lands. The tables are created lazily and live beside, never inside,
// the sharding catalog tables.
package protometadata

import (
	"context"
	"fmt"
)

// Statements create the prototype tables. They are idempotent so any caller —
// the default primary serving a read, or a test seeding fixtures — can run them
// first.
var Statements = []string{
	// Plaintext, insert-only connection catalog. Never UPDATE a row.
	`CREATE TABLE IF NOT EXISTS multigres.proto_connections (
  name TEXT PRIMARY KEY,
  dsn TEXT NOT NULL,
  expected_system_identifier TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`,
	// Per-tablegroup backing connection and admission state.
	`CREATE TABLE IF NOT EXISTS multigres.proto_tablegroup_serving (
  database TEXT NOT NULL,
  tablegroup TEXT NOT NULL,
  connection TEXT NULL REFERENCES multigres.proto_connections(name),
  admission_state TEXT NOT NULL DEFAULT 'UNFENCED'
    CHECK (admission_state IN ('UNFENCED','FENCING','FENCED','UNFENCING')),
  request_id TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (database, tablegroup)
)`,
	// Whole-database application tablegroup pointer that gateways poll.
	`CREATE TABLE IF NOT EXISTS multigres.proto_routing (
  database TEXT PRIMARY KEY,
  app_tablegroup TEXT NOT NULL,
  version BIGINT NOT NULL
)`,
}

// EnsureSchema creates the prototype tables if they do not exist. exec runs one
// statement with administrative privileges on the default primary.
func EnsureSchema(ctx context.Context, exec func(ctx context.Context, sql string) error) error {
	for _, stmt := range Statements {
		if err := exec(ctx, stmt); err != nil {
			return fmt.Errorf("ensure prototype metadata schema: %w", err)
		}
	}
	return nil
}
