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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnsureRoutingSchemaIsOneIdempotentStatementUnderAnAdvisoryLock: poolers that start
// together would race in CREATE TABLE IF NOT EXISTS (two sessions can both find a table missing,
// and the second fails on the catalog's unique index), and an exec that may run each statement
// of a list on a different connection could not hold a lock across them. So the schema is one
// statement that takes a transaction-level advisory lock first and creates only what is missing.
func TestEnsureRoutingSchemaIsOneIdempotentStatementUnderAnAdvisoryLock(t *testing.T) {
	var statements []string
	require.NoError(t, EnsureRoutingSchema(t.Context(), func(_ context.Context, sql string) error {
		statements = append(statements, sql)
		return nil
	}))
	require.Len(t, statements, 1, "one statement, so that one connection holds the lock for all of it")
	sql := statements[0]
	require.Less(t, strings.Index(sql, "pg_advisory_xact_lock"), strings.Index(sql, "CREATE TABLE"), "the lock comes before any table")
	require.Equal(t, strings.Count(sql, "CREATE TABLE"), strings.Count(sql, "CREATE TABLE IF NOT EXISTS"), "every table is created only if missing")
	for _, table := range []string{"catalog_state", "admission_fence", "operation", "routing_rule"} {
		require.Contains(t, sql, "multigres."+table)
	}
	require.Contains(t, sql, "'UNFENCED', 'FENCING', 'FENCED'", "the state column admits the three admission states")
}

// TestAdmissionFenceKeepsOneHolderPerScopeAndOnlyTablegroupScopes: the unique index is on the
// whole scope, so it keeps its meaning when shard and table scopes arrive, and the shard and table
// columns are present but must be NULL for now.
func TestAdmissionFenceKeepsOneHolderPerScopeAndOnlyTablegroupScopes(t *testing.T) {
	require.Contains(t, ensureRoutingSchemaSQL, "CREATE UNIQUE INDEX IF NOT EXISTS admission_fence_scope")
	require.Contains(t, ensureRoutingSchemaSQL, "(tablegroup, COALESCE(shard_id, 0), COALESCE(table_id, 0))")
	require.Contains(t, ensureRoutingSchemaSQL, "CHECK (shard_id IS NULL AND table_id IS NULL)")
	require.NotContains(t, ensureRoutingSchemaSQL, "REFERENCES multigres.operation", "the operation is not in the catalog")
}

func TestEnsureRoutingSchemaReportsAFailure(t *testing.T) {
	err := EnsureRoutingSchema(t.Context(), func(context.Context, string) error { return errors.New("permission denied") })
	require.ErrorContains(t, err, "permission denied")
}

// TestRoutingRuleTableHoldsOneRowAndOperationsHaveALifecycle: at most one rule applies, so the table
// is held to one row by a constant-key unique index and not only by the switch that checks it, and
// an operation is in one of the states of its lifecycle.
func TestRoutingRuleTableHoldsOneRowAndOperationsHaveALifecycle(t *testing.T) {
	require.Contains(t, ensureRoutingSchemaSQL, "CREATE UNIQUE INDEX IF NOT EXISTS routing_rule_single ON multigres.routing_rule ((true))")
	require.Contains(t, ensureRoutingSchemaSQL, "'PREPARE', 'SWITCHED', 'REVERTED', 'FOLDED', 'CLEANED'")
	require.NotContains(t, ensureRoutingSchemaSQL, "REFERENCES multigres.operation", "no foreign key to the operation")
}

// TestEveryStatementThatChangesTheRoutingLocksTheCatalogStateFirst: the catalog_state row is the lock
// they share, taken before any other so that they run one after the other and cannot deadlock.
func TestEveryStatementThatChangesTheRoutingLocksTheCatalogStateFirst(t *testing.T) {
	for name, sql := range map[string]string{
		"switch": SwitchRoutingSQL, "revert": RevertRoutingSQL, "fold": FoldRoutingSQL, "cleanup": CleanupMarkSQL,
	} {
		t.Run(name, func(t *testing.T) {
			require.Regexp(t, `^WITH cs AS MATERIALIZED \(\s+SELECT [a-z_, ]+ FROM multigres\.catalog_state FOR UPDATE`, sql)
			// Every other lock names the first one in a condition, so it is taken first.
			for _, lock := range []string{"FOR UPDATE", "FOR SHARE OF af"} {
				for _, at := range indexes(sql, lock)[min(1, len(indexes(sql, lock))):] {
					if lock == "FOR UPDATE" && at == strings.Index(sql, "FOR UPDATE") {
						continue
					}
					require.Contains(t, sql[max(0, at-160):at], csLocked, "%s at %d: %s", lock, at, sql[max(0, at-160):at])
					if lock == "FOR SHARE OF af" {
						// The fences are locked after the operation's row, the order the writers of a fence use.
						require.Contains(t, sql[max(0, at-160):at], opLocked)
					}
				}
			}
		})
	}
}

func indexes(s, sub string) []int {
	var out []int
	for from := 0; ; {
		i := strings.Index(s[from:], sub)
		if i < 0 {
			return out
		}
		out = append(out, from+i)
		from += i + len(sub)
	}
}

// TestFenceWritersLockTheOperationBeforeTheFence: the statements that write a fence take the
// operation's row first (they update it, or insert it), and refuse an operation that is CLEANED
// from the row they locked, not from a snapshot.
func TestFenceWritersLockTheOperationBeforeTheFence(t *testing.T) {
	for name, sql := range map[string]string{"insert": InsertFenceSQL, "update": UpdateFenceSQL} {
		t.Run(name, func(t *testing.T) {
			require.Contains(t, sql, "DO UPDATE SET updated_at = now() WHERE multigres.operation.state <> 'CLEANED'")
			require.Contains(t, sql, "EXISTS (SELECT 1 FROM op)", "the write is gated on the row the statement locked")
			require.NotContains(t, sql, "state = 'CLEANED')", "and not on a read of the snapshot")
		})
	}
}
