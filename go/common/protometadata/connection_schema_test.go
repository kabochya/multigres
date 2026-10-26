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

// TestEnsureConnectionSchemaIsOneIdempotentStatementUnderAnAdvisoryLock: the same reasons as
// the routing side, and the statement binds tablegroups to connections by adding a column to the
// existing tablegroup table rather than creating another.
func TestEnsureConnectionSchemaIsOneIdempotentStatementUnderAnAdvisoryLock(t *testing.T) {
	var statements []string
	require.NoError(t, EnsureConnectionSchema(t.Context(), func(_ context.Context, sql string) error {
		statements = append(statements, sql)
		return nil
	}))
	require.Len(t, statements, 1)
	sql := statements[0]
	require.Less(t, strings.Index(sql, "pg_advisory_xact_lock"), strings.Index(sql, "CREATE TABLE"), "the lock comes before any table")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS multigres.connection")
	require.Contains(t, sql, "ALTER TABLE multigres.tablegroup")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS backing_connection")
	require.NotContains(t, sql, "CREATE TABLE IF NOT EXISTS multigres.tablegroup", "the tablegroup table is the catalog's")
}

func TestEnsureConnectionSchemaReportsAFailure(t *testing.T) {
	err := EnsureConnectionSchema(t.Context(), func(context.Context, string) error { return errors.New("permission denied") })
	require.ErrorContains(t, err, "permission denied")
}
