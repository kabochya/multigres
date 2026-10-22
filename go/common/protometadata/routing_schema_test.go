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

// TestEnsureSchemaIsOneIdempotentStatementUnderAnAdvisoryLock: poolers that start together
// would race in CREATE TABLE IF NOT EXISTS (two sessions can both find a table missing, and the
// second fails on the catalog's unique index), and an exec that may run each statement of a list
// on a different connection could not hold a lock across them. So the schema is one statement
// that takes a transaction-level advisory lock first and creates only what is missing.
func TestEnsureSchemaIsOneIdempotentStatementUnderAnAdvisoryLock(t *testing.T) {
	var statements []string
	require.NoError(t, EnsureSchema(t.Context(), func(_ context.Context, sql string) error {
		statements = append(statements, sql)
		return nil
	}))
	require.Len(t, statements, 1, "one statement, so that one connection holds the lock for all of it")
	sql := statements[0]
	require.Less(t, strings.Index(sql, "pg_advisory_xact_lock"), strings.Index(sql, "CREATE TABLE"), "the lock comes before any table")
	require.Equal(t, strings.Count(sql, "CREATE TABLE"), strings.Count(sql, "CREATE TABLE IF NOT EXISTS"), "every table is created only if missing")
	for _, table := range []string{"proto_tablegroup_serving", "proto_routing"} {
		require.Contains(t, sql, "multigres."+table)
	}
	require.Contains(t, sql, "'UNFENCED', 'FENCING', 'FENCED'", "the state column admits the three admission states")
}

func TestEnsureSchemaReportsAFailure(t *testing.T) {
	err := EnsureSchema(t.Context(), func(context.Context, string) error { return errors.New("permission denied") })
	require.ErrorContains(t, err, "permission denied")
}
