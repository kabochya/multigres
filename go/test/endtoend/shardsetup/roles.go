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
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// ConnectPrimaryAdmin opens an administrative connection to the primary
// postgres of this cohort, for fixtures that must go around the pooler.
func (s *ShardSetup) ConnectPrimaryAdmin(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=%s sslmode=disable",
		s.PrimaryPgctld(t).PgPort, TestPostgresPassword, s.Database))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// RoleVerifier returns the stored SCRAM verifier of a role.
func RoleVerifier(ctx context.Context, conn *pgx.Conn, role string) (string, error) {
	var verifier string
	err := conn.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname = $1", role).Scan(&verifier)
	return verifier, err
}

// CreateRoleWithVerifier creates a login role whose stored verifier is exactly
// verifier. PostgreSQL stores a password that is already a SCRAM verifier
// verbatim, which is what lets one role authenticate against two databases with
// the same client proof: SCRAM key passthrough needs byte-identical verifiers on
// both sides.
func CreateRoleWithVerifier(ctx context.Context, conn *pgx.Conn, role, verifier string) error {
	literal := "'" + strings.ReplaceAll(verifier, "'", "''") + "'"
	_, err := conn.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN PASSWORD "+literal)
	return err
}
