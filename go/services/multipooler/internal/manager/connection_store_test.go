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

package manager

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// TestConnectionStoreCreatesItsTablesBeforeTheConnectionLookup: reads never create
// anything, but the lookup of a connection does, in one statement.
func TestConnectionStoreCreatesItsTablesBeforeTheConnectionLookup(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	qs.AddQueryPattern("DO \\$\\$", mock.MakeQueryResult(nil, nil))
	qs.AddQueryPattern("FROM multigres.connection", mock.MakeQueryResult([]string{"dsn", "sysid"}, [][]any{{"postgres://u@h/db", "42"}}))
	conn, found, err := newConnectionStore(pm).BackingConnection(t.Context(), "src")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "h", conn.GetHost())
	require.Equal(t, "u", conn.GetUser())
	require.Equal(t, "42", conn.GetExpectedSystemIdentifier())
	require.EqualValues(t, 2, qs.AdminQueryCount(), "the schema statement, then the lookup")
}

// TestTablegroupBindingIsReadWithoutCreatingAnything: the binding is the backing_connection
// column of the tablegroup table. Reading it creates nothing: a table without the column has
// never bound a tablegroup, so the tablegroup is managed, and a tablegroup that has no row is
// not found.
func TestTablegroupBindingIsReadWithoutCreatingAnything(t *testing.T) {
	t.Run("bound", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("COALESCE.*FROM multigres.tablegroup", mock.MakeQueryResult([]string{"c"}, [][]any{{"src"}}))
		name, found, err := newConnectionStore(pm).TablegroupBinding(t.Context(), "tg")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "src", name)
		require.EqualValues(t, 1, qs.AdminQueryCount(), "no schema statement")
	})
	t.Run("not found", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.tablegroup", mock.MakeQueryResult(nil, nil))
		_, found, err := newConnectionStore(pm).TablegroupBinding(t.Context(), "tg")
		require.NoError(t, err)
		require.False(t, found)
	})
	t.Run("no binding column", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("COALESCE.*FROM multigres.tablegroup",
			mterrors.NewPgError("ERROR", mterrors.PgSSUndefinedColumn, `column "backing_connection" does not exist`, ""))
		qs.AddQueryPattern("SELECT '' FROM multigres.tablegroup", mock.MakeQueryResult([]string{"c"}, [][]any{{""}}))
		name, found, err := newConnectionStore(pm).TablegroupBinding(t.Context(), "tg")
		require.NoError(t, err)
		require.True(t, found)
		require.Empty(t, name)
	})
	t.Run("any other failure is an error", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("FROM multigres.tablegroup", mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege, "permission denied", ""))
		_, _, err := newConnectionStore(pm).TablegroupBinding(t.Context(), "tg")
		require.Error(t, err)
	})
}

func TestParseBackingDSN(t *testing.T) {
	got, err := parseBackingDSN("postgres://alice:s3cret@db.example.com:6543/app?sslmode=verify-full&sslrootcert=/ca.pem&sslnegotiation=direct")
	require.NoError(t, err)
	require.Equal(t, "db.example.com", got.GetHost())
	require.EqualValues(t, 6543, got.GetPort())
	require.Equal(t, "app", got.GetDatabase())
	require.Equal(t, "alice", got.GetUser())
	require.Equal(t, "s3cret", got.GetPassword())
	require.Equal(t, "verify-full", got.GetSslMode())
	require.Equal(t, "/ca.pem", got.GetSslRootCert())
	require.Equal(t, "direct", got.GetSslNegotiation())

	got, err = parseBackingDSN("postgres://alice@db.example.com/app")
	require.NoError(t, err)
	require.EqualValues(t, 5432, got.GetPort())

	for _, bad := range []string{"", "mysql://u@h/db", "postgres://u@h:notaport/db"} {
		_, err := parseBackingDSN(bad)
		require.Error(t, err, bad)
	}
}

// TestParseBackingDSNRequiresASingleServer: the pooler checks the identity of the server its admin
// connection reaches, so a URL that could send the admin connection and the application
// connections to different servers is refused.
func TestParseBackingDSNRequiresASingleServer(t *testing.T) {
	for _, bad := range []string{
		"postgres://u@h1:5432,h2:5432/db",
		"postgres://u@h1/db?host=h2",
		"postgres://u@h1/db?hostaddr=10.0.0.1",
		"postgres://u@h1/db?port=5433",
		"postgres://u@h1/db?target_session_attrs=read-write",
		"postgres://u@h1/db?load_balance_hosts=random",
	} {
		_, err := parseBackingDSN(bad)
		require.Error(t, err, bad)
	}
}

// TestTheConnectionDirectoryIsRegistered: the directory of backing connections is what the
// prototype registers; without it (a build that drops connection_store.go) it stays nil and the
// calls answer UNIMPLEMENTED.
func TestTheConnectionDirectoryIsRegistered(t *testing.T) {
	require.NotNil(t, newConnectionDirectory)
}
