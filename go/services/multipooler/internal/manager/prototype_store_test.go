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
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// TestPrototypeStoreReadsAMissingRowAndAMissingTableAsUnfenced: a cluster that has never fenced
// anything has no row for the tablegroup, and the metadata tables may not exist either;
// reading does not create them. Any other failure is an error, never "unfenced".
func TestPrototypeStoreReadsAMissingRowAndAMissingTableAsUnfenced(t *testing.T) {
	req := &multipoolerservicepb.GetServingStateRequest{Database: "db", Tablegroups: []string{"default", "other"}}

	t.Run("no row", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("FROM multigres.proto_tablegroup_serving", mock.MakeQueryResult(nil, nil))
		qs.AddQueryPattern("FROM multigres.proto_routing", mock.MakeQueryResult(nil, nil))
		resp, err := readServingState(t.Context(), newPrototypeStore(pm), req)
		require.NoError(t, err)
		require.Len(t, resp.GetTablegroups(), 2)
		for _, tg := range resp.GetTablegroups() {
			require.Equal(t, stateUnfenced, tg.GetState())
			require.Zero(t, tg.GetVersion())
		}
	})

	t.Run("no table", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		missing := mterrors.NewPgError("ERROR", mterrors.PgSSUndefinedTable, `relation "multigres.proto_tablegroup_serving" does not exist`, "")
		qs.AddQueryPatternWithError("FROM multigres.proto_tablegroup_serving", missing)
		qs.AddQueryPatternWithError("FROM multigres.proto_routing", missing)
		resp, err := readServingState(t.Context(), newPrototypeStore(pm), req)
		require.NoError(t, err)
		require.Len(t, resp.GetTablegroups(), 2)
		require.Equal(t, stateUnfenced, resp.GetTablegroups()[0].GetState())
		require.Empty(t, resp.GetAppTablegroup())
		require.EqualValues(t, 3, qs.AdminQueryCount(), "a read per tablegroup and one of routing, and nothing is created")
	})

	t.Run("any other failure is an error", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("FROM multigres.proto_tablegroup_serving", mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege, "permission denied", ""))
		_, err := readServingState(t.Context(), newPrototypeStore(pm), req)
		require.Error(t, err)
	})
}

// TestPrototypeStoreCreatesItsTablesBeforeAWriteAndTheConnectionLookup: reads never create
// anything, but the writes and the lookup of a connection do, in one statement.
func TestPrototypeStoreCreatesItsTablesBeforeAWriteAndTheConnectionLookup(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	qs.AddQueryPattern("DO \\$\\$", mock.MakeQueryResult(nil, nil))
	qs.AddQueryPattern("FROM multigres.proto_connections", mock.MakeQueryResult([]string{"dsn", "sysid"}, [][]any{{"postgres://u@h/db", "42"}}))
	conn, found, err := newPrototypeStore(pm).BackingConnection(t.Context(), "src")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "h", conn.GetHost())
	require.Equal(t, "u", conn.GetUser())
	require.Equal(t, "42", conn.GetExpectedSystemIdentifier())
	require.EqualValues(t, 2, qs.AdminQueryCount(), "the schema statement, then the lookup")
}

// TestPrototypeStoreCreatesItsTablesOncePerProcess: once the statement that creates the tables
// has succeeded, later writes do not run it again, and the store of a manager is built once.
func TestPrototypeStoreCreatesItsTablesOncePerProcess(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	qs.AddQueryPattern("DO \\$\\$", mock.MakeQueryResult(nil, nil))
	qs.AddQueryPattern("UPDATE multigres.proto_tablegroup_serving", mock.MakeQueryResult([]string{"n"}, [][]any{{int64(1)}}))
	store := newServingStore(pm)
	require.Same(t, store, newServingStore(pm), "one store per manager")
	for range 3 {
		ok, err := store.CompareAndSet(t.Context(), "db", "tg", stateUnfenced, 0, stateFencing, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.EqualValues(t, 4, qs.AdminQueryCount(), "the schema statement once, then three writes")
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

// TestParseBackingDSNRequiresASingleServer: fence safety looks at the backends of the server the
// pooler's admin connection reaches, so a URL that could send the admin connection and the
// application connections to different servers is refused.
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
