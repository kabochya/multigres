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

package multipooler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBackingConnectionsAreServedFromTheCatalog verifies, against a real catalog shard, that the
// default primary says how a tablegroup is served from the catalog: GetBackingConnection returns
// the connection a tablegroup is bound to in multigres.connections, the table the migrator shares,
// structured, whether it was written in SQL as an operator does or created through the migrator,
// and says that a tablegroup with no binding is managed.
func TestBackingConnectionsAreServedFromTheCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	primaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.PrimaryPgctld.PoolerDir), setup.PrimaryPgctld.PgPort)
	defer primaryDB.Close()
	var database string
	require.NoError(t, primaryDB.QueryRow(`SELECT current_database()`).Scan(&database))

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	pooler := multipoolerservicepb.NewMultipoolerServiceClient(conn)
	migrator := migratorpb.NewMigratorClient(conn)
	manager := multipoolermanagerpb.NewMultipoolerManagerClient(conn)

	// The shared setup is used by other tests: remove what this one added. This runs before
	// primaryDB is closed (defers run last in, first out).
	defer func() {
		for _, stmt := range []string{
			`DELETE FROM multigres.shard WHERE tablegroup_id IN (SELECT tablegroup_id FROM multigres.tablegroup WHERE name IN ('backingtg', 'migratortg', 'managedtg'))`,
			`DELETE FROM multigres.operation WHERE kind = 'create_tablegroup' AND request->>'name' IN ('backingtg', 'migratortg', 'managedtg')`,
			`DELETE FROM multigres.tablegroup WHERE name IN ('backingtg', 'migratortg', 'managedtg')`,
			`DELETE FROM multigres.connections WHERE name IN ('backing-src', 'migrator-src')`,
		} {
			if _, err := primaryDB.Exec(stmt); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	}()

	// A connection for an unmanaged pooler, written in SQL as the operator does, and one the
	// migrator creates, in the keyword=value form it stores. Both name the database the cluster
	// serves, which a tablegroup bound to them must.
	_, err = primaryDB.Exec(fmt.Sprintf(`INSERT INTO multigres.connections (name, dsn)
		VALUES ('backing-src', 'postgres://alice:s3cret@db.example.com:6543/%s?sslmode=verify-full&sslrootcert=/ca.pem')`, database))
	require.NoError(t, err)
	_, err = migrator.CreateConnection(utils.WithShortDeadline(t), &migratorpb.CreateConnectionRequest{
		Connection: &migratorpb.Connection{Name: "migrator-src", Dsn: fmt.Sprintf("host=source.example.com port=5433 dbname=%s user=repl password='p w'", database)},
	})
	require.NoError(t, err)

	create := func(name, connection string) int64 {
		t.Helper()
		resp, err := manager.CreateTableGroup(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.CreateTableGroupRequest{Name: name, BackingConnection: connection})
		require.NoError(t, err)
		return resp.GetTablegroupId()
	}
	backingID := create("backingtg", "backing-src")
	migratorID := create("migratortg", "migrator-src")
	managedID := create("managedtg", "")

	get := func(id int64) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
		return pooler.GetBackingConnection(utils.WithShortDeadline(t),
			&multipoolerservicepb.GetBackingConnectionRequest{Database: database, TablegroupId: id})
	}

	t.Run("GetBackingConnection", func(t *testing.T) {
		resp, err := get(backingID)
		require.NoError(t, err)
		assert.False(t, resp.GetManaged())
		got := resp.GetConnection()
		assert.Equal(t, "backing-src", got.GetName())
		assert.Equal(t, []string{"db.example.com"}, got.GetHost())
		assert.Equal(t, []int32{6543}, got.GetPort())
		assert.Equal(t, database, got.GetDatabase())
		assert.Equal(t, "alice", got.GetUser())
		assert.Equal(t, "s3cret", got.GetPassword())
		assert.Equal(t, "verify-full", got.GetSslMode())
		assert.Equal(t, "/ca.pem", got.GetSslRootCert())
	})

	t.Run("a tablegroup with no binding is managed", func(t *testing.T) {
		resp, err := get(managedID)
		require.NoError(t, err)
		assert.True(t, resp.GetManaged())
		assert.Nil(t, resp.GetConnection())
	})

	t.Run("a tablegroup that does not exist is not found, and an id is required", func(t *testing.T) {
		_, err := get(migratorID + 1000)
		assert.Equal(t, codes.NotFound, status.Code(err))
		_, err = get(0)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("the table is shared with the migrator", func(t *testing.T) {
		resp, err := migrator.ListConnections(utils.WithShortDeadline(t), &migratorpb.ListConnectionsRequest{})
		require.NoError(t, err)
		var names []string
		for _, c := range resp.GetConnections() {
			names = append(names, c.GetName())
		}
		assert.Contains(t, names, "migrator-src")
		assert.Contains(t, names, "backing-src")
	})

	t.Run("a connection the migrator created is servable as it is", func(t *testing.T) {
		// Its dsn is the keyword=value form the migrator stores; both forms are read alike.
		resp, err := get(migratorID)
		require.NoError(t, err)
		assert.Equal(t, "migrator-src", resp.GetConnection().GetName())
		assert.Equal(t, []string{"source.example.com"}, resp.GetConnection().GetHost())
		assert.Equal(t, []int32{5433}, resp.GetConnection().GetPort())
		assert.Equal(t, database, resp.GetConnection().GetDatabase())
		assert.Equal(t, "repl", resp.GetConnection().GetUser())
		assert.Equal(t, "p w", resp.GetConnection().GetPassword())
	})

	t.Run("a connection a tablegroup is bound to cannot be dropped through the migrator", func(t *testing.T) {
		_, err := migrator.DropConnection(utils.WithShortDeadline(t), &migratorpb.DropConnectionRequest{
			Ref: &migratorpb.ConnectionRef{Ref: &migratorpb.ConnectionRef_Name{Name: "migrator-src"}},
		})
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Contains(t, err.Error(), "referenced by a migration or a tablegroup")
	})

	t.Run("a dsn that is not usable is refused", func(t *testing.T) {
		_, err := primaryDB.Exec(`UPDATE multigres.connections SET dsn = 'postgres://alice@db.example.com:5432/app?channel_binding=require' WHERE name = 'backing-src'`)
		require.NoError(t, err)
		_, err = get(backingID)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "a parameter that is not carried is refused, not dropped: %v", err)
		_, err = primaryDB.Exec(fmt.Sprintf(`UPDATE multigres.connections SET dsn = 'postgres://alice@db.example.com:5432/%s' WHERE name = 'backing-src'`, database))
		require.NoError(t, err)
	})
}
