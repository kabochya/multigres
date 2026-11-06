// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

const (
	importFlowConnection = "import_source"
	importFlowPooler     = "unmanaged-import-flow"
	importFlowOpID       = "bbbbbbbb-0000-0000-0000-000000000002"
)

// gatewayAdminConn opens a superuser session through the gateway, which is what the
// connection and tablegroup statements need. The session is the caller's: a statement that
// waits (CREATE TABLEGROUP) holds it, so one that must not wait for it opens another.
func gatewayAdminConn(t *testing.T, s *ShardSetup) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), GetTestUserDSN("localhost", s.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// execSimple runs sql over the simple protocol, as psql does.
func execSimple(ctx context.Context, conn *pgx.Conn, sql string) error {
	_, err := conn.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	return err
}

// TestImportFlowThroughSQL drives the import of a database from an external PostgreSQL with
// SQL and the routing rule call, in the order an operator runs it:
//
//  1. CREATE CONNECTION stores the external database's connection info.
//  2. CREATE TABLEGROUP ... CONNECTION records a tablegroup that is served from it. The
//     statement waits for the tablegroup to be active. This test stands in for the provisioner
//     (it starts the lone unmanaged pooler) and for the completion of the creation (it marks the
//     tablegroup active once that pooler serves), neither of which the catalog primary has yet.
//  3. The import target call writes the default → srctg rule, and the application's queries,
//     logins and LISTEN/NOTIFY follow it.
//
// A connection that a tablegroup is bound to cannot be dropped, and a tablegroup cannot be
// created, or skipped with IF NOT EXISTS, for a connection other than its own.
func TestImportFlowThroughSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	s, cleanup := NewIsolated(t, WithMultiadmin(),
		WithMultigatewayExtraArgs("--routing-poll-interval=500ms", "--enable-tablegroup-ddl"))
	t.Cleanup(cleanup)
	ctx := t.Context()
	admin := routingAdmin{t: t, base: fmt.Sprintf("http://localhost:%d", s.MultiadminHttpPort)}

	// The external PostgreSQL holds the application role and its marker row; the cluster gets the
	// role verbatim and a marker row of its own.
	extPort := startExternalPostgres(t)
	ext, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer ext.Close(context.Background())
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", routingAppRole, routingAppPassword),
		"CREATE TABLE routing_marker (side text)",
		"INSERT INTO routing_marker VALUES ('source')",
		"GRANT SELECT ON routing_marker TO " + routingAppRole,
	} {
		_, err := ext.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	var verifier string
	require.NoError(t, ext.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname = $1", routingAppRole).Scan(&verifier))
	catalog := s.ConnectPrimaryAdmin(t)
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", routingAppRole, verifier),
		"CREATE TABLE routing_marker (side text)",
		"INSERT INTO routing_marker VALUES ('default')",
		"GRANT SELECT ON routing_marker TO " + routingAppRole,
	} {
		_, err := catalog.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	sqlConn := gatewayAdminConn(t, s)
	createConnection := func(name string) string {
		return fmt.Sprintf("CREATE CONNECTION %s OPTIONS (host '127.0.0.1', port '%d', dbname 'postgres', user 'postgres', password '%s', sslmode 'disable')",
			name, extPort, externalPostgresPassword)
	}

	t.Run("a connection is stored and a tablegroup needs it to exist", func(t *testing.T) {
		require.NoError(t, execSimple(ctx, sqlConn, createConnection(importFlowConnection)))
		require.NoError(t, execSimple(ctx, sqlConn, createConnection("other_source")))

		var stored int
		require.NoError(t, catalog.QueryRow(ctx, "SELECT count(*) FROM multigres.connections WHERE name IN ($1, 'other_source')", importFlowConnection).Scan(&stored),
			"the connections are in the catalog the migrator reads")
		assert.Equal(t, 2, stored)

		err := execSimple(ctx, sqlConn, "CREATE TABLEGROUP "+routingTablegroup+" CONNECTION no_such_connection")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"no_such_connection" does not exist`)

		// The grammar refuses both clauses at once.
		err = execSimple(ctx, sqlConn, "CREATE TABLEGROUP "+routingTablegroup+" CONNECTION "+importFlowConnection+" SHARDS ('0-inf')")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "syntax error")
	})

	// CREATE TABLEGROUP waits for the creation to finish, so it runs on its own session while the
	// test provisions.
	created := make(chan error, 1)
	creating := gatewayAdminConn(t, s)
	go func() {
		created <- execSimple(ctx, creating, "CREATE TABLEGROUP "+routingTablegroup+" CONNECTION "+importFlowConnection)
	}()

	var tablegroupID int64
	t.Run("the statement records a tablegroup bound to the connection and waits", func(t *testing.T) {
		require.Eventually(t, func() bool {
			var bound bool
			err := catalog.QueryRow(ctx,
				"SELECT tablegroup_id, backing_connection_id IS NOT NULL FROM multigres.tablegroup WHERE name = $1", routingTablegroup).
				Scan(&tablegroupID, &bound)
			return err == nil && bound
		}, 30*time.Second, 100*time.Millisecond, "the tablegroup was never recorded")

		var state, shards string
		require.NoError(t, catalog.QueryRow(ctx, `SELECT t.state, string_agg(s.shard_name, ',')
			FROM multigres.tablegroup t JOIN multigres.shard s USING (tablegroup_id) WHERE t.tablegroup_id = $1 GROUP BY t.state`,
			tablegroupID).Scan(&state, &shards))
		assert.Equal(t, "creating", state)
		assert.Equal(t, "0-inf", shards, "a bound tablegroup has the one shard")

		select {
		case err := <-created:
			t.Fatalf("the statement returned before the tablegroup was complete: %v", err)
		case <-time.After(2 * time.Second):
		}
	})

	t.Run("the connection cannot be dropped while the tablegroup is bound to it", func(t *testing.T) {
		err := execSimple(ctx, sqlConn, "DROP CONNECTION "+importFlowConnection)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tablegroup")
		var n int
		require.NoError(t, catalog.QueryRow(ctx, "SELECT count(*) FROM multigres.connections WHERE name = $1", importFlowConnection).Scan(&n))
		assert.Equal(t, 1, n, "the connection is still there")
	})

	t.Run("another connection for the same name is an error, with or without IF NOT EXISTS", func(t *testing.T) {
		for _, stmt := range []string{
			"CREATE TABLEGROUP " + routingTablegroup + " CONNECTION other_source",
			"CREATE TABLEGROUP IF NOT EXISTS " + routingTablegroup + " CONNECTION other_source",
		} {
			err := execSimple(ctx, sqlConn, stmt)
			require.Error(t, err, stmt)
			assert.Contains(t, err.Error(), `with backing connection "`+importFlowConnection+`", not with backing connection "other_source"`, stmt)
		}
	})

	// The provisioner's part: the lone unmanaged pooler of the tablegroup. It is told its
	// tablegroup and learns the connection from the catalog.
	startUnmanagedPooler(t, s, importFlowPooler, "postgres", routingTablegroup, tablegroupID)
	unmanagedID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: importFlowPooler}
	require.Eventually(t, func() bool {
		rec, err := s.TopoServer.GetMultipooler(ctx, unmanagedID)
		return err == nil && rec.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING &&
			rec.LeadershipMode == clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED
	}, 60*time.Second, 200*time.Millisecond, "the unmanaged pooler never served")

	t.Run("the creation completes once the pooler serves", func(t *testing.T) {
		// What completes a creation, until the catalog primary does it: its shard, tablegroup and
		// operation become active and done.
		activateTablegroup(t, s, tablegroupID)
		select {
		case err := <-created:
			require.NoError(t, err, "CREATE TABLEGROUP ... CONNECTION")
		case <-time.After(30 * time.Second):
			t.Fatal("CREATE TABLEGROUP never returned after the tablegroup was complete")
		}

		// Run again, it finds the tablegroup, and the listing carries its binding.
		require.NoError(t, execSimple(ctx, sqlConn, "CREATE TABLEGROUP IF NOT EXISTS "+routingTablegroup+" CONNECTION "+importFlowConnection))
		primary := s.NewPrimaryClient(t)
		defer primary.Close()
		list, err := primary.Manager.ListTableGroups(ctx, &multipoolermanagerdata.ListTableGroupsRequest{})
		require.NoError(t, err)
		var found bool
		for _, tg := range list.GetTableGroups() {
			if tg.GetName() == routingTablegroup {
				found = true
				assert.Equal(t, importFlowConnection, tg.GetBackingConnection())
			}
		}
		require.True(t, found, "the tablegroup is listed")
	})

	t.Run("before the rule the application is served by default", func(t *testing.T) {
		side, err := servingSide(t, s)
		require.NoError(t, err)
		assert.Equal(t, "default", side)
	})

	listener := gatewayAppConn(t, s)
	_, err = listener.Exec(ctx, "LISTEN routing_ch")
	require.NoError(t, err)

	t.Run("the import target routes the application to the external PostgreSQL", func(t *testing.T) {
		// The tablegroup exists, is active and is bound, which the call requires; it writes the
		// default -> srctg rule itself.
		// The rule is the next routing version after the one in place, whatever that is: completing
		// a tablegroup's creation advances it too.
		before, _ := admin.routingVersion()
		current, err := strconv.ParseInt(before, 10, 64)
		require.NoError(t, err)
		want := strconv.FormatInt(current+1, 10)
		resp := admin.do(http.MethodPut, "/api/v1/databases/postgres/routing/operations/"+importFlowOpID+"/import-target",
			map[string]any{"tablegroup": routingTablegroup})
		assert.Equal(t, want, resp["routingVersion"], "ensuring the import target writes its default rule")
		again := admin.do(http.MethodPut, "/api/v1/databases/postgres/routing/operations/"+importFlowOpID+"/import-target",
			map[string]any{"tablegroup": routingTablegroup})
		assert.Equal(t, want, again["routingVersion"], "running it again changes nothing")

		require.Eventually(t, func() bool {
			side, err := servingSide(t, s)
			return err == nil && side == "source"
		}, 15*time.Second, 100*time.Millisecond, "the gateway should route the application to the tablegroup")

		conn := gatewayAppConn(t, s)
		var port string
		require.NoError(t, conn.QueryRow(ctx, "SELECT current_setting('port')").Scan(&port))
		assert.Equal(t, strconv.Itoa(extPort), port, "the application reaches the external PostgreSQL")
	})

	t.Run("LISTEN and NOTIFY follow the rule", func(t *testing.T) {
		awaitNotification(t, listener, "from-source", func(ctx context.Context) error {
			_, err := ext.Exec(ctx, "NOTIFY routing_ch, 'from-source'")
			return err
		})
		notifier := gatewayAppConn(t, s)
		awaitNotification(t, listener, "via-gateway", func(ctx context.Context) error {
			_, err := notifier.Exec(ctx, "NOTIFY routing_ch, 'via-gateway'")
			return err
		})
	})

	t.Run("the connection is still the migrator's to read", func(t *testing.T) {
		rows, err := sqlConn.Query(ctx, "SHOW CONNECTIONS", pgx.QueryExecModeSimpleProtocol)
		require.NoError(t, err)
		defer rows.Close()
		var names []string
		for rows.Next() {
			vals, err := rows.Values()
			require.NoError(t, err)
			names = append(names, fmt.Sprint(vals[0]))
		}
		require.NoError(t, rows.Err())
		assert.Contains(t, strings.Join(names, ","), importFlowConnection)
	})
}
