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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const (
	// routingAppRole is the application's login role. It is created on the
	// external PostgreSQL and copied into the cluster verbatim.
	routingAppRole     = "routing_app"
	routingAppPassword = "routing-app-password"
	routingImportOpID  = "bbbbbbbb-0000-0000-0000-000000000001"
	routingTablegroup  = "srctg"
)

// routingAdmin calls multiadmin's routing REST routes.
type routingAdmin struct {
	t    *testing.T
	base string
}

func (a routingAdmin) do(method, path string, body any) map[string]any {
	a.t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(a.t, err)
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(a.t.Context(), method, a.base+path, reader)
	require.NoError(a.t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(a.t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(a.t, err)
	require.Equal(a.t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, data)
	out := map[string]any{}
	require.NoError(a.t, json.Unmarshal(data, &out))
	return out
}

// routingVersion reads the database's routing version (an int64, which JSON
// carries as a string; absent when zero).
func (a routingAdmin) routingVersion() (string, []any) {
	snap := a.do(http.MethodGet, "/api/v1/databases/postgres/routing", nil)
	version, _ := snap["routingVersion"].(string)
	if version == "" {
		version = "0"
	}
	rules, _ := snap["rules"].([]any)
	return version, rules
}

// gatewayAppConn opens a new client session through the gateway as the
// application role.
func gatewayAppConn(t *testing.T, s *ShardSetup) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), fmt.Sprintf(
		"host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		s.MultigatewayPgPort, routingAppRole, routingAppPassword))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// servingSide reads, through a fresh gateway session, which PostgreSQL serves
// the application: the marker row each side holds.
func servingSide(t *testing.T, s *ShardSetup) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"host=localhost port=%d user=%s password=%s dbname=postgres sslmode=disable connect_timeout=5",
		s.MultigatewayPgPort, routingAppRole, routingAppPassword))
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	var side string
	err = conn.QueryRow(ctx, "SELECT side FROM routing_marker").Scan(&side)
	return side, err
}

// awaitNotification waits until conn receives a notification with payload,
// skipping others, while notify is called every 200ms to issue it. Neither
// the wait nor a notify is ever cancelled midway, since pgx closes a
// connection whose call is cancelled; notify has returned for the last time
// when this returns, so its connection can be used again.
func awaitNotification(t *testing.T, conn *pgx.Conn, payload string, notify func(context.Context) error) {
	t.Helper()
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			_ = notify(context.Background())
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		close(stop)
		<-stopped
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for {
		n, err := conn.WaitForNotification(ctx)
		require.NoError(t, err, "no %q notification arrived", payload)
		if n.Payload == payload {
			return
		}
	}
}

// TestRoutingRules_ImportRoutesTheApplication routes a database's application
// traffic to an external PostgreSQL behind an unmanaged pooler with a
// default → srctg routing rule, and back by deleting it. Queries, logins and
// LISTEN/NOTIFY follow the rule; control calls keep working.
//
// Each assertion reads state only one side has (a marker row, the server port,
// a notification issued directly on one side), never logs.
func TestRoutingRules_ImportRoutesTheApplication(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	s, cleanup := NewIsolated(t, WithMultiadmin(),
		WithMultigatewayExtraArgs("--routing-poll-interval=500ms"))
	t.Cleanup(cleanup)
	ctx := t.Context()
	admin := routingAdmin{t: t, base: fmt.Sprintf("http://localhost:%d", s.MultiadminHttpPort)}

	// The external PostgreSQL: the application role, and a marker row.
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

	// Copy the role into the cluster verbatim (the SCRAM verifier, not the
	// password), as the import's role copy does, and give the cluster's side
	// its own marker row.
	var verifier string
	require.NoError(t, ext.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname = $1", routingAppRole).Scan(&verifier))
	mg := s.ConnectPrimaryAdmin(t)
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", routingAppRole, verifier),
		"CREATE TABLE routing_marker (side text)",
		"INSERT INTO routing_marker VALUES ('default')",
		"GRANT SELECT ON routing_marker TO " + routingAppRole,
	} {
		_, err := mg.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	// The unmanaged pooler that serves the external PostgreSQL as srctg.
	tablegroupID, _ := seedExternalTablegroup(t, s, routingTablegroup, "routing-source", externalKeywordDSN(extPort))
	activateTablegroup(t, s, tablegroupID)
	startUnmanagedPooler(t, s, "unmanaged-routing", "postgres", routingTablegroup, tablegroupID)
	unmanagedID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: "unmanaged-routing"}
	require.Eventually(t, func() bool {
		rec, err := s.TopoServer.GetMultipooler(ctx, unmanagedID)
		return err == nil && rec.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING
	}, 60*time.Second, 200*time.Millisecond, "the unmanaged pooler never served")

	t.Run("before the rule the application is served by default", func(t *testing.T) {
		side, err := servingSide(t, s)
		require.NoError(t, err)
		assert.Equal(t, "default", side)
		_, rules := admin.routingVersion()
		assert.Empty(t, rules)
	})

	// A session that listens before and after the switch.
	listener := gatewayAppConn(t, s)
	_, err = listener.Exec(ctx, "LISTEN routing_ch")
	require.NoError(t, err)

	t.Run("a default to srctg rule sends the application to the external PostgreSQL", func(t *testing.T) {
		admin.do(http.MethodPut, "/api/v1/databases/postgres/routing/operations/"+routingImportOpID+"/import-target",
			map[string]any{"tablegroup": routingTablegroup})
		version, _ := admin.routingVersion()
		// The write is the next version after the one it is conditional on.
		current, err := strconv.ParseInt(version, 10, 64)
		require.NoError(t, err)
		resp := admin.do(http.MethodPut, "/api/v1/databases/postgres/routing/operations/"+routingImportOpID+"/rules",
			map[string]any{
				"rules":                  []map[string]any{{"fromTablegroup": "default", "toTablegroup": routingTablegroup, "toShard": "0-inf"}},
				"expectedRoutingVersion": version,
			})
		assert.Equal(t, strconv.FormatInt(current+1, 10), resp["routingVersion"])

		require.Eventually(t, func() bool {
			side, err := servingSide(t, s)
			return err == nil && side == "source"
		}, 15*time.Second, 100*time.Millisecond, "the gateway should route the application to srctg")

		// A fresh login works (the credential lookup stays on default) and
		// reaches the external PostgreSQL.
		conn := gatewayAppConn(t, s)
		var port string
		require.NoError(t, conn.QueryRow(ctx, "SELECT current_setting('port')").Scan(&port))
		assert.Equal(t, strconv.Itoa(extPort), port)

		// Control calls keep working and see the rule.
		_, rules := admin.routingVersion()
		require.Len(t, rules, 1)
	})

	t.Run("LISTEN follows the rule", func(t *testing.T) {
		// A notification issued on the external PostgreSQL itself reaches
		// the gateway session: its stream moved to srctg.
		awaitNotification(t, listener, "from-source", func(ctx context.Context) error {
			_, err := ext.Exec(ctx, "NOTIFY routing_ch, 'from-source'")
			return err
		})

		// NOTIFY through the gateway is application traffic too.
		notifier := gatewayAppConn(t, s)
		awaitNotification(t, listener, "via-gateway", func(ctx context.Context) error {
			_, err := notifier.Exec(ctx, "NOTIFY routing_ch, 'via-gateway'")
			return err
		})
	})

	t.Run("deleting the rule sends the application back to default", func(t *testing.T) {
		version, _ := admin.routingVersion()
		admin.do(http.MethodDelete,
			"/api/v1/databases/postgres/routing/operations/"+routingImportOpID+"/rules?expectedRoutingVersion="+version, nil)

		require.Eventually(t, func() bool {
			side, err := servingSide(t, s)
			return err == nil && side == "default"
		}, 15*time.Second, 100*time.Millisecond, "the gateway should route the application back to default")

		awaitNotification(t, listener, "from-default", func(ctx context.Context) error {
			_, err := mg.Exec(ctx, "NOTIFY routing_ch, 'from-default'")
			return err
		})
	})
}
