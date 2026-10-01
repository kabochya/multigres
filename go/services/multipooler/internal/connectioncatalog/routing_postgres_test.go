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

package connectioncatalog_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/routingpolicy"
	"github.com/multigres/multigres/go/tools/executil"
)

func TestPostgresRoutingUncertainCommitAndReplay(t *testing.T) {
	dsn, dir, port := disposablePostgres(t)
	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer observer.Close(context.Background())
	_, err = observer.Exec(t.Context(), "CREATE SCHEMA multigres")
	require.NoError(t, err)
	key := bytes.Repeat([]byte{1}, 32)
	catalog, err := connectioncatalog.New(postgresQueries{dsn: dsn}, key)
	require.NoError(t, err)
	require.NoError(t, catalog.Initialize(t.Context()))
	require.NoError(t, catalog.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return routingpolicy.Initialize(ctx, tx, "postgres")
	}))
	_, err = observer.Exec(t.Context(), "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (routing_standby)'")
	require.NoError(t, err)
	_, err = observer.Exec(t.Context(), "SELECT pg_reload_conf()")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- catalog.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
			return routingpolicy.Write(ctx, tx, "postgres", &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED}, catalog)
		})
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event='SyncRep')").Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	cancel()
	require.Error(t, <-done)
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)
	var mode int32
	require.NoError(t, observer.QueryRow(t.Context(), "SELECT destination FROM multigres.gateway_routing WHERE database='postgres'").Scan(&mode))
	require.Equal(t, int32(pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED), mode)
	restarted, err := connectioncatalog.New(postgresQueries{dsn: dsn}, key)
	require.NoError(t, err)
	confirm := func(ctx context.Context) (*pb.GatewayRoutingPolicy, error) {
		var p *pb.GatewayRoutingPolicy
		err := restarted.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
			var err error
			p, err = routingpolicy.Confirm(ctx, tx, "postgres")
			return err
		})
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	timeout, stop := context.WithTimeout(t.Context(), 200*time.Millisecond)
	p, err := confirm(timeout)
	stop()
	require.Error(t, err)
	require.Nil(t, p, "uncertain local routing cannot be published after process restart")
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)

	_, syncReplica := startCatalogReplica(t, dir, port, "routing_standby")
	require.Eventually(t, func() bool {
		var sync bool
		err := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_replication WHERE application_name='routing_standby' AND sync_state='sync')").Scan(&sync)
		return err == nil && sync
	}, 10*time.Second, 20*time.Millisecond)
	confirmedCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	p, err = confirm(confirmedCtx)
	require.NoError(t, err)
	require.Equal(t, pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED, p.Destination)
	_, lagging := startCatalogReplica(t, dir, port, "routing_lagging")
	_, err = lagging.Exec(t.Context(), "SELECT pg_wal_replay_pause()")
	require.NoError(t, err)
	require.NoError(t, restarted.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return routingpolicy.Write(ctx, tx, "postgres", &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_MANAGED}, restarted)
	}))
	require.NoError(t, lagging.QueryRow(t.Context(), "SELECT destination FROM multigres.gateway_routing WHERE database='postgres'").Scan(&mode))
	require.Equal(t, int32(pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED), mode, "async follower can lag a confirmed routing write")
	_, err = lagging.Exec(t.Context(), "SELECT pg_wal_replay_resume()")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		err := lagging.QueryRow(t.Context(), "SELECT destination FROM multigres.gateway_routing WHERE database='postgres'").Scan(&mode)
		return err == nil && mode == int32(pb.RoutingDestination_ROUTING_DESTINATION_MANAGED)
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, syncReplica.QueryRow(t.Context(), "SELECT destination FROM multigres.gateway_routing WHERE database='postgres'").Scan(&mode))
	require.Equal(t, int32(pb.RoutingDestination_ROUTING_DESTINATION_MANAGED), mode)
}

func startCatalogReplica(t *testing.T, dir string, port int, name string) (string, *pgx.Conn) {
	t.Helper()
	var err error
	data := filepath.Join(dir, name)
	out, err := executil.Command(t.Context(), "pg_basebackup", "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-U", "postgres", "-D", data, "-X", "stream").CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.NoError(t, os.WriteFile(filepath.Join(data, "standby.signal"), nil, 0o600))
	info := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres application_name=%s", port, name)
	require.NoError(t, os.WriteFile(filepath.Join(data, "postgresql.auto.conf"), []byte("primary_conninfo='"+info+"'\n"), 0o600))
	replicaPort := postgresPort(t)
	runPostgres(t, data, replicaPort)
	replicaDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres dbname=postgres sslmode=disable", replicaPort)
	var conn *pgx.Conn
	require.Eventually(t, func() bool { conn, err = pgx.Connect(t.Context(), replicaDSN); return err == nil }, 10*time.Second, 20*time.Millisecond)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return replicaDSN, conn
}
