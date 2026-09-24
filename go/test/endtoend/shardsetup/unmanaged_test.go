// Copyright 2025 Supabase, Inc.
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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolermanager"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestUnmanagedStandalone deliberately starts no pgctld or multiorch, and uses
// stock PostgreSQL without Multigres extensions or sidecar tables.
func TestUnmanagedStandalone(t *testing.T) {
	if testing.Short() {
		t.Skip("requires PostgreSQL, etcd, and built binaries")
	}
	ctx := t.Context()
	dir := t.TempDir()
	pgPort := utils.GetFreePort(t)
	pgDir := filepath.Join(dir, "pg")
	pwFile := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(pwFile, []byte("prototype-test-password\n"), 0o600))
	init := executil.Command(ctx, "initdb", "-D", pgDir, "-U", "postgres", "--pwfile", pwFile, "--auth-host=scram-sha-256", "--auth-local=trust")
	output, err := init.CombinedOutput()
	require.NoError(t, err, "%s", output)
	startUnmanagedTestProcess(t, "postgres", "-D", pgDir, "-h", "127.0.0.1", "-p", strconv.Itoa(pgPort), "-k", dir)
	sourceDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=prototype-test-password dbname=postgres sslmode=disable", pgPort)
	var source *pgx.Conn
	require.Eventually(t, func() bool { source, err = pgx.Connect(ctx, sourceDSN); return err == nil }, 10*time.Second, 100*time.Millisecond)
	defer source.Close(context.Background())
	// The pooler's catalog reader is not a superuser. Client passwords stay in PG.
	_, err = source.Exec(ctx, "CREATE ROLE credential_reader LOGIN PASSWORD 'reader-password'; GRANT SELECT ON pg_authid TO credential_reader; CREATE ROLE alice LOGIN PASSWORD 'alice-password'; CREATE TABLE unmanaged_writes (id int PRIMARY KEY); GRANT SELECT, INSERT ON unmanaged_writes TO alice")
	require.NoError(t, err)
	readerFile := filepath.Join(dir, "reader-password")
	require.NoError(t, os.WriteFile(readerFile, []byte("reader-password"), 0o600))
	runningCtx := t.Context()
	etcdDir := filepath.Join(dir, "etcd")
	require.NoError(t, os.MkdirAll(etcdDir, 0o700))
	etcdAddr, etcdCmd, err := startEtcd(runningCtx, t, etcdDir)
	require.NoError(t, err)
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = etcdCmd.Stop(stopCtx)
	}()
	ts, err := topoclient.OpenServer(topoclient.DefaultTopoImplementation, "/multigres/global", []string{etcdAddr}, topoclient.NewDefaultTopoConfig())
	require.NoError(t, err)
	defer ts.Close()
	require.NoError(t, ts.CreateCell(ctx, "test-cell", &pb.Cell{ServerAddresses: []string{etcdAddr}, Root: "/multigres/test-cell"}))
	require.NoError(t, ts.CreateDatabase(ctx, "postgres", &pb.Database{}))
	grpcPort := utils.GetFreePort(t)
	stopPooler := startUnmanagedTestProcess(t, "multipooler",
		"--management-mode=unmanaged", "--backend-host=127.0.0.1", "--pg-port", strconv.Itoa(pgPort),
		"--database=postgres", "--table-group=default", "--shard=0-inf", "--cell=test-cell", "--service-id=external",
		"--hostname=127.0.0.1", "--grpc-port", strconv.Itoa(grpcPort), "--http-port", strconv.Itoa(utils.GetFreePort(t)),
		"--topo-global-server-addresses", etcdAddr, "--topo-global-root=/multigres/global",
		"--connpool-admin-user=credential_reader", "--connpool-admin-password-file", readerFile,
		"--pg-client-sslmode=disable", "--service-map=grpc-pooler,grpc-poolermanager,grpc-consensus,grpc-backup")
	id := &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "test-cell", Name: "external"}
	require.Eventually(t, func() bool {
		mp, e := ts.GetMultipooler(ctx, id)
		return e == nil && mp.ServingStatus == pb.PoolerServingStatus_SERVING && mp.Type == pb.PoolerType_PRIMARY
	}, 20*time.Second, 100*time.Millisecond)
	// Even explicitly requesting management services must not expose them.
	rpc, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer rpc.Close()
	_, err = multipoolermanager.NewMultipoolerManagerClient(rpc).Status(ctx, &multipoolermanagerdata.StatusRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	gatewayPort := utils.GetFreePort(t)
	startUnmanagedTestProcess(t, "multigateway", "--cell=test-cell", "--service-id=gateway", "--pg-port", strconv.Itoa(gatewayPort),
		"--pg-bind-address=127.0.0.1", "--hostname=127.0.0.1", "--grpc-port", strconv.Itoa(utils.GetFreePort(t)), "--http-port", strconv.Itoa(utils.GetFreePort(t)),
		"--topo-global-server-addresses", etcdAddr, "--topo-global-root=/multigres/global")
	gatewayDSN := fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=alice-password dbname=postgres sslmode=disable", gatewayPort)
	var client *pgx.Conn
	require.Eventually(t, func() bool { client, err = pgx.Connect(ctx, gatewayDSN); return err == nil }, 20*time.Second, 100*time.Millisecond)
	defer client.Close(context.Background())
	_, err = client.Exec(ctx, "INSERT INTO unmanaged_writes VALUES (1)")
	require.NoError(t, err)
	var count int
	require.NoError(t, source.QueryRow(ctx, "SELECT count(*) FROM unmanaged_writes").Scan(&count))
	require.Equal(t, 1, count)
	_, err = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=wrong dbname=postgres sslmode=disable", gatewayPort))
	require.Error(t, err)
	var sidecars int
	require.NoError(t, source.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = 'multigres'").Scan(&sidecars))
	require.Zero(t, sidecars)
	// A source made read-only must withdraw write routing, then recover.
	_, err = source.Exec(ctx, "ALTER SYSTEM SET default_transaction_read_only = on")
	require.NoError(t, err)
	_, err = source.Exec(ctx, "SELECT pg_reload_conf()")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mp, e := ts.GetMultipooler(ctx, id)
		return e == nil && mp.ServingStatus == pb.PoolerServingStatus_DISABLED
	}, 15*time.Second, 100*time.Millisecond)
	_, err = source.Exec(ctx, "SET default_transaction_read_only = off")
	require.NoError(t, err)
	_, err = source.Exec(ctx, "ALTER SYSTEM RESET default_transaction_read_only")
	require.NoError(t, err)
	_, err = source.Exec(ctx, "SELECT pg_reload_conf()")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mp, e := ts.GetMultipooler(ctx, id)
		return e == nil && mp.ServingStatus == pb.PoolerServingStatus_SERVING
	}, 15*time.Second, 100*time.Millisecond)
	_, err = client.Exec(ctx, "INSERT INTO unmanaged_writes VALUES (2)")
	require.NoError(t, err)
	stopPooler()
	require.NoError(t, source.Ping(ctx), "decommissioning must leave external PostgreSQL running")
	require.Eventually(t, func() bool {
		mp, e := ts.GetMultipooler(ctx, id)
		return e == nil && mp.ServingStatus != pb.PoolerServingStatus_SERVING
	}, 10*time.Second, 100*time.Millisecond)
}

func startUnmanagedTestProcess(t *testing.T, binary string, args ...string) func() {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), binary+".log")
	log, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), binary, args...).WithProcessGroup()
	cmd.SetStdout(log)
	cmd.SetStderr(log)
	require.NoError(t, startAndReap(cmd))
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = cmd.Stop(ctx)
	}
	t.Cleanup(func() {
		stop()
		_ = log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("%s log:\n%s", binary, data)
		}
	})
	return stop
}
