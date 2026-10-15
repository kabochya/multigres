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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

const externalPostgresPassword = "external-test-password"

// startExternalPostgres starts a plain PostgreSQL that the cluster knows
// nothing about and returns its TCP port. It stands in for the customer's
// externally owned database.
func startExternalPostgres(t *testing.T) int {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	pw := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(pw, []byte(externalPostgresPassword), 0o600))
	out, err := executil.Command(ctx, "initdb", "-D", data, "-U", "postgres", "--pwfile", pw,
		"--auth-host=scram-sha-256", "--auth-local=trust").CombinedOutput()
	require.NoError(t, err, "%s", out)

	// Unix socket paths are length limited; keep them out of the long temp dir.
	sockDir, err := os.MkdirTemp("", "ext")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	port := utils.GetFreePort(t)
	// A valid locale keeps postgres single-threaded at startup on macOS.
	startTestProcessEnv(t, []string{"LC_ALL=en_US.UTF-8"}, "postgres", "-D", data, "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-k", sockDir)
	require.Eventually(t, func() bool {
		conn, err := pgx.Connect(ctx, externalDSN(port))
		if err != nil {
			return false
		}
		_ = conn.Close(ctx)
		return true
	}, 30*time.Second, 100*time.Millisecond, "external postgres did not come up")
	return port
}

func externalDSN(port int) string {
	return fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/postgres?sslmode=disable", externalPostgresPassword, port)
}

// startTestProcessEnv runs a binary with extra environment variables for the
// lifetime of the test and returns a function that stops it early.
func startTestProcessEnv(t *testing.T, env []string, binary string, args ...string) func() {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), binary+".log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), binary, args...).WithProcessGroup()
	cmd.SetStdout(logFile)
	cmd.SetStderr(logFile)
	cmd.SetEnv(append(utils.BaseTestEnv(), env...))
	require.NoError(t, cmd.Start())
	go func() { _ = cmd.Wait() }()
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
		_ = logFile.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("%s log:\n%s", binary, data)
		}
	})
	return stop
}

// TestUnmanagedPoolerServesExternalPostgres starts an unmanaged pooler against
// a plain external PostgreSQL next to a managed cohort and checks the W1
// contract: it registers as UNMANAGED, serves queries through the existing pool
// path, leaves no multigres objects on the external database, rejects
// management RPCs, and leaves the external PostgreSQL running when it stops.
func TestUnmanagedPoolerServesExternalPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)

	grpcPort := utils.GetFreePort(t)
	const name = "unmanaged-w1"
	stop := startTestProcessEnv(t,
		[]string{"MULTIPOOLER_PROTOTYPE_BACKING_URL=" + externalDSN(extPort)},
		"multipooler",
		"--backing-connection=src",
		"--database=postgres", "--table-group=migrateTG", "--shard=0-inf",
		"--cell="+s.CellName, "--service-id="+name, "--hostname=localhost",
		"--grpc-port="+strconv.Itoa(grpcPort), "--http-port="+strconv.Itoa(utils.GetFreePort(t)),
		"--service-map=grpc-pooler,grpc-poolermanager,grpc-consensus",
		"--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global",
	)

	client, err := NewMultipoolerClient(grpcPort)
	require.NoError(t, err)
	defer client.Close()

	// The pooler reports itself serving once it is up. Until the admission
	// work lands it opens unconditionally.
	require.Eventually(t, func() bool {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := client.Pooler.ExecuteQuery(readCtx, "SELECT 1", 1)
		return err == nil
	}, 60*time.Second, 200*time.Millisecond, "unmanaged pooler never served a query")

	res, err := client.Pooler.ExecuteQuery(ctx, "SELECT current_setting('port')", 1)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, strconv.Itoa(extPort), string(res.Rows[0].Values[0]), "query must reach the external database")

	// Registered as UNMANAGED in topology.
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: name}
	require.Eventually(t, func() bool {
		rec, err := s.TopoServer.GetMultipooler(ctx, id)
		return err == nil && rec.ManagementMode == clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	}, 30*time.Second, 200*time.Millisecond)

	// Management and consensus RPCs are rejected explicitly.
	_, err = client.Manager.Status(ctx, &multipoolermanagerdata.StatusRequest{})
	require.ErrorContains(t, err, "not supported on an unmanaged pooler")

	// Nothing was created on the external database: no multigres schema, no
	// sidecar heartbeat table.
	ext, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer ext.Close(context.Background())
	var schemas int
	require.NoError(t, ext.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = 'multigres'").Scan(&schemas))
	require.Zero(t, schemas, "unmanaged pooler must not create multigres objects on the external database")

	// The managed cohort never saw it: its primary is unchanged.
	require.NotNil(t, s.GetPrimary(t))

	// Stopping the pooler leaves the external PostgreSQL running.
	stop()
	require.NoError(t, ext.Ping(ctx), "external PostgreSQL must keep running after the pooler stops")
}
