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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/protometadata"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/testpoll"
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
// lifetime of the test. It returns a function that stops the process early and a
// channel that closes when the process has exited.
func startTestProcessEnv(t *testing.T, env []string, binary string, args ...string) (func(), <-chan struct{}) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), binary+".log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), binary, args...).WithProcessGroup()
	cmd.SetStdout(logFile)
	cmd.SetStderr(logFile)
	cmd.SetEnv(append(utils.BaseTestEnv(), env...))
	require.NoError(t, cmd.Start())
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
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
	return stop, exited
}

// externalSystemIdentifier reads the external database's system identifier.
func externalSystemIdentifier(t *testing.T, port int) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, externalDSN(port))
	require.NoError(t, err)
	defer conn.Close(ctx)
	var sysID string
	require.NoError(t, conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&sysID))
	return sysID
}

// seedBackingConnection inserts a connection row on the managed default
// primary, creating the prototype tables first.
func seedBackingConnection(t *testing.T, s *ShardSetup, name, url, expectedSystemIdentifier string) {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		s.PrimaryPgctld(t).PgPort, TestPostgresPassword))
	require.NoError(t, err)
	defer conn.Close(context.Background())
	require.NoError(t, protometadata.EnsureSchema(ctx, func(ctx context.Context, sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}))
	_, err = conn.Exec(ctx, "INSERT INTO multigres.proto_connections (name, dsn, expected_system_identifier) VALUES ($1, $2, $3)",
		name, url, expectedSystemIdentifier)
	require.NoError(t, err)
}

// unmanagedPooler is a running unmanaged multipooler process.
type unmanagedPooler struct {
	name     string
	grpcPort int
	stop     func()
	exited   <-chan struct{}
}

func startUnmanagedPooler(t *testing.T, s *ShardSetup, name, database, connection string) *unmanagedPooler {
	t.Helper()
	grpcPort := utils.GetFreePort(t)
	stop, exited := startTestProcessEnv(t, nil, "multipooler",
		"--backing-connection="+connection,
		"--database="+database, "--table-group=migrateTG", "--shard=0-inf",
		"--cell="+s.CellName, "--service-id="+name, "--hostname=localhost",
		"--grpc-port="+strconv.Itoa(grpcPort), "--http-port="+strconv.Itoa(utils.GetFreePort(t)),
		"--service-map=grpc-pooler,grpc-poolermanager,grpc-consensus",
		"--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global",
	)
	return &unmanagedPooler{name: name, grpcPort: grpcPort, stop: stop, exited: exited}
}

// firstHealth reads one message from the pooler's health stream.
func (p *unmanagedPooler) firstHealth(ctx context.Context) (*multipoolerservicepb.StreamPoolerHealthResponse, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", p.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	stream, err := multipoolerservicepb.NewMultipoolerServiceClient(conn).StreamPoolerHealth(ctx, &multipoolerservicepb.StreamPoolerHealthRequest{})
	if err != nil {
		return nil, err
	}
	return stream.Recv()
}

// TestUnmanagedPoolerServesExternalPostgres starts an unmanaged pooler against
// a plain external PostgreSQL next to a managed cohort. It bootstraps only from
// --backing-connection plus the connection row on the default primary, serves
// queries, registers as UNMANAGED, leaves no multigres objects on the external
// database, rejects management RPCs, and leaves the external PostgreSQL running
// when it stops.
func TestUnmanagedPoolerServesExternalPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, "w2-serves", externalDSN(extPort), externalSystemIdentifier(t, extPort))

	const name = "unmanaged-serves"
	p := startUnmanagedPooler(t, s, name, "postgres", "w2-serves")

	client, err := NewMultipoolerClient(p.grpcPort)
	require.NoError(t, err)
	defer client.Close()

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

	health, err := p.firstHealth(ctx)
	require.NoError(t, err)
	require.True(t, health.GetBackendReady())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_SERVING, health.GetServingStatus())

	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: name}
	require.Eventually(t, func() bool {
		rec, err := s.TopoServer.GetMultipooler(ctx, id)
		return err == nil && rec.ManagementMode == clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED &&
			rec.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING
	}, 30*time.Second, 200*time.Millisecond)

	// Management and consensus RPCs are rejected explicitly.
	_, err = client.Manager.Status(ctx, &multipoolermanagerdata.StatusRequest{})
	require.ErrorContains(t, err, "not supported on an unmanaged pooler")

	// Nothing was created on the external database.
	ext, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer ext.Close(context.Background())
	var schemas int
	require.NoError(t, ext.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = 'multigres'").Scan(&schemas))
	require.Zero(t, schemas, "unmanaged pooler must not create multigres objects on the external database")

	p.stop()
	require.NoError(t, ext.Ping(ctx), "external PostgreSQL must keep running after the pooler stops")
}

// TestUnmanagedPoolerRefusesIdentityMismatch points the connection row at an
// endpoint that is reachable and writable but is not the database the row names.
// The pooler must come up and stay closed.
func TestUnmanagedPoolerRefusesIdentityMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, "w2-mismatch", externalDSN(extPort), "1234567890")

	p := startUnmanagedPooler(t, s, "unmanaged-mismatch", "postgres", "w2-mismatch")

	// The process is up and reports a closed, not-ready backend.
	require.Eventually(t, func() bool {
		health, err := p.firstHealth(ctx)
		return err == nil && health.GetServingStatus() == clustermetadatapb.PoolerServingStatus_DISABLED && !health.GetBackendReady()
	}, 60*time.Second, 200*time.Millisecond, "pooler should start closed")

	// And it never opens.
	testpoll.Never(t, func() bool {
		health, err := p.firstHealth(ctx)
		return err == nil && (health.GetBackendReady() || health.GetServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING)
	}, 5*time.Second, 250*time.Millisecond)

	client, err := NewMultipoolerClient(p.grpcPort)
	require.NoError(t, err)
	defer client.Close()
	_, err = client.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	require.Error(t, err, "a pooler that fails identity validation must reject queries")
}

// TestUnmanagedPoolerStaysClosedWithoutDefaultPrimary starts a pooler for a
// database that has no default primary. Bootstrap cannot read its connection, so
// the process gives up without ever serving, leaving only a DISABLED topology
// entry.
func TestUnmanagedPoolerStaysClosedWithoutDefaultPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, "w2-nodp", externalDSN(extPort), externalSystemIdentifier(t, extPort))

	const name = "unmanaged-nodp"
	p := startUnmanagedPooler(t, s, name, "nodb", "w2-nodp")

	select {
	case <-p.exited:
	case <-time.After(90 * time.Second):
		t.Fatal("pooler without a default primary should give up and exit")
	}

	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: s.CellName, Name: name}
	rec, err := s.TopoServer.GetMultipooler(ctx, id)
	require.NoError(t, err, "it registers before reading metadata")
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, rec.ServingStatus)
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, rec.ManagementMode)
}
