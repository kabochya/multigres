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
	"crypto/rand"
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

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestCatalogSourceBootstrap exercises actual managed authority transactions,
// protected provisioning/bootstrap and external identity/readiness. It does not
// test routing cutover or claim exclusive application admission.
func TestCatalogSourceBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	dir := t.TempDir()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	keyPath := filepath.Join(dir, "migration-key")
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))
	s, cleanup := NewIsolated(t, WithMultipoolerCount(3), WithMultigateway(), WithMultipoolerExtraArgs("--migration-key-file="+keyPath))
	defer cleanup()
	target, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable", s.PrimaryPgctld(t).PgPort, TestPostgresPassword))
	require.NoError(t, err)
	defer target.Close(context.Background())
	var verifier string
	require.NoError(t, target.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='postgres'").Scan(&verifier))
	hash, err := scram.ParseScramSHA256Hash(verifier)
	require.NoError(t, err)
	salted := scram.ComputeSaltedPassword(TestPostgresPassword, hash.Salt, hash.Iterations)
	authority, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", s.PrimaryMultipooler(t).GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer authority.Close()
	client := rpc.NewMultipoolerServiceClient(authority)
	protected := migrationcontrol.AuthorizedContext(ctx, key)
	sourcePort := utils.GetFreePort(t)
	data := filepath.Join(dir, "source")
	pw := filepath.Join(dir, "source-password")
	require.NoError(t, os.WriteFile(pw, []byte("source-test-password"), 0o600))
	out, err := executil.Command(ctx, "initdb", "-D", data, "-U", "postgres", "--pwfile", pw, "--auth-host=scram-sha-256", "--auth-local=trust").CombinedOutput()
	require.NoError(t, err, "%s", out)
	startUnmanagedTestProcess(t, "postgres", "-D", data, "-h", "127.0.0.1", "-p", strconv.Itoa(sourcePort), "-k", "/tmp")
	var source *pgx.Conn
	require.Eventually(t, func() bool {
		source, err = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=source-test-password dbname=postgres sslmode=disable", sourcePort))
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)
	defer source.Close(context.Background())
	var sysid string
	require.NoError(t, source.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&sysid))
	configuration := &rpc.SourceConnection{Name: "source", Host: "127.0.0.1", Port: uint32(sourcePort), Database: "postgres", Username: "postgres", Password: "source-test-password", SslMode: "disable", ExpectedSystemIdentifier: sysid}
	request := &rpc.CreateSourceConnectionRequest{Database: "postgres", Username: "postgres", UserAuth: &query.UserAuth{ClientKey: scram.ComputeClientKey(salted), ServerKey: scram.ComputeServerKey(salted)}, Connection: configuration}
	_, err = client.CreateSourceConnection(ctx, request)
	require.Error(t, err, "control proof required")
	created, err := client.CreateSourceConnection(protected, request)
	require.NoError(t, err)
	require.NotEmpty(t, created.ConfigurationBinding)
	retry, err := client.CreateSourceConnection(protected, request)
	require.NoError(t, err)
	require.Equal(t, created.ConfigurationBinding, retry.ConfigurationBinding)
	configuration.Password = "unsupported-rotation"
	_, err = client.CreateSourceConnection(protected, request)
	require.Error(t, err)
	configuration.Password = "source-test-password"
	sourceGrpc := utils.GetFreePort(t)
	stopSource := startUnmanagedTestProcess(t, "multipooler", "--management-mode=unmanaged", "--source-connection=source", "--migration-key-file="+keyPath, "--database=postgres", "--table-group=default", "--shard=0-inf", "--cell="+s.CellName, "--service-id=bootstrap-source", "--hostname=127.0.0.1", "--grpc-port="+strconv.Itoa(sourceGrpc), "--http-port="+strconv.Itoa(utils.GetFreePort(t)), "--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global")
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", sourceGrpc), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	require.Eventually(t, func() bool {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		stream, err := rpc.NewMultipoolerServiceClient(conn).StreamPoolerHealth(readCtx, &rpc.StreamPoolerHealthRequest{})
		if err != nil {
			return false
		}
		h, err := stream.Recv()
		return err == nil && h.ServingStatus == pb.PoolerServingStatus_SERVING && h.RoutingState.GetRole() == pb.RoutingRole_ROUTING_ROLE_PRIMARY && h.RoutingState.GetRule() == nil
	}, 30*time.Second, 100*time.Millisecond)
	id := &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: s.CellName, Name: "bootstrap-source"}
	record, err := s.TopoServer.GetMultipooler(ctx, id)
	require.NoError(t, err)
	require.Equal(t, created.ConfigurationBinding, record.SourceConfigurationBinding)
	_, err = target.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD 'alice-routing-test'; CREATE TABLE routing_marker(value TEXT); INSERT INTO routing_marker VALUES('managed'); GRANT SELECT ON routing_marker TO alice")
	require.NoError(t, err)
	var aliceVerifier string
	require.NoError(t, target.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='alice'").Scan(&aliceVerifier))
	_, err = source.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD '"+aliceVerifier+"'; CREATE TABLE routing_marker(value TEXT); INSERT INTO routing_marker VALUES('source'); GRANT SELECT ON routing_marker TO alice")
	require.NoError(t, err)
	setPolicy := func(policy *pb.GatewayRoutingPolicy) {
		_, err := client.SetRoutingPolicy(protected, &rpc.SetRoutingPolicyRequest{Database: "postgres", Username: "postgres", UserAuth: request.UserAuth, Policy: policy})
		require.NoError(t, err)
	}
	sourcePolicy := &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_SOURCE, SourceConnection: "source", SourceConfigurationBinding: created.ConfigurationBinding, SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: sysid, Database: "postgres"}}
	setPolicy(sourcePolicy)
	var app *pgx.Conn
	require.Eventually(t, func() bool {
		app, err = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=alice-routing-test dbname=postgres sslmode=disable", s.MultigatewayPgPort))
		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
	defer app.Close(context.Background())
	marker := func(expected string) {
		require.Eventually(t, func() bool {
			var marker string
			err := app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&marker)
			return err == nil && marker == expected
		}, 10*time.Second, 100*time.Millisecond)
	}
	marker("source")
	setPolicy(&pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED})
	require.Eventually(t, func() bool { _, err := app.Exec(ctx, "SELECT value FROM routing_marker"); return err != nil }, 10*time.Second, 100*time.Millisecond)
	// Routing BLOCKED is not completed fencing: source application admission is
	// still open in M1. The independent admission milestone supplies that barrier.
	require.NoError(t, source.Ping(ctx))
	setPolicy(&pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_MANAGED})
	marker("managed")
	setPolicy(sourcePolicy)
	marker("source")

	stopSource()
	require.NoError(t, source.Ping(ctx), "source PostgreSQL remains running after pooler shutdown")
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
