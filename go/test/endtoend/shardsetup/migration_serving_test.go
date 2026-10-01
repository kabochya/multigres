//go:build migration_demo

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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
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
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// This uses actual catalog transactions, health streams, gateways, and source
// drain RPCs. The only simulated component is the replication controller.
func TestMigrationServingCoexistence(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built migration_demo binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	dir := t.TempDir()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	keyPath := filepath.Join(dir, "migration-key")
	tokenPath := filepath.Join(dir, "serving-token")
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))
	token := sha256.Sum256(key)
	require.NoError(t, os.WriteFile(tokenPath, []byte(hex.EncodeToString(token[:])), 0o600))
	adminPort := utils.GetFreePort(t)
	s, cleanup := NewIsolated(t, WithMultipoolerCount(3), WithMultigateway(), WithMultipoolerExtraArgs("--migration-key-file="+keyPath), WithMultigatewayExtraArgs("--serving-control-token-file="+tokenPath, "--pg-admin-port="+strconv.Itoa(adminPort)), WithLogLevel("info"))
	defer cleanup()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for name, instance := range s.Multipoolers {
			log, e := os.ReadFile(instance.Multipooler.LogFile)
			if e == nil {
				if len(log) > 24000 {
					log = log[len(log)-24000:]
				}
				t.Logf("%s pooler log: %s", name, log)
			}
		}
	})
	connect := func(port int, user, password string) *pgx.Conn {
		var c *pgx.Conn
		var err error
		require.Eventually(t, func() bool {
			c, err = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=%s password=%s dbname=postgres sslmode=disable", port, user, password))
			return err == nil
		}, 30*time.Second, 100*time.Millisecond)
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return c
	}
	target := connect(s.PrimaryPgctld(t).PgPort, "postgres", TestPostgresPassword)
	admin := connect(adminPort, "postgres", TestPostgresPassword)
	// Explicit no-migration state remains usable through the normal gateway.
	var mode string
	require.NoError(t, admin.QueryRow(ctx, "SHOW SERVING MODE").Scan(&mode, new(string), new(string)))
	require.Equal(t, "MIGRATION_MODE_UNSET", mode)
	_, err = target.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD 'alice-demo-password'; CREATE TABLE migration_writes(id TEXT PRIMARY KEY); CREATE TABLE routing_marker(value TEXT); INSERT INTO routing_marker VALUES('managed'); GRANT SELECT,INSERT ON migration_writes TO alice; GRANT SELECT ON routing_marker TO alice")
	require.NoError(t, err)
	var verifier string
	require.NoError(t, target.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='alice'").Scan(&verifier))

	sourcePort := utils.GetFreePort(t)
	sourceDir := filepath.Join(dir, "source-pg")
	pwPath := filepath.Join(dir, "source-password")
	require.NoError(t, os.WriteFile(pwPath, []byte("source-demo-password"), 0o600))
	init := executil.Command(ctx, "initdb", "-D", sourceDir, "-U", "postgres", "--pwfile", pwPath, "--auth-host=scram-sha-256", "--auth-local=trust")
	output, err := init.CombinedOutput()
	require.NoError(t, err, "%s", output)
	// macOS test directories exceed PostgreSQL's Unix socket path limit.
	startUnmanagedTestProcess(t, "postgres", "-D", sourceDir, "-h", "127.0.0.1", "-p", strconv.Itoa(sourcePort), "-k", "/tmp")
	source := connect(sourcePort, "postgres", "source-demo-password")
	_, err = source.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD '"+verifier+"'; CREATE TABLE migration_writes(id TEXT PRIMARY KEY); CREATE TABLE routing_marker(value TEXT); INSERT INTO routing_marker VALUES('unmanaged'); GRANT SELECT,INSERT ON migration_writes TO alice; GRANT SELECT ON routing_marker TO alice")
	require.NoError(t, err)
	createSQL := fmt.Sprintf("CREATE CONNECTION source WITH (host='127.0.0.1',port='%d',database='postgres',username='postgres',password='source-demo-password',sslmode='disable') REQUEST ID 'create-source'", sourcePort)
	_, err = admin.Exec(ctx, createSQL)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, createSQL)
	require.NoError(t, err, "completed retry is idempotent")
	require.NoError(t, s.TopoServer.CreateCell(ctx, "source-remote", &pb.Cell{ServerAddresses: []string{s.EtcdClientAddr}, Root: "/multigres/source-remote"}))
	startSource := func(name, cell string) (int, func()) {
		port := utils.GetFreePort(t)
		stop := startUnmanagedTestProcess(t, "multipooler", "--management-mode=unmanaged", "--source-connection=source", "--migration-key-file="+keyPath, "--database=postgres", "--table-group=default", "--shard=0-inf", "--cell="+cell, "--service-id="+name, "--hostname=127.0.0.1", "--grpc-port="+strconv.Itoa(port), "--http-port="+strconv.Itoa(utils.GetFreePort(t)), "--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global")
		require.Eventually(t, func() bool {
			conn, e := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if e != nil {
				return false
			}
			defer conn.Close()
			readCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			stream, e := rpc.NewMultipoolerServiceClient(conn).StreamPoolerHealth(readCtx, &rpc.StreamPoolerHealthRequest{})
			if e != nil {
				return false
			}
			h, e := stream.Recv()
			return e == nil && h.BackendReady && h.ServingStatus != pb.PoolerServingStatus_SERVING
		}, 30*time.Second, 100*time.Millisecond)
		return port, stop
	}
	localPort, stopLocal := startSource("source-local", s.CellName)
	_, stopRemote := startSource("source-remote", "source-remote")
	_, err = admin.Exec(ctx, "ATTACH CONNECTION source REQUEST ID 'attach-source'")
	require.NoError(t, err)
	app := connect(s.MultigatewayPgPort, "alice", "alice-demo-password")
	marker := func(c *pgx.Conn) string {
		var v string
		require.NoError(t, c.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&v))
		return v
	}
	require.Equal(t, "unmanaged", marker(app))
	_, err = app.Exec(ctx, "INSERT INTO migration_writes VALUES('source-write')")
	require.NoError(t, err)
	var count int
	require.NoError(t, source.QueryRow(ctx, "SELECT count(*) FROM migration_writes").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, target.QueryRow(ctx, "SELECT count(*) FROM migration_writes").Scan(&count))
	require.Zero(t, count)
	_, err = app.Exec(ctx, "SHOW SERVING MODE")
	require.Error(t, err, "application role is not a target administrator")
	// Warm routers retain the accepted source destination while target catalog
	// authority is unavailable. A cold gateway must not accept cached health mode.
	var resumes []func()
	for name := range s.Multipoolers {
		resumes = append(resumes, s.StopPostgres(t, name, "fast"))
	}
	require.Eventually(t, func() bool {
		c, e := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", s.PrimaryMultipooler(t).GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			return false
		}
		defer c.Close()
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		stream, e := rpc.NewMultipoolerServiceClient(c).StreamPoolerHealth(readCtx, &rpc.StreamPoolerHealthRequest{})
		if e != nil {
			return false
		}
		h, e := stream.Recv()
		return e == nil && h.MigrationRouting == nil
	}, 10*time.Second, 100*time.Millisecond)
	require.Equal(t, "unmanaged", marker(app))
	coldPort := utils.GetFreePort(t)
	stopCold := startUnmanagedTestProcess(t, "multigateway", "--pg-port="+strconv.Itoa(coldPort), "--grpc-port="+strconv.Itoa(utils.GetFreePort(t)), "--http-port="+strconv.Itoa(utils.GetFreePort(t)), "--cell="+s.CellName, "--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global")
	require.Eventually(t, func() bool {
		c, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", coldPort), time.Second)
		if e != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond)
	coldCtx, coldCancel := context.WithTimeout(ctx, time.Second)
	coldConn, coldErr := pgx.Connect(coldCtx, fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=alice-demo-password dbname=postgres sslmode=disable", coldPort))
	coldCancel()
	if coldConn != nil {
		_ = coldConn.Close(ctx)
	}
	require.Error(t, coldErr, "cold gateway cannot bootstrap routing from unavailable authority")
	stopCold()
	for _, resume := range resumes {
		resume()
	}
	for _, instance := range s.Multipoolers {
		c, e := NewPgctldClient(instance.Pgctld.GrpcPort)
		require.NoError(t, e)
		_, e = c.Start(ctx, &pgctldpb.StartRequest{})
		require.NoError(t, e)
		_ = c.Close()
	}
	recovery, recoveryCleanup := s.CreateMultiorchInstance(t, "outage-recovery", []string{"postgres/default/0-inf"}, &SetupConfig{CellName: s.CellName, LogLevel: "info"})
	require.NoError(t, recovery.Start(ctx, t))
	require.Eventually(t, func() bool { _, ok := s.TryFindPrimary(t); return ok }, 30*time.Second, 100*time.Millisecond)
	defer recoveryCleanup()
	require.Eventually(t, func() bool {
		return admin.QueryRow(ctx, "SHOW SERVING MODE").Scan(&mode, new(string), new(string)) == nil
	}, 30*time.Second, 100*time.Millisecond)
	// Status can briefly report the recovered old physical primary before its
	// demotion. Select the highest confirmed authority, as production clients do.
	require.Eventually(t, func() bool {
		poolers, e := migrationcontrol.Poolers(ctx, s.TopoServer, "postgres")
		if e != nil {
			return false
		}
		authority, e := migrationcontrol.Authority(poolers)
		if e != nil {
			return false
		}
		conn, e := migrationcontrol.Dial(authority, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			return false
		}
		defer conn.Close()
		reply, e := rpc.NewMultipoolerServiceClient(conn).GetMigrationMode(ctx, &rpc.GetMigrationModeRequest{Database: "postgres"})
		if e != nil || reply.Routing.Mode != pb.MigrationMode_MIGRATION_MODE_UNMANAGED {
			return false
		}
		s.PrimaryName = authority.Id.Name
		return true
	}, 30*time.Second, 100*time.Millisecond)
	fenceIDs := map[string]string{}
	controller := func(operation, id string) error {
		c, e := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", s.PrimaryMultipooler(t).GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			return e
		}
		defer c.Close()
		client := rpc.NewMultipoolerServiceClient(c)
		controlCtx, controlCancel := context.WithTimeout(ctx, 5*time.Second)
		defer controlCancel()
		if operation == "wait-timeout" {
			var cancel context.CancelFunc
			controlCtx, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			operation = "wait"
		}
		protected := migrationcontrol.AuthorizedContext(controlCtx, key)
		creds, e := client.GetAuthCredentials(protected, &rpc.GetAuthCredentialsRequest{Database: "postgres", Username: "postgres", ServingAdmin: true})
		if e != nil {
			return e
		}
		hash, e := scram.ParseScramSHA256Hash(creds.ScramHash)
		if e != nil {
			return e
		}
		salted := scram.ComputeSaltedPassword(TestPostgresPassword, hash.Salt, hash.Iterations)
		fenceID := fenceIDs[id]
		if (operation == "demo-managed" || operation == "demo-unmanaged") && fenceID == "" {
			state, err := client.GetMigrationMode(protected, &rpc.GetMigrationModeRequest{Database: "postgres"})
			if err != nil {
				return err
			}
			fenceID = state.Routing.ActiveRequestId
			fenceIDs[id] = fenceID
		}
		response, e := client.ServingControl(protected, &rpc.ServingControlRequest{Database: "postgres", Username: "postgres", Operation: operation, RequestId: id, ConnectionName: fenceID, UserAuth: &query.UserAuth{ClientKey: scram.ComputeClientKey(salted), ServerKey: scram.ComputeServerKey(salted)}})
		if e == nil && (operation == "status" || operation == "wait") {
			require.NotNil(t, response.OperationStatus)
			require.Equal(t, id, response.OperationStatus.RequestId)
			if operation == "wait" {
				require.True(t, response.OperationStatus.Completed)
			}
			if operation == "status" && id == "pause-source" {
				require.False(t, response.OperationStatus.Completed)
			}
		}
		return e
	}
	// New traffic buffers while a reserved transaction finishes on its owner.
	tx, err := app.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO migration_writes VALUES('reserved-write')")
	require.NoError(t, err)
	pause := make(chan error, 1)
	go func() { _, e := admin.Exec(ctx, "PAUSE SERVING REQUEST ID 'pause-source'"); pause <- e }()
	require.Eventually(t, func() bool {
		c, e := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", s.PrimaryMultipooler(t).GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			return false
		}
		defer c.Close()
		select {
		case err := <-pause:
			require.NoError(t, err, "pause failed before intent publication")
			t.Fatal("pause completed before reserved work drained")
		default:
		}
		r, e := rpc.NewMultipoolerServiceClient(c).GetMigrationMode(ctx, &rpc.GetMigrationModeRequest{Database: "postgres"})
		return e == nil && r.Routing.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, controller("status", "pause-source"), "routing FENCED precedes completed drain")
	require.Error(t, controller("wait-timeout", "pause-source"), "wait timeout must not undo the fence")
	require.NoError(t, controller("status", "pause-source"))
	// Election while the source reservation prevents drain completion: the new
	// authority must recover FENCED intent and the same incomplete operation.
	oldLeader := s.PrimaryName
	resumeOld := s.StopPostgres(t, oldLeader, "fast")
	defer resumeOld()
	newLeader := WaitForNewPrimary(t, s, oldLeader, 60*time.Second)
	require.NotEqual(t, oldLeader, newLeader)
	s.PrimaryName = newLeader
	var statusErr error
	require.Eventually(t, func() bool { statusErr = controller("status", "pause-source"); return statusErr == nil }, 30*time.Second, 100*time.Millisecond)
	require.Error(t, controller("demo-managed", "bypass-incomplete-fence"))
	require.NoError(t, tx.Commit(ctx))
	// The old request may finish its source RPC but cannot commit under lost authority.
	require.Error(t, <-pause)
	resumeOld()
	require.Eventually(t, func() bool {
		_, e := admin.Exec(ctx, "PAUSE SERVING REQUEST ID 'pause-source'")
		return e == nil
	}, 60*time.Second, 200*time.Millisecond)
	target = connect(s.PrimaryPgctld(t).PgPort, "postgres", TestPostgresPassword)
	require.NoError(t, controller("wait", "pause-source"))
	// Admin reconnect/SHOW work during fencing; app auth is bounded by deadline.
	fencedAdmin := connect(adminPort, "postgres", TestPostgresPassword)
	require.NoError(t, fencedAdmin.QueryRow(ctx, "SHOW SERVING MODE").Scan(&mode, new(string), new(string)))
	require.Equal(t, "MIGRATION_MODE_FENCED", mode)
	deadline, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = pgx.Connect(deadline, fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=alice-demo-password dbname=postgres sslmode=disable", s.MultigatewayPgPort))
	cancel()
	require.Error(t, err)
	require.Eventually(t, func() bool { _, err = admin.Exec(ctx, "RESUME SERVING REQUEST ID 'resume-source'"); return err == nil }, 30*time.Second, 100*time.Millisecond, "same-operation retry waits for every recovering target to read its catalog")
	require.Eventually(t, func() bool {
		var v string
		e := app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&v)
		return e == nil && v == "unmanaged"
	}, 15*time.Second, 100*time.Millisecond)

	// A rollback must remove BOTH the mode and simulated journal decision.
	require.Error(t, controller("demo-rollback", "rolled-back-decision"))
	require.NoError(t, admin.QueryRow(ctx, "SHOW SERVING MODE").Scan(&mode, new(string), new(string)))
	require.Equal(t, "MIGRATION_MODE_UNMANAGED", mode)
	// The failed first journal transaction creates no table at all.
	var journalExists bool
	require.NoError(t, target.QueryRow(ctx, "SELECT to_regclass('multigres.demo_controller_journal') IS NOT NULL").Scan(&journalExists))
	require.False(t, journalExists)
	require.NoError(t, controller("demo-fence", "controller-fence"))
	// A joining source observes committed FENCED intent before it can admit work.
	_, stopJoining := startSource("source-joining-fenced", "source-remote")
	defer stopJoining()
	_, err = admin.Exec(ctx, "ATTACH CONNECTION source REQUEST ID 'steal-controller-fence'")
	require.Error(t, err)
	require.NoError(t, controller("demo-fence", "controller-fence"), "lost response retry recovers the completed fence")
	// Restart only the pooler; PostgreSQL and its WAL/consensus tables survive.
	restarting := s.PrimaryMultipooler(t)
	killCtx, stopKill := context.WithTimeout(ctx, 5*time.Second)
	_, killed := restarting.Process.Kill(killCtx)
	stopKill()
	require.True(t, killed)
	require.NoError(t, target.Ping(ctx), "pooler-only crash leaves PostgreSQL running")
	require.NoError(t, restarting.Start(ctx, t))
	require.Eventually(t, func() bool { return controller("status", "controller-fence") == nil }, 60*time.Second, 100*time.Millisecond)
	require.NoError(t, controller("demo-fence", "controller-fence"))
	require.NoError(t, admin.QueryRow(ctx, "SHOW SERVING MODE").Scan(&mode, new(string), new(string)))
	require.Equal(t, "MIGRATION_MODE_FENCED", mode)

	_, err = admin.Exec(ctx, "RESUME SERVING REQUEST ID 'illegal-admin-resume'")
	require.Error(t, err)
	require.Eventually(t, func() bool { return controller("demo-managed", "controller-managed") == nil }, 30*time.Second, 100*time.Millisecond, "same-ID activation retry waits for recovering target readiness")
	require.NoError(t, controller("demo-managed", "controller-managed"), "activation retry retains its original fence precondition")
	require.Eventually(t, func() bool {
		var v string
		e := app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&v)
		return e == nil && v == "managed"
	}, 15*time.Second, 100*time.Millisecond)
	_, err = app.Exec(ctx, "INSERT INTO migration_writes VALUES('target-write')")
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "DETACH CONNECTION source REQUEST ID 'too-early-detach'")
	require.Error(t, err)
	require.NoError(t, controller("demo-fence", "reverse-fence"))
	require.NoError(t, controller("demo-unmanaged", "reverse-source"))
	require.Eventually(t, func() bool {
		var v string
		e := app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&v)
		return e == nil && v == "unmanaged"
	}, 15*time.Second, 100*time.Millisecond)
	// Source endpoint failover preserves the destination; no consensus election.
	stopLocal()
	require.Eventually(t, func() bool {
		var v string
		e := app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&v)
		return e == nil && v == "unmanaged"
	}, 15*time.Second, 100*time.Millisecond)
	require.NoError(t, controller("demo-fence", "final-fence"))
	require.NoError(t, controller("demo-managed", "final-managed"))
	require.NoError(t, controller("demo-complete", "migration-complete"))
	_, err = admin.Exec(ctx, "DETACH CONNECTION source REQUEST ID 'completed-detach'")
	require.NoError(t, err)
	stopRemote()
	stopJoining()
	require.NoError(t, source.Ping(ctx), "decommission leaves external database alive")
	// Source is stock PostgreSQL and remains free of Multigres sidecars.
	var sidecars int
	require.NoError(t, source.QueryRow(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname='multigres'").Scan(&sidecars))
	require.Zero(t, sidecars)
	// Persisted credentials are authenticated ciphertext, not plaintext.
	var encrypted string
	require.NoError(t, target.QueryRow(ctx, "SELECT configuration FROM multigres.connections WHERE name='source'").Scan(&encrypted))
	require.NotContains(t, encrypted, "source-demo-password")
	t.Logf("Verified source preparation, two cells, attach, writes, reserved drain, fenced auth, resume, journal rollback, forward/reverse controller hooks, completion and decommission. Source RPC port: %d", localPort)
	if holdDir := os.Getenv("MULTIGRES_MIGRATION_DEMO_DIR"); holdDir != "" {
		require.NoError(t, os.MkdirAll(holdDir, 0o700))
		// Restore source serving for manual inspection; controllers remain absent.
		startSource("source-local-demo", s.CellName)
		startSource("source-remote-demo", "source-remote")
		_, err = admin.Exec(ctx, "ATTACH CONNECTION source REQUEST ID 'manual-demo-attach'")
		require.NoError(t, err)
		pgpass := fmt.Sprintf("127.0.0.1:%d:postgres:alice:alice-demo-password\n127.0.0.1:%d:postgres:postgres:%s\n127.0.0.1:%d:postgres:postgres:%s\n127.0.0.1:%d:postgres:postgres:source-demo-password\n", s.MultigatewayPgPort, adminPort, TestPostgresPassword, s.PrimaryPgctld(t).PgPort, TestPostgresPassword, sourcePort)
		require.NoError(t, os.WriteFile(filepath.Join(holdDir, "pgpass"), []byte(pgpass), 0o600))
		report := map[string]any{"gateway_port": s.MultigatewayPgPort, "admin_port": adminPort, "source_port": sourcePort, "target_pg_port": s.PrimaryPgctld(t).PgPort, "target_rpc_port": s.PrimaryMultipooler(t).GrpcPort, "etcd": s.EtcdClientAddr, "key_file": keyPath, "token_file": tokenPath, "cluster_dir": s.TempDir, "pgpass_file": filepath.Join(holdDir, "pgpass"), "source_dir": sourceDir, "mode": "UNMANAGED", "stop_file": filepath.Join(holdDir, "stop")}
		data, e := json.MarshalIndent(report, "", "  ")
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(holdDir, "endpoints.json"), data, 0o600))
		t.Logf("Local demo ready: %s", filepath.Join(holdDir, "endpoints.json"))
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if _, e := os.Stat(filepath.Join(holdDir, "stop")); e == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}
