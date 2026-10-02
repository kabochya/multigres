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

package multipooler_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler"
	"github.com/multigres/multigres/go/services/multipooler/testutil/demo"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/pathutil"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// TestDemoPoolerProcess is selected only by the isolated child launcher. The
// production binary contains neither the demo controller nor this pipe protocol.
func TestDemoPoolerProcess(t *testing.T) {
	replyPath := os.Getenv("MULTIGRES_DEMO_REPLY_PATH")
	if replyPath == "" {
		t.Skip("child process helper")
	}
	var args []string
	for n, a := range os.Args {
		if a == "--" {
			args = os.Args[n+1:]
			break
		}
	}
	var keyPath string
	for _, a := range args {
		if value, ok := strings.CutPrefix(a, "--migration-key-file="); ok {
			keyPath = value
		}
	}
	key, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	reply, err := os.OpenFile(replyPath, os.O_WRONLY, 0)
	require.NoError(t, err)
	defer reply.Close()
	mp := multipooler.NewMultipooler(telemetry.NewTelemetry())
	cmd := &cobra.Command{Use: "multipooler", Args: cobra.NoArgs, PreRunE: func(cmd *cobra.Command, _ []string) error { return mp.CobraPreRunE(cmd) }, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := mp.Init(cmd.Context()); err != nil {
			return err
		}
		go demo.RunHarness(cmd.Context(), mp.ManagerForTesting(), mp.TopologyForTesting(), key, os.Stdin, reply)
		return mp.RunDefault()
	}}
	mp.RegisterFlags(cmd.Flags())
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute())
}

type demoDriver struct {
	mu, writeMu sync.Mutex
	next        int
	pending     map[string]chan demo.Reply
	input       *os.File
}

func launchDemoPooler(t *testing.T, drivers *sync.Map) func(context.Context, *shardsetup.ProcessInstance, []string) *executil.Cmd {
	t.Helper()
	return func(ctx context.Context, p *shardsetup.ProcessInstance, args []string) *executil.Cmd {
		exe, err := os.Executable()
		require.NoError(t, err)
		dir := t.TempDir()
		fifo := filepath.Join(dir, "replies")
		require.NoError(t, unix.Mkfifo(fifo, 0o600))
		out, err := os.OpenFile(fifo, os.O_RDWR, 0)
		require.NoError(t, err)
		in, write, err := os.Pipe()
		require.NoError(t, err)
		d := &demoDriver{pending: map[string]chan demo.Reply{}, input: write}
		drivers.Store(p.Name, d)
		go func() {
			decoder := json.NewDecoder(out)
			for {
				var r demo.Reply
				if decoder.Decode(&r) != nil {
					return
				}
				d.mu.Lock()
				ch := d.pending[r.CallID]
				d.mu.Unlock()
				if ch != nil {
					ch <- r
				}
			}
		}()
		t.Cleanup(func() { _ = write.Close(); _ = in.Close(); _ = out.Close() })
		command := executil.Command(ctx, exe, append([]string{"-test.run=^TestDemoPoolerProcess$", "--"}, args...)...).WithProcessGroup()
		command.SetStdin(in)
		command.AddEnv("MULTIGRES_DEMO_REPLY_PATH=" + fifo)
		return command
	}
}

func (d *demoDriver) call(ctx context.Context, method string, r demo.Request) (*demo.Operation, error) {
	d.mu.Lock()
	d.next++
	id := strconv.Itoa(d.next)
	ch := make(chan demo.Reply, 1)
	d.pending[id] = ch
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.pending, id); d.mu.Unlock() }()
	timeout := 30000
	if end, ok := ctx.Deadline(); ok {
		timeout = int(time.Until(end).Milliseconds())
		if timeout < 1 {
			return nil, ctx.Err()
		}
	}
	d.writeMu.Lock()
	err := json.NewEncoder(d.input).Encode(demo.Command{CallID: id, Method: method, Request: r, TimeoutMillis: timeout})
	d.writeMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case reply := <-ch:
		if reply.Error != "" {
			return nil, errors.New(reply.Error)
		}
		return reply.Operation, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Only the catch-up barrier is simulated. Catalog durability, RPCs, drains,
// managed election and gateway health routing use real processes and PostgreSQL.
func TestDemoControllerElectionDuringDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	require.NoError(t, pathutil.PrependBinToPath())
	ctx := t.Context()
	dir := t.TempDir()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	keyPath := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))
	drivers := &sync.Map{}
	driver := func(name string) *demoDriver {
		v, ok := drivers.Load(name)
		require.True(t, ok, "driver for %s", name)
		return v.(*demoDriver)
	}
	s, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(3), shardsetup.WithMultiorchCount(1), shardsetup.WithMultigateway(), shardsetup.WithMultipoolerExtraArgs("--migration-key-file="+keyPath), shardsetup.WithMultipoolerLauncher(launchDemoPooler(t, drivers)))
	defer cleanup()
	target, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable", s.PrimaryPgctld(t).PgPort, shardsetup.TestPostgresPassword))
	require.NoError(t, err)
	defer target.Close(context.Background())
	var verifier string
	require.NoError(t, target.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='postgres'").Scan(&verifier))
	hash, err := scram.ParseScramSHA256Hash(verifier)
	require.NoError(t, err)
	salted := scram.ComputeSaltedPassword(shardsetup.TestPostgresPassword, hash.Salt, hash.Iterations)
	authority, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", s.PrimaryMultipooler(t).GrpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer authority.Close()
	sourcePort := utils.GetFreePort(t)
	data := filepath.Join(dir, "source")
	pw := filepath.Join(dir, "password")
	require.NoError(t, os.WriteFile(pw, []byte("source-password"), 0o600))
	out, err := executil.Command(ctx, "initdb", "-D", data, "-U", "postgres", "--pwfile", pw, "--auth-host=scram-sha-256", "--auth-local=trust").CombinedOutput()
	require.NoError(t, err, "%s", out)
	startUnmanagedTestProcess(t, "postgres", "-D", data, "-h", "127.0.0.1", "-p", strconv.Itoa(sourcePort), "-k", "/tmp")
	var source *pgx.Conn
	require.Eventually(t, func() bool {
		source, err = pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=source-password dbname=postgres sslmode=disable", sourcePort))
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)
	defer source.Close(context.Background())
	var sysid string
	require.NoError(t, source.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&sysid))
	created, err := rpc.NewMultipoolerServiceClient(authority).CreateSourceConnection(migrationcontrol.AuthorizedContext(ctx, key), &rpc.CreateSourceConnectionRequest{Database: "postgres", Username: "postgres", UserAuth: &query.UserAuth{ClientKey: scram.ComputeClientKey(salted), ServerKey: scram.ComputeServerKey(salted)}, Connection: &rpc.SourceConnection{Name: "source", Host: "127.0.0.1", Port: uint32(sourcePort), Database: "postgres", Username: "postgres", Password: "source-password", SslMode: "disable", ExpectedSystemIdentifier: sysid}})
	require.NoError(t, err)
	sourceGrpc := utils.GetFreePort(t)
	startUnmanagedTestProcess(t, "multipooler", "--management-mode=unmanaged", "--source-connection=source", "--migration-key-file="+keyPath, "--database=postgres", "--table-group=default", "--shard=0-inf", "--cell="+s.CellName, "--service-id=demo-source", "--hostname=127.0.0.1", "--grpc-port="+strconv.Itoa(sourceGrpc), "--http-port="+strconv.Itoa(utils.GetFreePort(t)), "--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global")
	require.Eventually(t, func() bool {
		r, e := s.TopoServer.GetMultipooler(ctx, &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: s.CellName, Name: "demo-source"})
		return e == nil && r.SourceConfigurationBinding == created.ConfigurationBinding && r.ServingStatus == pb.PoolerServingStatus_SERVING
	}, 30*time.Second, 100*time.Millisecond)
	_, err = target.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD 'demo-password'; CREATE TABLE routing_marker(value TEXT); INSERT INTO routing_marker VALUES('managed'); GRANT SELECT ON routing_marker TO alice")
	require.NoError(t, err)
	var aliceVerifier string
	require.NoError(t, target.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='alice'").Scan(&aliceVerifier))
	_, err = source.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD '"+aliceVerifier+"'; CREATE TABLE routing_marker(value TEXT); INSERT INTO routing_marker VALUES('source'); GRANT SELECT ON routing_marker TO alice")
	require.NoError(t, err)
	policy := &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_SOURCE, SourceConnection: "source", SourceConfigurationBinding: created.ConfigurationBinding, SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: sysid, Database: "postgres"}}
	previous := ""
	request := func(phase demo.Phase) demo.Request {
		return demo.Request{ID: string(phase), Owner: "e2e-controller", Predecessor: previous, Phase: phase, Source: policy}
	}
	advance := func(phase demo.Phase) {
		r := request(phase)
		var op *demo.Operation
		var lastErr error
		require.Eventually(t, func() bool {
			attempt, stop := context.WithTimeout(ctx, 10*time.Second)
			defer stop()
			op, lastErr = driver(s.PrimaryName).call(attempt, "advance", r)
			if lastErr != nil {
				t.Logf("retry %s: %v", phase, lastErr)
			}
			return lastErr == nil && op.Complete
		}, 45*time.Second, 200*time.Millisecond, "recover same operation %s: %v", phase, lastErr)
		require.True(t, op.Complete)
		previous = r.ID
		t.Logf("completed %s", phase)
	}
	advance(demo.Enable)
	advance(demo.OpenSource)
	advance(demo.RouteSource)
	dsn := fmt.Sprintf("host=127.0.0.1 port=%d user=alice password=demo-password dbname=postgres sslmode=disable", s.MultigatewayPgPort)
	var app *pgx.Conn
	require.Eventually(t, func() bool { app, err = pgx.Connect(ctx, dsn); return err == nil }, 15*time.Second, 100*time.Millisecond)
	defer app.Close(context.Background())
	var marker string
	require.NoError(t, app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&marker))
	require.Equal(t, "source", marker)
	_, err = app.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	require.NoError(t, app.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&marker))
	closeRequest := request(demo.CloseSource)
	oldPrimary := s.PrimaryName
	oldDriver := driver(oldPrimary)
	closed := make(chan error, 1)
	go func() { _, e := oldDriver.call(ctx, "advance", closeRequest); closed <- e }()
	// A fresh connection through a stale source route must be rejected while the
	// existing reservation keeps the durable close operation incomplete.
	require.Eventually(t, func() bool {
		probe, e := pgx.Connect(ctx, dsn)
		if e != nil {
			return true
		}
		defer probe.Close(context.Background())
		_, e = probe.Exec(ctx, "SELECT value FROM routing_marker")
		return e != nil
	}, 15*time.Second, 100*time.Millisecond)
	pending, e := oldDriver.call(ctx, "status", closeRequest)
	require.NoError(t, e)
	require.False(t, pending.Complete)
	s.StartMultiorchs(ctx, t)
	resume := s.StopPostgres(t, oldPrimary, "immediate")
	defer resume()
	s.PrimaryName = shardsetup.WaitForNewPrimary(t, s, oldPrimary, 45*time.Second)
	require.NotEqual(t, oldPrimary, s.PrimaryName)
	select {
	case e := <-closed:
		require.Error(t, e)
	case <-time.After(15 * time.Second):
		t.Fatal("obsolete controller did not stop on leadership loss")
	}
	_, err = app.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	recovered, e := driver(s.PrimaryName).call(ctx, "advance", closeRequest)
	require.NoError(t, e)
	require.True(t, recovered.Complete)
	previous = closeRequest.ID
	resume() // Restore the old target before it can acknowledge OPEN.
	// Old OPEN is only an idempotent completed-request read, never reenforcement.
	oldOpen := demo.Request{ID: string(demo.OpenSource), Owner: "e2e-controller", Predecessor: string(demo.Enable), Phase: demo.OpenSource, Source: policy}
	_, e = driver(s.PrimaryName).call(ctx, "advance", oldOpen)
	require.NoError(t, e)
	advance(demo.SimulatedBarrier)
	advance(demo.OpenTarget)
	advance(demo.RouteTarget)
	advance(demo.Finish)
	advance(demo.Retire)
	require.Eventually(t, func() bool {
		fresh, e := pgx.Connect(ctx, dsn)
		if e != nil {
			return false
		}
		defer fresh.Close(context.Background())
		var v string
		e = fresh.QueryRow(ctx, "SELECT value FROM routing_marker").Scan(&v)
		return e == nil && v == "managed"
	}, 15*time.Second, 100*time.Millisecond)
	// A newly registered source process still initializes CLOSED against terminal
	// intent; backend readiness and delayed OPEN cannot bypass that decision.
	joiningGrpc := utils.GetFreePort(t)
	startUnmanagedTestProcess(t, "multipooler", "--management-mode=unmanaged", "--source-connection=source", "--migration-key-file="+keyPath, "--database=postgres", "--table-group=default", "--shard=0-inf", "--cell="+s.CellName, "--service-id=terminal-source-join", "--hostname=127.0.0.1", "--grpc-port="+strconv.Itoa(joiningGrpc), "--http-port="+strconv.Itoa(utils.GetFreePort(t)), "--topo-global-server-addresses="+s.EtcdClientAddr, "--topo-global-root=/multigres/global")
	var joining *pb.Multipooler
	require.Eventually(t, func() bool {
		p, e := s.TopoServer.GetMultipooler(ctx, &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: s.CellName, Name: "terminal-source-join"})
		if e != nil {
			return false
		}
		joining = p.Multipooler
		return p.SourceConfigurationBinding == policy.SourceConfigurationBinding && p.ServingStatus == pb.PoolerServingStatus_SERVING
	}, 30*time.Second, 100*time.Millisecond)
	joiningConn, e := migrationcontrol.Dial(joining, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, e)
	defer joiningConn.Close()
	terminal := &pb.AdmissionIntent{Owner: "e2e-controller", IntentId: closeRequest.ID + "/source", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED, SourceConnection: policy.SourceConnection, SourceConfigurationBinding: policy.SourceConfigurationBinding, SourceIdentity: policy.SourceIdentity}
	refresh := &rpc.RefreshAdmissionRequest{Database: "postgres", Expected: terminal, ExpectedProcessIncarnation: joining.ProcessIncarnation}
	reply, e := rpc.NewMultipoolerServiceClient(joiningConn).RefreshAdmission(migrationcontrol.AuthorizedContext(ctx, key), refresh)
	require.NoError(t, e)
	require.False(t, reply.AdmissionOpen)
	terminal.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN
	terminal.IntentId = string(demo.OpenSource) + "/source"
	_, e = rpc.NewMultipoolerServiceClient(joiningConn).RefreshAdmission(migrationcontrol.AuthorizedContext(ctx, key), refresh)
	require.Error(t, e, "delayed OPEN must not reopen the joining source")
	t.Log("demo complete: simulated replication barrier, no data-consistency claim")
}

func startUnmanagedTestProcess(t *testing.T, binary string, args ...string) func() {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), binary+".log")
	log, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), binary, args...).WithProcessGroup()
	cmd.SetStdout(log)
	cmd.SetStderr(log)
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
		_ = log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("%s log:\n%s", binary, data)
		}
	})
	return stop
}
