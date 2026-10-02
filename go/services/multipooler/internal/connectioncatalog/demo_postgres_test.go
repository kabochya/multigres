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
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/routingpolicy"
	"github.com/multigres/multigres/go/services/multipooler/testutil/demo"
)

// This adapter isolates journal durability; real manager/action-lock and actual
// admission enforcement are exercised by TestDemoControllerElectionDuringDrain.
type demoPostgresWriter struct {
	c            *connectioncatalog.Catalog
	ctx          context.Context
	transactions atomic.Int64
}

func (w *demoPostgresWriter) Context() context.Context { return w.ctx }
func (w *demoPostgresWriter) ShardKey() *pb.ShardKey {
	return &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}
}

func (w *demoPostgresWriter) Transaction(ctx context.Context, f func(context.Context, executor.InternalTx, *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error)) error {
	w.transactions.Add(1)
	return w.c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error { _, e := f(ctx, tx, w.c); return e })
}

type journalAckServer struct {
	rpc.UnimplementedMultipoolerServiceServer
	record *pb.Multipooler
	calls  *atomic.Int64
}

func (s *journalAckServer) RefreshAdmission(_ context.Context, r *rpc.RefreshAdmissionRequest) (*rpc.RefreshAdmissionResponse, error) {
	s.calls.Add(1)
	if r.ExpectedProcessIncarnation != s.record.ProcessIncarnation {
		return nil, errors.New("obsolete incarnation")
	}
	return &rpc.RefreshAdmissionResponse{Observed: proto.Clone(r.Expected).(*pb.AdmissionIntent), ProcessId: s.record.Id, ProcessIncarnation: s.record.ProcessIncarnation, AdmissionOpen: r.Expected.Permission == pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN}, nil
}

func journalPeer(t *testing.T, ts topoclient.Store, source bool, cell, name, binding string, calls *atomic.Int64) *pb.Multipooler {
	t.Helper()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	p := &pb.Multipooler{Id: &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: cell, Name: name}, ProcessIncarnation: name + "-process", ShardKey: &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}, Hostname: "127.0.0.1", PortMap: map[string]int32{"grpc": int32(listener.Addr().(*net.TCPAddr).Port)}, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}}
	if source {
		p.ManagementMode = pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
		p.SourceConnection = "source"
		p.SourceConfigurationBinding = binding
	}
	server := grpc.NewServer()
	rpc.RegisterMultipoolerServiceServer(server, &journalAckServer{record: p, calls: calls})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	require.NoError(t, ts.RegisterMultipooler(t.Context(), p, false))
	return p
}

func TestPostgresDemoJournalUncertainIntentCompletionAndReplay(t *testing.T) {
	dsn, dir, port := disposablePostgres(t)
	ctx := t.Context()
	observer, e := pgx.Connect(ctx, dsn)
	require.NoError(t, e)
	defer observer.Close(context.Background())
	_, e = observer.Exec(ctx, "CREATE SCHEMA multigres")
	require.NoError(t, e)
	c, e := connectioncatalog.New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, e)
	require.NoError(t, c.Initialize(ctx))
	writer := &demoPostgresWriter{c: c, ctx: ctx}
	var configured *connectioncatalog.Record
	require.NoError(t, c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
		if e := admission.InitializeOrdinary(ctx, tx, writer.ShardKey()); e != nil {
			return e
		}
		if e := routingpolicy.Initialize(ctx, tx, "postgres"); e != nil {
			return e
		}
		var e error
		configured, e = c.Create(ctx, tx, &rpc.SourceConnection{Name: "source", Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", SslMode: "require", ExpectedSystemIdentifier: "123"})
		return e
	}))
	ts := memorytopo.NewServer(ctx, "target-cell", "source-cell", "joining-cell")
	defer ts.Close()
	var calls atomic.Int64
	journalPeer(t, ts, false, "target-cell", "target", configured.Binding, &calls)
	journalPeer(t, ts, true, "source-cell", "source", configured.Binding, &calls)
	newController := func(w *demoPostgresWriter) *demo.Controller {
		v, e := demo.New(ctx, w, ts, bytes.Repeat([]byte{1}, 32), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, e)
		return v
	}
	controller := newController(writer)
	policy := &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_SOURCE, SourceConnection: "source", SourceConfigurationBinding: configured.Binding, SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}
	r := demo.Request{ID: "enable", Owner: "owner", Phase: demo.Enable, Source: policy}
	syncNames := func(names string) {
		// Failure injection runs in the controller goroutine; use its own connection
		// while the observer concurrently waits for the SyncRep event.
		control, e := pgx.Connect(ctx, dsn)
		require.NoError(t, e)
		defer control.Close(context.Background())
		_, e = control.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names='"+names+"'")
		require.NoError(t, e)
		_, e = control.Exec(ctx, "SELECT pg_reload_conf()")
		require.NoError(t, e)
	}
	cancelSync := func() {
		_, e := observer.Exec(ctx, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
		require.NoError(t, e)
	}
	waitSync := func() {
		require.Eventually(t, func() bool {
			var waiting bool
			e := observer.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event='SyncRep')").Scan(&waiting)
			return e == nil && waiting
		}, 10*time.Second, 20*time.Millisecond)
	}
	syncNames("FIRST 1 (absent)")
	attempt, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		op, e := controller.Advance(attempt, r)
		if op != nil {
			done <- errors.New("uncertain intent returned authoritative operation")
			return
		}
		done <- e
	}()
	waitSync()
	stop()
	require.Error(t, <-done)
	cancelSync()
	require.Zero(t, calls.Load(), "no fanout on uncertain intent commit")
	syncNames("")
	controller = newController(&demoPostgresWriter{c: c, ctx: ctx})
	var unacknowledged *pb.Multipooler
	controller.AfterAcknowledgment = func(p *pb.Multipooler) error {
		unacknowledged = proto.Clone(p).(*pb.Multipooler)
		return errors.New("lost acknowledgment")
	}
	op, e := controller.Advance(ctx, r)
	require.Error(t, e)
	require.Nil(t, op)
	status, e := controller.Status(ctx, r.ID)
	require.NoError(t, e)
	require.False(t, status.Complete)
	require.Empty(t, status.Acknowledged)
	controller = newController(&demoPostgresWriter{c: c, ctx: ctx})
	// A process replacement cannot silently erase an unacknowledged incarnation.
	require.NoError(t, ts.UnregisterMultipooler(ctx, unacknowledged.Id))
	_, e = controller.Advance(ctx, r)
	require.ErrorContains(t, e, "actual termination proof required")
	require.Error(t, controller.RecordTermination(ctx, r.ID, unacknowledged, nil))
	require.NoError(t, ts.RegisterMultipooler(ctx, unacknowledged, true))
	// Join another cell while the same enablement barrier is pending.
	joined := journalPeer(t, ts, false, "joining-cell", "joined", configured.Binding, &calls)
	joined.RoutingState.Role = pb.RoutingRole_ROUTING_ROLE_REPLICA
	require.NoError(t, ts.RegisterMultipooler(ctx, joined, true))
	op, e = controller.Advance(ctx, r)
	require.NoError(t, e)
	require.True(t, op.Complete)
	require.Len(t, op.Required, 3, "cross-cell membership and unacknowledged replacement retained")
	bad := r
	bad.Owner = "other"
	_, e = controller.Advance(ctx, bad)
	require.Error(t, e)
	r = demo.Request{ID: "open", Owner: "owner", Predecessor: "enable", Phase: demo.OpenSource, Source: policy}
	controller.BeforeCompletion = func() error { syncNames("FIRST 1 (absent)"); return nil }
	attempt, stop = context.WithCancel(ctx)
	go func() {
		op, e := controller.Advance(attempt, r)
		if op != nil {
			done <- errors.New("uncertain completion acknowledged")
			return
		}
		done <- e
	}()
	waitSync()
	stop()
	require.Error(t, <-done)
	cancelSync()
	// COMMIT may be locally visible even though its synchronous acknowledgment
	// was canceled. Restart and completed-request shortcuts must not trust it.
	var encoded string
	require.NoError(t, observer.QueryRow(ctx, "SELECT operation FROM multigres.demo_operations WHERE request_id='open'").Scan(&encoded))
	local := &demo.Operation{}
	require.NoError(t, json.Unmarshal([]byte(encoded), local))
	require.True(t, local.Complete)
	timeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	restarted, e := demo.New(timeout, &demoPostgresWriter{c: c, ctx: ctx}, ts, bytes.Repeat([]byte{1}, 32), grpc.WithTransportCredentials(insecure.NewCredentials()))
	cancel()
	require.Error(t, e)
	require.Nil(t, restarted)
	cancelSync()
	timeout, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
	status, e = controller.Status(timeout, "open")
	cancel()
	require.Error(t, e)
	require.Nil(t, status)
	cancelSync()
	syncNames("")
	restarted = newController(&demoPostgresWriter{c: c, ctx: ctx})
	_, syncReplica := startCatalogReplica(t, dir, port, "demo_sync")
	syncNames("FIRST 1 (demo_sync)")
	status, e = restarted.Status(ctx, "open")
	require.NoError(t, e)
	require.True(t, status.Complete)
	_, lagging := startCatalogReplica(t, dir, port, "demo_lagging")
	_, e = lagging.Exec(ctx, "SELECT pg_wal_replay_pause()")
	require.NoError(t, e)
	route := demo.Request{ID: "route", Owner: "owner", Predecessor: "open", Phase: demo.RouteSource, Source: policy}
	op, e = restarted.Advance(ctx, route)
	require.NoError(t, e)
	require.True(t, op.Complete)
	var current string
	require.NoError(t, lagging.QueryRow(ctx, "SELECT request_id FROM multigres.demo_current").Scan(&current))
	require.Equal(t, "open", current)
	require.NoError(t, syncReplica.QueryRow(ctx, "SELECT request_id FROM multigres.demo_current").Scan(&current))
	require.Equal(t, "route", current)
	_, e = lagging.Exec(ctx, "SELECT pg_wal_replay_resume()")
	require.NoError(t, e)
	require.Eventually(t, func() bool {
		e := lagging.QueryRow(ctx, "SELECT request_id FROM multigres.demo_current").Scan(&current)
		return e == nil && current == "route"
	}, 5*time.Second, 20*time.Millisecond)
}
