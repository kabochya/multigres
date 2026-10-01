// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
)

type sourceValidationServer struct {
	rpc.UnimplementedMultipoolerServiceServer
	config    *rpc.SourceConnection
	mode      atomic.Int32
	calls     atomic.Int32
	modeCalls atomic.Int32
	protected atomic.Bool
}

func (s *sourceValidationServer) GetMigrationMode(context.Context, *rpc.GetMigrationModeRequest) (*rpc.GetMigrationModeResponse, error) {
	s.modeCalls.Add(1)
	return nil, errors.New("separate mode callback must not be used")
}

func (s *sourceValidationServer) GetSourceConnection(ctx context.Context, _ *rpc.GetSourceConnectionRequest) (*rpc.GetSourceConnectionResponse, error) {
	s.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	s.protected.Store(len(md.Get("x-multigres-serving-key")) == 1)
	return &rpc.GetSourceConnectionResponse{Connection: proto.Clone(s.config).(*rpc.SourceConnection), Routing: &pb.MigrationRouting{ActiveRequestId: pb.MigrationMode(s.mode.Load()).String(), Mode: pb.MigrationMode(s.mode.Load()), SourceConnection: "source", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}}, nil
}

func TestSourceCommandUsesOneProtectedAuthorityCallback(t *testing.T) {
	ctx := t.Context()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	configuration := &rpc.SourceConnection{Name: "source", Host: "127.0.0.1", Database: "postgres", Username: "postgres", Port: 5432, SslMode: "require", Password: "test-only"}
	validation := &sourceValidationServer{config: configuration}
	validation.mode.Store(int32(pb.MigrationMode_MIGRATION_MODE_UNMANAGED))
	rpc.RegisterMultipoolerServiceServer(server, validation)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	topo, _ := memorytopo.NewServerAndFactory(ctx, "a")
	defer topo.Close()
	require.NoError(t, topo.CreateMultipooler(ctx, &pb.Multipooler{Id: &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "a", Name: "leader"}, Hostname: "127.0.0.1", PortMap: map[string]int32{"grpc": int32(listener.Addr().(*net.TCPAddr).Port)}, ShardKey: &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"}, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}}))
	pm, gate := newMigrationGateManager()
	pm.config.TopoClient = topo
	pm.config.ControlTransport = grpc.WithTransportCredentials(insecure.NewCredentials())
	pm.config.MigrationKey = bytes.Repeat([]byte{1}, 32)
	pm.config.SourceConfiguration = proto.Clone(configuration).(*rpc.SourceConnection)
	pm.healthStreamer = newHealthStreamer(slog.Default(), &pb.ID{}, "default", "0")
	pm.healthStreamer.backendReady = true
	pm.healthStreamer.backendIdentity = &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}
	require.NoError(t, pm.refreshRoutingForTest(ctx, pb.MigrationMode_MIGRATION_MODE_UNMANAGED))
	require.True(t, gate.open.Load())
	require.Equal(t, int32(1), validation.calls.Load())
	require.True(t, validation.protected.Load())
	// Configuration mismatch must reject opening, but never prevent closing/drain.
	pm.config.SourceConfiguration.Host = "obsolete-host"
	require.Error(t, pm.refreshRoutingForTest(ctx, pb.MigrationMode_MIGRATION_MODE_UNMANAGED))
	validation.mode.Store(int32(pb.MigrationMode_MIGRATION_MODE_FENCED))
	require.NoError(t, pm.refreshRoutingForTest(ctx, pb.MigrationMode_MIGRATION_MODE_FENCED))
	require.False(t, gate.open.Load())
	require.Equal(t, int32(3), validation.calls.Load())
	require.Zero(t, validation.modeCalls.Load())
}
