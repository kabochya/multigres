// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

type drainAckServer struct {
	rpc.UnimplementedMultipoolerServiceServer
	lost atomic.Bool
	id   *pb.ID
}

func (s *drainAckServer) RefreshRouting(_ context.Context, r *rpc.RefreshRoutingRequest) (*rpc.RefreshRoutingResponse, error) {
	if s.lost.Load() {
		return nil, errors.New("drain acknowledgment lost")
	}
	return &rpc.RefreshRoutingResponse{OperationId: r.OperationId, Mode: r.ExpectedMode, ProcessId: s.id}, nil
}

func TestMissingRequiredProcessDoesNotCompleteRecoveredFence(t *testing.T) {
	ctx := t.Context()
	pm, q, _ := newTransitionManager(t)
	_, err := pm.adminServingTransition(ctx, &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	topo, _ := memorytopo.NewServerAndFactory(ctx, "a")
	defer topo.Close()
	pm.config.TopoClient = topo
	pm.config.ControlTransport = grpc.WithTransportCredentials(insecure.NewCredentials())
	pm.servingPeers = nil
	var obsolete *pb.Multipooler
	var replacementServer *drainAckServer
	for _, name := range []string{"lost-ack", "healthy"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		server := grpc.NewServer()
		ack := &drainAckServer{}
		ack.lost.Store(name == "lost-ack")
		rpc.RegisterMultipoolerServiceServer(server, ack)
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		record := &pb.Multipooler{Id: &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "a", Name: name}, Hostname: "127.0.0.1", PortMap: map[string]int32{"grpc": int32(listener.Addr().(*net.TCPAddr).Port)}, ShardKey: &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"}, ManagementMode: pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, SourceConnection: "source"}
		ack.id = record.Id
		require.NoError(t, topo.CreateMultipooler(ctx, record))
		if name == "lost-ack" {
			obsolete = record
			replacementServer = ack
		}
	}
	journal := func(context.Context, executor.InternalTx) error { return nil }
	require.Error(t, pm.ControllerTransition(ctx, "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	require.False(t, q.requests["fence"].done)
	require.NoError(t, topo.UnregisterMultipooler(ctx, &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "a", Name: "lost-ack"}))
	require.Error(t, pm.ControllerTransition(ctx, "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal), "missing record is not proof of stopped process or completed drain")
	require.False(t, q.requests["fence"].done)
	obsolete.LifecycleStatus = &pb.PoolerLifecycle{Status: pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN}
	require.NoError(t, topo.CreateMultipooler(ctx, obsolete))
	replacement := proto.Clone(obsolete).(*pb.Multipooler)
	replacement.Id.Name = "fresh-process"
	replacement.LifecycleStatus = nil
	require.NoError(t, topo.CreateMultipooler(ctx, replacement))
	require.Error(t, pm.ControllerTransition(ctx, "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal), "old shutdown proof does not acknowledge the fresh replacement")
	require.False(t, q.requests["fence"].done)
	replacementServer.id = replacement.Id
	replacementServer.lost.Store(false)
	require.NoError(t, pm.ControllerTransition(ctx, "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	require.True(t, q.requests["fence"].done)
	require.Len(t, q.required, 3)
}
