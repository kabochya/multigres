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

package manager

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func TestMetadataWriterActivationAndLeadershipInvalidation(t *testing.T) {
	pm := &MultipoolerManager{shutdownCtx: t.Context(), config: &Config{}, actionLock: actionlock.NewActionLock(), record: newRecordFromProto(&pb.Multipooler{ShardKey: &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}}), healthStreamer: newHealthStreamer(slog.Default(), nil, "default", "0")}
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}
	first, err := pm.ActivateMetadataWriter(t.Context(), "postgres")
	require.NoError(t, err)
	second, err := pm.ActivateMetadataWriter(t.Context(), "postgres")
	require.NoError(t, err)
	require.ErrorIs(t, first.Context().Err(), context.Canceled)
	called := false
	callback := func(context.Context, executor.InternalTx, *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
		called = true
		return nil, nil
	}
	require.Error(t, first.Transaction(t.Context(), callback))
	require.False(t, called)
	sk := second.ShardKey()
	sk.Shard = "other"
	require.Equal(t, "0", second.ShardKey().Shard)
	require.NoError(t, (metadataLifecycle{pm}).OnStateChange(t.Context(), servingstate.State{}))
	require.ErrorIs(t, second.Context().Err(), context.Canceled)
	require.Error(t, second.Transaction(t.Context(), callback))
	require.False(t, called)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	_, err = pm.ActivateMetadataWriter(t.Context(), "postgres")
	require.Error(t, err)
}
