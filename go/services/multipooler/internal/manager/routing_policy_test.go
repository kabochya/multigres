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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	qmock "github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

type publicationTx struct {
	executor.InternalTx
	commits int
	err     error
}

func (tx *publicationTx) Query(context.Context, string) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (tx *publicationTx) QueryArgs(_ context.Context, q string, _ ...any) (*sqltypes.Result, error) {
	if q == "SELECT destination,source_connection,source_system_identifier,source_database,source_configuration_binding FROM multigres.gateway_routing WHERE database=$1 FOR UPDATE" {
		return qmock.MakeQueryResult([]string{"destination", "name", "sysid", "db", "binding"}, [][]any{{int32(1), "", "", "", ""}}), nil
	}
	return &sqltypes.Result{}, nil
}
func (tx *publicationTx) Commit(context.Context) error   { tx.commits++; return tx.err }
func (tx *publicationTx) Rollback(context.Context) error { return nil }

type publicationQueries struct {
	executor.InternalQueryService
	tx *publicationTx
}

func (q publicationQueries) BeginAdmin(context.Context) (executor.InternalTx, error) {
	return q.tx, nil
}

func TestConfirmedRoutingPublicationAndIdleReuse(t *testing.T) {
	tx := &publicationTx{err: errors.New("uncertain commit")}
	catalog, err := connectioncatalog.New(publicationQueries{tx: tx}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	pm := &MultipoolerManager{config: &Config{MigrationKey: bytes.Repeat([]byte{1}, 32)}, record: newRecordFromProto(&pb.Multipooler{ShardKey: &pb.ShardKey{Database: "postgres"}}), healthStreamer: newHealthStreamer(newTestLogger(), nil, "default", "0"), actionLock: actionlock.NewActionLock(), connectionCatalog: catalog}
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}
	policy, err := pm.confirmRoutingPolicy(t.Context())
	require.Error(t, err)
	require.Nil(t, policy)
	require.Nil(t, pm.healthStreamer.getState().RoutingPolicy)
	// A local catalog read could see the row, but no authoritative snapshot may
	// escape until a synchronous confirmation succeeds.
	tx.err = nil
	policy, err = pm.confirmRoutingPolicy(t.Context())
	require.NoError(t, err)
	require.Equal(t, pb.RoutingDestination_ROUTING_DESTINATION_MANAGED, policy.Destination)
	commits := tx.commits
	for range 10 {
		_, err = pm.confirmRoutingPolicy(t.Context())
		require.NoError(t, err)
	}
	require.Equal(t, commits, tx.commits, "unchanged repeated publication reuses confirmed state")
	state := servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: pb.PoolerServingStatus_SERVING}
	require.NoError(t, (routingLifecycle{pm}).OnStateChange(t.Context(), state))
	require.Nil(t, pm.healthStreamer.getState().RoutingPolicy)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	_, err = pm.confirmRoutingPolicy(t.Context())
	require.Error(t, err)
	require.Equal(t, commits, tx.commits)
}
