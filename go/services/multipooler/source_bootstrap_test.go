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

package multipooler

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/stretchr/testify/require"

	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func TestBootstrapRequiresPreparedConfigurationIdentity(t *testing.T) {
	reply := &rpc.GetSourceConnectionResponse{Connection: &rpc.SourceConnection{Name: "source", Host: "db", Port: 5432, Database: "postgres", Username: "admin", SslMode: "require", ExpectedSystemIdentifier: "123"}, ConfigurationBinding: "binding"}
	require.NoError(t, validateSourceBootstrap("source", reply))
	require.Error(t, validateSourceBootstrap("other", reply))
	require.Error(t, validateSourceBootstrap("source", nil))
	reply.ConfigurationBinding = ""
	require.Error(t, validateSourceBootstrap("source", reply))
	reply.ConfigurationBinding = "binding"
	reply.Connection.ExpectedSystemIdentifier = ""
	require.Error(t, validateSourceBootstrap("source", reply))
}

func TestBootstrapRetriesBoundedAndCancellationAware(t *testing.T) {
	attempts := 0
	require.NoError(t, retrySourceBootstrap(t.Context(), func(ctx context.Context) error {
		_, ok := ctx.Deadline()
		require.True(t, ok)
		attempts++
		if attempts == 1 {
			return errors.New("authority temporarily unavailable")
		}
		return nil
	}))
	require.Equal(t, 2, attempts)
	ctx, cancel := context.WithCancel(t.Context())
	attempts = 0
	err := retrySourceBootstrap(ctx, func(context.Context) error { attempts++; cancel(); return errors.New("unavailable") })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

type bootstrapTopology struct {
	topoclient.Store
	registered *pb.Multipooler
	order      []string
	cancel     context.CancelFunc
}

func (ts *bootstrapTopology) RegisterMultipooler(_ context.Context, p *pb.Multipooler, update bool) error {
	ts.order = append(ts.order, "register")
	ts.registered = p
	return nil
}

func (ts *bootstrapTopology) GetCellNames(context.Context) ([]string, error) {
	ts.order = append(ts.order, "authority-read")
	ts.cancel()
	return nil, errors.New("authority unavailable")
}

func TestBootstrapRegistersClosedProcessBeforeAuthorityRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ts := &bootstrapTopology{cancel: cancel}
	initial := &pb.Multipooler{Id: &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "zone1", Name: "fresh-incarnation"}, ShardKey: &pb.ShardKey{Database: "db", TableGroup: "default", Shard: "0-inf"}, ServingStatus: pb.PoolerServingStatus_DISABLED, ManagementMode: pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, SourceConnection: "source"}
	reply, err := bootstrapSource(ctx, ts, initial, slog.Default(), make([]byte, 32), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Error(t, err)
	require.Nil(t, reply)
	require.Equal(t, []string{"register", "authority-read"}, ts.order)
	require.Equal(t, pb.PoolerServingStatus_DISABLED, ts.registered.ServingStatus)
	require.Equal(t, "fresh-incarnation", ts.registered.Id.Name)
}
