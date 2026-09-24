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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestConstructorRejectsExternalOwnershipBeforeSideEffects(t *testing.T) {
	for _, mode := range []pb.PoolerManagementMode{99} {
		// Nil config would panic if construction got as far as creating clients.
		mgr, err := NewMultipoolerManager(slog.Default(), &pb.Multipooler{ManagementMode: mode}, nil)
		require.Nil(t, mgr)
		require.ErrorContains(t, err, "management mode is not supported")
	}
}

func TestUnmanagedConstructorOmitsManagementComponents(t *testing.T) {
	pm, err := NewMultipoolerManager(slog.Default(), &pb.Multipooler{
		Id:             &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "zone1", Name: "external"},
		ShardKey:       &pb.ShardKey{Database: "db", TableGroup: "default", Shard: "0-inf"},
		ManagementMode: pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED,
	}, &Config{PgctldAddr: "localhost:1", ConsensusEnabled: true})
	require.NoError(t, err)
	defer pm.cancel()
	defer pm.shutdownCancel()
	require.True(t, pm.IsUnmanaged())
	require.Nil(t, pm.pgctldClient)
	require.Nil(t, pm.consensusMgr)
	require.Nil(t, pm.backup)
	require.NotNil(t, pm.QueryServiceControl())
	pm.StartBackupHealth()
	require.False(t, pm.backupHealthEnabled)
	pm.GracefulShutdown(t.Context())
	pm.GracefulShutdown(t.Context())
	require.Equal(t, pb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
}
