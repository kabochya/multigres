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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func preparingRecord(name string, mode clustermetadatapb.PoolerManagementMode) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
		Hostname:       "localhost",
		PortMap:        map[string]int32{"grpc": 1},
		ShardKey:       &clustermetadatapb.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		ManagementMode: mode,
		ServingStatus:  clustermetadatapb.PoolerServingStatus_SERVING,
	}
}

func TestRegisterPreparingReplacesItsOwnStaleEntry(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	unmanaged := clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED

	// A previous process of the same pooler left a SERVING entry.
	require.NoError(t, ts.CreateMultipooler(t.Context(), preparingRecord("ext", unmanaged)))
	require.NoError(t, RegisterPreparing(t.Context(), ts, preparingRecord("ext", unmanaged)))

	info, err := ts.GetMultipooler(t.Context(), preparingRecord("ext", unmanaged).Id)
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, info.GetServingStatus(), "a restart must not leave a stale SERVING entry")
}

func TestRegisterPreparingNeverReplacesAManagedPooler(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	managed := clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED
	unmanaged := clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED

	for name, mode := range map[string]clustermetadatapb.PoolerManagementMode{
		"managed":     managed,
		"legacy":      clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNSPECIFIED,
		"future mode": 99,
	} {
		require.NoError(t, ts.CreateMultipooler(t.Context(), preparingRecord(name, mode)))
		require.ErrorContains(t, RegisterPreparing(t.Context(), ts, preparingRecord(name, unmanaged)), "refusing to replace", name)
		info, err := ts.GetMultipooler(t.Context(), preparingRecord(name, mode).Id)
		require.NoError(t, err)
		require.Equal(t, mode, info.GetManagementMode(), "%s: the existing entry is untouched", name)
	}

	// A new pooler id registers normally.
	require.NoError(t, RegisterPreparing(t.Context(), ts, preparingRecord("fresh", unmanaged)))
}

func TestMarkPreparingFailedLeavesAShutdownEntry(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	unmanaged := clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	rec := preparingRecord("ext", unmanaged)
	require.NoError(t, RegisterPreparing(t.Context(), ts, rec))
	require.NoError(t, MarkPreparingFailed(t.Context(), ts, rec))

	info, err := ts.GetMultipooler(t.Context(), rec.Id)
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN, info.GetLifecycleStatus().GetStatus())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, info.GetServingStatus())
	require.Nil(t, info.GetRoutingState())
}
