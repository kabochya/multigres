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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

func TestAllowNonDefaultTableGroupLiftsMVPValidation(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()

	newManager := func(tableGroup, shard string, allow bool) (*MultipoolerManager, error) {
		return NewMultipoolerManager(newTestLogger(), &clustermetadatapb.Multipooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "dest-1"},
			Hostname: "localhost",
			PortMap:  map[string]int32{"grpc": 8080},
			ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: tableGroup, Shard: shard},
		}, &Config{TopoClient: ts, AllowNonDefaultTableGroup: allow})
	}

	_, err := newManager("destTG", constants.DefaultShard, false)
	require.ErrorContains(t, err, "only default tablegroup is supported", "the restriction stays on by default")

	pm, err := newManager("destTG", constants.DefaultShard, true)
	require.NoError(t, err)
	pm.ShutdownForTest(t.Context())

	// An empty tablegroup or shard is still invalid.
	_, err = newManager("", constants.DefaultShard, true)
	require.ErrorContains(t, err, "TableGroup is required")
	_, err = newManager("destTG", "", true)
	require.ErrorContains(t, err, "Shard is required")
}

// TestNonDefaultCohortSkipsGlobalMultischemaTables verifies that a pooler of a
// non-default cohort creates the sidecar schema without the tablegroup,
// tablegroup_table and shard tables, and inserts no multischema rows.
func TestNonDefaultCohortSkipsGlobalMultischemaTables(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, "destTG", constants.DefaultShard)
	pm.config.AllowNonDefaultTableGroup = true

	for _, stmt := range []string{
		"CREATE SCHEMA multigres",
		"CREATE TABLE multigres.heartbeat",
		"CREATE UNLOGGED TABLE IF NOT EXISTS multigres.backend_vpid",
		"CREATE TABLE IF NOT EXISTS multigres.pgbackrest_repos",
		"CREATE TABLE multigres.current_rule",
		"INSERT INTO multigres.current_rule",
		"CREATE TABLE multigres.rule_history",
	} {
		qs.AddQueryPatternOnce(stmt, mock.MakeQueryResult(nil, nil))
	}
	require.NoError(t, pm.createSidecarSchema(t.Context(), testBootstrapPolicy()))
	// Any global-table statement would have failed as unexpected by the mock.
	require.NoError(t, qs.ExpectationsWereMet())

	// No tablegroup or shard rows are written either.
	require.NoError(t, pm.initializeMultischemaData(t.Context()))
}

func TestNonDefaultTableGroupStillRejectedWithoutTheFlag(t *testing.T) {
	pm, _ := newTestManagerWithMock(t, "destTG", constants.DefaultShard)
	err := pm.initializeMultischemaData(t.Context())
	require.ErrorContains(t, err, "only default tablegroup is supported")
}
