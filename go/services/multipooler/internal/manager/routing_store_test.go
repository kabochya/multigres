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
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// TestTheRoutingStoreIsRegistered: the store of admission fences and routing rules is what the
// prototype registers; without it (a build that drops routing_store.go) it stays nil and the
// calls answer UNIMPLEMENTED.
func TestTheRoutingStoreIsRegistered(t *testing.T) {
	require.NotNil(t, newRoutingStore)
}

const snapshotJSONTemplate = `{"routing_version": 3, "home": "src",
 "rules": [{"operation_id": "0a0a0a0a-0000-4000-8000-00000000000a", "from": "src", "to": "default", "to_shard": "0-inf", "created_version": 3}],
 "fences": [{"id": 7, "operation_id": "0a0a0a0a-0000-4000-8000-00000000000a", "tablegroup": "src", "state": "FENCED", "version": 2, "updated_at": 1700000000.5}]}`

// TestPrototypeRoutingStoreReadsEverythingInOneStatement: the snapshot is one query, so what it
// returns is one snapshot, and it decodes the home, the rules and the fences.
func TestPrototypeRoutingStoreReadsEverythingInOneStatement(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	qs.AddQueryPattern("json_build_object", mock.MakeQueryResult([]string{"j"}, [][]any{{snapshotJSONTemplate}}))
	snap, err := newPrototypeRoutingStore(pm).Snapshot(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, qs.AdminQueryCount(), "one statement, and nothing is created")
	require.EqualValues(t, 3, snap.routingVersion)
	require.Equal(t, "src", snap.home)
	require.Equal(t, "default", snap.effective())
	require.Equal(t, []ruleRow{{operationID: "0a0a0a0a-0000-4000-8000-00000000000a", from: "src", to: "default", toShard: "0-inf", createdVersion: 3}}, snap.rules)
	require.Len(t, snap.fences, 1)
	f := snap.fenceOf("src")
	require.NotNil(t, f)
	require.EqualValues(t, 7, f.id)
	require.Equal(t, stateFenced, f.state)
	require.EqualValues(t, 2, f.version)
	require.False(t, f.updatedAt.IsZero())
	require.Nil(t, snap.fenceOf("default"))
}

// TestPrototypeRoutingStoreReadsAMissingTableAsEmpty: a cluster that has never fenced anything
// has no tables for fences; reading does not create them, and any other failure is an error,
// never "unfenced".
func TestPrototypeRoutingStoreReadsAMissingTableAsEmpty(t *testing.T) {
	t.Run("no table", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("json_build_object", mterrors.NewPgError("ERROR", mterrors.PgSSUndefinedTable, `relation "multigres.catalog_state" does not exist`, ""))
		snap, err := newPrototypeRoutingStore(pm).Snapshot(t.Context())
		require.NoError(t, err)
		require.Empty(t, snap.fences)
		require.Empty(t, snap.home)
		require.EqualValues(t, 1, qs.AdminQueryCount(), "nothing is created")
	})
	t.Run("an empty catalog", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPattern("json_build_object", mock.MakeQueryResult([]string{"j"}, [][]any{{`{"routing_version": null, "home": null, "rules": [], "fences": []}`}}))
		snap, err := newPrototypeRoutingStore(pm).Snapshot(t.Context())
		require.NoError(t, err)
		require.Empty(t, snap.home)
		require.Zero(t, snap.routingVersion)
	})
	t.Run("any other failure is an error", func(t *testing.T) {
		pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
		qs.AddQueryPatternWithError("json_build_object", mterrors.NewPgError("ERROR", mterrors.PgSSInsufficientPrivilege, "permission denied", ""))
		_, err := newPrototypeRoutingStore(pm).Snapshot(t.Context())
		require.Error(t, err)
	})
}

// TestPrototypeRoutingStoreCreatesItsTablesOncePerProcess: reads never create anything, a write
// creates the tables in one statement the first time, and once that has succeeded later writes do
// not run it again; the store of a manager is built once.
func TestPrototypeRoutingStoreCreatesItsTablesOncePerProcess(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	qs.AddQueryPattern("DO \\$\\$", mock.MakeQueryResult(nil, nil))
	qs.AddQueryPattern("UPDATE multigres.admission_fence", mock.MakeQueryResult([]string{"n"}, [][]any{{int64(1)}}))
	store := newRoutingStore(pm)
	require.Same(t, store, newRoutingStore(pm), "one store per manager")
	prev := &fenceRow{id: 1, operationID: opA, state: stateUnfenced}
	for range 3 {
		ok, err := store.WriteFence(t.Context(), fenceWrite{tablegroup: "tg", operationID: opA, prev: prev, state: stateFencing, version: 1})
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.EqualValues(t, 4, qs.AdminQueryCount(), "the schema statement once, then three writes")
}

// TestPrototypeRoutingStoreWritesAFenceWithTheInsertOrTheCompareAndSet: a write that names no
// fence inserts one and loses to a concurrent insert (no row returned); a write that names a
// fence updates it only if it is still exactly that.
func TestPrototypeRoutingStoreWritesAFenceWithTheInsertOrTheCompareAndSet(t *testing.T) {
	pm, qs := newTestManagerWithMock(t, constants.DefaultTableGroup, "0-inf")
	qs.AddQueryPattern("DO \\$\\$", mock.MakeQueryResult(nil, nil))
	qs.AddQueryPattern("INSERT INTO multigres.admission_fence", mock.MakeQueryResult(nil, nil))
	qs.AddQueryPattern("UPDATE multigres.admission_fence", mock.MakeQueryResult(nil, nil))
	store := newPrototypeRoutingStore(pm)
	ok, err := store.WriteFence(t.Context(), fenceWrite{tablegroup: "tg", operationID: opA, state: stateFencing, version: 1})
	require.NoError(t, err)
	require.False(t, ok, "the insert returned no row: another operation created the fence first")
	ok, err = store.WriteFence(t.Context(), fenceWrite{tablegroup: "tg", operationID: opA, prev: &fenceRow{id: 4, operationID: opB, state: stateUnfenced, version: 2}, state: stateFencing, version: 3})
	require.NoError(t, err)
	require.False(t, ok, "the update matched no row: the fence changed")
}
