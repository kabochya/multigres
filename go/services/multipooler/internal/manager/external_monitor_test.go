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
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/tools/timer"
)

func newExternalMonitorManager(t *testing.T, expectedIdentity string) (*MultipoolerManager, *mock.QueryService) {
	t.Helper()
	initial := newTestMultipooler(clustermetadatapb.PoolerType_REPLICA, clustermetadatapb.PoolerServingStatus_DISABLED)
	initial.ShardKey = &clustermetadatapb.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"}
	initial.ManagementMode = clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	pm, err := NewMultipoolerManager(newTestLogger(), initial, &Config{
		ExternalBackend: &ExternalBackend{Host: "db.example.com", Port: 5432, Database: "db", ExpectedSystemIdentifier: expectedIdentity},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		pm.cancel()
		pm.shutdownCancel()
	})
	qs := mock.NewQueryService()
	pm.qsc = &mockPoolerController{queryService: qs}
	return pm, qs
}

func exactly(sql string) string { return "^" + regexp.QuoteMeta(sql) + "$" }

func TestExternalReadinessFailsClosedAndRecovers(t *testing.T) {
	pm, qs := newExternalMonitorManager(t, "123")
	for _, step := range []struct {
		name    string
		value   any
		err     error
		serving bool
	}{
		{name: "writable", value: true, serving: true},
		{name: "read only", value: false},
		{name: "recovered", value: true, serving: true},
		{name: "disconnected", err: errors.New("connection lost")},
		{name: "malformed", value: "invalid"},
		{name: "reconnected", value: true, serving: true},
	} {
		t.Run(step.name, func(t *testing.T) {
			if step.err != nil {
				qs.AddQueryPatternOnceWithError(exactly(externalReadinessQuery), step.err)
			} else {
				qs.AddQueryPatternOnce(exactly(externalReadinessQuery), mock.MakeQueryResult([]string{"writable"}, [][]any{{step.value}}))
			}
			if step.serving {
				qs.AddQueryPatternOnce(exactly(externalIdentityQuery), mock.MakeQueryResult([]string{"sysid", "database"}, [][]any{{"123", "db"}}))
			}
			pm.checkExternalReadiness(t.Context())
			require.Equal(t, step.serving, pm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING)
			require.Equal(t, step.serving, pm.record.RoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
			require.Nil(t, pm.record.RoutingState().GetRule(), "an external backend never carries a consensus rule")
			require.Equal(t, step.serving, pm.healthStreamer.getState().BackendReady)
			require.NoError(t, qs.ExpectationsWereMet())
		})
	}
}

// TestExternalIdentityMismatchWithdrawsReadiness verifies that an endpoint that
// answers but reports another system identifier or database is never served
// from, and that readiness returns once the intended identity is back.
func TestExternalIdentityMismatchWithdrawsReadiness(t *testing.T) {
	pm, qs := newExternalMonitorManager(t, "expected")
	for _, tc := range []struct {
		sysID, database string
		serving         bool
	}{
		{"expected", "db", true},
		{"another", "db", false},
		{"expected", "otherdb", false},
		{"expected", "db", true},
	} {
		qs.AddQueryPatternOnce(exactly(externalReadinessQuery), mock.MakeQueryResult([]string{"writable"}, [][]any{{true}}))
		qs.AddQueryPatternOnce(exactly(externalIdentityQuery), mock.MakeQueryResult([]string{"sysid", "database"}, [][]any{{tc.sysID, tc.database}}))
		pm.checkExternalReadiness(t.Context())
		require.Equal(t, tc.serving, pm.record.ServingStatus() == clustermetadatapb.PoolerServingStatus_SERVING, "%+v", tc)
		require.Equal(t, tc.serving, pm.healthStreamer.getState().BackendReady, "%+v", tc)
	}
	require.NoError(t, qs.ExpectationsWereMet())
}

func TestExternalBackendWithoutExpectedIdentityNeverServes(t *testing.T) {
	pm, qs := newExternalMonitorManager(t, "")
	qs.AddQueryPatternOnce(exactly(externalReadinessQuery), mock.MakeQueryResult([]string{"writable"}, [][]any{{true}}))
	qs.AddQueryPatternOnce(exactly(externalIdentityQuery), mock.MakeQueryResult([]string{"sysid", "database"}, [][]any{{"123", "db"}}))
	pm.checkExternalReadiness(t.Context())
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
}

func TestUnmanagedOpensClosed(t *testing.T) {
	pm, _ := newExternalMonitorManager(t, "123")
	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)
	// The monitor is what opens an unmanaged pooler; Open alone must not serve.
	pm.pgMonitor = noopRunner{}
	pm.Open(lockCtx)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, pm.record.ServingStatus())
}

type noopRunner struct{}

func (noopRunner) StartWithOptions(func(ctx context.Context), ...timer.StartOption) bool { return true }
func (noopRunner) Stop()                                                                 {}
