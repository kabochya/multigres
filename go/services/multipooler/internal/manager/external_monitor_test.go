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
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

func TestExternalReadinessFailsClosedAndRecovers(t *testing.T) {
	initial := newTestMultipooler(pb.PoolerType_REPLICA, pb.PoolerServingStatus_DISABLED)
	initial.ShardKey = &pb.ShardKey{Database: "db", TableGroup: "default", Shard: "0-inf"}
	initial.ManagementMode = pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	pm, err := NewMultipoolerManager(newTestLogger(), initial, &Config{})
	require.NoError(t, err)
	defer pm.cancel()
	defer pm.shutdownCancel()
	qs := mock.NewQueryService()
	pm.qsc = &mockPoolerController{queryService: qs}
	pattern := "^" + regexp.QuoteMeta(externalReadinessQuery) + "$"
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
				qs.AddQueryPatternOnceWithError(pattern, step.err)
			} else {
				qs.AddQueryPatternOnce(pattern, mock.MakeQueryResult([]string{"writable"}, [][]any{{step.value}}))
			}
			pm.checkExternalReadiness(t.Context())
			require.Equal(t, step.serving, pm.record.ServingStatus() == pb.PoolerServingStatus_SERVING)
			require.Equal(t, step.serving, pm.record.RoutingState().GetRole() == pb.RoutingRole_ROUTING_ROLE_PRIMARY)
			require.Nil(t, pm.record.RoutingState().GetRule())
			require.NoError(t, qs.ExpectationsWereMet())
		})
	}
	require.Equal(t, 6, qs.AdminQueryCount())
}
