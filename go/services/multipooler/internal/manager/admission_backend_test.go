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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// TestManagedAdmissionBackend: a pooler of a managed cohort guards against its own promotion and
// is listed in topology before it reads.
func TestManagedAdmissionBackend(t *testing.T) {
	managed := newAdmissionHarness(t, false).pm.admissionBackend
	require.IsType(t, managedAdmissionBackend{}, managed)
	require.True(t, managed.ClosesOnRoleChange())
	require.False(t, managed.RegisteredBeforeStart())
	require.Empty(t, managed.ConnectionName())
}

// TestManagedBackendMayOpenBeforeItIsReadyUnlessItIsTheLeader: a managed pooler that is not the
// writable leader cannot commit, and the serving status refuses requests until its backend is
// ready, so it may open; the leader must be serving.
func TestManagedBackendMayOpenBeforeItIsReadyUnlessItIsTheLeader(t *testing.T) {
	h := newAdmissionHarness(t, false)
	backend := h.pm.admissionBackend
	require.NoError(t, backend.ReadyToAdmit(), "serving")
	require.NoError(t, h.pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	require.NoError(t, backend.ReadyToAdmit(), "a replica that is not serving yet may open")
}
