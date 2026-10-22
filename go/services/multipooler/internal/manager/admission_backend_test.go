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

// TestAdmissionBackendIsChosenFromTheManagementMode: a pooler of a managed cohort guards against
// its own promotion and is listed in topology before it reads; one that fronts an external
// endpoint does neither, and fronts the connection it was told to.
func TestAdmissionBackendIsChosenFromTheManagementMode(t *testing.T) {
	managed := newAdmissionHarness(t, false).pm.admissionBackend
	require.IsType(t, managedAdmissionBackend{}, managed)
	require.True(t, managed.ClosesOnRoleChange())
	require.False(t, managed.RegisteredBeforeStart())
	require.Empty(t, managed.ConnectionName())

	unmanaged := newAdmissionHarness(t, true).pm.admissionBackend
	require.IsType(t, unmanagedAdmissionBackend{}, unmanaged)
	require.False(t, unmanaged.ClosesOnRoleChange())
	require.True(t, unmanaged.RegisteredBeforeStart())
	require.Equal(t, "src", unmanaged.ConnectionName())
}

// TestUnmanagedBackendIsReadyOnlyWhenServingAndValidated: the external endpoint is the writable
// one, so a pooler that is not serving, or whose endpoint has not passed identity validation,
// must not open.
func TestUnmanagedBackendIsReadyOnlyWhenServingAndValidated(t *testing.T) {
	h := newAdmissionHarness(t, true)
	backend := h.pm.admissionBackend
	require.NoError(t, backend.ReadyToAdmit())

	h.pm.healthStreamer.setBackendReady(false)
	require.ErrorContains(t, backend.ReadyToAdmit(), "external backend is not ready")

	h.pm.healthStreamer.setBackendReady(true)
	require.NoError(t, h.pm.healthStreamer.OnStateChange(t.Context(), servingstate.State{ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))
	require.ErrorContains(t, backend.ReadyToAdmit(), "backend is not serving")
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
