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

package shardsetup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// TestManagedCohortAdmissionSurvivesFailover: a managed cohort run with
// --admission-control follows the persisted admission state, and a failover in a
// fenced cohort does not reopen it: the new primary starts closed, reads FENCED,
// and stays closed until it is told to unfence.
func TestManagedCohortAdmissionSurvivesFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()

	def, cleanupDef := NewIsolated(t, WithMultipoolerCount(2))
	defer cleanupDef()
	// The cohort's bootstrap checks its poolers by querying them, so they must be
	// admitting: seed UNFENCED before they start.
	setServingRow(t, def, "destTG", "", "UNFENCED", "r0")
	dest, cleanupDest := NewIsolated(t,
		WithParentCluster(def), WithTableGroup("destTG"), WithNamePrefix("dest"),
		// Three poolers: failing over needs a majority of the outgoing cohort.
		WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"),
		WithMultipoolerExtraArgs("--admission-control", "--admission-drain-timeout=3s"))
	defer cleanupDest()

	primary := dest.GetPrimary(t)
	requireAdmits(t, primary.Multipooler.GrpcPort, "an UNFENCED managed cohort must admit queries")

	// Fence the whole cohort.
	setServingRow(t, def, "destTG", "", "FENCING", "fence-1")
	for name, inst := range dest.Multipoolers {
		resp, err := refreshAdmission(ctx, inst.Multipooler.GrpcPort, dest.Database, "destTG", "FENCING", "fence-1")
		require.NoError(t, err, name)
		require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.AppliedState, name)
	}
	setServingRow(t, def, "destTG", "", "FENCED", "fence-1")
	require.False(t, admits(t, primary.Multipooler.GrpcPort))
	requireRejectedAsPlannedFailover(t, primary.Multipooler.GrpcPort)

	// Fail over the fenced cohort.
	dest.StartMultiorchs(ctx, t)
	oldPrimary := primary.Name
	client, err := NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	dest.KillPostgres(t, oldPrimary)
	newPrimaryName := WaitForNewPrimary(t, dest, oldPrimary, 90*time.Second)
	require.NotEqual(t, oldPrimary, newPrimaryName)
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)
	client.Close()

	// The new primary is writable but closed: promotion never reopens admission.
	newPrimary := dest.GetMultipoolerInstance(newPrimaryName)
	requireStaysClosed(t, newPrimary.Multipooler.GrpcPort, 6*time.Second, "a failover in a fenced cohort must not reopen it")
	requireRejectedAsPlannedFailover(t, newPrimary.Multipooler.GrpcPort)

	// The coordinator unfences; the new primary reads the state again and opens.
	setServingRow(t, def, "destTG", "", "UNFENCING", "unfence-1")
	require.Eventually(t, func() bool {
		resp, err := refreshAdmission(ctx, newPrimary.Multipooler.GrpcPort, dest.Database, "destTG", "UNFENCING", "unfence-1")
		return err == nil && resp.AppliedState == multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	}, 30*time.Second, time.Second)
	requireAdmits(t, newPrimary.Multipooler.GrpcPort, "the new primary did not open after the unfence")
}
