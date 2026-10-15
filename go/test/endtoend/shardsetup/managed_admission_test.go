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

// TestDefaultCohortAdmissionSurvivesFailover: the default cohort, run with
// --admission-control, follows the persisted admission state of the default
// tablegroup. It starts closed, and a failover in a fenced cohort does not reopen
// it: the new primary starts closed, reads FENCED from its own copy, and stays
// closed until it is told to unfence.
func TestDefaultCohortAdmissionSurvivesFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()

	// Three poolers: failing over needs a majority of the outgoing cohort.
	def, cleanup := NewIsolated(t,
		WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"),
		WithMultipoolerExtraArgs("--admission-control", "--admission-drain-timeout=3s"))
	defer cleanup()

	// Closed until a state says otherwise: there is no row for the tablegroup yet.
	primary := def.GetPrimary(t)
	requireStaysClosed(t, primary.Multipooler.GrpcPort, 4*time.Second, "a cohort with no admission state must stay closed")
	requireRejectedAsPending(t, primary.Multipooler.GrpcPort)

	setServingRow(t, def, "default", "", "UNFENCED", "r0")
	requireAdmits(t, primary.Multipooler.GrpcPort, "an UNFENCED default cohort must admit queries")

	// Fence the whole cohort.
	setServingRow(t, def, "default", "", "FENCING", "fence-1")
	for name, inst := range def.Multipoolers {
		resp, err := refreshAdmission(ctx, inst.Multipooler.GrpcPort, def.Database, "default", "FENCING", "fence-1")
		require.NoError(t, err, name)
		require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.AppliedState, name)
	}
	setServingRow(t, def, "default", "", "FENCED", "fence-1")
	require.False(t, admits(t, primary.Multipooler.GrpcPort))
	requireRejectedAsAdmissionClosed(t, primary.Multipooler.GrpcPort)

	// Fail over the fenced cohort.
	def.StartMultiorchs(ctx, t)
	oldPrimary := primary.Name
	client, err := NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	def.KillPostgres(t, oldPrimary)
	newPrimaryName := WaitForNewPrimary(t, def, oldPrimary, 90*time.Second)
	require.NotEqual(t, oldPrimary, newPrimaryName)
	def.PrimaryName = newPrimaryName
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)
	client.Close()

	// The new primary is writable but closed: promotion never reopens admission.
	newPrimary := def.GetMultipoolerInstance(newPrimaryName)
	requireStaysClosed(t, newPrimary.Multipooler.GrpcPort, 6*time.Second, "a failover in a fenced cohort must not reopen it")
	requireRejectedAsAdmissionClosed(t, newPrimary.Multipooler.GrpcPort)

	// The coordinator unfences; the new primary reads the state again and opens.
	setServingRow(t, def, "default", "", "UNFENCING", "unfence-1")
	require.Eventually(t, func() bool {
		resp, err := refreshAdmission(ctx, newPrimary.Multipooler.GrpcPort, def.Database, "default", "UNFENCING", "unfence-1")
		return err == nil && resp.AppliedState == multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	}, 30*time.Second, time.Second)
	requireAdmits(t, newPrimary.Multipooler.GrpcPort, "the new primary did not open after the unfence")
}
