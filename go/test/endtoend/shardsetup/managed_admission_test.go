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

// TestManagedPoolerRestartedAcrossAFenceComesBackClosed: a pooler of the default cohort that was
// open, announced shutdown, and was restarted after its cohort was fenced without
// it must come back closed, register, take part in the next operation, and open
// only when the cohort is unfenced.
func TestManagedPoolerRestartedAcrossAFenceComesBackClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()

	def := startControlledDefaultCohort(t)
	dest := def
	primary := dest.GetPrimary(t)
	var restarted *MultipoolerInstance
	for name, inst := range dest.Multipoolers {
		if name != primary.Name {
			restarted = inst
			break
		}
	}
	require.NotNil(t, restarted)
	requireAdmits(t, restarted.Multipooler.GrpcPort, "the replica must be open before it is restarted")

	// It announces shutdown, so no membership snapshot waits for it.
	restarted.Multipooler.TerminateGracefully(t.Logf, 15*time.Second)

	ctl := newControlClient(t, def.PrimaryMultipooler(t).GrpcPort)
	resp, err := ctl.fence(ctx, "default", "fence-restart-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 2, "only the two running poolers are asked")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.AdmissionState)

	// The restarted pooler reads FENCED and stays closed.
	require.NoError(t, restarted.Multipooler.Start(ctx, t))
	requireStaysClosed(t, restarted.Multipooler.GrpcPort, 6*time.Second, "a pooler restarted into a fenced cohort must stay closed")

	// It is registered, so the unfence includes it, and it opens.
	resp, err = ctl.unfence(ctx, "default", "unfence-restart-1", "fence-restart-1")
	require.NoError(t, err)
	require.Len(t, resp.Acks, 3, "the restarted pooler is part of the unfence")
	requireAdmits(t, restarted.Multipooler.GrpcPort, "the restarted pooler did not open after the unfence")
}
