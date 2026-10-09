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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// gatewayQuery runs a scalar query through the cluster's multigateway.
func gatewayQuery(t *testing.T, s *ShardSetup, sql string) string {
	t.Helper()
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		s.MultigatewayPgPort, TestPostgresPassword))
	require.NoError(t, err)
	defer conn.Close(context.Background())
	var out string
	require.NoError(t, conn.QueryRow(ctx, sql).Scan(&out))
	return out
}

func tableExists(t *testing.T, conn *pgx.Conn, table string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists))
	return exists
}

// TestSecondManagedCohortRunsBesideDefault runs a "destTG" cohort next to the
// default cohort in one cluster: separate postgres instances, one etcd, one
// database. It checks the cohorts are isolated, that the non-default cohort
// carries no global multischema tables, that a role can have an identical SCRAM
// verifier on both, and that a failover in destTG leaves the default cohort
// untouched.
func TestSecondManagedCohortRunsBesideDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()

	def, cleanupDef := NewIsolated(t, WithMultipoolerCount(2), WithMultigateway())
	defer cleanupDef()
	dest, cleanupDest := NewIsolated(t,
		WithParentCluster(def), WithTableGroup("destTG"), WithNamePrefix("dest"),
		// Three poolers: failing over needs a majority of the outgoing cohort.
		WithMultipoolerCount(3), WithMultiorchCount(1), WithLeaderFailoverGracePeriod("0s", "0s"))
	defer cleanupDest()

	// Each cohort elected its own primary and registered its own shard key.
	defPrimary, destPrimary := def.GetPrimary(t), dest.GetPrimary(t)
	require.NotEqual(t, defPrimary.Name, destPrimary.Name)
	rec, err := dest.TopoServer.GetMultipooler(ctx, dest.GetMultipoolerID(destPrimary.Name))
	require.NoError(t, err)
	require.Equal(t, "destTG", rec.ShardKey.TableGroup)
	require.Equal(t, "0-inf", rec.ShardKey.Shard)
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED, rec.ManagementMode, "destTG is a managed cohort")

	// Separate postgres instances: a table made on one is absent on the other.
	defConn, destConn := def.ConnectPrimaryAdmin(t), dest.ConnectPrimaryAdmin(t)
	_, err = destConn.Exec(ctx, "CREATE TABLE only_on_dest (id int)")
	require.NoError(t, err)
	require.False(t, tableExists(t, defConn, "only_on_dest"))

	// Only the default cohort holds the global multischema tables.
	require.True(t, tableExists(t, defConn, "multigres.tablegroup"))
	require.False(t, tableExists(t, destConn, "multigres.tablegroup"))
	require.False(t, tableExists(t, destConn, "multigres.shard"))
	// Both still have their own sidecar schema.
	require.True(t, tableExists(t, destConn, "multigres.heartbeat"))

	// A role can carry a byte-identical SCRAM verifier on both cohorts.
	_, err = defConn.Exec(ctx, "CREATE ROLE alice LOGIN PASSWORD 'alice-password'")
	require.NoError(t, err)
	verifier, err := RoleVerifier(ctx, defConn, "alice")
	require.NoError(t, err)
	require.NoError(t, CreateRoleWithVerifier(ctx, destConn, "alice", verifier))
	got, err := RoleVerifier(ctx, destConn, "alice")
	require.NoError(t, err)
	require.Equal(t, verifier, got)

	// The default cohort keeps serving through its gateway.
	require.Equal(t, "1", gatewayQuery(t, def, "SELECT 1"))

	// Fail over destTG: kill its primary's postgres and let its multiorch elect
	// another. The default cohort must not notice.
	dest.StartMultiorchs(ctx, t)
	oldDestPrimary := destPrimary.Name
	oldDefPrimary := defPrimary.Name

	client, err := NewMultipoolerClient(destPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)
	dest.KillPostgres(t, oldDestPrimary)

	newDestPrimary := WaitForNewPrimary(t, dest, oldDestPrimary, 90*time.Second)
	require.NotEmpty(t, newDestPrimary)
	require.NotEqual(t, oldDestPrimary, newDestPrimary)
	_, err = client.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)
	client.Close()

	// The default cohort is unchanged and still serving.
	stillPrimary := def.RefreshPrimary(t)
	require.Equal(t, oldDefPrimary, stillPrimary.Name, "a destTG failover must not move the default primary")
	require.Equal(t, "1", gatewayQuery(t, def, "SELECT 1"))
	var defTerm int64
	require.NoError(t, defConn.QueryRow(ctx, "SELECT decision_coordinator_term FROM multigres.current_rule").Scan(&defTerm))
	require.EqualValues(t, 1, defTerm, "the default cohort's consensus term must not have moved")
}
