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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/protometadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// setServingRow writes a tablegroup's persisted serving row on the default
// primary, standing in for the coordinator that will own these writes. backing
// is the backing connection name, empty for a managed cohort.
func setServingRow(t *testing.T, s *ShardSetup, tablegroup, backing, state, requestID string) {
	t.Helper()
	ctx := t.Context()
	conn := s.ConnectPrimaryAdmin(t)
	require.NoError(t, protometadata.EnsureSchema(ctx, func(ctx context.Context, sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}))
	_, err := conn.Exec(ctx, `INSERT INTO multigres.proto_tablegroup_serving (database, tablegroup, connection, admission_state, request_id)
VALUES ($1, $2, NULLIF($3, ''), $4, $5)
ON CONFLICT (database, tablegroup) DO UPDATE
SET connection = EXCLUDED.connection, admission_state = EXCLUDED.admission_state,
    request_id = EXCLUDED.request_id, updated_at = now()`, s.Database, tablegroup, backing, state, requestID)
	require.NoError(t, err)
}

func stateOf(name string) multipoolerservicepb.AdmissionState {
	return multipoolerservicepb.AdmissionState(multipoolerservicepb.AdmissionState_value["ADMISSION_STATE_"+name])
}

// refreshAdmission calls RefreshAdmission on one pooler, as the coordinator does.
func refreshAdmission(ctx context.Context, grpcPort int, database, tablegroup, state, requestID string) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return multipoolerservicepb.NewMultipoolerServiceClient(conn).RefreshAdmission(ctx, &multipoolerservicepb.RefreshAdmissionRequest{
		Database: database, Tablegroup: tablegroup, ExpectedState: stateOf(state), RequestId: requestID,
	})
}

// admits reports whether the pooler admits application queries.
func admits(t *testing.T, grpcPort int) bool {
	t.Helper()
	client, err := NewMultipoolerClient(grpcPort)
	if err != nil {
		return false
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = client.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	return err == nil
}

func requireAdmits(t *testing.T, grpcPort int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return admits(t, grpcPort) }, 30*time.Second, 200*time.Millisecond, msg)
}

func requireStaysClosed(t *testing.T, grpcPort int, d time.Duration, msg string) {
	t.Helper()
	testpoll.Never(t, func() bool { return admits(t, grpcPort) }, d, 250*time.Millisecond, msg)
}

func requireRejectedAsPlannedFailover(t *testing.T, grpcPort int) {
	t.Helper()
	client, err := NewMultipoolerClient(grpcPort)
	require.NoError(t, err)
	defer client.Close()
	_, err = client.Pooler.ExecuteQuery(t.Context(), "SELECT 1", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "planned failover", "a closed pooler rejects with the bufferable error")
}

// waitBackendReady waits until the pooler has validated its external backend.
func waitBackendReady(t *testing.T, p *unmanagedPooler) {
	t.Helper()
	require.Eventually(t, func() bool {
		h, err := p.firstHealth(t.Context())
		return err == nil && h.GetBackendReady()
	}, 60*time.Second, 200*time.Millisecond, "backend never became ready")
}

func startAdmissionPooler(t *testing.T, s *ShardSetup, name, connection string, extra ...string) (*unmanagedPooler, int) {
	t.Helper()
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, connection, externalDSN(extPort), externalSystemIdentifier(t, extPort))
	p := startUnmanagedPooler(t, s, name, "postgres", connection, extra...)
	waitBackendReady(t, p)
	return p, extPort
}

// TestUnmanagedAdmissionStartsClosedAndFollowsPersistedState: a pooler with a
// ready backend still admits nothing until it has read an UNFENCED state, and
// afterwards follows fence and unfence refreshes, never acting on an expectation
// that differs from what the default primary persisted.
func TestUnmanagedAdmissionStartsClosedAndFollowsPersistedState(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	p, _ := startAdmissionPooler(t, s, "unmanaged-adm-1", "w4-follow")

	// No admission row yet: closed, with a backend that is ready.
	requireStaysClosed(t, p.grpcPort, 4*time.Second, "a pooler with no admission state must stay closed")
	requireRejectedAsPlannedFailover(t, p.grpcPort)

	// Persisting UNFENCED opens it, with no refresh.
	setServingRow(t, s, "migrateTG", "w4-follow", "UNFENCED", "r0")
	requireAdmits(t, p.grpcPort, "pooler never opened after UNFENCED was persisted")

	// Fence: the coordinator persists FENCING, then refreshes.
	setServingRow(t, s, "migrateTG", "w4-follow", "FENCING", "fence-1")
	resp, err := refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "FENCING", "fence-1")
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.AppliedState)
	require.NotEmpty(t, resp.ProcessIncarnation)
	require.Equal(t, "unmanaged-adm-1", resp.PoolerId.GetName())
	require.False(t, admits(t, p.grpcPort))
	requireRejectedAsPlannedFailover(t, p.grpcPort)

	// A repeat is acknowledged again.
	_, err = refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "FENCING", "fence-1")
	require.NoError(t, err)

	// An expectation that differs from the persisted state is refused and applies
	// nothing, including a delayed unfence of an older operation.
	_, err = refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "FENCING", "some-other-request")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	_, err = refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "UNFENCING", "stale-unfence")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.False(t, admits(t, p.grpcPort), "a rejected refresh must not open the pooler")

	// Unfence.
	setServingRow(t, s, "migrateTG", "w4-follow", "UNFENCING", "unfence-1")
	resp, err = refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "UNFENCING", "unfence-1")
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, resp.AppliedState)
	requireAdmits(t, p.grpcPort, "pooler did not admit after UNFENCING")

	// A control RPC still works while closed: fence again and read back health.
	setServingRow(t, s, "migrateTG", "w4-follow", "FENCED", "fence-2")
	_, err = refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "FENCED", "fence-2")
	require.NoError(t, err)
	h, err := p.firstHealth(ctx)
	require.NoError(t, err)
	require.True(t, h.GetBackendReady(), "a fenced pooler still reports its backend as ready")
}

// TestUnmanagedPoolerStaysClosedAcrossRestartAndWhenJoiningDuringFencing: a pooler
// restarted while FENCED, and a new one that joins while FENCING, never admit.
func TestUnmanagedPoolerStaysClosedAcrossRestartAndWhenJoiningDuringFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	s := getSharedSetup(t)
	extPort := startExternalPostgres(t)
	seedBackingConnection(t, s, "w4-restart", externalDSN(extPort), externalSystemIdentifier(t, extPort))
	setServingRow(t, s, "migrateTG", "w4-restart", "FENCED", "fence-1")

	first := startUnmanagedPooler(t, s, "unmanaged-adm-2", "postgres", "w4-restart")
	waitBackendReady(t, first)
	requireStaysClosed(t, first.grpcPort, 4*time.Second, "a pooler started while FENCED must stay closed")

	// Restart it while still FENCED.
	first.stop()
	restarted := startUnmanagedPooler(t, s, "unmanaged-adm-2", "postgres", "w4-restart")
	waitBackendReady(t, restarted)
	requireStaysClosed(t, restarted.grpcPort, 4*time.Second, "a restarted pooler must stay closed while FENCED")

	// A pooler that joins while a fence is in progress stays closed too.
	setServingRow(t, s, "migrateTG", "w4-restart", "FENCING", "fence-2")
	joined := startUnmanagedPooler(t, s, "unmanaged-adm-3", "postgres", "w4-restart")
	waitBackendReady(t, joined)
	requireStaysClosed(t, joined.grpcPort, 4*time.Second, "a pooler that joins during FENCING must stay closed")

	// A pooler that has decided to stay closed reopens only when the coordinator
	// asks it to refresh: persisting UNFENCING alone changes nothing.
	setServingRow(t, s, "migrateTG", "w4-restart", "UNFENCING", "unfence-1")
	requireStaysClosed(t, restarted.grpcPort, 3*time.Second, "persisted state alone must not reopen a pooler that decided to stay closed")
	for _, p := range []*unmanagedPooler{restarted, joined} {
		resp, err := refreshAdmission(t.Context(), p.grpcPort, s.Database, "migrateTG", "UNFENCING", "unfence-1")
		require.NoError(t, err)
		require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, resp.AppliedState)
		requireAdmits(t, p.grpcPort, "pooler did not open after the unfence refresh")
	}
}

// TestFenceTerminatesLongTransactionAtDrainDeadline: a transaction that outlives
// the drain deadline is terminated, and only then is the fence acknowledged.
func TestFenceTerminatesLongTransactionAtDrainDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	ctx := t.Context()
	s := getSharedSetup(t)
	const drain = 2 * time.Second
	p, extPort := startAdmissionPooler(t, s, "unmanaged-adm-4", "w4-drain", "--admission-drain-timeout="+drain.String())
	setServingRow(t, s, "migrateTG", "w4-drain", "UNFENCED", "r0")
	requireAdmits(t, p.grpcPort, "pooler never opened")

	// Open a transaction through the pooler and leave it open.
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", p.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := multipoolerservicepb.NewMultipoolerServiceClient(conn)
	stream, err := client.StreamExecute(ctx, &multipoolerservicepb.StreamExecuteRequest{
		Query:              "SELECT 1",
		Target:             &querypb.Target{},
		Options:            &querypb.ExecuteOptions{},
		ReservationOptions: &querypb.ReservationOptions{Reasons: uint32(multipoolerservicepb.ReservationReason_RESERVATION_REASON_TRANSACTION)},
	})
	require.NoError(t, err)
	var reservedID uint64
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if rs := resp.GetReservedState(); rs.GetReservedConnectionId() != 0 {
			reservedID = rs.GetReservedConnectionId()
		}
	}
	require.NotZero(t, reservedID, "the transaction must hold a reserved connection")

	ext, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer ext.Close(context.Background())
	idleInTx := func() int {
		var n int
		require.NoError(t, ext.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname = 'postgres' AND state = 'idle in transaction'").Scan(&n))
		return n
	}
	require.Equal(t, 1, idleInTx(), "the pooler's transaction is open on the external database")

	// Fence. The transaction never finishes, so the call must take about the drain
	// deadline, terminate it, and then acknowledge.
	setServingRow(t, s, "migrateTG", "w4-drain", "FENCING", "fence-1")
	start := time.Now()
	resp, err := refreshAdmission(ctx, p.grpcPort, s.Database, "migrateTG", "FENCING", "fence-1")
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.AppliedState)
	require.GreaterOrEqual(t, elapsed, drain-250*time.Millisecond, "the fence must wait for the drain deadline before terminating")
	require.Less(t, elapsed, drain+10*time.Second)

	// Acknowledged means nothing of the application's is left on the source.
	require.Zero(t, idleInTx(), "the long transaction must be terminated before the fence is acknowledged")
	require.False(t, admits(t, p.grpcPort))
}
