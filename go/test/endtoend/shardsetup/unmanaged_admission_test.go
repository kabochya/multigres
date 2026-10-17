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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/protometadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/testpoll"
)

// setServingRow writes a tablegroup's persisted serving row on the default
// primary, standing in for the coordinator that owns these writes. backing is the
// backing connection name, empty for a managed cohort.
// Admission states, as the e2e tests name them.
const (
	admUnfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	admFencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	admFenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
)

// admStateOf is the state a fenced flag stands for in these tests: a completed fence.
func admStateOf(fenced bool) multipoolerservicepb.AdmissionState {
	if fenced {
		return admFenced
	}
	return admUnfenced
}

// closedRow reports whether a tablegroup refuses application work: FENCING or FENCED.
func closedRow(row *multipoolerservicepb.TablegroupServingState) bool {
	return row.GetState() != admUnfenced
}

func setServingRow(t *testing.T, s *ShardSetup, tablegroup, backing string, fenced bool, version int64) {
	setServingRowState(t, s, tablegroup, backing, admStateOf(fenced), version)
}

// setServingRowState writes a tablegroup's persisted serving row in the given state.
func setServingRowState(t *testing.T, s *ShardSetup, tablegroup, backing string, state multipoolerservicepb.AdmissionState, version int64) {
	t.Helper()
	ctx := t.Context()
	conn := s.ConnectPrimaryAdmin(t)
	require.NoError(t, protometadata.EnsureSchema(ctx, func(ctx context.Context, sql string) error {
		_, err := conn.Exec(ctx, sql)
		return err
	}))
	_, err := conn.Exec(ctx, `INSERT INTO multigres.proto_tablegroup_serving (database, tablegroup, connection, state, version)
VALUES ($1, $2, NULLIF($3, ''), $4, $5)
ON CONFLICT (database, tablegroup) DO UPDATE
SET connection = EXCLUDED.connection, state = EXCLUDED.state, version = EXCLUDED.version, updated_at = now()`,
		s.Database, tablegroup, backing, strings.TrimPrefix(state.String(), "ADMISSION_STATE_"), version)
	require.NoError(t, err)
}

// refreshAdmission calls RefreshAdmission on one pooler, as the coordinator does.
func refreshAdmission(ctx context.Context, grpcPort int, database, tablegroup string, fenced bool, version int64) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	return refreshAdmissionWithin(ctx, grpcPort, database, tablegroup, fenced, version, 3*time.Second)
}

// refreshAdmissionWithin is refreshAdmission with the drain timeout the coordinator would
// pass on from its fence request.
func refreshAdmissionWithin(ctx context.Context, grpcPort int, database, tablegroup string, fenced bool, version int64, drain time.Duration) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	return refreshAdmissionState(ctx, grpcPort, database, tablegroup, admStateOf(fenced), version, drain)
}

// refreshAdmissionState is refreshAdmissionWithin naming the state the coordinator expects.
func refreshAdmissionState(ctx context.Context, grpcPort int, database, tablegroup string, state multipoolerservicepb.AdmissionState, version int64, drain time.Duration) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return multipoolerservicepb.NewMultipoolerServiceClient(conn).RefreshAdmission(ctx, &multipoolerservicepb.RefreshAdmissionRequest{
		Database: database, Tablegroup: tablegroup, ExpectedState: state, ExpectedVersion: version, DrainTimeout: durationpb.New(drain),
	})
}

// admits reports whether the pooler admits application queries.
func admits(t *testing.T, grpcPort int) bool {
	t.Helper()
	return admissionError(t, grpcPort) == nil
}

// admissionError is the error of an application query through the pooler, nil when it
// is admitted.
func admissionError(t *testing.T, grpcPort int) error {
	t.Helper()
	client, err := NewMultipoolerClient(grpcPort)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = client.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	return err
}

func requireAdmits(t *testing.T, grpcPort int, msg string) {
	t.Helper()
	var last error
	ok := assert.Eventually(t, func() bool {
		last = admissionError(t, grpcPort)
		return last == nil
	}, 30*time.Second, 200*time.Millisecond, msg)
	require.True(t, ok, "%s: last rejection: %v", msg, last)
}

// waitBackendReady waits until the pooler has validated its external backend.
func waitBackendReady(t *testing.T, p *unmanagedPooler) {
	t.Helper()
	require.Eventually(t, func() bool {
		h, err := p.firstHealth(t.Context())
		return err == nil && h.GetBackendReady()
	}, 60*time.Second, 200*time.Millisecond, "backend never became ready")
}

// startRoleScopedSource starts an external server with an application role "app" that has
// no privilege beyond connecting and, when stats is true, pg_read_all_stats, as a
// customer's application role is, and seeds the backing connection that reaches it as that
// role. It returns the external server's port.
func startRoleScopedSource(t *testing.T, s *ShardSetup, connection string, stats bool) int {
	t.Helper()
	ctx := t.Context()
	extPort := startExternalPostgres(t)
	admin, err := pgx.Connect(ctx, externalDSN(extPort))
	require.NoError(t, err)
	defer admin.Close(context.Background())
	queries := []string{
		"CREATE ROLE app LOGIN PASSWORD 'app-pw'",
		"CREATE ROLE other LOGIN PASSWORD 'other-pw'",
		"CREATE DATABASE elsewhere",
		"GRANT CONNECT ON DATABASE postgres TO app, other",
	}
	if stats {
		queries = append(queries, "GRANT pg_read_all_stats TO app")
	}
	for _, q := range queries {
		_, err := admin.Exec(ctx, q)
		require.NoError(t, err, q)
	}
	appDSN := fmt.Sprintf("postgres://app:app-pw@127.0.0.1:%d/postgres?sslmode=disable", extPort)
	seedBackingConnection(t, s, connection, appDSN, externalSystemIdentifier(t, extPort))
	return extPort
}

// TestUnmanagedPoolerDoesNotServeWithARoleThatCannotSeeOtherSessions: the pooler must see every
// other session's state to tell running work from an idle session (or an autovacuum worker),
// or every fence would stay busy while one exists. It does not serve, and says why, until the
// role can see them. Two ways to be blind, one after the other (a tablegroup has one unmanaged
// pooler):
//
//   - a role with neither superuser nor pg_read_all_stats;
//   - a member of pg_read_all_stats that does not inherit its privileges (a grant WITH INHERIT
//     FALSE, or NOINHERIT when it was granted), which sees other roles' sessions with no state.
func TestUnmanagedPoolerDoesNotServeWithARoleThatCannotSeeOtherSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	s := getSharedSetup(t)

	t.Run("neither superuser nor pg_read_all_stats", func(t *testing.T) {
		ctx := t.Context()
		extPort := startRoleScopedSource(t, s, "blind-role", false)
		p := startUnmanagedPooler(t, s, "unmanaged-adm-blind", "postgres", "blind-role")
		requireNeverServes(t, p)

		admin, err := pgx.Connect(ctx, externalDSN(extPort))
		require.NoError(t, err)
		defer admin.Close(context.Background())
		_, err = admin.Exec(ctx, "GRANT pg_monitor TO app")
		require.NoError(t, err)
		waitBackendReady(t, p)
	})

	t.Run("a member that does not inherit pg_read_all_stats", func(t *testing.T) {
		ctx := t.Context()
		extPort := startRoleScopedSource(t, s, "noinherit-role", true)
		admin, err := pgx.Connect(ctx, externalDSN(extPort))
		require.NoError(t, err)
		defer admin.Close(context.Background())
		_, err = admin.Exec(ctx, "REVOKE pg_read_all_stats FROM app")
		require.NoError(t, err)
		_, err = admin.Exec(ctx, "GRANT pg_read_all_stats TO app WITH INHERIT FALSE")
		require.NoError(t, err)
		p := startUnmanagedPooler(t, s, "unmanaged-adm-noinherit", "postgres", "noinherit-role")
		requireNeverServes(t, p)

		_, err = admin.Exec(ctx, "GRANT pg_read_all_stats TO app WITH INHERIT TRUE")
		require.NoError(t, err)
		waitBackendReady(t, p)
	})

	t.Run("a role that does not inherit", func(t *testing.T) {
		ctx := t.Context()
		extPort := startRoleScopedSource(t, s, "noinherit-role-level", true)
		admin, err := pgx.Connect(ctx, externalDSN(extPort))
		require.NoError(t, err)
		defer admin.Close(context.Background())
		// A grant made to a role that has NOINHERIT does not inherit, whatever it says about
		// the privileges it grants.
		for _, sql := range []string{
			"ALTER ROLE app NOINHERIT",
			"REVOKE pg_read_all_stats FROM app",
			"GRANT pg_read_all_stats TO app",
		} {
			_, err = admin.Exec(ctx, sql)
			require.NoError(t, err, sql)
		}
		p := startUnmanagedPooler(t, s, "unmanaged-adm-role-noinherit", "postgres", "noinherit-role-level")
		requireNeverServes(t, p)

		_, err = admin.Exec(ctx, "GRANT pg_read_all_stats TO app WITH INHERIT TRUE")
		require.NoError(t, err)
		waitBackendReady(t, p)
	})
}

// requireNeverServes requires that the pooler, asked repeatedly for a few seconds, reports
// that its backend is not ready (so it was reachable and answered, not merely silent), and
// that it says why in its log.
func requireNeverServes(t *testing.T, p *unmanagedPooler) {
	t.Helper()
	answers := 0
	testpoll.Never(t, func() bool {
		h, err := p.firstHealth(t.Context())
		if err != nil {
			return false
		}
		answers++
		return h.GetBackendReady()
	}, 5*time.Second, 200*time.Millisecond, "a pooler whose role cannot see other sessions must not serve")
	require.Positive(t, answers, "the pooler never answered its health stream, so it was not shown to refuse")
	require.Eventually(t, func() bool {
		log, err := os.ReadFile(p.logPath)
		return err == nil && strings.Contains(string(log), "needs pg_read_all_stats")
	}, 10*time.Second, 200*time.Millisecond, "the pooler does not say which privilege its role lacks")
}
