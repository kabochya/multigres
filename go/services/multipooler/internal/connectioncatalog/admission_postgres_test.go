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

package connectioncatalog_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

func TestPostgresAdmissionApplicabilityAndUncertainCommit(t *testing.T) {
	dsn, _, _ := disposablePostgres(t)
	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer observer.Close(context.Background())
	_, err = observer.Exec(t.Context(), "CREATE SCHEMA multigres")
	require.NoError(t, err)
	c, err := connectioncatalog.New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	sk := &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return admission.InitializeOrdinary(ctx, tx, sk)
	}))
	source := &pb.AdmissionIntent{Owner: "owner", IntentId: "close", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED, SourceConnection: "source", SourceConfigurationBinding: "binding", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}
	target := &pb.AdmissionIntent{Owner: "owner", IntentId: "close", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED}
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return admission.Enable(ctx, tx, sk, source, target)
	}))
	// Reinitialization cannot clear sticky controlled applicability or owner.
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return admission.InitializeOrdinary(ctx, tx, sk)
	}))
	var snapshot *pb.AdmissionSnapshot
	confirm := func(ctx context.Context) (*pb.AdmissionSnapshot, error) {
		snapshot = nil
		err := c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
			var err error
			snapshot, err = admission.Confirm(ctx, tx, sk, target.Subject)
			return err
		})
		if err != nil {
			return nil, err
		}
		return snapshot, nil
	}
	snapshot, err = confirm(t.Context())
	require.NoError(t, err)
	require.True(t, snapshot.Controlled)
	_, err = observer.Exec(t.Context(), "DELETE FROM multigres.admission_intents WHERE subject=1")
	require.NoError(t, err)
	snapshot, err = confirm(t.Context())
	require.Error(t, err)
	require.Nil(t, snapshot)
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error { return admission.Write(ctx, tx, sk, target) }))
	_, err = observer.Exec(t.Context(), "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (absent_standby)'")
	require.NoError(t, err)
	_, err = observer.Exec(t.Context(), "SELECT pg_reload_conf()")
	require.NoError(t, err)
	target.IntentId = "open"
	target.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error { return admission.Write(ctx, tx, sk, target) })
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := observer.QueryRow(t.Context(), "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event='SyncRep')").Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 20*time.Millisecond)
	cancel()
	require.Error(t, <-done)
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)
	// Pooler-only restart must still synchronously confirm before activation.
	c, err = connectioncatalog.New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	timeout, stop := context.WithTimeout(t.Context(), 200*time.Millisecond)
	snapshot, err = confirm(timeout)
	stop()
	require.Error(t, err)
	require.Nil(t, snapshot)
	_, err = observer.Exec(t.Context(), "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'")
	require.NoError(t, err)
}

func TestPostgresAdmissionReplayAndMissingScope(t *testing.T) {
	dsn, dir, port := disposablePostgres(t)
	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer observer.Close(context.Background())
	_, err = observer.Exec(t.Context(), "CREATE SCHEMA multigres")
	require.NoError(t, err)
	c, err := connectioncatalog.New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	sk := &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return admission.InitializeOrdinary(ctx, tx, sk)
	}))
	source := &pb.AdmissionIntent{Owner: "owner", IntentId: "close-a", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED, SourceConnection: "source", SourceConfigurationBinding: "binding", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}
	target := &pb.AdmissionIntent{Owner: "owner", IntentId: "close-a", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED}
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return admission.Enable(ctx, tx, sk, source, target)
	}))
	_, syncReplica := startCatalogReplica(t, dir, port, "admission_sync")
	_, err = observer.Exec(t.Context(), "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (admission_sync)'")
	require.NoError(t, err)
	_, err = observer.Exec(t.Context(), "SELECT pg_reload_conf()")
	require.NoError(t, err)
	_, lagging := startCatalogReplica(t, dir, port, "admission_lagging")
	_, err = lagging.Exec(t.Context(), "SELECT pg_wal_replay_pause()")
	require.NoError(t, err)
	target.IntentId = "close-b" // Repeated CLOSED mode must still await exact identity.
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error { return admission.Write(ctx, tx, sk, target) }))
	read := func(conn *pgx.Conn) *pb.AdmissionSnapshot {
		t.Helper()
		q := postgresQueries{dsn: conn.Config().ConnString()}
		tx, err := q.BeginAdmin(t.Context())
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.Background()) }()
		s, err := admission.Read(t.Context(), tx, sk, target.Subject)
		require.NoError(t, err)
		return s
	}
	require.False(t, proto.Equal(read(lagging).Intent, target))
	require.True(t, proto.Equal(read(syncReplica).Intent, target))
	_, err = lagging.Exec(t.Context(), "SELECT pg_wal_replay_resume()")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return proto.Equal(read(lagging).Intent, target) }, 5*time.Second, 20*time.Millisecond)
	// Deleting a controlled scope must not restore ordinary admission on restart.
	_, err = observer.Exec(t.Context(), "DELETE FROM multigres.admission_intents; DELETE FROM multigres.admission_scopes")
	require.NoError(t, err)
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		return admission.InitializeOrdinary(ctx, tx, sk)
	}))
	require.Error(t, c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		_, err := admission.Confirm(ctx, tx, sk, target.Subject)
		return err
	}))
}

func TestPostgresSourceLifecycleAuthorization(t *testing.T) {
	dsn, _, _ := disposablePostgres(t)
	ctx := t.Context()
	observer, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer observer.Close(context.Background())
	_, err = observer.Exec(ctx, "CREATE SCHEMA multigres")
	require.NoError(t, err)
	c, err := connectioncatalog.New(postgresQueries{dsn: dsn}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	sk := &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}
	source := &pb.AdmissionIntent{Owner: "owner", IntentId: "closed-source", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED, SourceConnection: "source", SourceConfigurationBinding: "binding", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}
	target := &pb.AdmissionIntent{Owner: "owner", IntentId: "closed-target", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED}
	authorization := &pb.SourceLifecycleAuthorization{Owner: "owner", ClosedIntentId: source.IntentId, SourceConnection: "source", SourceConfigurationBinding: "binding", RetireSource: true}
	require.NoError(t, c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
		if err := admission.InitializeOrdinary(ctx, tx, sk); err != nil {
			return err
		}
		if err := admission.Enable(ctx, tx, sk, source, target); err != nil {
			return err
		}
		if err := admission.InitializeLifecycle(ctx, tx); err != nil {
			return err
		}
		return admission.WriteLifecycle(ctx, tx, sk, authorization)
	}))
	var observed *pb.SourceLifecycleAuthorization
	require.NoError(t, c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
		var err error
		observed, err = admission.ConfirmLifecycle(ctx, tx, sk)
		return err
	}))
	require.True(t, proto.Equal(authorization, observed))
}
