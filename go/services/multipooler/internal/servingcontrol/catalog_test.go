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

package servingcontrol

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	qmock "github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

func TestEncryptedConnectionBinding(t *testing.T) {
	c, err := New(nil, bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	v := &rpc.SourceConnection{Name: "source", Host: "localhost", Port: 5432, Database: "postgres", Username: "reader", Password: "secret", SslMode: "require"}
	encoded, err := c.seal(v)
	require.NoError(t, err)
	require.False(t, strings.Contains(encoded, "secret"))
	decoded, err := c.open("source", encoded)
	require.NoError(t, err)
	require.Equal(t, v.Password, decoded.Password)
	_, err = c.open("other", encoded)
	require.Error(t, err)
	wrong, _ := New(nil, bytes.Repeat([]byte{8}, 32))
	_, err = wrong.open("source", encoded)
	require.Error(t, err)
	_, err = c.open("source", "broken")
	require.Error(t, err)
}

func TestValidateConnection(t *testing.T) {
	_, err := New(nil, []byte("short"))
	require.Error(t, err)
	require.Error(t, ValidateConnection(nil))
	require.Error(t, ValidateConnection(&rpc.SourceConnection{Name: "source"}))
}

// Recording transactions assert that the journal and mode use the same handle,
// and that failure never commits. Real PostgreSQL coverage is added in the E2E PR.
type recordingTx struct {
	calls     []string
	args      []any
	commitErr error
}

func (tx *recordingTx) Query(_ context.Context, q string) (*sqltypes.Result, error) {
	tx.calls = append(tx.calls, q)
	if strings.Contains(q, "FOR UPDATE") {
		return qmock.MakeQueryResult([]string{"mode", "source_connection", "sysid", "database", "completed", "resume", "request"}, [][]any{{int32(0), "", "", "", false, false, ""}}), nil
	}
	return &sqltypes.Result{}, nil
}

func (tx *recordingTx) QueryArgs(_ context.Context, q string, args ...any) (*sqltypes.Result, error) {
	tx.calls = append(tx.calls, q)
	tx.args = args
	return &sqltypes.Result{}, nil
}

func (tx *recordingTx) Commit(context.Context) error {
	tx.calls = append(tx.calls, "COMMIT")
	return tx.commitErr
}

func (tx *recordingTx) Rollback(context.Context) error {
	tx.calls = append(tx.calls, "ROLLBACK")
	return nil
}

type recordingQueries struct {
	executor.InternalQueryService
	tx *recordingTx
}

func (q recordingQueries) BeginAdmin(context.Context) (executor.InternalTx, error) { return q.tx, nil }
func TestRoutingJournalTransaction(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(strconv.FormatBool(failure), func(t *testing.T) {
			tx := &recordingTx{}
			c, err := New(recordingQueries{tx: tx}, bytes.Repeat([]byte{1}, 32))
			require.NoError(t, err)
			err = c.Update(context.Background(), func(actual executor.InternalTx, state *pb.MigrationRouting) error {
				require.Same(t, tx, actual)
				require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNSET, state.Mode)
				_, err := actual.Query(context.Background(), "INSERT INTO migration_journal VALUES ('decision')")
				require.NoError(t, err)
				state.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
				state.SourceConnection = "source"
				if failure {
					return errors.New("journal rejected")
				}
				return nil
			})
			require.Contains(t, tx.calls, "SET LOCAL synchronous_commit = 'remote_apply'")
			require.Contains(t, tx.calls, "SELECT mode, source_connection, source_system_identifier, source_database, migration_completed, resume_source_allowed, active_request_id FROM multigres.migration_routing WHERE singleton FOR UPDATE")
			require.Equal(t, "ROLLBACK", tx.calls[len(tx.calls)-1])
			if failure {
				require.Error(t, err)
				require.NotContains(t, tx.calls, "COMMIT")
				require.Nil(t, tx.args)
			} else {
				require.NoError(t, err)
				require.Contains(t, tx.calls, "COMMIT")
				require.Equal(t, []any{int32(2), "source", "", "", false, false, ""}, tx.args)
			}
		})
	}
}

func TestConnectionValidation(t *testing.T) {
	valid := &rpc.SourceConnection{Name: "s", Host: "db", Port: 5432, Database: "postgres", Username: "pooler", SslMode: "require"}
	require.NoError(t, ValidateConnection(valid))
	for _, mode := range []string{"", "prefer", "requre"} {
		v := proto.Clone(valid).(*rpc.SourceConnection)
		v.SslMode = mode
		require.Error(t, ValidateConnection(v))
	}
	v := proto.Clone(valid).(*rpc.SourceConnection)
	v.Port = 65536
	require.Error(t, ValidateConnection(v))
	c, _ := New(nil, bytes.Repeat([]byte{1}, 32))
	ciphertext, err := c.seal(valid)
	require.NoError(t, err)
	data, err := base64.StdEncoding.DecodeString(strings.Split(ciphertext, ".")[2])
	require.NoError(t, err)
	data[len(data)-1] ^= 1
	_, err = c.open("s", "v1."+c.keyID+"."+base64.StdEncoding.EncodeToString(data))
	require.ErrorContains(t, err, "cannot decrypt")
}
