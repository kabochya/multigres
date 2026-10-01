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

package connectioncatalog

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
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

type recordingTx struct {
	executor.InternalTx
	calls            []string
	encoded, binding string
	commitErr        error
	createErr        error
}

func (tx *recordingTx) Query(_ context.Context, q string) (*sqltypes.Result, error) {
	tx.calls = append(tx.calls, q)
	return &sqltypes.Result{}, nil
}

func (tx *recordingTx) QueryArgs(_ context.Context, q string, args ...any) (*sqltypes.Result, error) {
	tx.calls = append(tx.calls, q)
	if strings.HasPrefix(q, "INSERT") {
		if tx.createErr != nil {
			return nil, tx.createErr
		}
		tx.encoded = args[1].(string)
		tx.binding = args[2].(string)
	}
	if strings.HasPrefix(q, "SELECT") {
		return qmock.MakeQueryResult([]string{"configuration", "binding"}, [][]any{{tx.encoded, tx.binding}}), nil
	}
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

func TestProvisioningAndConfirmation(t *testing.T) {
	tx := &recordingTx{}
	key := bytes.Repeat([]byte{1}, 32)
	c, err := New(recordingQueries{tx: tx}, key)
	require.NoError(t, err)
	key[0] = 99 // Constructor owns a copy, not caller storage.
	v := &rpc.SourceConnection{Name: "source", Host: "db", Port: 5432, Database: "postgres", Username: "admin", Password: "secret", SslMode: "require"}
	var created *Record
	require.NoError(t, c.Transaction(t.Context(), func(ctx context.Context, actual executor.InternalTx) error {
		require.Same(t, tx, actual)
		var err error
		created, err = c.Create(ctx, actual, v)
		return err
	}))
	require.Len(t, created.Binding, 64)
	require.NotContains(t, tx.encoded, v.Password)
	v.Password = "changed"
	require.Equal(t, "secret", created.Configuration.Password)
	require.Contains(t, tx.calls, "SET LOCAL synchronous_commit = 'remote_apply'")
	require.Contains(t, tx.calls, "COMMIT")
	record, err := c.Confirmed(t.Context(), "source")
	require.NoError(t, err)
	require.Equal(t, created.Binding, record.Binding)
	require.Equal(t, "secret", record.Configuration.Password)
	require.Contains(t, tx.calls, "UPDATE multigres.connections SET binding=binding WHERE name=$1")
	tx.commitErr = errors.New("uncertain commit")
	record, err = c.Confirmed(t.Context(), "source")
	require.ErrorContains(t, err, "uncertain")
	require.Nil(t, record)
	restarted, err := New(recordingQueries{tx: tx}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	record, err = restarted.Confirmed(t.Context(), "source")
	require.Error(t, err)
	require.Nil(t, record)
}

func TestCreateFailureNeverCommits(t *testing.T) {
	tx := &recordingTx{createErr: errors.New("backend secret leaked")}
	c, err := New(recordingQueries{tx: tx}, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	err = c.Transaction(t.Context(), func(ctx context.Context, tx executor.InternalTx) error {
		_, err := c.Create(ctx, tx, &rpc.SourceConnection{Name: "source", Host: "db", Port: 5432, Database: "postgres", Username: "admin", SslMode: "require"})
		return err
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "backend secret")
	require.NotContains(t, tx.calls, "COMMIT")
	require.Contains(t, tx.calls, "ROLLBACK")
}

func TestTLSAndHostValidation(t *testing.T) {
	valid := &rpc.SourceConnection{Name: "s", Host: "db", Port: 5432, Database: "postgres", Username: "admin", SslMode: "require", SslNegotiation: "direct"}
	require.NoError(t, ValidateConnection(valid))
	for _, host := range []string{" ", "db/password", "user@db", "db\nsecret"} {
		v := proto.Clone(valid).(*rpc.SourceConnection)
		v.Host = host
		require.Error(t, ValidateConnection(v))
	}
	valid.SslMode = "disable"
	require.Error(t, ValidateConnection(valid))
	valid.SslNegotiation = "postgres"
	valid.SslMode = "verify-full"
	require.Error(t, ValidateConnection(valid))
	valid.SslRootCert = "/protected/ca.pem"
	require.NoError(t, ValidateConnection(valid))
}
