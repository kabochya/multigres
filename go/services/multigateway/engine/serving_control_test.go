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

package engine

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

type fakeServingExecute struct {
	IExecute
	request *rpc.ServingControlRequest
}

func (f *fakeServingExecute) ServingControl(_ context.Context, _ *server.Conn, r *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error) {
	f.request = r
	return &rpc.ServingControlResponse{Routing: &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_FENCED, SourceConnection: "source"}}, nil
}

func TestServingControlUsesNarrowAPIAndRejectsTransaction(t *testing.T) {
	statements, err := parser.ParseSQL("PAUSE SERVING REQUEST ID 'pause-1'")
	require.NoError(t, err)
	primitive := &ServingControl{Statement: statements[0].(*ast.ServingControlStmt)}
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	state := &handler.MultigatewayConnectionState{}
	exec := &fakeServingExecute{}
	var result *sqltypes.Result
	callback := func(_ context.Context, r *sqltypes.Result) error { result = r; return nil }
	require.NoError(t, primitive.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, callback))
	require.Equal(t, "pause-1", exec.request.RequestId)
	require.Equal(t, "pause", exec.request.Operation)
	require.Len(t, result.Fields, 3)
	require.NoError(t, primitive.PortalStreamExecute(t.Context(), exec, conn, state, nil, 0, false, PlanExecInfo{}, callback))
	require.Empty(t, result.Fields, "extended Execute must not duplicate Describe")
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	exec.request = nil
	require.Error(t, primitive.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, callback))
	require.Nil(t, exec.request)
}

func TestConnectionCommandValidationDoesNotEchoSecrets(t *testing.T) {
	statements, err := parser.ParseSQL("CREATE CONNECTION source WITH (port='invalid-secret',password='password-secret') REQUEST ID 'c'")
	require.NoError(t, err)
	primitive := &ServingControl{Statement: statements[0].(*ast.ServingControlStmt)}
	_, err = primitive.request()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	require.NotContains(t, primitive.String(), "secret")
}
