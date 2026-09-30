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
	"context"
	"errors"
	"strconv"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

type ServingControl struct{ Statement *ast.ServingControlStmt }

func (s *ServingControl) GetTableGroup() string { return "" }
func (s *ServingControl) GetQuery() string      { return "" }
func (s *ServingControl) String() string        { return "ServingControl(" + s.Statement.Operation + ")" }
func ServingControlDescription() *query.StatementDescription {
	return &query.StatementDescription{HasFields: true, Fields: []*query.Field{textField("serving_mode"), textField("source_connection"), textField("migration_completed")}}
}

func (s *ServingControl) request() (*rpc.ServingControlRequest, error) {
	st := s.Statement
	r := &rpc.ServingControlRequest{Operation: st.Operation, ConnectionName: st.ConnectionName, RequestId: st.RequestID}
	if st.Operation == "create" || st.Operation == "alter" {
		port, err := strconv.ParseUint(st.Option("port"), 10, 16)
		if err != nil || port == 0 {
			return nil, errors.New("connection requires a valid port")
		}
		if st.Option("host") == "" || st.Option("database") == "" || st.Option("username") == "" || st.Option("password") == "" {
			return nil, errors.New("connection requires host, database, username and password")
		}
		mode := st.Option("sslmode")
		if mode == "" {
			mode = "verify-full"
		}
		r.Connection = &rpc.SourceConnection{Name: st.ConnectionName, Host: st.Option("host"), Port: uint32(port), Database: st.Option("database"), Username: st.Option("username"), Password: st.Option("password"), SslMode: mode, SslRootCert: st.Option("sslrootcert"), SslNegotiation: st.Option("sslnegotiation")}
	}
	return r, nil
}

func (s *ServingControl) execute(ctx context.Context, exec IExecute, conn *server.Conn, state *handler.MultigatewayConnectionState, fields bool, callback func(context.Context, *sqltypes.Result) error) error {
	if conn.TxnStatus() != protocol.TxnStatusIdle || state.HasReservedConnectionFor(constants.DefaultTableGroup, constants.DefaultShard) {
		return errors.New("serving control requires an idle session without reserved backend state")
	}
	control, ok := exec.(interface {
		ServingControl(context.Context, *server.Conn, *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error)
	})
	if !ok {
		return errors.New("serving control unavailable")
	}
	request, err := s.request()
	if err != nil {
		return err
	}
	response, err := control.ServingControl(ctx, conn, request)
	if err != nil {
		return err
	}
	if response.GetRouting() == nil {
		return errors.New("serving control returned unknown state")
	}
	r := response.Routing
	result := &sqltypes.Result{Rows: []*sqltypes.Row{sqltypes.MakeRow([][]byte{[]byte(r.Mode.String()), []byte(r.SourceConnection), []byte(strconv.FormatBool(r.MigrationCompleted))})}, CommandTag: "SERVING CONTROL"}
	if fields {
		result.Fields = ServingControlDescription().Fields
	}
	return callback(ctx, result)
}

func (s *ServingControl) StreamExecute(ctx context.Context, exec IExecute, conn *server.Conn, state *handler.MultigatewayConnectionState, _ []*ast.A_Const, _ PlanExecInfo, callback func(context.Context, *sqltypes.Result) error) error {
	return s.execute(ctx, exec, conn, state, true, callback)
}

func (s *ServingControl) PortalStreamExecute(ctx context.Context, exec IExecute, conn *server.Conn, state *handler.MultigatewayConnectionState, _ *preparedstatement.PortalInfo, _ int32, includeDescribe bool, _ PlanExecInfo, callback func(context.Context, *sqltypes.Result) error) error {
	return s.execute(ctx, exec, conn, state, includeDescribe, callback)
}

var _ Primitive = (*ServingControl)(nil)
