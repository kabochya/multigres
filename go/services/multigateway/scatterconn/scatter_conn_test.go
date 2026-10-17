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

package scatterconn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mockGateway is a mock implementation of poolergateway.Gateway for testing.
type mockGateway struct {
	// StreamExecute tracking
	streamExecuteCalled         bool
	streamExecuteSQL            string
	streamExecuteOpts           *querypb.ExecuteOptions
	streamExecuteTarget         *querypb.Target
	releasedConnectionIDs       []uint64
	discardedConnectionIDs      []uint64
	streamExecuteReservationOps *querypb.ReservationOptions
	streamExecuteErr            error
	streamExecuteReturnState    *querypb.ReservedState

	// PortalStreamExecute tracking
	portalCalled         bool
	portalOpts           *querypb.ExecuteOptions
	portalReservationOps *querypb.ReservationOptions
	portalReturnState    *querypb.ReservedState
	portalErr            error

	// QueryServiceByID tracking
	queryServiceByIDCalled bool
	queryServiceByIDErr    error

	// ConcludeTransaction tracking
	concludeTransactionResult          *sqltypes.Result
	concludeRollbackSessionSettings    map[string]string
	releaseReservedConnectionSettings  map[string]string
	concludeRollbackSessionSettingsSet bool
	concludeTransactionReturnState     *querypb.ReservedState
	concludeTransactionErr             error

	// CopyReady tracking
	copyReadyFormat         int16
	copyReadyColumnFormats  []int16
	copyReadyReturnState    *querypb.ReservedState
	copyReadyReservationOps *querypb.ReservationOptions
	copyReadyErr            error
	copyReadyCalled         bool

	// CopyOutReady tracking
	copyOutReadyReturnState    *querypb.ReservedState
	copyOutReadyReservationOps *querypb.ReservationOptions
	copyOutReadyCalled         bool

	// CopyFinalize tracking
	copyFinalizeResult      *sqltypes.Result
	copyFinalizeReturnState *querypb.ReservedState
	copyFinalizeErr         error

	// CopyAbort tracking
	copyAbortReturnState *querypb.ReservedState
	copyAbortErr         error

	// Callback control
	callbackResult *sqltypes.Result

	// StreamReplication tracking
	streamReplicationInit   *multipoolerpb.StreamReplicationInit
	streamReplicationStream multipoolerpb.MultipoolerService_StreamReplicationClient
	streamReplicationErr    error

	// ReleaseReservedConnection tracking
	releaseReservedConnectionKeepSticky  bool
	releaseReservedConnectionReturnState *querypb.ReservedState
	releaseReservedConnectionErr         error
}

// StreamExecute implements queryservice.QueryService.
func (m *mockGateway) StreamExecute(_ context.Context, target *querypb.Target, sql string, opts *querypb.ExecuteOptions, reservationOpts *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	m.streamExecuteCalled = true
	m.streamExecuteTarget = target
	m.streamExecuteSQL = sql
	m.streamExecuteOpts = opts
	m.streamExecuteReservationOps = reservationOpts
	if m.streamExecuteErr != nil {
		return m.streamExecuteReturnState, m.streamExecuteErr
	}
	if m.callbackResult != nil {
		if err := callback(context.Background(), m.callbackResult); err != nil {
			return m.streamExecuteReturnState, err
		}
	}
	return m.streamExecuteReturnState, nil
}

// QueryServiceByID implements poolergateway.Gateway.
func (m *mockGateway) QueryServiceByID(_ context.Context, _ *clustermetadatapb.ID, _ *querypb.Target) (queryservice.QueryService, error) {
	m.queryServiceByIDCalled = true
	if m.queryServiceByIDErr != nil {
		return nil, m.queryServiceByIDErr
	}
	return m, nil
}

// Unused interface methods.
func (m *mockGateway) ExecuteQuery(context.Context, *querypb.Target, string, *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockGateway) PortalStreamExecute(_ context.Context, _ *querypb.Target, _ *querypb.PreparedStatement, _ *querypb.Portal, opts *querypb.ExecuteOptions, _ *multipoolerpb.PortalExecuteOptions, reservationOpts *querypb.ReservationOptions, callback func(context.Context, *sqltypes.Result) error) (*querypb.ReservedState, error) {
	m.portalCalled = true
	m.portalOpts = opts
	m.portalReservationOps = reservationOpts
	if m.portalErr != nil {
		return m.portalReturnState, m.portalErr
	}
	if m.callbackResult != nil {
		if err := callback(context.Background(), m.callbackResult); err != nil {
			return m.portalReturnState, err
		}
	}
	return m.portalReturnState, nil
}

func (m *mockGateway) Describe(context.Context, *querypb.Target, *querypb.PreparedStatement, *querypb.Portal, *querypb.ExecuteOptions) (*querypb.StatementDescription, error) {
	return nil, nil
}

func (m *mockGateway) Close() error { return nil }

func (m *mockGateway) CopyReady(_ context.Context, _ *querypb.Target, _ string, _ *querypb.ExecuteOptions, reservationOpts *querypb.ReservationOptions) (int16, []int16, *querypb.ReservedState, error) {
	m.copyReadyCalled = true
	m.copyReadyReservationOps = reservationOpts
	return m.copyReadyFormat, m.copyReadyColumnFormats, m.copyReadyReturnState, m.copyReadyErr
}

func (m *mockGateway) CopySendData(context.Context, *querypb.Target, []byte, *querypb.ExecuteOptions) error {
	return nil
}

func (m *mockGateway) CopyFinalize(_ context.Context, _ *querypb.Target, _ []byte, _ *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	return m.copyFinalizeResult, m.copyFinalizeReturnState, m.copyFinalizeErr
}

func (m *mockGateway) CopyAbort(_ context.Context, _ *querypb.Target, _ string, _ *querypb.ExecuteOptions) (*querypb.ReservedState, error) {
	return m.copyAbortReturnState, m.copyAbortErr
}

func (m *mockGateway) CopyOutReady(_ context.Context, _ *querypb.Target, _ string, _ *querypb.ExecuteOptions, reservationOpts *querypb.ReservationOptions) (int16, []int16, []*mterrors.PgDiagnostic, *querypb.ReservedState, error) {
	m.copyOutReadyCalled = true
	m.copyOutReadyReservationOps = reservationOpts
	return 0, nil, nil, m.copyOutReadyReturnState, nil
}

func (m *mockGateway) CopyOutStream(_ context.Context, _ *querypb.Target, _ *querypb.ExecuteOptions, _ func(pgClient.CopyOutMessage) error) (*sqltypes.Result, *querypb.ReservedState, error) {
	return nil, nil, nil
}

func (m *mockGateway) ReleaseReservedConnection(_ context.Context, _ *querypb.Target, options *querypb.ExecuteOptions, keepStickyReservations bool) (*querypb.ReservedState, error) {
	m.releasedConnectionIDs = append(m.releasedConnectionIDs, options.GetReservedConnectionId())
	m.releaseReservedConnectionKeepSticky = keepStickyReservations
	m.releaseReservedConnectionSettings = options.GetSessionSettings()
	return m.releaseReservedConnectionReturnState, m.releaseReservedConnectionErr
}

func (m *mockGateway) ConcludeTransaction(_ context.Context, _ *querypb.Target, _ *querypb.ExecuteOptions, _ multipoolerpb.TransactionConclusion, _ []string, _ bool, _ bool, rollbackSessionSettings map[string]string) (*sqltypes.Result, *querypb.ReservedState, error) {
	m.concludeRollbackSessionSettings = rollbackSessionSettings
	m.concludeRollbackSessionSettingsSet = true
	return m.concludeTransactionResult, m.concludeTransactionReturnState, m.concludeTransactionErr
}

func (m *mockGateway) DiscardTempTables(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	m.discardedConnectionIDs = append(m.discardedConnectionIDs, options.GetReservedConnectionId())
	return nil, nil, nil
}

func (m *mockGateway) StreamReplication(_ context.Context, init *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	m.streamReplicationInit = init
	return m.streamReplicationStream, m.streamReplicationErr
}

// newTestConn creates a test server.Conn for ScatterConn tests.
func newTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}).Conn
}

func TestScatterConn_Case1_ExistingReservedConnection(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	// Simulates a COMMIT/ROLLBACK AND CHAIN that kept this reservation active:
	// transaction_primitive.go restores PendingBeginQuery to signal the new
	// transaction has not run a statement yet, even though PostgreSQL already
	// started it via CHAIN, so ReservationReasons already includes
	// ReasonTransaction below.
	state.PendingBeginQuery = "BEGIN"

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.queryServiceByIDCalled, "should use QueryServiceByID for existing reserved connection")
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute on the found query service")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.Equal(t, uint64(42), gw.streamExecuteOpts.ReservedConnectionId)
	require.Nil(t, gw.streamExecuteReservationOps,
		"already transactional, so this must not be resent as a real BeginQuery")
	require.Empty(t, state.PendingBeginQuery,
		"a statement reaching the backend on the reused reservation answers it, even though it was not sent as a real BeginQuery")
}

func TestScatterConn_Case2_InTransactionNoReservedConn(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 77,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN" // the BEGIN is deferred to the first statement

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.NotNil(t, gw.streamExecuteReservationOps, "should pass ReservationOptions")
	require.NotEqual(t, uint32(0), gw.streamExecuteReservationOps.GetReasons(), "should set reasons")
	require.False(t, gw.queryServiceByIDCalled)

	// Verify state was updated with the reserved connection
	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(77), ss.ReservedState.GetReservedConnectionId())
}

func TestScatterConn_Case2_ReserveError(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: errors.New("reserve failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN" // the BEGIN is deferred to the first statement

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "reserve failed")
	require.True(t, gw.streamExecuteCalled, "should call StreamExecute")
	require.NotNil(t, gw.streamExecuteReservationOps, "should pass ReservationOptions")
	require.NotEqual(t, uint32(0), gw.streamExecuteReservationOps.GetReasons(), "should set reasons")
	// State should not be updated
	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	require.Nil(t, state.GetMatchingShardState(target))
}

// TestScatterConn_Case1_LogicalReplicationSlotPromotesExistingReservation
// verifies that a statement creating a logical replication slot on an
// already-reserved connection (e.g. inside an open transaction) ORs the
// logical-replication reason onto the existing reservation, exercising
// reservationReasonsForExecInfo's LogicalReplicationSlot branch from Case 1.
func TestScatterConn_Case1_LogicalReplicationSlotPromotesExistingReservation(t *testing.T) {
	gw := &mockGateway{
		callbackResult:           &sqltypes.Result{CommandTag: "SELECT 1"},
		streamExecuteReturnState: &querypb.ReservedState{ReservedConnectionId: 42, ReservationReasons: protoutil.ReasonTransaction | protoutil.ReasonLogicalReplication},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT pg_create_logical_replication_slot('s1', 'test_decoding', true)", nil, state,
		engine.PlanExecInfo{LogicalReplicationSlot: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.queryServiceByIDCalled, "should use QueryServiceByID for existing reserved connection")
	require.NotNil(t, gw.streamExecuteReservationOps, "should carry the promoted reservation reason")
	require.True(t, protoutil.HasLogicalReplicationReason(gw.streamExecuteReservationOps.GetReasons()))
}

// TestScatterConn_Case2_LogicalReplicationSlotReservesNewConn verifies that a
// statement creating a logical replication slot with no existing reservation
// and no open transaction still reserves a new connection with
// ReasonLogicalReplication.
func TestScatterConn_Case2_LogicalReplicationSlotReservesNewConn(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 77,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonLogicalReplication,
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn() // not in a transaction

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT pg_create_logical_replication_slot('s1', 'test_decoding', true)", nil, state,
		engine.PlanExecInfo{LogicalReplicationSlot: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled)
	require.NotNil(t, gw.streamExecuteReservationOps)
	require.True(t, protoutil.HasLogicalReplicationReason(gw.streamExecuteReservationOps.GetReasons()))

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(77), ss.ReservedState.GetReservedConnectionId())
}

// TestScatterConn_Case2_AdvisoryLockReservesNewConn verifies that a statement
// acquiring a session-level advisory lock with no existing reservation and no
// open transaction still reserves a new connection with
// ReasonSessionAdvisoryLock, exercising reservationReasonsForExecInfo's
// AdvisoryLock branch (otherwise only covered by the endtoend advisory-lock
// suite, which doesn't count toward this package's own unit coverage).
func TestScatterConn_Case2_AdvisoryLockReservesNewConn(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 88,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonSessionAdvisoryLock,
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn() // not in a transaction

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT pg_advisory_lock(1)", nil, state,
		engine.PlanExecInfo{AdvisoryLock: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled)
	require.NotNil(t, gw.streamExecuteReservationOps)
	require.True(t, protoutil.HasSessionAdvisoryLockReason(gw.streamExecuteReservationOps.GetReasons()))

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(88), ss.ReservedState.GetReservedConnectionId())
}

// TestScatterConn_Case1_SetSeedPromotesExistingReservation verifies that a
// setseed(...) call on an already-reserved connection (e.g. inside an open
// transaction) ORs the set-seed reason onto the existing reservation,
// exercising reservationReasonsForExecInfo's SetSeed branch from Case 1.
func TestScatterConn_Case1_SetSeedPromotesExistingReservation(t *testing.T) {
	gw := &mockGateway{
		callbackResult:           &sqltypes.Result{CommandTag: "SELECT 1"},
		streamExecuteReturnState: &querypb.ReservedState{ReservedConnectionId: 42, ReservationReasons: protoutil.ReasonTransaction | protoutil.ReasonSetSeed},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT setseed(0.5)", nil, state,
		engine.PlanExecInfo{SetSeed: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.queryServiceByIDCalled, "should use QueryServiceByID for existing reserved connection")
	require.NotNil(t, gw.streamExecuteReservationOps, "should carry the promoted reservation reason")
	require.True(t, protoutil.HasSetSeedReason(gw.streamExecuteReservationOps.GetReasons()))
}

// TestScatterConn_Case2_SetSeedReservesNewConn verifies that a setseed(...)
// call with no existing reservation and no open transaction still reserves a
// new connection with ReasonSetSeed.
func TestScatterConn_Case2_SetSeedReservesNewConn(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 89,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonSetSeed,
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn() // not in a transaction

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT setseed(0.5)", nil, state,
		engine.PlanExecInfo{SetSeed: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled)
	require.NotNil(t, gw.streamExecuteReservationOps)
	require.True(t, protoutil.HasSetSeedReason(gw.streamExecuteReservationOps.GetReasons()))

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(89), ss.ReservedState.GetReservedConnectionId())
}

// TestScatterConn_UnsafeConnection_PlainStatementReserves guards the crux of
// unsafe connection: even a plain statement (no transaction/temp/lock — what
// would otherwise take the unreserved Case 3 path) must reserve+quarantine its
// own backend, carrying ReasonUnsafeConnection. Without the gate including
// conn.UnsafeConnection(), such a statement would run on a shared pooled backend
// and leak any untracked state it changed.
func TestScatterConn_UnsafeConnection_PlainStatementReserves(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 91,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonUnsafeConnection,
		},
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn // direct, not in a txn

	// A plain SELECT: no transaction, no temp table, no lock — the one that used
	// to take the unreserved path.
	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state,
		engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled)
	require.NotNil(t, gw.streamExecuteReservationOps, "an unsafe connection must reserve even for a plain statement")
	require.True(t, protoutil.HasUnsafeConnectionReason(gw.streamExecuteReservationOps.GetReasons()))
}

// TestScatterConn_UnsafeConnection_CopyReserves guards that a first COPY
// FROM/TO STDIN on an unsafe connection (no transaction, no prior reservation)
// still reserves and quarantines its backend with ReasonUnsafeConnection.
// Without it, a trigger or function that changes untracked session state during
// COPY would run on a pooled backend that is then recycled to another client.
func TestScatterConn_UnsafeConnection_CopyReserves(t *testing.T) {
	newDirectConn := func() *server.Conn {
		return server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn // direct, not in a txn
	}
	reservedState := func() *querypb.ReservedState {
		return &querypb.ReservedState{
			ReservedConnectionId: 77,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonUnsafeConnection,
		}
	}

	t.Run("COPY FROM STDIN", func(t *testing.T) {
		gw := &mockGateway{copyReadyReturnState: reservedState()}
		sc := NewScatterConn(gw, slog.Default())
		state := handler.NewMultigatewayConnectionState()

		_, _, err := sc.CopyInitiate(context.Background(), newDirectConn(), "tg1", "", "COPY t FROM STDIN", state,
			func(_ context.Context, _ *sqltypes.Result) error { return nil })

		require.NoError(t, err)
		require.NotNil(t, gw.copyReadyReservationOps, "an unsafe connection must reserve for a first COPY FROM STDIN")
		require.True(t, protoutil.HasUnsafeConnectionReason(gw.copyReadyReservationOps.GetReasons()))
	})

	t.Run("COPY TO STDOUT", func(t *testing.T) {
		gw := &mockGateway{copyOutReadyReturnState: reservedState()}
		sc := NewScatterConn(gw, slog.Default())
		state := handler.NewMultigatewayConnectionState()

		_, _, _, err := sc.CopyOutInitiate(context.Background(), newDirectConn(), "tg1", "", "COPY t TO STDOUT", state)

		require.NoError(t, err)
		require.NotNil(t, gw.copyOutReadyReservationOps, "an unsafe connection must reserve for a first COPY TO STDOUT")
		require.True(t, protoutil.HasUnsafeConnectionReason(gw.copyOutReadyReservationOps.GetReasons()))
	})
}

// TestScatterConn_ReleaseAllReservedConnections_StickyUpdatesNotClears
// verifies that when the multipooler leaves a connection reserved because a
// sticky reason (ReasonSetSeed) survived (keepStickyReservations=true, the
// DISCARD ALL path), ScatterConn updates the shard's local state to the
// returned reservation rather than clearing it, mirroring DiscardTempTables.
func TestScatterConn_ReleaseAllReservedConnections_StickyUpdatesNotClears(t *testing.T) {
	gw := &mockGateway{
		releaseReservedConnectionReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonSetSeed,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonSetSeed,
	})

	err := sc.ReleaseAllReservedConnections(context.Background(), conn, state, true)

	require.NoError(t, err)
	require.True(t, gw.releaseReservedConnectionKeepSticky, "DISCARD ALL must request sticky preservation")

	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state must be updated, not cleared, while a sticky reason survives")
	assert.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	assert.True(t, protoutil.HasSetSeedReason(ss.ReservedState.GetReservationReasons()))
}

// TestScatterConn_ReleaseAllReservedConnections_FullyReleasedClears verifies
// that when the multipooler fully releases a connection (no sticky reason
// remained, or keepStickyReservations was false for a real disconnect),
// ScatterConn clears the shard's local state.
func TestScatterConn_ReleaseAllReservedConnections_FullyReleasedClears(t *testing.T) {
	gw := &mockGateway{} // releaseReservedConnectionReturnState left nil: fully released
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonSetSeed,
	})

	err := sc.ReleaseAllReservedConnections(context.Background(), conn, state, false)

	require.NoError(t, err)
	require.False(t, gw.releaseReservedConnectionKeepSticky, "real disconnect must not request sticky preservation")
	assert.Nil(t, state.GetMatchingShardState(target), "shard state must be cleared once fully released")
}

// testPortalInfo builds a minimal PortalInfo for portal-path ScatterConn tests.
func testPortalInfo() *preparedstatement.PortalInfo {
	return &preparedstatement.PortalInfo{
		Portal: &querypb.Portal{Name: "portal1"},
		PreparedStatementInfo: &preparedstatement.PreparedStatementInfo{
			PreparedStatement: &querypb.PreparedStatement{Name: "stmt1", Query: "SELECT 1"},
		},
	}
}

// TestScatterConn_Portal_FirstStatementReservesViaPortalRPC verifies that a
// portal which opens a transaction while no reserved connection exists yet
// reserves atomically through PortalStreamExecute's reservation options — with
// no separate no-op "SELECT 1" StreamExecute round trip.
func TestScatterConn_Portal_FirstStatementReservesViaPortalRPC(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 99,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN" // the BEGIN is deferred to the first statement

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.portalCalled, "should call PortalStreamExecute")
	require.False(t, gw.streamExecuteCalled, "must NOT issue a no-op SELECT 1 StreamExecute reserve")
	require.NotNil(t, gw.portalReservationOps, "should pass reservation options on the portal RPC")
	require.True(t, protoutil.HasTransactionReason(gw.portalReservationOps.GetReasons()),
		"reservation options should carry the transaction reason")
	require.Zero(t, gw.portalOpts.GetReservedConnectionId(),
		"no reserved connection id yet — the multipooler reserves one")

	// State updated with the authoritative reserved connection from the portal RPC.
	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(99), ss.ReservedState.GetReservedConnectionId())
}

// TestScatterConn_Portal_TempTableReservesViaPortalRPC verifies the temp-table
// reservation reason is carried on the portal RPC (and the pending one-shot flag
// is cleared) when a portal is the first statement needing a reserved backend.
func TestScatterConn_Portal_TempTableReservesViaPortalRPC(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 100,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn() // not in a transaction

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{TempTable: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.portalCalled)
	require.False(t, gw.streamExecuteCalled, "must NOT issue a no-op SELECT 1 reserve")
	require.NotNil(t, gw.portalReservationOps)
	require.True(t, protoutil.HasTempTableReason(gw.portalReservationOps.GetReasons()))
}

// TestScatterConn_Portal_ExistingReservedConnNoReserveReasons verifies that when
// a reserved connection already exists, the portal runs on it and carries no new
// reservation reasons (behavior preserved from before the refactor).
func TestScatterConn_Portal_ExistingReservedConnNoReserveReasons(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	// Simulates a COMMIT/ROLLBACK AND CHAIN that kept this reservation active:
	// see TestScatterConn_Case1_ExistingReservedConnection.
	state.PendingBeginQuery = "BEGIN"

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.portalCalled)
	require.True(t, gw.queryServiceByIDCalled, "should route to the reserved connection's pooler")
	require.False(t, gw.streamExecuteCalled)
	require.Equal(t, uint64(42), gw.portalOpts.GetReservedConnectionId())
	require.Nil(t, gw.portalReservationOps, "existing reservation needs no new reasons")
	require.Empty(t, state.PendingBeginQuery,
		"a portal reaching the backend on the reused reservation answers it, even though it was not sent as a real BeginQuery")
}

// TestScatterConn_Portal_LogicalReplicationSlotReservesViaPortalRPC verifies
// the logical-replication reservation reason is carried on the portal RPC
// when a portal creating a logical replication slot is the first statement
// needing a reserved backend (Case 2, extended protocol).
func TestScatterConn_Portal_LogicalReplicationSlotReservesViaPortalRPC(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 100,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonLogicalReplication,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn() // not in a transaction

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{LogicalReplicationSlot: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.portalCalled)
	require.False(t, gw.streamExecuteCalled, "must NOT issue a no-op SELECT 1 reserve")
	require.NotNil(t, gw.portalReservationOps)
	require.True(t, protoutil.HasLogicalReplicationReason(gw.portalReservationOps.GetReasons()))
}

// TestScatterConn_Portal_LogicalReplicationSlotPromotesExistingReservation
// verifies that a portal creating a logical replication slot on an
// already-reserved connection ORs the logical-replication reason onto the
// existing reservation (Case 1, extended protocol) — the narrower promotion
// path that, unlike StreamExecute's Case 1, does not also promote TempTable.
func TestScatterConn_Portal_LogicalReplicationSlotPromotesExistingReservation(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonLogicalReplication,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{LogicalReplicationSlot: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.portalCalled)
	require.True(t, gw.queryServiceByIDCalled, "should route to the reserved connection's pooler")
	require.NotNil(t, gw.portalReservationOps, "should carry the promoted reservation reason")
	require.True(t, protoutil.HasLogicalReplicationReason(gw.portalReservationOps.GetReasons()))
}

// TestScatterConn_Portal_AdvisoryLockPromotesExistingReservation verifies that
// a portal acquiring a session-level advisory lock on an already-reserved
// connection ORs the advisory-lock reason onto the existing reservation
// (Case 1, extended protocol).
func TestScatterConn_Portal_AdvisoryLockPromotesExistingReservation(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonSessionAdvisoryLock,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{AdvisoryLock: true}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.portalCalled)
	require.True(t, gw.queryServiceByIDCalled, "should route to the reserved connection's pooler")
	require.NotNil(t, gw.portalReservationOps, "should carry the promoted reservation reason")
	require.True(t, protoutil.HasSessionAdvisoryLockReason(gw.portalReservationOps.GetReasons()))
}

func TestScatterConn_Portal_ErrorAppliesReturnedReservedState(t *testing.T) {
	pgErr := mterrors.NewPgError("ERROR", mterrors.PgSSSyntaxError, "boom", "")
	gw := &mockGateway{
		portalErr: pgErr,
		portalReturnState: &querypb.ReservedState{
			ReservedConnectionId: 43,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget(conn.Database(), "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.PortalStreamExecute(context.Background(), "tg1", "", conn, state,
		testPortalInfo(), 0, false,
		engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.ErrorIs(t, err, pgErr)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(43), ss.ReservedState.GetReservedConnectionId(),
		"gateway must keep the multipooler's surviving reserved state after a portal error")
}

func TestScatterConn_NewTransactionErrorAppliesReturnedReservedState(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: errors.New("insert failed"),
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 88,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN" // the BEGIN is deferred to the first statement

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "INSERT INTO t VALUES (1)", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	target := protoutil.NewTarget(conn.Database(), "tg1", "", querypb.Mode_MODE_WRITABLE)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(88), ss.ReservedState.GetReservedConnectionId(),
		"gateway must track the reserved backend returned with a first-statement transaction error")
}

func TestScatterConn_Case3_NotInTransaction(t *testing.T) {
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	// TxState is Idle (default)

	err := sc.StreamExecute(context.Background(), newTestConn(), "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.True(t, gw.streamExecuteCalled, "should call regular StreamExecute")
	require.Equal(t, "SELECT 1", gw.streamExecuteSQL)
	require.False(t, gw.queryServiceByIDCalled, "should not call QueryServiceByID")
}

func TestScatterConn_Case3_StreamExecuteError(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: errors.New("query failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()

	err := sc.StreamExecute(context.Background(), newTestConn(), "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "query failed")
}

func TestScatterConn_StreamExecute_ReservedConn_UpdatesShardState(t *testing.T) {
	// When StreamExecute succeeds on a reserved connection and the multipooler says
	// the connection is still active, shard state should be updated with the
	// authoritative reservation reasons.
	gw := &mockGateway{
		callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss)
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_StreamExecute_ReservedConn_DestroyedSetsTxnFailed(t *testing.T) {
	// When StreamExecute returns a nil ReservedState (connection destroyed) while
	// in a transaction, the shard state must be cleared and TxnStatus set to Failed.
	gw := &mockGateway{
		streamExecuteErr: errors.New("reserved connection 42 not found"),
		// nil streamExecuteReturnState → destroyed
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

func TestScatterConn_ConcludeTransaction_RollbackOnDestroyedConn(t *testing.T) {
	// ROLLBACK on a destroyed connection should succeed with a synthetic result
	// and not propagate an error.
	gw := &mockGateway{
		concludeTransactionErr: errors.New("reserved connection not found"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	var callbackResult *sqltypes.Result
	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		nil, false, false,
		func(_ context.Context, result *sqltypes.Result) error {
			callbackResult = result
			return nil
		})

	require.NoError(t, err, "ROLLBACK on destroyed connection should not error")
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_ConcludeTransaction_RollbackAndChainOnDestroyedConnFails(t *testing.T) {
	// ROLLBACK AND CHAIN must preserve backend continuity. If the reserved
	// backend is gone, do not synthesize a successful ROLLBACK result and let the
	// gateway transparently move the chained transaction to another backend.
	gw := &mockGateway{
		concludeTransactionErr: errors.New("reserved connection not found"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{
			TableGroup: "tg1",
		},
		Mode: querypb.Mode_MODE_WRITABLE,
	}
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	callbackCalled := false
	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		nil, false, true,
		func(_ context.Context, _ *sqltypes.Result) error {
			callbackCalled = true
			return nil
		})

	require.Error(t, err, "ROLLBACK AND CHAIN on destroyed connection must fail closed")
	require.Contains(t, err.Error(), "conclude transaction failed")
	require.False(t, callbackCalled, "must not emit a synthetic ROLLBACK command tag")
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_ConcludeTransaction_CommitOnDestroyedConn(t *testing.T) {
	// COMMIT on a destroyed connection must propagate the error because the
	// client needs to know their COMMIT didn't happen (data may not be persisted).
	gw := &mockGateway{
		concludeTransactionErr: errors.New("reserved connection not found"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err, "COMMIT on destroyed connection must propagate error")
	require.Contains(t, err.Error(), "conclude transaction failed")
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_ConcludeTransaction_CommitStillReserved(t *testing.T) {
	// After COMMIT, if the connection is still reserved (e.g., temp tables),
	// the shard state should be updated with the new reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		concludeTransactionResult: &sqltypes.Result{CommandTag: "COMMIT"},
		concludeTransactionReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
	})

	var callbackResult *sqltypes.Result
	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false,
		func(_ context.Context, result *sqltypes.Result) error {
			callbackResult = result
			return nil
		})

	require.NoError(t, err)
	require.NotNil(t, callbackResult)
	require.Equal(t, "COMMIT", callbackResult.CommandTag)
	// Shard state should still exist but with updated reasons
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_ConcludeTransaction_CommitFailsButReservationSurvives(t *testing.T) {
	// A COMMIT that fails on a deferred constraint still leaves the backend
	// healthy and reserved (e.g. for a temp table). The error must still
	// propagate to the client, but the surviving reservation must be tracked
	// instead of being wiped as if the connection were destroyed.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		concludeTransactionErr: errors.New("commit failed: foreign key violation"),
		concludeTransactionReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
	})

	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT,
		nil, false, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err, "COMMIT failure must still propagate to the client")
	require.Contains(t, err.Error(), "conclude transaction failed")

	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "surviving reservation must not be cleared")
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_ConcludeTransaction_RollbackFailsButReservationSurvives(t *testing.T) {
	// Same surviving-reservation scenario but for plain ROLLBACK, which
	// swallows the error and synthesizes a ROLLBACK result. The surviving
	// reservation must still be tracked rather than cleared.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		concludeTransactionErr: errors.New("rollback failed"),
		concludeTransactionReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonTempTable,
	})

	var callbackResult *sqltypes.Result
	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		nil, false, false,
		func(_ context.Context, result *sqltypes.Result) error {
			callbackResult = result
			return nil
		})

	require.NoError(t, err, "plain ROLLBACK must not propagate the underlying error")
	require.NotNil(t, callbackResult)
	require.Equal(t, "ROLLBACK", callbackResult.CommandTag)

	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "surviving reservation must not be cleared")
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, protoutil.ReasonTempTable, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyFinalize_ErrorClearsShardState(t *testing.T) {
	// CopyFinalize error should clear all shard state because the
	// multipooler destroys the connection on all error paths.
	gw := &mockGateway{
		copyFinalizeErr: errors.New("COPY operation failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "COPY operation failed")
	// All shard state should be cleared
	require.Nil(t, state.GetMatchingShardState(target))
}

func TestScatterConn_CopyFinalize_ErrorSetsTxnFailed(t *testing.T) {
	// When CopyFinalize fails while in a transaction, TxnStatus should be set to Failed
	// so subsequent queries are rejected until ROLLBACK.
	gw := &mockGateway{
		copyFinalizeErr: errors.New("COPY operation failed"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "COPY operation failed")
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed (defense-in-depth)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

func TestScatterConn_CopyFinalize_ErrorPreservesReservedConn(t *testing.T) {
	// When CopyFinalize fails on a PG-level error (e.g., constraint violation)
	// but the multipooler kept the reserved connection alive because of an
	// unrelated reason (transaction, temp table), it returns the surviving
	// ReservedState alongside the error. The gateway must keep tracking that
	// connection — clearing state here would orphan the transaction and the
	// next statement would fail with "reserved connection not found".
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyFinalizeErr: errors.New("constraint violation"),
		copyFinalizeReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint violation")
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "reserved connection must survive PG-level COPY error when other reasons remain")
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyFinalize_ErrorForwardsNoticesBeforeError(t *testing.T) {
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyFinalizeResult: &sqltypes.Result{
			Notices: []*mterrors.PgDiagnostic{{MessageType: 'N', Severity: "NOTICE", Message: "input = {\"f1\":0}"}},
		},
		copyFinalizeReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
		copyFinalizeErr: errors.New("new row violates check constraint"),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget(conn.Database(), "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	var callbacks []*sqltypes.Result
	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, result *sqltypes.Result) error {
			callbacks = append(callbacks, result)
			return nil
		})

	require.Error(t, err)
	require.Contains(t, err.Error(), "check constraint")
	require.Len(t, callbacks, 1, "notice result must be forwarded before returning the COPY error")
	require.Len(t, callbacks[0].Notices, 1)
	require.Equal(t, "input = {\"f1\":0}", callbacks[0].Notices[0].Message)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "surviving reserved connection state should still be applied")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyInitiate_ErrorPreservesReservedConn(t *testing.T) {
	// When CopyInitiate fails because PG rejected the COPY (e.g., column
	// "xyz" does not exist) but the reserved connection was already held
	// for a temp table or transaction, the multipooler returns the surviving
	// state through the ERROR phase. Gateway tracking must remain pointed at
	// that connection so the next statement on the session can reuse it.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyReadyErr: errors.New("column \"xyz\" of relation \"x\" does not exist"),
		copyReadyReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTempTable,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonTempTable,
	})

	_, _, err := sc.CopyInitiate(context.Background(), conn, "tg1", "", "COPY x (xyz) FROM stdin", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Contains(t, err.Error(), "column \"xyz\"")
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "reserved connection must survive PG-level COPY init error when other reasons remain")
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
}

// TestScatterConn_CopyInitiate_ReuseReservedConnClearsPendingBeginQuery covers
// a COPY reaching an already-reserved, already-transactional connection right
// after a COMMIT/ROLLBACK AND CHAIN: see
// TestScatterConn_Case1_ExistingReservedConnection for why PendingBeginQuery
// can be non-empty here without there being a real BEGIN left to send, and why
// it must still be cleared once this statement reaches the backend.
func TestScatterConn_CopyInitiate_ReuseReservedConnClearsPendingBeginQuery(t *testing.T) {
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyReadyReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN"

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	_, _, err := sc.CopyInitiate(context.Background(), conn, "tg1", "", "COPY x FROM stdin", state,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	require.Empty(t, state.PendingBeginQuery,
		"a statement reaching the backend on the reused reservation answers it, even though it was not sent as a real BeginQuery")
}

func TestScatterConn_CopyFinalize_SuccessStillReserved(t *testing.T) {
	// After CopyFinalize, if the connection is still reserved (e.g., in a transaction),
	// the shard state should be updated with the authoritative reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyFinalizeResult: &sqltypes.Result{CommandTag: "COPY 10"},
		copyFinalizeReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyFinalize(context.Background(), conn, "tg1", "", state, nil,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyAbort_StillReserved(t *testing.T) {
	// After CopyAbort, if the connection is still reserved (e.g., in a transaction),
	// the shard state should be updated with the authoritative reasons.
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
	gw := &mockGateway{
		copyAbortReturnState: &querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             poolerID,
			ReservationReasons:   protoutil.ReasonTransaction,
		},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             poolerID,
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	require.NoError(t, err)
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "shard state should still exist")
	require.Equal(t, protoutil.ReasonTransaction, ss.ReservedState.GetReservationReasons())
}

func TestScatterConn_CopyAbort_ConnectionDestroyed(t *testing.T) {
	// After CopyAbort with a destroyed connection (nil ReservedState),
	// the shard state should be cleared.
	gw := &mockGateway{
		// nil copyAbortReturnState → destroyed
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy | protoutil.ReasonTransaction,
	})

	err := sc.CopyAbort(context.Background(), conn, "tg1", "", state)

	require.NoError(t, err)
	// Shard state must be cleared
	require.Nil(t, state.GetMatchingShardState(target))
	// Transaction status must be set to Failed (was in a transaction)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestIsCancellationError exercises every branch of isCancellationError: nil,
// the two context sentinels (direct and wrapped), a query-canceled PgDiagnostic
// (57014, the statement_timeout / explicit-cancel code), a non-cancellation
// PgDiagnostic (40001), and a plain error.
func TestIsCancellationError(t *testing.T) {
	require.False(t, isCancellationError(nil))
	require.True(t, isCancellationError(context.Canceled))
	require.True(t, isCancellationError(context.DeadlineExceeded))
	require.True(t, isCancellationError(fmt.Errorf("stream aborted: %w", context.DeadlineExceeded)))
	require.True(t, isCancellationError(mterrors.NewQueryCanceled()), "query_canceled (57014)")
	require.True(t, isCancellationError(mterrors.NewStatementTimeout()), "statement_timeout (57014)")
	require.False(t, isCancellationError(mterrors.NewReservedConnectionTerminated(42)),
		"serialization_failure (40001) is not a cancellation")
	require.False(t, isCancellationError(errors.New("connection refused")))
}

// TestScatterConn_StreamExecute_ReservedConn_KeptOnCancellation is the
// regression test for the statement_timeout fix: a cancellation on a reserved
// (temp-table) connection that is NOT in a transaction block must NOT drop the
// reservation. The cancelled stream returns a nil reserved state, which
// applyReservedState would otherwise treat as "connection destroyed" and clear
// — losing the session's temp tables. The backend only rolled back the
// cancelled statement and is still alive, so the reservation must survive.
func TestScatterConn_StreamExecute_ReservedConn_KeptOnCancellation(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: mterrors.NewStatementTimeout(),
		// nil streamExecuteReturnState → would be treated as destroyed without the fix
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusIdle) // temp-table reservation, not in a txn

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTempTable,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT pg_sleep(5)", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err, "the cancelled query still surfaces an error")
	ss := state.GetMatchingShardState(target)
	require.NotNil(t, ss, "reservation must survive a statement_timeout cancellation")
	require.Equal(t, uint64(42), ss.ReservedState.GetReservedConnectionId())
	require.NotEqual(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestScatterConn_StreamExecute_ReservedConn_CancellationInTransactionClears
// guards transaction safety: the keep-reservation path is gated on the
// connection NOT being in a transaction block. A cancellation while IN a
// transaction must still clear the reservation and mark the transaction failed
// (PostgreSQL aborts the transaction on a statement_timeout inside BEGIN), so
// transaction semantics are unchanged by the fix.
func TestScatterConn_StreamExecute_ReservedConn_CancellationInTransactionClears(t *testing.T) {
	gw := &mockGateway{
		streamExecuteErr: mterrors.NewStatementTimeout(),
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	target := protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.StreamExecute(context.Background(), conn, "tg1", "", "SELECT pg_sleep(5)", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })

	require.Error(t, err)
	require.Nil(t, state.GetMatchingShardState(target),
		"an in-transaction cancellation still clears the reservation")
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestScatterConn_StreamReplication verifies that StreamReplication fills in
// init.Target from the tablegroup/shard/state via buildTarget and forwards to
// the gateway, returning whatever stream/error it produces.
func TestScatterConn_StreamReplication(t *testing.T) {
	wantStream := multipoolerpb.MultipoolerService_StreamReplicationClient(nil)
	gw := &mockGateway{streamReplicationStream: wantStream}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()

	init := &multipoolerpb.StreamReplicationInit{User: "repluser"}
	stream, err := sc.StreamReplication(context.Background(), conn, "tg1", "0", state, init)

	require.NoError(t, err)
	require.Equal(t, wantStream, stream)
	require.NotNil(t, gw.streamReplicationInit)
	require.Same(t, init, gw.streamReplicationInit, "the same init must be forwarded, not a copy")
	require.Equal(t, "repluser", gw.streamReplicationInit.User, "caller-populated fields must survive")
	require.Equal(t, &clustermetadatapb.ShardKey{TableGroup: "tg1", Shard: "0"}, gw.streamReplicationInit.Target.ShardKey)
	require.Equal(t, querypb.Mode_MODE_WRITABLE, gw.streamReplicationInit.Target.Mode,
		"replication must route WRITABLE (leader) regardless of session replica-targeting")
}

// TestScatterConn_StreamReplication_TargetReplicaProducesInconsistentTarget
// proves buildTarget does NOT special-case replication: a connection that
// arrived on the replica-reads listener (state.TargetReplica()==true) still
// produces an INCONSISTENT target from this layer. The WRITABLE-forcing
// invariant that TestScatterConn_StreamReplication's "regardless of session
// replica-targeting" assertion describes is enforced one layer up, in
// PoolerGateway.StreamReplication (which clones the target and forces
// Mode_MODE_WRITABLE before dispatching to the pooler) — not here. This test
// exists so a future change to either layer can't silently stop enforcing
// that invariant without some test failing.
func TestScatterConn_StreamReplication_TargetReplicaProducesInconsistentTarget(t *testing.T) {
	wantStream := multipoolerpb.MultipoolerService_StreamReplicationClient(nil)
	gw := &mockGateway{streamReplicationStream: wantStream}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	state.SetTargetReplica(true)
	conn := newTestConn()

	init := &multipoolerpb.StreamReplicationInit{User: "repluser"}
	_, err := sc.StreamReplication(context.Background(), conn, "tg1", "0", state, init)

	require.NoError(t, err)
	require.NotNil(t, gw.streamReplicationInit)
	require.Equal(t, querypb.Mode_MODE_INCONSISTENT, gw.streamReplicationInit.Target.Mode,
		"ScatterConn itself does not force WRITABLE for replica-targeted connections; "+
			"PoolerGateway.StreamReplication is responsible for enforcing that")
}

// TestScatterConn_StreamReplication_PropagatesError verifies that a gateway
// error (e.g. no leader observed) is returned to the caller unchanged.
func TestScatterConn_StreamReplication_PropagatesError(t *testing.T) {
	wantErr := errors.New("no leader available")
	gw := &mockGateway{streamReplicationErr: wantErr}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()

	stream, err := sc.StreamReplication(context.Background(), conn, "tg1", "0", state, &multipoolerpb.StreamReplicationInit{})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, stream)
}

// TestConcludeTransaction_AlwaysSendsRollbackSessionSettings pins the strict
// contract: every conclude RPC carries rollback_session_settings. On the
// plain-ROLLBACK path the gateway has already reverted its state, so the
// current (post-revert) map is sent; the pooler treats absence as an
// invariant violation and would taint the backend.
func TestConcludeTransaction_AlwaysSendsRollbackSessionSettings(t *testing.T) {
	mock := &mockGateway{concludeTransactionResult: &sqltypes.Result{CommandTag: "ROLLBACK"}}
	sc := NewScatterConn(mock, slog.Default())
	conn := newTestConn()
	state := handler.NewMultigatewayConnectionState()
	// Simulate executeRollback's ordering: gateway state already reverted, no
	// transaction frames left.
	state.SetSessionVariable("work_mem", "1MB")
	state.SetReservedConnection(&querypb.Target{}, &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	err := sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		nil, false, false,
		func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	require.True(t, mock.concludeRollbackSessionSettingsSet)
	require.NotNil(t, mock.concludeRollbackSessionSettings,
		"conclude must always carry a rollback map; absence is an invariant violation pooler-side")
	assert.Equal(t, "1MB", mock.concludeRollbackSessionSettings["work_mem"],
		"with no transaction frame, the (already reverted) current map is the rollback map")
}

// TestReleaseAll_MidTransactionStampsPreBeginMap pins the disconnect-release
// label: the multipooler rolls an open transaction back before releasing, so
// PostgreSQL discards every in-transaction SET — the release options must
// carry the pre-BEGIN snapshot, never the current map still holding the
// abandoned transaction's settings. Outside a transaction no rollback runs
// and the current map is the truth.
func TestReleaseAll_MidTransactionStampsPreBeginMap(t *testing.T) {
	mock := &mockGateway{}
	sc := NewScatterConn(mock, slog.Default())
	conn := newTestConn()
	state := handler.NewMultigatewayConnectionState()
	state.SetSessionVariable("work_mem", "7MB")
	state.BeginTransaction()
	state.SetSessionVariable("work_mem", "9MB")
	state.SetReservedConnection(protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE),
		&querypb.ReservedState{
			ReservedConnectionId: 42,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction,
		})

	require.NoError(t, sc.ReleaseAllReservedConnections(context.Background(), conn, state, false))
	require.NotNil(t, mock.releaseReservedConnectionSettings)
	assert.Equal(t, "7MB", mock.releaseReservedConnectionSettings["work_mem"],
		"mid-transaction disconnect must stamp the pre-BEGIN value the rollback restores")

	// Same shape without a transaction: no rollback will run, so the current
	// map is what the backend really holds.
	mock2 := &mockGateway{}
	sc2 := NewScatterConn(mock2, slog.Default())
	state2 := handler.NewMultigatewayConnectionState()
	state2.SetSessionVariable("work_mem", "9MB")
	state2.SetReservedConnection(protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE),
		&querypb.ReservedState{
			ReservedConnectionId: 43,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTempTable,
		})

	require.NoError(t, sc2.ReleaseAllReservedConnections(context.Background(), newTestConn(), state2, false))
	require.NotNil(t, mock2.releaseReservedConnectionSettings)
	assert.Equal(t, "9MB", mock2.releaseReservedConnectionSettings["work_mem"],
		"without a transaction the current map is the correct label")
}

// pinnedCopyGateway models a shard served by several interchangeable poolers
// with application routing that re-resolves a COPY's target while it starts: the
// COPY begins on dst-2, under the destination tablegroup, though the statement
// was planned for the source.
type pinnedCopyGateway struct {
	*mockGateway
	rewriteTo string
	poolerID  *clustermetadatapb.ID

	directDataCalls int
	byIDRequests    []*clustermetadatapb.ID
	pinned          *pinnedCopyService
}

func (g *pinnedCopyGateway) CopyReady(_ context.Context, target *querypb.Target, _ string, _ *querypb.ExecuteOptions, _ *querypb.ReservationOptions) (int16, []int16, *querypb.ReservedState, error) {
	// Rewrite replaces the key, as application routing does.
	target.ShardKey = &clustermetadatapb.ShardKey{Database: target.GetShardKey().GetDatabase(), TableGroup: g.rewriteTo, Shard: target.GetShardKey().GetShard()}
	return 0, nil, &querypb.ReservedState{ReservedConnectionId: 7, PoolerId: g.poolerID, ReservationReasons: protoutil.ReasonCopy}, nil
}

// The unpinned data-phase methods stand for "some other pooler": a stream is not
// there.
func (g *pinnedCopyGateway) CopySendData(context.Context, *querypb.Target, []byte, *querypb.ExecuteOptions) error {
	g.directDataCalls++
	return errors.New("no active COPY stream")
}

func (g *pinnedCopyGateway) CopyFinalize(context.Context, *querypb.Target, []byte, *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	g.directDataCalls++
	return nil, nil, errors.New("no active COPY stream")
}

func (g *pinnedCopyGateway) QueryServiceByID(_ context.Context, id *clustermetadatapb.ID, _ *querypb.Target) (queryservice.QueryService, error) {
	g.byIDRequests = append(g.byIDRequests, id)
	g.pinned = &pinnedCopyService{QueryService: g}
	return g.pinned, nil
}

// pinnedCopyService is the pooler the COPY stream lives on.
type pinnedCopyService struct {
	queryservice.QueryService
	sent      []*querypb.Target
	finalized []*querypb.Target
}

func (s *pinnedCopyService) CopySendData(_ context.Context, target *querypb.Target, _ []byte, _ *querypb.ExecuteOptions) error {
	s.sent = append(s.sent, target)
	return nil
}

func (s *pinnedCopyService) CopyFinalize(_ context.Context, target *querypb.Target, _ []byte, _ *querypb.ExecuteOptions) (*sqltypes.Result, *querypb.ReservedState, error) {
	s.finalized = append(s.finalized, target)
	return &sqltypes.Result{CommandTag: "COPY 1"}, nil, nil
}

// TestScatterConn_CopyDataPhaseFollowsThePoolerAndTargetThatStartedIt: the data
// phase of a COPY must reach the pooler that holds the stream, under the target
// the reservation was recorded with, even when routing moved the COPY off the
// tablegroup it was planned for and the shard has other interchangeable poolers.
func TestScatterConn_CopyDataPhaseFollowsThePoolerAndTargetThatStartedIt(t *testing.T) {
	poolerID := &clustermetadatapb.ID{Cell: "cell1", Name: "dst-2"}
	gw := &pinnedCopyGateway{mockGateway: &mockGateway{}, rewriteTo: "destTG", poolerID: poolerID}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	ctx := context.Background()

	_, _, err := sc.CopyInitiate(ctx, conn, "migrateTG", "", "COPY t FROM STDIN", state,
		func(context.Context, *sqltypes.Result) error { return nil })
	require.NoError(t, err)

	// The statement still says migrateTG, as its plan does.
	require.NoError(t, sc.CopySendData(ctx, conn, "migrateTG", "", state, []byte("1\n")))
	require.Zero(t, gw.directDataCalls, "the data phase must not be sent to a pooler selected afresh")
	require.Len(t, gw.byIDRequests, 1)
	require.Equal(t, "dst-2", gw.byIDRequests[0].GetName())
	require.Len(t, gw.pinned.sent, 1)
	require.Equal(t, "destTG", gw.pinned.sent[0].GetShardKey().GetTableGroup(), "under the target the COPY started with")

	require.NoError(t, sc.CopyFinalize(ctx, conn, "migrateTG", "", state, nil,
		func(context.Context, *sqltypes.Result) error { return nil }))
	require.Zero(t, gw.directDataCalls)
	require.Len(t, gw.pinned.finalized, 1)
	require.Nil(t, state.CopyTarget(), "the COPY's target is forgotten once it ends")
}

// TestScatterConn_AnAutonomousStatementIgnoresTheSessionsTransaction: what a transaction
// sends after it has committed (its NOTIFYs) runs on a pooled connection, as control
// traffic, whatever the session's transaction status is, and reserves nothing.
func TestScatterConn_AnAutonomousStatementIgnoresTheSessionsTransaction(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	require.NoError(t, sc.StreamExecute(context.Background(), conn, "default", "", "SELECT pg_notify('a', 'b')", nil, state,
		engine.PlanExecInfo{Control: true, Autonomous: true}, false, func(context.Context, *sqltypes.Result) error { return nil }))
	require.True(t, gw.streamExecuteCalled)
	require.True(t, gw.streamExecuteTarget.GetControl())
	require.Nil(t, gw.streamExecuteReservationOps, "no reservation, in a transaction or not")
	require.Empty(t, state.ShardStates)
}

// TestScatterConn_AnAutonomousPortalIgnoresTheSessionsState: the delivery of a committed
// transaction's NOTIFYs is a parameterized statement, and it too neither uses the session's
// reservation on the default tablegroup nor starts one.
func TestScatterConn_AnAutonomousPortalIgnoresTheSessionsState(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.SetReservedConnection(protoutil.NewTarget("", "default", "", querypb.Mode_MODE_WRITABLE), &querypb.ReservedState{
		ReservedConnectionId: 8, PoolerId: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, ReservationReasons: protoutil.ReasonTempTable,
	})

	require.NoError(t, sc.PortalStreamExecute(context.Background(), "default", "", conn, state, testPortalInfo(), 0, false,
		engine.PlanExecInfo{Control: true, Autonomous: true}, false, func(context.Context, *sqltypes.Result) error { return nil }))
	require.True(t, gw.portalCalled)
	require.False(t, gw.queryServiceByIDCalled, "the session's pooler is not looked up")
	require.Zero(t, gw.portalOpts.GetReservedConnectionId(), "the session's reservation is not used")
	require.Nil(t, gw.portalReservationOps, "and none is started")
}

// TestScatterConn_ControlStatementsAreMarkedControl: a statement the planner marks
// as control reaches the gateway on a control target; others do not.
func TestScatterConn_ControlStatementsAreMarkedControl(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "NOTIFY"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	noop := func(context.Context, *sqltypes.Result) error { return nil }

	require.NoError(t, sc.StreamExecute(context.Background(), conn, "default", "", "NOTIFY c", nil, state, engine.PlanExecInfo{Control: true}, false, noop))
	require.True(t, gw.streamExecuteTarget.GetControl())
	require.NoError(t, sc.StreamExecute(context.Background(), conn, "default", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false, noop))
	require.False(t, gw.streamExecuteTarget.GetControl())
}

// TestScatterConn_CopyAbortForgetsTheCopyTargetEvenWhenAlreadyCleanedUp.
func TestScatterConn_CopyAbortForgetsTheCopyTargetEvenWhenAlreadyCleanedUp(t *testing.T) {
	gw := &mockGateway{}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	state.SetCopyTarget(protoutil.NewTarget("", "tg1", "", querypb.Mode_MODE_WRITABLE))

	require.NoError(t, sc.CopyAbort(context.Background(), conn, "tg1", "", state))
	require.Nil(t, state.CopyTarget())
}

// beginTransactionOn runs a BEGIN and a first statement on tg1 so that the session holds a
// transaction reservation there, the way a client's does.
func beginTransactionOn(t *testing.T, sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState, tg string) {
	t.Helper()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN"
	gw.streamExecuteReturnState = &querypb.ReservedState{
		ReservedConnectionId: 77,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	}
	require.NoError(t, sc.StreamExecute(context.Background(), conn, tg, "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil }))
	require.Equal(t, uint64(77), state.GetMatchingShardState(protoutil.NewTarget("", tg, "", querypb.Mode_MODE_WRITABLE)).ReservedState.GetReservedConnectionId())
	gw.streamExecuteCalled = false
}

// TestScatterConn_ATransactionNeverSpansTablegroups: a transaction whose reservation is on one
// tablegroup does not take a new reservation on another when the next statement targets it,
// as it does after the application's traffic was moved: the statement fails with 40001, which
// ends the transaction, and nothing is sent to a pooler.
func TestScatterConn_ATransactionNeverSpansTablegroups(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	beginTransactionOn(t, sc, gw, conn, state, "old")

	err := sc.StreamExecute(context.Background(), conn, "new", "", "INSERT INTO t VALUES (1)", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })
	require.Error(t, err)
	var pgErr *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, mterrors.PgSSSerializationFailure, pgErr.Code, "an error that ends the transaction and invites a retry of all of it")
	require.Contains(t, err.Error(), `"old"`)
	require.Contains(t, err.Error(), `"new"`)
	require.False(t, gw.streamExecuteCalled, "no reservation is started on the other tablegroup")
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus(), "the transaction is over until the client rolls back")
	require.Nil(t, state.GetMatchingShardState(protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE)),
		"the entry for the old reservation is stale, since the fence that preceded the move waited for it to be released, and is dropped")
	requireRollbackThenANewTransactionOn(t, sc, gw, conn, state, "new")
}

// requireRollbackThenANewTransactionOn: after the 40001 the client rolls back, and a new
// transaction starts and runs on the tablegroup that has the traffic.
func requireRollbackThenANewTransactionOn(t *testing.T, sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState, tg string) {
	t.Helper()
	require.NoError(t, sc.ConcludeTransaction(context.Background(), conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, nil, false, false,
		func(context.Context, *sqltypes.Result) error { return nil }), "there is nothing left to roll back on a pooler")
	conn.SetTxnStatus(protocol.TxnStatusIdle)
	beginTransactionOn(t, sc, gw, conn, state, tg)
}

// TestScatterConn_ATransactionWhoseReservationWasLostIsNotRestarted: the pooler reaped an idle
// transaction's reservation (the gateway's state for it is gone), and traffic was moved. The
// next statement of the transaction fails instead of starting a new transaction on the new
// tablegroup that would commit only its second half.
func TestScatterConn_ATransactionWhoseReservationWasLostIsNotRestarted(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	beginTransactionOn(t, sc, gw, conn, state, "old")
	state.ClearReservedConnection(protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE))

	err := sc.StreamExecute(context.Background(), conn, "new", "", "INSERT INTO t VALUES (2)", nil, state, engine.PlanExecInfo{}, false,
		func(_ context.Context, _ *sqltypes.Result) error { return nil })
	var pgErr *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, mterrors.PgSSSerializationFailure, pgErr.Code)
	require.Contains(t, err.Error(), "is gone")
	require.False(t, gw.streamExecuteCalled)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestScatterConn_AReservationOnAnotherTablegroupIsLostWith40001: a temporary table's reservation
// is on one tablegroup, and a statement arrives for another, as after the application's traffic
// moved. The reservation is gone with the old tablegroup's backend (a fence waits for reservations
// to be released), and the move hides that loss, so the statement fails with 40001 and the stale
// entry is dropped, whether or not the statement needs a reservation of its own: a plain
// autocommit statement included. The session then goes on, as it does when a failover or a reap
// takes a reservation: what else the lost reservation held is the business of the general
// reservation-loss handling.
func TestScatterConn_AReservationOnAnotherTablegroupIsLostWith40001(t *testing.T) {
	newSession := func(t *testing.T) (*mockGateway, *ScatterConn, *handler.MultigatewayConnectionState, *server.Conn) {
		gw := &mockGateway{
			streamExecuteReturnState: &querypb.ReservedState{
				ReservedConnectionId: 5,
				PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
				ReservationReasons:   protoutil.ReasonTempTable,
			},
			callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"},
		}
		sc := NewScatterConn(gw, slog.Default())
		state := handler.NewMultigatewayConnectionState()
		conn := newTestConn()
		cb := func(context.Context, *sqltypes.Result) error { return nil }
		require.NoError(t, sc.StreamExecute(context.Background(), conn, "old", "", "CREATE TEMP TABLE x (i int)", nil, state, engine.PlanExecInfo{TempTable: true}, false, cb))
		gw.streamExecuteCalled = false
		return gw, sc, state, conn
	}
	cb := func(context.Context, *sqltypes.Result) error { return nil }
	old := protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE)

	t.Run("a plain statement", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		err := sc.StreamExecute(context.Background(), conn, "new", "", "UPDATE t SET i = 1", nil, state, engine.PlanExecInfo{}, false, cb)
		requireStaleReservationRefused(t, err)
		require.False(t, gw.streamExecuteCalled, "nothing runs on the new tablegroup on the strength of a lost reservation")
		requireTheSessionContinues(t, sc, gw, conn, state)
		require.Nil(t, state.GetMatchingShardState(old), "the stale entry is dropped")
	})
	t.Run("a statement that needs a reservation of its own", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		err := sc.StreamExecute(context.Background(), conn, "new", "", "CREATE TEMP TABLE y (i int)", nil, state, engine.PlanExecInfo{TempTable: true}, false, cb)
		requireStaleReservationRefused(t, err)
		require.False(t, gw.streamExecuteCalled)
	})
	t.Run("a portal that cannot suspend", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		err := sc.PortalStreamExecute(context.Background(), "new", "", conn, state, testPortalInfo(), 0, false, engine.PlanExecInfo{}, false, cb)
		requireStaleReservationRefused(t, err)
		require.False(t, gw.portalCalled)
	})
	t.Run("a portal that may suspend", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		err := sc.PortalStreamExecute(context.Background(), "new", "", conn, state, testPortalInfo(), 10, false, engine.PlanExecInfo{}, false, cb)
		requireStaleReservationRefused(t, err)
		require.Contains(t, err.Error(), `"old"`)
		require.False(t, gw.portalCalled, "nothing is sent to a pooler")
	})
	t.Run("COPY FROM and COPY TO outside a transaction", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		_, _, err := sc.CopyInitiate(context.Background(), conn, "new", "", "COPY t FROM STDIN", state, cb)
		requireStaleReservationRefused(t, err)
		require.False(t, gw.copyReadyCalled)

		gw, sc, state, conn = newSession(t)
		_, _, _, err = sc.CopyOutInitiate(context.Background(), conn, "new", "", "COPY t TO STDOUT", state)
		requireStaleReservationRefused(t, err)
		require.False(t, gw.copyOutReadyCalled)
	})
	t.Run("autonomous and control statements are not part of the session", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		require.NoError(t, sc.StreamExecute(context.Background(), conn, "new", "", "SELECT pg_notify('c', 'p')", nil, state, engine.PlanExecInfo{Autonomous: true}, false, cb))
		require.True(t, gw.streamExecuteCalled)
		gw.streamExecuteCalled = false
		require.NoError(t, sc.StreamExecute(context.Background(), conn, "default", "", "SELECT 1", nil, state, engine.PlanExecInfo{Control: true}, false, cb))
		require.True(t, gw.streamExecuteCalled)
		require.NotNil(t, state.GetMatchingShardState(old), "neither touched the session's state")
	})
	t.Run("on the tablegroup that holds the state nothing is refused", func(t *testing.T) {
		gw, sc, state, conn := newSession(t)
		require.NoError(t, sc.StreamExecute(context.Background(), conn, "old", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false, cb))
		require.NoError(t, sc.PortalStreamExecute(context.Background(), "old", "", conn, state, testPortalInfo(), 10, false, engine.PlanExecInfo{}, false, cb))
		require.True(t, gw.portalCalled)
	})
}

// TestScatterConn_AFirstStatementOfATransactionStartsItOnTheCurrentTablegroup: a transaction
// that has not begun on any pooler yet starts where its first statement goes, whichever
// tablegroup the traffic is on.
func TestScatterConn_AFirstStatementOfATransactionStartsItOnTheCurrentTablegroup(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	beginTransactionOn(t, sc, gw, conn, state, "new")
}

// requireStaleReservationRefused requires the error of a statement that found the session's
// reservation lost to a move of the traffic: 40001, and not FATAL, so the session goes on.
func requireStaleReservationRefused(t *testing.T, err error) {
	t.Helper()
	var pgErr *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, mterrors.PgSSSerializationFailure, pgErr.Code)
	require.False(t, pgErr.IsFatal(), "the session is not ended")
}

// requireTheSessionContinues requires that the next statement of the session goes to the
// tablegroup that has the traffic.
func requireTheSessionContinues(t *testing.T, sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState) {
	t.Helper()
	gw.streamExecuteCalled = false
	require.NoError(t, sc.StreamExecute(context.Background(), conn, "new", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false,
		func(context.Context, *sqltypes.Result) error { return nil }))
	require.True(t, gw.streamExecuteCalled, "the statement ran on the tablegroup that has the traffic")
}

// requireEndedWith40001 requires the error that ends a transaction and invites a retry of all
// of it.
func requireEndedWith40001(t *testing.T, err error) {
	t.Helper()
	var pgErr *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, mterrors.PgSSSerializationFailure, pgErr.Code)
}

// TestScatterConn_TheTransactionRuleHoldsOnEveryPath: a transaction that began on one
// tablegroup does not start a reservation on another through any path that can: an extended
// protocol portal, COPY FROM STDIN, and COPY TO STDOUT. Each fails with 40001, ends the
// transaction, and sends nothing to a pooler.
func TestScatterConn_TheTransactionRuleHoldsOnEveryPath(t *testing.T) {
	ctx := context.Background()
	begun := func(t *testing.T) (*mockGateway, *ScatterConn, *handler.MultigatewayConnectionState, *server.Conn) {
		gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
		sc := NewScatterConn(gw, slog.Default())
		state := handler.NewMultigatewayConnectionState()
		conn := newTestConn()
		beginTransactionOn(t, sc, gw, conn, state, "old")
		return gw, sc, state, conn
	}
	paths := map[string]func(t *testing.T, gw *mockGateway, sc *ScatterConn, state *handler.MultigatewayConnectionState, conn *server.Conn) (called bool, err error){
		"portal": func(t *testing.T, gw *mockGateway, sc *ScatterConn, state *handler.MultigatewayConnectionState, conn *server.Conn) (bool, error) {
			err := sc.PortalStreamExecute(ctx, "new", "", conn, state, testPortalInfo(), 0, false, engine.PlanExecInfo{}, false,
				func(context.Context, *sqltypes.Result) error { return nil })
			return gw.portalCalled, err
		},
		"copy from stdin": func(t *testing.T, gw *mockGateway, sc *ScatterConn, state *handler.MultigatewayConnectionState, conn *server.Conn) (bool, error) {
			_, _, err := sc.CopyInitiate(ctx, conn, "new", "", "COPY t FROM STDIN", state, func(context.Context, *sqltypes.Result) error { return nil })
			return gw.copyReadyCalled, err
		},
		"copy to stdout": func(t *testing.T, gw *mockGateway, sc *ScatterConn, state *handler.MultigatewayConnectionState, conn *server.Conn) (bool, error) {
			_, _, _, err := sc.CopyOutInitiate(ctx, conn, "new", "", "COPY t TO STDOUT", state)
			return gw.copyOutReadyCalled, err
		},
	}
	for name, run := range paths {
		t.Run(name+": transaction on another tablegroup", func(t *testing.T) {
			gw, sc, state, conn := begun(t)
			called, err := run(t, gw, sc, state, conn)
			requireEndedWith40001(t, err)
			require.Contains(t, err.Error(), `"old"`)
			require.False(t, called, "nothing is sent to a pooler")
			require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
		})
		t.Run(name+": transaction whose reservation was lost", func(t *testing.T) {
			gw, sc, state, conn := begun(t)
			state.ClearReservedConnection(protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE))
			called, err := run(t, gw, sc, state, conn)
			requireEndedWith40001(t, err)
			require.Contains(t, err.Error(), "is gone")
			require.False(t, called)
			require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
		})
	}
}

// TestScatterConn_DiscardTempInsideATransactionKeepsItsDeferredBegin: BEGIN is deferred to the
// first statement, so a DISCARD TEMP right after it releases the temporary table's
// reservation and leaves the transaction not yet started. The next statement starts it, with
// the BEGIN's options, and is not refused as a transaction whose reservation is gone.
func TestScatterConn_DiscardTempInsideATransactionKeepsItsDeferredBegin(t *testing.T) {
	gw := &mockGateway{
		streamExecuteReturnState: &querypb.ReservedState{
			ReservedConnectionId: 5,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTempTable,
		},
		callbackResult: &sqltypes.Result{CommandTag: "CREATE TABLE"},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	cb := func(context.Context, *sqltypes.Result) error { return nil }
	require.NoError(t, sc.StreamExecute(context.Background(), conn, "tg", "", "CREATE TEMP TABLE x (i int)", nil, state, engine.PlanExecInfo{TempTable: true}, false, cb))

	// BEGIN ISOLATION LEVEL SERIALIZABLE is deferred.
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN ISOLATION LEVEL SERIALIZABLE"

	// DISCARD TEMP, as the primitive runs it; the pooler releases the connection.
	prim := engine.NewDiscardTempPrimitive("DISCARD TEMP", "tg")
	require.NoError(t, prim.StreamExecute(context.Background(), sc, conn, state, nil, engine.PlanExecInfo{}, cb))
	require.False(t, state.HasTempTableReservation())
	require.Equal(t, "BEGIN ISOLATION LEVEL SERIALIZABLE", state.PendingBeginQuery, "the BEGIN has not been sent anywhere and is still owed")

	gw.streamExecuteCalled = false
	gw.streamExecuteReturnState = &querypb.ReservedState{
		ReservedConnectionId: 6,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	}
	require.NoError(t, sc.StreamExecute(context.Background(), conn, "tg", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false, cb))
	require.True(t, gw.streamExecuteCalled)
	require.Equal(t, "BEGIN ISOLATION LEVEL SERIALIZABLE", gw.streamExecuteReservationOps.GetBeginQuery(), "the transaction starts with its options")
}

// TestScatterConn_AReapedReservationAndAMoveFailWith40001: the pooler reaped a reservation of the
// session's and then the traffic moved. What the gateway still tracks is stale, and the move hides
// the loss, so the next statement that needs a reservation fails with 40001 whatever the
// reservation held: a transaction (the client rolls back, and the next transaction starts on the
// tablegroup that has the traffic) or a temporary table (the session goes on). On every path that
// can start a reservation.
func TestScatterConn_AReapedReservationAndAMoveFailWith40001(t *testing.T) {
	ctx := context.Background()
	cb := func(context.Context, *sqltypes.Result) error { return nil }
	paths := map[string]func(sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState) (called bool, err error){
		"statement": func(sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState) (bool, error) {
			err := sc.StreamExecute(ctx, conn, "new", "", "CREATE TEMP TABLE y (i int)", nil, state, engine.PlanExecInfo{TempTable: true}, false, cb)
			return gw.streamExecuteCalled, err
		},
		"portal": func(sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState) (bool, error) {
			err := sc.PortalStreamExecute(ctx, "new", "", conn, state, testPortalInfo(), 0, false, engine.PlanExecInfo{TempTable: true}, false, cb)
			return gw.portalCalled, err
		},
		"copy from stdin": func(sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState) (bool, error) {
			conn.SetTxnStatus(protocol.TxnStatusInBlock)
			_, _, err := sc.CopyInitiate(ctx, conn, "new", "", "COPY t FROM STDIN", state, cb)
			return gw.copyReadyCalled, err
		},
		"copy to stdout": func(sc *ScatterConn, gw *mockGateway, conn *server.Conn, state *handler.MultigatewayConnectionState) (bool, error) {
			conn.SetTxnStatus(protocol.TxnStatusInBlock)
			_, _, _, err := sc.CopyOutInitiate(ctx, conn, "new", "", "COPY t TO STDOUT", state)
			return gw.copyOutReadyCalled, err
		},
	}
	old := protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE)
	for name, run := range paths {
		t.Run(name+": a lost temporary table is a 40001 and the session goes on", func(t *testing.T) {
			gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
			sc := NewScatterConn(gw, slog.Default())
			state := handler.NewMultigatewayConnectionState()
			conn := newTestConn()
			// A reservation for a temporary table on "old", which the pooler then reaped.
			state.SetReservedConnection(old, &querypb.ReservedState{
				ReservedConnectionId: 5,
				PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
				ReservationReasons:   protoutil.ReasonTempTable,
			})
			called, err := run(sc, gw, conn, state)
			requireStaleReservationRefused(t, err)
			require.Contains(t, err.Error(), "temp_table")
			require.False(t, called, "nothing is sent to a pooler")
			require.Nil(t, state.GetMatchingShardState(old), "the stale entry is dropped")
			conn.SetTxnStatus(protocol.TxnStatusIdle)
			requireTheSessionContinues(t, sc, gw, conn, state)
		})
		t.Run(name+": a lost transaction is a 40001, a rollback, and a new transaction on the new tablegroup", func(t *testing.T) {
			gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
			sc := NewScatterConn(gw, slog.Default())
			state := handler.NewMultigatewayConnectionState()
			conn := newTestConn()
			beginTransactionOn(t, sc, gw, conn, state, "old") // the pooler then reaps it
			gw.streamExecuteCalled = false
			called, err := run(sc, gw, conn, state)
			requireEndedWith40001(t, err)
			require.False(t, called)
			require.Nil(t, state.GetMatchingShardState(old), "the stale entry is dropped")
			requireRollbackThenANewTransactionOn(t, sc, gw, conn, state, "new")
		})
	}
}

// TestScatterConn_ASuspendedPortalInATransactionIsALostTransaction: the pooler adds the portal
// reason to a transaction's reservation whenever a portal suspends, which a fetch-size cursor
// does all the time. When that reservation is lost and the traffic moved it is a lost
// transaction: 40001, a rollback, and a new transaction on the new tablegroup. A session that
// also holds WITH HOLD cursors gets the same 40001: what is lost with the reservation is the
// business of the general reservation-loss handling, not of this rule.
func TestScatterConn_ASuspendedPortalInATransactionIsALostTransaction(t *testing.T) {
	ctx := context.Background()
	old := protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE)
	setup := func() (*mockGateway, *ScatterConn, *handler.MultigatewayConnectionState, *server.Conn) {
		gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
		sc := NewScatterConn(gw, slog.Default())
		state := handler.NewMultigatewayConnectionState()
		conn := newTestConn()
		conn.SetTxnStatus(protocol.TxnStatusInBlock)
		state.SetReservedConnection(old, &querypb.ReservedState{
			ReservedConnectionId: 9,
			PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ReservationReasons:   protoutil.ReasonTransaction | protoutil.ReasonPortal,
		})
		return gw, sc, state, conn
	}
	run := func(sc *ScatterConn, conn *server.Conn, state *handler.MultigatewayConnectionState) error {
		return sc.StreamExecute(ctx, conn, "new", "", "UPDATE t SET i = 1", nil, state, engine.PlanExecInfo{}, false,
			func(context.Context, *sqltypes.Result) error { return nil })
	}

	for _, withHold := range []bool{false, true} {
		gw, sc, state, conn := setup()
		if withHold {
			state.OpenHoldCursors["c"] = true
		}
		err := run(sc, conn, state)
		requireEndedWith40001(t, err)
		require.False(t, gw.streamExecuteCalled)
		requireRollbackThenANewTransactionOn(t, sc, gw, conn, state, "new")
	}
}

// TestScatterConn_ASuspendedPortalOutsideATransactionIsRefusedWith40001: the pooler reserves a
// portal suspended outside a transaction with the portal reason alone. When that reservation is
// lost and the traffic moved, the portal must not be bound again on the new tablegroup on its next
// Execute, returning its first rows again with no error: the statement fails with 40001, and the
// stale entry is dropped.
func TestScatterConn_ASuspendedPortalOutsideATransactionIsRefusedWith40001(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	old := protoutil.NewTarget("", "old", "", querypb.Mode_MODE_WRITABLE)
	state.SetReservedConnection(old, &querypb.ReservedState{
		ReservedConnectionId: 9,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonPortal,
	})
	err := sc.PortalStreamExecute(context.Background(), "new", "", conn, state, testPortalInfo(), 10, false, engine.PlanExecInfo{}, false,
		func(context.Context, *sqltypes.Result) error { return nil })
	requireStaleReservationRefused(t, err)
	require.Contains(t, err.Error(), "portal")
	require.False(t, gw.portalCalled, "the portal is not bound again on the new tablegroup")
	require.Nil(t, state.GetMatchingShardState(old))
	requireTheSessionContinues(t, sc, gw, conn, state)
}

// TestScatterConn_AControlStatementOnAnUnsafeConnectionMakesNoReservation: a control NOTIFY
// outside a transaction goes to the default tablegroup while the application is elsewhere. On an
// unsafe connection, which reserves a backend for every statement, it must not take a reservation
// of the session's there: the next application statement would then find session state on
// another tablegroup and end the session.
func TestScatterConn_AControlStatementOnAnUnsafeConnectionMakesNoReservation(t *testing.T) {
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "NOTIFY"}}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := server.NewTestConn(&bytes.Buffer{}, server.WithTestUnsafeConnection()).Conn
	cb := func(context.Context, *sqltypes.Result) error { return nil }

	require.NoError(t, sc.StreamExecute(context.Background(), conn, "default", "", "NOTIFY c, 'p'", nil, state, engine.PlanExecInfo{Control: true}, false, cb))
	require.True(t, gw.streamExecuteCalled)
	require.Nil(t, gw.streamExecuteReservationOps, "no reservation is asked for")

	// The application's next statement, on its own tablegroup, is not refused.
	gw.streamExecuteCalled = false
	require.NoError(t, sc.StreamExecute(context.Background(), conn, "app", "", "SELECT 1", nil, state, engine.PlanExecInfo{}, false, cb))
	require.True(t, gw.streamExecuteCalled)
}

// TestScatterConn_AControlNotifyInATransactionStillJoinsIt: when the application is on the default
// tablegroup, a NOTIFY inside a transaction is an ordinary statement of the transaction even
// though it is marked control: it must reserve the transaction's backend, or it would run
// outside the transaction and be delivered at once instead of at the commit.
func TestScatterConn_AControlNotifyInATransactionStillJoinsIt(t *testing.T) {
	gw := &mockGateway{
		callbackResult:           &sqltypes.Result{CommandTag: "NOTIFY"},
		streamExecuteReturnState: &querypb.ReservedState{ReservedConnectionId: 3, PoolerId: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}, ReservationReasons: protoutil.ReasonTransaction},
	}
	sc := NewScatterConn(gw, slog.Default())
	state := handler.NewMultigatewayConnectionState()
	conn := newTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.PendingBeginQuery = "BEGIN"
	require.NoError(t, sc.StreamExecute(context.Background(), conn, "default", "", "NOTIFY c, 'p'", nil, state, engine.PlanExecInfo{Control: true}, false,
		func(context.Context, *sqltypes.Result) error { return nil }))
	require.NotNil(t, gw.streamExecuteReservationOps)
	require.True(t, protoutil.HasTransactionReason(gw.streamExecuteReservationOps.GetReasons()), "it joins the transaction")
}
