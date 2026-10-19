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
	"context"
	"fmt"
	"log/slog"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// The tests here cover the one invariant of reservation_loss.go: a session's reserved
// connections are live and on the tablegroup the statement targets, and when that
// fails the client is told what it lost.

var testPoolerID = &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}

func tgTarget(tablegroup string) *querypb.Target {
	return protoutil.NewTarget("", tablegroup, "", querypb.Mode_MODE_WRITABLE)
}

// session is a client session with one reservation, the gateway in front of it and the
// ScatterConn under test.
type session struct {
	sc    *ScatterConn
	gw    *mockGateway
	state *handler.MultigatewayConnectionState
	conn  *server.Conn
}

// newSession returns a session with a reservation (id 42) on tablegroup tg holding reasons.
// With ReasonTransaction among them the connection is in a transaction that has begun.
func newSession(t *testing.T, tg string, reasons uint32) *session {
	t.Helper()
	gw := &mockGateway{callbackResult: &sqltypes.Result{CommandTag: "SELECT 1"}}
	s := &session{
		sc:    NewScatterConn(gw, slog.Default()),
		gw:    gw,
		state: handler.NewMultigatewayConnectionState(),
		conn:  newTestConn(),
	}
	if protoutil.HasTransactionReason(reasons) {
		s.conn.SetTxnStatus(protocol.TxnStatusInBlock)
	}
	s.state.SetReservedConnection(tgTarget(tg), &querypb.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             testPoolerID,
		ReservationReasons:   reasons,
	})
	return s
}

func (s *session) tracked(tg string) *handler.ShardState {
	return s.state.GetMatchingShardState(tgTarget(tg))
}

func discard(context.Context, *sqltypes.Result) error { return nil }

// A loss is revealed by an error on an RPC. These are the shapes it comes in.
var lossErrors = map[string]error{
	"unavailable":                    mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "write: broken pipe"),
	"broken pipe":                    fmt.Errorf("write: %w", syscall.EPIPE),
	"reserved connection terminated": mterrors.NewReservedConnectionTerminated(42),
}

// pathsOnReservation are the paths that use a reservation and fail with err when the
// reservation is lost. Each returns the error the client would get.
var pathsOnReservation = map[string]func(s *session, err error) error{
	"StreamExecute": func(s *session, err error) error {
		s.gw.streamExecuteErr = err
		return s.sc.StreamExecute(context.Background(), s.conn, "tg1", "", "SELECT 1", nil, s.state, engine.PlanExecInfo{}, false, discard)
	},
	"PortalStreamExecute": func(s *session, err error) error {
		s.gw.portalErr = err
		return s.sc.PortalStreamExecute(context.Background(), "tg1", "", s.conn, s.state, testPortalInfo(), 0, false, engine.PlanExecInfo{}, false, discard)
	},
	"CopyFinalize": func(s *session, err error) error {
		s.gw.copyFinalizeErr = err
		return s.sc.CopyFinalize(context.Background(), s.conn, "tg1", "", s.state, nil, discard)
	},
	"CopyOutStream": func(s *session, err error) error {
		s.gw.copyOutStreamErr = err
		_, cerr := s.sc.CopyOutStream(context.Background(), s.conn, "tg1", "", s.state, nil)
		return cerr
	},
	"CopySendData": func(s *session, err error) error {
		s.gw.copySendDataErr = err
		return s.sc.CopySendData(context.Background(), s.conn, "tg1", "", s.state, []byte("x"))
	},
	"CopyInitiate": func(s *session, err error) error {
		s.gw.copyReadyErr = err
		_, _, cerr := s.sc.CopyInitiate(context.Background(), s.conn, "tg1", "", "COPY t FROM STDIN", s.state, discard)
		return cerr
	},
	"pooler lookup": func(s *session, _ error) error {
		s.gw.queryServiceByIDErr = mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "no connection found for pooler ID: cell1-pooler1")
		return s.sc.StreamExecute(context.Background(), s.conn, "tg1", "", "SELECT 1", nil, s.state, engine.PlanExecInfo{}, false, discard)
	},
}

func requireFatalShutdown(t *testing.T, err error, lostState string) {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag, "got %v", err)
	require.Equal(t, "FATAL", diag.Severity, "the session must end")
	require.Equal(t, mterrors.PgSSAdminShutdown, diag.Code)
	require.Contains(t, diag.Message, lostState, "the message names the lost state")
	require.Contains(t, diag.Message, "reconnect")
}

func requireRetryable(t *testing.T, err error) {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag, "got %v", err)
	require.Equal(t, "ERROR", diag.Severity)
	require.Equal(t, mterrors.PgSSSerializationFailure, diag.Code)
}

// TestReservationLost_SessionStateEndsSession: when a reservation that holds state of the
// session is lost, whatever path observes it ends the session with FATAL 57P01 and
// stops tracking the reservation.
func TestReservationLost_SessionStateEndsSession(t *testing.T) {
	reasons := map[string]uint32{
		"temp_table":            protoutil.ReasonTempTable,
		"session_advisory_lock": protoutil.ReasonSessionAdvisoryLock,
		"set_seed":              protoutil.ReasonSetSeed,
		"unsafe_connection":     protoutil.ReasonUnsafeConnection,
		"logical_replication":   protoutil.ReasonLogicalReplication,
		"portal outside txn":    protoutil.ReasonPortal,
		"temp_table in txn":     protoutil.ReasonTempTable | protoutil.ReasonTransaction,
		"lock after commit":     protoutil.ReasonSessionAdvisoryLock | protoutil.ReasonCopy,
	}
	for rname, r := range reasons {
		for pname, path := range pathsOnReservation {
			for ename, lossErr := range lossErrors {
				t.Run(rname+"/"+pname+"/"+ename, func(t *testing.T) {
					s := newSession(t, "tg1", r)
					err := path(s, lossErr)
					requireFatalShutdown(t, err, protoutil.ReasonsString(r&sessionLevelReasons|r&protoutil.ReasonPortal))
					require.Nil(t, s.tracked("tg1"), "the lost reservation must not stay tracked")
				})
			}
		}
	}
}

// TestReservationLost_TransactionOnlyIs40001: a reservation that only holds a transaction
// fails the statement with 40001, marks the transaction failed and is cleared.
func TestReservationLost_TransactionOnlyIs40001(t *testing.T) {
	reasons := map[string]uint32{
		"transaction":                 protoutil.ReasonTransaction,
		"transaction and copy":        protoutil.ReasonTransaction | protoutil.ReasonCopy,
		"portal on a transaction":     protoutil.ReasonTransaction | protoutil.ReasonPortal,
		"transaction with listen":     protoutil.ReasonTransaction | protoutil.ReasonListen,
		"transaction and portal+copy": protoutil.ReasonTransaction | protoutil.ReasonPortal | protoutil.ReasonCopy,
	}
	for rname, r := range reasons {
		for pname, path := range pathsOnReservation {
			for ename, lossErr := range lossErrors {
				t.Run(rname+"/"+pname+"/"+ename, func(t *testing.T) {
					s := newSession(t, "tg1", r)
					err := path(s, lossErr)
					requireRetryable(t, err)
					require.Nil(t, s.tracked("tg1"))
					require.Equal(t, protocol.TxnStatusFailed, s.conn.TxnStatus())
				})
			}
		}
	}
}

// TestReservationLost_PortalOnTransactionIsSessionLevelWithHoldCursor: a portal reason on a
// transaction's reservation is the transaction's, unless WITH HOLD cursors are open.
func TestReservationLost_PortalOnTransactionIsSessionLevelWithHoldCursor(t *testing.T) {
	s := newSession(t, "tg1", protoutil.ReasonTransaction|protoutil.ReasonPortal)
	s.state.AddOpenHoldCursor("c")
	err := pathsOnReservation["StreamExecute"](s, fmt.Errorf("write: %w", syscall.EPIPE))
	requireFatalShutdown(t, err, "portal")
	require.Nil(t, s.tracked("tg1"))
}

// TestReservationLost_NotALoss: the pooler vouching for a reservation, or releasing it
// cleanly, never ends anything.
func TestReservationLost_NotALoss(t *testing.T) {
	someReasons := protoutil.ReasonTempTable | protoutil.ReasonSessionAdvisoryLock
	vouch := &querypb.ReservedState{ReservedConnectionId: 42, PoolerId: testPoolerID, ReservationReasons: someReasons}
	pgErr := mterrors.NewPgError("ERROR", "23505", "duplicate key value violates unique constraint", "")

	t.Run("ordinary error with the reservation vouched for", func(t *testing.T) {
		s := newSession(t, "tg1", someReasons)
		s.gw.streamExecuteErr, s.gw.streamExecuteReturnState = pgErr, vouch
		err := pathsOnReservation["StreamExecute"](s, pgErr)
		require.ErrorIs(t, err, pgErr)
		require.NotNil(t, s.tracked("tg1"))
		require.False(t, isFatalError(err))
	})
	t.Run("class 08 error from a healthy backend, vouched", func(t *testing.T) {
		// a foreign data wrapper that cannot reach its server
		fdw := mterrors.NewPgError("ERROR", "08001", "could not connect to server", "")
		s := newSession(t, "tg1", someReasons)
		s.gw.streamExecuteReturnState = vouch
		err := pathsOnReservation["StreamExecute"](s, fdw)
		require.ErrorIs(t, err, fdw)
		require.NotNil(t, s.tracked("tg1"))
	})
	t.Run("real serialization failure, vouched", func(t *testing.T) {
		ser := mterrors.NewPgError("ERROR", mterrors.PgSSSerializationFailure, "could not serialize access due to concurrent update", "")
		s := newSession(t, "tg1", protoutil.ReasonTransaction)
		s.gw.streamExecuteReturnState = &querypb.ReservedState{ReservedConnectionId: 42, PoolerId: testPoolerID, ReservationReasons: protoutil.ReasonTransaction}
		err := pathsOnReservation["StreamExecute"](s, ser)
		require.ErrorIs(t, err, ser)
		require.NotNil(t, s.tracked("tg1"))
		require.Equal(t, protocol.TxnStatusInBlock, s.conn.TxnStatus())
	})
	t.Run("released by a clean statement error", func(t *testing.T) {
		// the pooler returns ID 0 with the PostgreSQL error of a statement that failed
		// before it made the state it asked to keep
		s := newSession(t, "tg1", protoutil.ReasonTempTable)
		err := pathsOnReservation["StreamExecute"](s, pgErr)
		require.ErrorIs(t, err, pgErr)
		require.False(t, isFatalError(err))
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("clean release without an error", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonSessionAdvisoryLock)
		err := s.sc.StreamExecute(context.Background(), s.conn, "tg1", "", "SELECT pg_advisory_unlock(1)", nil, s.state,
			engine.PlanExecInfo{RecheckAdvisoryLocks: true}, false, discard)
		require.NoError(t, err)
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("cancellation outside a transaction keeps the reservation", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTempTable)
		err := pathsOnReservation["StreamExecute"](s, context.DeadlineExceeded)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, isFatalError(err))
		require.NotNil(t, s.tracked("tg1"), "the backend and its temp tables are alive")
	})
	t.Run("a failed COMMIT releases the transaction", func(t *testing.T) {
		deferred := mterrors.NewPgError("ERROR", "23505", "deferred constraint", "")
		s := newSession(t, "tg1", protoutil.ReasonTransaction)
		s.gw.concludeTransactionErr = deferred
		err := s.sc.ConcludeTransaction(context.Background(), s.conn, s.state,
			multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, nil, false, false, discard)
		require.ErrorIs(t, err, deferred)
		require.False(t, isFatalError(err))
		require.Nil(t, s.tracked("tg1"))
	})
}

// TestReservationLost_ReasonsAreCurrentAtTheTimeOfTheLoss: the reasons that decide the
// classification are the pooler's last word, so a lock released by a pg_locks recheck no
// longer makes the loss of the transaction fatal.
func TestReservationLost_ReasonsAreCurrentAtTheTimeOfTheLoss(t *testing.T) {
	s := newSession(t, "tg1", protoutil.ReasonTransaction|protoutil.ReasonSessionAdvisoryLock)
	// pg_advisory_unlock: the pooler rechecks pg_locks and answers without the advisory reason.
	s.gw.streamExecuteReturnState = &querypb.ReservedState{
		ReservedConnectionId: 42, PoolerId: testPoolerID, ReservationReasons: protoutil.ReasonTransaction,
	}
	require.NoError(t, s.sc.StreamExecute(context.Background(), s.conn, "tg1", "", "SELECT pg_advisory_unlock(1)", nil, s.state,
		engine.PlanExecInfo{RecheckAdvisoryLocks: true}, false, discard))
	require.Equal(t, protoutil.ReasonTransaction, s.tracked("tg1").ReservedState.GetReservationReasons())

	s.gw.streamExecuteReturnState = nil
	err := pathsOnReservation["StreamExecute"](s, fmt.Errorf("write: %w", syscall.EPIPE))
	requireRetryable(t, err)
}

// TestReservationLost_RequestedReasonsCount: a statement that asked the pooler to keep
// session state and fails with the connection lost may have created it.
func TestReservationLost_RequestedReasonsCount(t *testing.T) {
	s := newSession(t, "tg1", protoutil.ReasonTransaction)
	s.gw.streamExecuteErr = fmt.Errorf("write: %w", syscall.EPIPE)
	err := s.sc.StreamExecute(context.Background(), s.conn, "tg1", "", "SELECT pg_advisory_lock(1)", nil, s.state,
		engine.PlanExecInfo{AdvisoryLock: true}, false, discard)
	requireFatalShutdown(t, err, "session_advisory_lock")
}

func TestReservationLost_ConcludeTransaction(t *testing.T) {
	conclude := func(s *session, c multipoolerpb.TransactionConclusion, chain bool) error {
		return s.sc.ConcludeTransaction(context.Background(), s.conn, s.state, c, nil, false, chain, discard)
	}
	rollback := multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK
	commit := multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT

	t.Run("a lost transaction fails COMMIT with 40001", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTransaction)
		s.gw.concludeTransactionErr = mterrors.NewReservedConnectionTerminated(42)
		requireRetryable(t, conclude(s, commit, false))
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("a lost transaction is how a plain ROLLBACK ends", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTransaction)
		s.gw.concludeTransactionErr = mterrors.NewReservedConnectionTerminated(42)
		require.NoError(t, conclude(s, rollback, false))
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("ROLLBACK AND CHAIN fails closed", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTransaction)
		s.gw.concludeTransactionErr = mterrors.NewReservedConnectionTerminated(42)
		requireRetryable(t, conclude(s, rollback, true))
	})
	for _, c := range []multipoolerpb.TransactionConclusion{commit, rollback} {
		t.Run("session state ends the session whatever the conclusion "+c.String(), func(t *testing.T) {
			s := newSession(t, "tg1", protoutil.ReasonTransaction|protoutil.ReasonTempTable)
			s.gw.concludeTransactionErr = mterrors.NewReservedConnectionTerminated(42)
			requireFatalShutdown(t, conclude(s, c, false), "temp_table")
			require.Nil(t, s.tracked("tg1"))
		})
	}
	t.Run("a lookup failure of the pooler", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTransaction|protoutil.ReasonSessionAdvisoryLock)
		s.gw.queryServiceByIDErr = mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "no connection")
		requireFatalShutdown(t, conclude(s, commit, false), "session_advisory_lock")
	})
}

func TestReservationLost_DiscardTempTables(t *testing.T) {
	discardTemp := func(s *session) error { return s.sc.DiscardTempTables(context.Background(), s.conn, s.state, discard) }
	lost := fmt.Errorf("write: %w", syscall.EPIPE)

	t.Run("losing the temp tables being discarded is not losing state", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTempTable)
		s.gw.discardTempErr = lost
		err := discardTemp(s)
		require.Error(t, err)
		require.False(t, isFatalError(err))
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("the rest of the reservation is", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTempTable|protoutil.ReasonSessionAdvisoryLock)
		s.gw.discardTempErr = lost
		requireFatalShutdown(t, discardTemp(s), "session_advisory_lock")
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("a lookup failure of the pooler", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTempTable|protoutil.ReasonSetSeed)
		s.gw.queryServiceByIDErr = mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "no connection")
		requireFatalShutdown(t, discardTemp(s), "set_seed")
	})
}

func TestReservationLost_ReleaseAllReservedConnections(t *testing.T) {
	release := func(s *session, keepSticky bool) error {
		return s.sc.ReleaseAllReservedConnections(context.Background(), s.conn, s.state, keepSticky)
	}
	lost := fmt.Errorf("write: %w", syscall.EPIPE)

	t.Run("DISCARD ALL loses a sticky reason it was to keep", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonSetSeed|protoutil.ReasonTempTable)
		s.gw.releaseReservedConnectionErr = lost
		requireFatalShutdown(t, release(s, true), "set_seed")
		require.Nil(t, s.tracked("tg1"))
	})
	t.Run("DISCARD ALL of what it discards", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonTempTable|protoutil.ReasonSessionAdvisoryLock)
		s.gw.releaseReservedConnectionErr = lost
		err := release(s, true)
		require.Error(t, err)
		require.False(t, isFatalError(err))
	})
	t.Run("a disconnect has nobody to tell", func(t *testing.T) {
		s := newSession(t, "tg1", protoutil.ReasonSetSeed|protoutil.ReasonTempTable)
		s.gw.releaseReservedConnectionErr = lost
		err := release(s, false)
		require.Error(t, err)
		require.False(t, isFatalError(err))
	})
}

// TestReservationLost_CopyAbortReportsLostSessionState: the abort of a COPY is best effort,
// but it does not hide that the session's state is gone.
func TestReservationLost_CopyAbortReportsLostSessionState(t *testing.T) {
	s := newSession(t, "tg1", protoutil.ReasonCopy|protoutil.ReasonTempTable)
	s.gw.copyAbortErr = fmt.Errorf("write: %w", syscall.EPIPE)
	requireFatalShutdown(t, s.sc.CopyAbort(context.Background(), s.conn, "tg1", "", s.state), "temp_table")
	require.Nil(t, s.tracked("tg1"))

	s = newSession(t, "tg1", protoutil.ReasonCopy|protoutil.ReasonTransaction)
	s.gw.copyAbortErr = fmt.Errorf("write: %w", syscall.EPIPE)
	require.NoError(t, s.sc.CopyAbort(context.Background(), s.conn, "tg1", "", s.state))
	require.Nil(t, s.tracked("tg1"))
	require.Equal(t, protocol.TxnStatusFailed, s.conn.TxnStatus())
}

func TestSessionLevel(t *testing.T) {
	state := handler.NewMultigatewayConnectionState()
	require.Zero(t, sessionLevel(protoutil.ReasonTransaction|protoutil.ReasonCopy|protoutil.ReasonListen, state))
	require.Equal(t, protoutil.ReasonTempTable, sessionLevel(protoutil.ReasonTransaction|protoutil.ReasonTempTable, state))
	require.Equal(t, protoutil.ReasonPortal, sessionLevel(protoutil.ReasonPortal, state))
	require.Zero(t, sessionLevel(protoutil.ReasonPortal|protoutil.ReasonTransaction, state))
	state.AddOpenHoldCursor("c")
	require.Equal(t, protoutil.ReasonPortal, sessionLevel(protoutil.ReasonPortal|protoutil.ReasonTransaction, state))
}

func TestIsConnectionLostError(t *testing.T) {
	require.False(t, isConnectionLostError(nil))
	require.True(t, isConnectionLostError(mterrors.NewReservedConnectionTerminated(1)))
	require.True(t, isConnectionLostError(mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "no connection")))
	require.True(t, isConnectionLostError(mterrors.NewPgError("FATAL", mterrors.PgSSAdminShutdown, "terminating connection", "")))
	require.False(t, isConnectionLostError(mterrors.NewPgError("ERROR", "08001", "fdw", "")))
	require.False(t, isConnectionLostError(mterrors.NewPgError("ERROR", mterrors.PgSSSerializationFailure, "could not serialize access", "")))
	require.False(t, isConnectionLostError(mterrors.NewPgError("ERROR", mterrors.PgSSSyntaxError, "syntax error", "")))
}

func TestReservationLost_Predicate(t *testing.T) {
	vouched := &querypb.ReservedState{ReservedConnectionId: 42}
	lostByPortal := mterrors.NewPgError("ERROR", mterrors.PgSSConnectionFailure, "connection to server lost", "failed to parse statement: write: broken pipe")

	// The pooler releases the reservation of a portal whose socket broke and reports
	// it as 08006 at ERROR with no reserved state.
	require.True(t, reservationLost(nil, true, lostByPortal))
	require.False(t, reservationLost(vouched, true, lostByPortal), "the pooler vouches for the connection")
	require.False(t, reservationLost(nil, true, nil), "a clean release is not a loss")
	require.False(t, reservationLost(nil, true, mterrors.NewPgError("ERROR", "23505", "duplicate key", "")))
	require.True(t, reservationLost(nil, true, context.DeadlineExceeded), "an indeterminate failure released the backend")
	// Without reserved state only the error can say.
	require.False(t, reservationLost(nil, false, lostByPortal), "08006 at ERROR may come from a foreign data wrapper")
	require.True(t, reservationLost(nil, false, mterrors.NewReservedConnectionTerminated(1)))
}
