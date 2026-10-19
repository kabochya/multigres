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
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/protoutil"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// A session's reserved connections must be live. A reservation is lost when the pooler
// reaped it while idle, the backend was terminated or crashed, or the pooler it lives on
// failed over or went away. Every path that can observe this goes through reservationLost
// and ends in endLostReservations, which says what the client lost and tells it:
//
//   - State of the client's session (see sessionLevel) cannot be rebuilt by the gateway: a
//     new backend does not have the temporary table, the advisory lock, or the seed, and
//     the client would carry on as if it did. The session ends with FATAL 57P01 and the
//     client reconnects.
//   - A transaction alone is something a client already knows how to lose: the statement
//     fails with 40001, the transaction is marked failed and the client retries it.

// sessionLevelReasons are the reservation reasons that stand for state of the client's
// session, which a new backend does not have: a temporary table, a logical replication
// slot, a session advisory lock, a set seed, an unsafe connection, and a portal (see
// sessionLevel). A reservation with only a transaction reason holds the transaction.
const sessionLevelReasons = protoutil.ReasonTempTable | protoutil.ReasonLogicalReplication |
	protoutil.ReasonSessionAdvisoryLock | protoutil.ReasonSetSeed | protoutil.ReasonUnsafeConnection

// sessionLevel returns the reasons of a reservation that are state of the session. A portal
// reason is the session's, except when it rides on a transaction's reservation: the pooler
// adds it to a transaction's reservation whenever a portal suspends, which fetch-size
// cursors do all the time, and that portal is the transaction's, unless the session also
// holds WITH HOLD cursors. A portal suspended outside a transaction is reserved with the
// portal reason alone, and is the session's: dropping it would re-bind the portal on a new
// backend and return its first rows again.
func sessionLevel(reasons uint32, state *handler.MultigatewayConnectionState) uint32 {
	lost := reasons & sessionLevelReasons
	if reasons&protoutil.ReasonPortal != 0 && (reasons&protoutil.ReasonTransaction == 0 || state.HasAnyOpenHoldCursor()) {
		lost |= protoutil.ReasonPortal
	}
	return lost
}

// isConnectionLostError reports whether err says the reserved connection it was sent
// to is gone, whatever the pooler returned with it: the reservation's pooler is not
// reachable or no longer serves the target (UNAVAILABLE), the pooler reports the
// reserved connection terminated (40001, see mterrors.NewReservedConnectionTerminated),
// or the backend's connection broke (class 08, a FATAL shutdown, a transport error).
//
// A class-08 ERROR that PostgreSQL raised on a healthy backend (a foreign data wrapper
// that cannot reach its server) is not a lost connection; the pooler vouches for the
// connection then, see reservationLost.
func isConnectionLostError(err error) bool {
	if err == nil {
		return false
	}
	if mterrors.IsReservedConnectionTerminated(err) || mterrors.Code(err) == mtrpcpb.Code_UNAVAILABLE {
		return true
	}
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		return diag.IsFatal() && mterrors.IsConnectionDead(err)
	}
	return mterrors.IsConnectionDead(err)
}

// isCleanStatementError reports whether err is a PostgreSQL ERROR that left its backend
// usable: a failed COMMIT, or a statement that failed before it created the state it
// asked the pooler to keep. A pooler that releases a reservation because of such an error
// returns reserved connection ID 0 with it, which is not a loss: the backend is fine and
// every reason the gateway tracked for it is either gone or already reported as gone.
// This is the pooler's own test of whether it can still vouch for a connection after an
// error (class 08 and FATAL are not clean), plus the diagnostics that say it already could not.
func isCleanStatementError(err error) bool {
	var diag *mterrors.PgDiagnostic
	return errors.As(err, &diag) && !mterrors.IsConnectionDead(err) && !isConnectionLostError(err)
}

// reservationLost reports whether an RPC that used a reserved connection says that the
// reservation is gone.
//
// reportsState is true for RPCs that return the authoritative ReservedState. For those, a
// non-zero ID is the pooler vouching for the connection and nothing else counts; a zero
// ID with an error is a loss unless the error is a clean statement error (see
// isCleanStatementError). A zero ID with no
// error is a clean release (COMMIT, an unlock, an unreserve), never a loss. RPCs that
// report no state (CopySendData) can only be judged by the error.
func reservationLost(rs *querypb.ReservedState, reportsState bool, err error) bool {
	if err == nil {
		return false
	}
	if reportsState {
		if rs.GetReservedConnectionId() != 0 {
			return false
		}
		return !isCleanStatementError(err)
	}
	return isConnectionLostError(err)
}

// lostReservation is a reservation of the session that cannot be used any more.
type lostReservation struct {
	target *querypb.Target
	// id is the reserved connection id, 0 when the gateway found the loss itself.
	id uint64
	// reasons are the reasons tracked for the reservation, plus any the failed statement
	// asked the pooler to add. The reasons must be current at the time of the loss (the
	// pooler's authoritative state replaces them after every RPC, and an advisory
	// lock the pooler released after a pg_locks recheck is no longer in them),
	// since they decide whether the client is told the session ended.
	reasons uint32
}

// lostFrom describes a tracked reservation as it is before an RPC on it, plus the reasons
// that RPC asks to add. The reasons are read before the RPC because its answer replaces them.
func lostFrom(ss *handler.ShardState, requested uint32) lostReservation {
	return lostReservation{
		target:  ss.Target,
		id:      ss.ReservedState.GetReservedConnectionId(),
		reasons: ss.ReservedState.GetReservationReasons() | requested,
	}
}

// endLostReservations is where a session learns that reservations it relied on are gone.
// It drops them from the gateway's tracking (the pooler no longer has them), marks the
// transaction failed if one was lost, and returns what the client sees:
//
//   - the session's state was lost (see sessionLevel): FATAL 57P01, whose message names
//     it and says to reconnect; why completes "the session's state (...) was lost because
//     <why>". A FATAL cause is returned as it is.
//   - only a transaction was lost: the terminated-connection diagnostic (40001), or cause
//     when it already is a cancellation, which names the real reason.
//   - neither: cause, which is nil when the gateway found the loss itself.
//
// Callers must not hold an iteration over state.ShardStates: clearing swaps and truncates it.
func (sc *ScatterConn) endLostReservations(
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	lost []lostReservation,
	why string,
	cause error,
) error {
	var session uint32
	var txn bool
	var id uint64
	for _, l := range lost {
		session |= sessionLevel(l.reasons, state)
		txn = txn || protoutil.HasTransactionReason(l.reasons)
		if id == 0 {
			id = l.id
		}
		state.ClearReservedConnection(l.target)
	}
	if len(lost) > 0 {
		// Only the SQLSTATE of the cause: an error text can carry the client's data.
		sc.logger.Warn("session reservations are no longer usable",
			"connection_id", conn.ConnectionID(),
			"tablegroup", lost[0].target.GetShardKey().GetTableGroup(),
			"session_state", protoutil.ReasonsString(session),
			"transaction", txn,
			"cause_sqlstate", mterrors.ExtractSQLSTATE(cause))
	}
	if txn && conn.TxnStatus() == protocol.TxnStatusInBlock {
		conn.SetTxnStatus(protocol.TxnStatusFailed)
	}

	var diag *mterrors.PgDiagnostic
	if errors.As(cause, &diag) && diag.IsFatal() {
		return cause
	}
	switch {
	case session != 0:
		return mterrors.NewSessionStateLost(protoutil.ReasonsString(session), why)
	case txn:
		if cause != nil && isCancellationError(cause) {
			return cause
		}
		return mterrors.NewReservedConnectionTerminated(id)
	}
	return cause
}

// reservationLostError is endLostReservations for one reservation whose RPC failed with
// cause. requested is what the statement asked the pooler to add to the reservation.
func (sc *ScatterConn) reservationLostError(
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	l lostReservation,
	cause error,
) error {
	return sc.endLostReservations(conn, state, []lostReservation{l},
		fmt.Sprintf("its reserved connection on tablegroup %q is gone", l.target.GetShardKey().GetTableGroup()),
		cause)
}
