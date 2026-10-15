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
	"fmt"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// NotifyPrimitive executes NOTIFY. LISTEN subscribes on the default cohort, so a
// NOTIFY is always sent there.
//
// While the database's application tablegroup is not the default one, a NOTIFY
// inside a transaction is not sent when it is issued. The transaction runs on the
// application's tablegroup, and sending it to the default cohort in the middle
// would make the transaction span two cohorts: a second reservation that has to be
// committed too (the application's backend could commit while the other fails), the
// deferred BEGIN, with its isolation level and access mode, landing on the wrong
// backend, and a rollback that cannot take back what was already committed. The
// NOTIFY is recorded instead, and sent after the transaction has committed, last, as
// PostgreSQL itself delivers notifications at commit. A rollback discards it; a
// rollback to a savepoint discards those recorded after it. Outside a transaction it
// is sent at once. Once the application tablegroup is the default one the NOTIFY is
// an ordinary statement of the transaction, on the transaction's own backend.
//
// PROTOTYPE STUB: part of the application routing of the unmanaged pooler prototype.
type NotifyPrimitive struct {
	channel string
	payload string
	query   string
	// route sends the NOTIFY to the default tablegroup.
	route *Route
	// deferInTransaction is set when the application tablegroup is not the default one.
	deferInTransaction bool
}

// NewNotifyPrimitive returns the primitive for a NOTIFY statement. deferInTransaction
// is true when the application tablegroup of the statement's database is not the
// default one.
func NewNotifyPrimitive(stmt *ast.NotifyStmt, query string, deferInTransaction bool) *NotifyPrimitive {
	return &NotifyPrimitive{
		channel:            stmt.Conditionname,
		payload:            stmt.Payload,
		query:              query,
		route:              NewRoute(constants.DefaultTableGroup, constants.DefaultShard, query, nil),
		deferInTransaction: deferInTransaction,
	}
}

const (
	// maxNotifyPayloadLength is PostgreSQL's limit (BLCKSZ - NAMEDATALEN - 128): a
	// payload of that many bytes or more is refused. The parser already truncates a
	// channel name to 63 bytes, as PostgreSQL does, so there is no limit to check on it.
	maxNotifyPayloadLength = 8000

	// notifyChunkTargets and notifyChunkBytes bound one statement of notifications: a
	// select list holds at most 1664 entries, and a statement is not worth making large.
	notifyChunkTargets = 500
	notifyChunkBytes   = 256 << 10
)

// notifyDeliveryTimeout bounds how long the notifications of a committed transaction
// wait for the default cohort. A notification never turns a commit into an error and
// is not worth the whole buffer window of a request.
const notifyDeliveryTimeout = 3 * time.Second

func (n *NotifyPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	bindVars []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if n.deferInTransaction && conn.IsInTransaction() {
		if conn.TxnStatus() == protocol.TxnStatusFailed {
			return mterrors.NewPgError("ERROR", "25P02",
				"current transaction is aborted, commands ignored until end of transaction block", "")
		}
		// PostgreSQL's own checks, made now: a notification refused when the
		// transaction commits would be dropped, with everything else the transaction
		// notified, long after the statement that caused it.
		if len(n.payload) >= maxNotifyPayloadLength {
			return mterrors.NewPgError("ERROR", mterrors.PgSSInvalidParameterValue, "payload string too long", "")
		}
		if !state.AddPendingNotification(n.channel, n.payload) {
			return mterrors.NewPgError("ERROR", "54000",
				"too many NOTIFY statements in one transaction",
				fmt.Sprintf("A transaction can have at most %d NOTIFY statements while the application is not on the default tablegroup.", handler.MaxPendingNotifications))
		}
		return callback(ctx, &sqltypes.Result{CommandTag: "NOTIFY"})
	}
	return n.route.StreamExecute(ctx, exec, conn, state, bindVars, info, callback)
}

func (n *NotifyPrimitive) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if n.deferInTransaction && conn.IsInTransaction() {
		return n.StreamExecute(ctx, exec, conn, state, nil, info, callback)
	}
	return n.route.PortalStreamExecute(ctx, exec, conn, state, portalInfo, maxRows, includeDescribe, info, callback)
}

func (n *NotifyPrimitive) GetTableGroup() string { return constants.DefaultTableGroup }

// DefersInTransaction reports whether a NOTIFY inside a transaction is held until the
// transaction commits, as it is while the application tablegroup is not the default one.
func (n *NotifyPrimitive) DefersInTransaction() bool { return n.deferInTransaction }

func (n *NotifyPrimitive) GetQuery() string { return n.query }

func (n *NotifyPrimitive) String() string { return "Notify(" + n.channel + ")" }

// sendPendingNotifications sends the notifications recorded by a transaction that has
// just committed, in order, as one statement to the default tablegroup: PostgreSQL
// collapses identical channel and payload pairs within one transaction, and the
// statement is one. It runs outside the session's transaction and as control
// traffic, so it passes the gate of a fenced default tablegroup. A notification
// never turns a commit into an error: if it cannot be delivered within a short time
// it is dropped with a warning on the result. The notifications go in statements of a
// bounded size, in order, each committing on its own: a listener can see the first
// chunks of a large batch interleaved with other sessions' notifications and not the
// rest if a later chunk fails, and a chunk that timed out may have been delivered all the
// same.
func sendPendingNotifications(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	result *sqltypes.Result,
) {
	notifications := state.TakePendingNotifications()
	if len(notifications) == 0 {
		return
	}
	sendCtx, cancel := context.WithTimeout(ctx, notifyDeliveryTimeout)
	defer cancel()

	// In order, in statements small enough to be accepted. The first one that fails
	// drops itself and the rest: what comes after must not overtake what did not
	// arrive.
	delivered := 0
	for start := 0; start < len(notifications); {
		var calls []string
		size := 0
		for end := start; end < len(notifications) && len(calls) < notifyChunkTargets && size < notifyChunkBytes; end++ {
			n := notifications[end]
			calls = append(calls, "pg_notify("+quoteSQLLiteral(n.Channel)+", "+quoteSQLLiteral(n.Payload)+")")
			size += len(n.Channel) + len(n.Payload)
		}
		err := exec.StreamExecute(sendCtx, conn, constants.DefaultTableGroup, constants.DefaultShard,
			"SELECT "+strings.Join(calls, ", "), nil, state,
			PlanExecInfo{Control: true, Autonomous: true}, false,
			func(context.Context, *sqltypes.Result) error { return nil })
		if err != nil {
			// Whatever the error is, it is a warning. The statement ran as a pooled
			// control statement outside the session, so a FATAL error can only be a
			// backend of the default cohort dying (a failover, an administrator's
			// terminate): it says nothing about the client's session, and the commit
			// it follows has happened. The caller reports the COMMIT, with this warning.
			// The error says where the default cohort is and what it answered, which
			// is not for the client; the warning says what happened.
			result.Notices = append(result.Notices, mterrors.NewPgNotice("WARNING", "01000",
				"the transaction committed, but its NOTIFY could not be delivered",
				fmt.Sprintf("%d of %d notifications were delivered; the rest were dropped.", delivered, len(notifications))))
			return
		}
		delivered += len(calls)
		start += len(calls)
	}
}

// quoteSQLLiteral quotes s as a string literal that reads the same whatever
// standard_conforming_strings is: a literal with a backslash is written as an escape
// string.
func quoteSQLLiteral(s string) string {
	quoted := strings.ReplaceAll(s, "'", "''")
	if strings.Contains(s, `\`) {
		return "E'" + strings.ReplaceAll(quoted, `\`, `\\`) + "'"
	}
	return "'" + quoted + "'"
}
