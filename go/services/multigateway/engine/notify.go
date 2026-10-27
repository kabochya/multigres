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
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
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
// The deferral applies only while the application tablegroup is not the default one, that is
// during and after a migration to it: it keeps the transaction on one tablegroup.
//
// The NOTIFY runs on the default cohort as the client's own role, with the keys of the client's
// SCRAM login passed through, as every statement does. A client that logs in against an
// unmanaged tablegroup therefore needs its role on the default cohort as well, with the same
// SCRAM verifier, or its NOTIFY fails to authenticate there. The identity model is not changed
// for control traffic.
//
// Known limit: only a NOTIFY inside an explicit transaction is deferred. One in an implicit
// transaction, a multi-statement simple query or an extended-protocol pipeline that has not
// issued BEGIN, is sent at once, so a later failure in that batch does not retract it, where
// PostgreSQL would have discarded it with the aborted transaction.
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
	// payload of that many bytes or more is refused.
	maxNotifyPayloadLength = 8000

	// maxNotifyChannelLength is PostgreSQL's limit on an identifier, NAMEDATALEN - 1: a
	// NOTIFY statement truncates a longer channel name, where pg_notify() refuses it.
	maxNotifyChannelLength = 63

	// notifyDeliverySQL sends every recorded notification in one statement whose text
	// does not depend on how many there are, with the channels and payloads as two
	// array parameters, in the order they were issued.
	notifyDeliverySQL = "SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS t(c, p, i) ORDER BY i"
)

// truncateNotifyChannel cuts a channel name to PostgreSQL's identifier length, at a
// character boundary, as the NOTIFY statement does with the name it is given (pg_mbcliplen).
func truncateNotifyChannel(channel string) string {
	if len(channel) <= maxNotifyChannelLength {
		return channel
	}
	cut := maxNotifyChannelLength
	for cut > 0 && !utf8.RuneStart(channel[cut]) {
		cut--
	}
	return channel[:cut]
}

// textArrayLiteral is the text form of an array of non-null strings, as PostgreSQL reads
// it: every element quoted, with backslash and double quote escaped.
func textArrayLiteral(elems []string) []byte {
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range elems {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		for j := 0; j < len(e); j++ {
			if e[j] == '\\' || e[j] == '"' {
				b.WriteByte('\\')
			}
			b.WriteByte(e[j])
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return []byte(b.String())
}

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
		switch err := state.AddPendingNotification(truncateNotifyChannel(n.channel), n.payload); {
		case errors.Is(err, handler.ErrTooManyNotifications):
			return mterrors.NewPgError("ERROR", "54000",
				"too many NOTIFY statements in one transaction",
				fmt.Sprintf("A transaction can have at most %d NOTIFY statements while the application is not on the default tablegroup.", handler.MaxPendingNotifications))
		case errors.Is(err, handler.ErrNotificationsTooLarge):
			return mterrors.NewPgError("ERROR", "54000",
				"the NOTIFY statements of one transaction are too large",
				fmt.Sprintf("A transaction can notify at most %d bytes of channels and payloads while the application is not on the default tablegroup.", handler.MaxPendingNotificationBytes))
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
// just committed, in order, as one parameterized statement to the default tablegroup:
// PostgreSQL collapses identical channel and payload pairs within one transaction, and
// the recorded ones are already collapsed. It runs outside the session's transaction and
// as control traffic, so it passes the gate of a fenced default tablegroup. A
// notification never turns a commit into an error: if it cannot be delivered within a
// short time it is dropped with a warning on the result. The statement commits on its own,
// so all of them are delivered or none is (one that timed out may have been delivered all
// the same).
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

	channels := make([]string, len(notifications))
	payloads := make([]string, len(notifications))
	for i, n := range notifications {
		channels[i], payloads[i] = n.Channel, n.Payload
	}
	psi, err := preparedstatement.NewPreparedStatementInfo(protoutil.NewPreparedStatement("", notifyDeliverySQL, []uint32{0, 0}))
	if err == nil {
		portal := protoutil.NewPortal("", "", [][]byte{textArrayLiteral(channels), textArrayLiteral(payloads)}, []int16{0, 0}, nil)
		err = exec.PortalStreamExecute(sendCtx, constants.DefaultTableGroup, constants.DefaultShard, conn, state,
			preparedstatement.NewPortalInfo(psi, portal), 0, false,
			PlanExecInfo{Control: true, Autonomous: true}, false,
			func(context.Context, *sqltypes.Result) error { return nil })
	}
	if err != nil {
		// Whatever the error is, it is a warning. The statement ran as a pooled control
		// statement outside the session, so a FATAL error can only be a backend of the
		// default cohort dying (a failover, an administrator's terminate): it says
		// nothing about the client's session, and the commit it follows has happened. The
		// caller reports the COMMIT, with this warning. The error says where the default
		// cohort is and what it answered, which is not for the client; the warning says
		// what happened.
		result.Notices = append(result.Notices, mterrors.NewPgNotice("WARNING", "01000",
			"the transaction committed, but its NOTIFY could not be delivered",
			fmt.Sprintf("%d notifications were dropped.", len(notifications))))
	}
}
