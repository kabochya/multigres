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
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

func noopCallback(_ context.Context, _ *sqltypes.Result) error { return nil }

func TestNotifyPrimitive_OutsideATransactionIsSentAtOnceAsControl(t *testing.T) {
	exec := &txMockIExecute{}
	conn := newTxTestConn()
	state := handler.NewMultigatewayConnectionState()
	p := NewNotifyPrimitive(ast.NewNotifyStmt("chan", "x"), "NOTIFY chan, 'x'", true)

	require.NoError(t, p.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{Control: true}, noopCallback))
	require.Equal(t, []string{"NOTIFY chan, 'x'"}, exec.streamExecuteSQL)
	require.True(t, exec.streamExecuteResv[0].Control)
	require.False(t, state.HasPendingNotifications())
}

func TestNotifyPrimitive_InATransactionIsRecordedNotSentWhileTheApplicationIsElsewhere(t *testing.T) {
	exec := &txMockIExecute{}
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	p := NewNotifyPrimitive(ast.NewNotifyStmt("chan", "x"), "NOTIFY chan, 'x'", true)

	var result *sqltypes.Result
	require.NoError(t, p.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{Control: true},
		func(_ context.Context, r *sqltypes.Result) error { result = r; return nil }))
	require.Zero(t, exec.streamExecuteCount, "nothing reaches a pooler while the transaction is open")
	require.True(t, state.HasPendingNotifications())
	require.Equal(t, "NOTIFY", result.CommandTag)
}

func TestNotifyPrimitive_InATransactionOnTheDefaultTablegroupIsAnOrdinaryStatement(t *testing.T) {
	exec := &txMockIExecute{}
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	p := NewNotifyPrimitive(ast.NewNotifyStmt("chan", "x"), "NOTIFY chan, 'x'", false)

	require.NoError(t, p.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{Control: true}, noopCallback))
	require.Equal(t, 1, exec.streamExecuteCount, "it runs in the transaction, on its own backend")
	require.False(t, state.HasPendingNotifications())
}

func TestNotifyPrimitive_InAFailedTransactionIsRefused(t *testing.T) {
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusFailed)
	p := NewNotifyPrimitive(ast.NewNotifyStmt("chan", ""), "NOTIFY chan", true)
	err := p.StreamExecute(t.Context(), &txMockIExecute{}, conn, handler.NewMultigatewayConnectionState(), nil, PlanExecInfo{}, noopCallback)
	require.ErrorContains(t, err, "current transaction is aborted")
}

// notifying begins a transaction that has run NOTIFYs, on a connection with no reservation.
func notifying(t *testing.T, notifies ...[2]string) (*txMockIExecute, *handler.MultigatewayConnectionState, func() *sqltypes.Result) {
	t.Helper()
	exec := &txMockIExecute{}
	conn := newTxTestConn()
	state := handler.NewMultigatewayConnectionState()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state.BeginTransaction()
	for _, n := range notifies {
		require.NoError(t, state.AddPendingNotification(n[0], n[1]))
	}
	commit := func() *sqltypes.Result {
		var result *sqltypes.Result
		tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
		require.NoError(t, tp.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{},
			func(_ context.Context, r *sqltypes.Result) error { result = r; return nil }))
		return result
	}
	return exec, state, commit
}

func TestCommit_SendsTheRecordedNotificationsOnceAsOneControlStatement(t *testing.T) {
	exec, state, commit := notifying(t, [2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"a", "1"}, [2]string{"a", "3"})
	result := commit()

	require.Equal(t, "COMMIT", result.CommandTag)
	require.Empty(t, result.Notices)
	require.Equal(t, []string{notifyDeliverySQL}, exec.portalSQL, "one parameterized statement, whatever the number of notifications")
	require.Equal(t, [][]byte{[]byte(`{"a","b","a"}`), []byte(`{"1","2","3"}`)}, exec.portalParams[0],
		"in order, with the repeat of a channel and payload collapsed")
	require.True(t, exec.portalInfo[0].Control)
	require.True(t, exec.portalInfo[0].Autonomous, "outside the session's transaction")
	require.Zero(t, exec.streamExecuteCount, "nothing else is sent")
	require.False(t, state.HasPendingNotifications())
}

func TestCommit_FailedDeliveryIsAWarningNotAnError(t *testing.T) {
	exec, state, commit := notifying(t, [2]string{"a", "1"})
	exec.portalErr = errors.New("no leader")
	result := commit()

	require.Equal(t, "COMMIT", result.CommandTag, "the data commit is still reported as a success")
	require.Len(t, result.Notices, 1)
	assert.Equal(t, "WARNING", result.Notices[0].Severity)
	assert.Contains(t, result.Notices[0].Message, "NOTIFY could not be delivered")
	require.False(t, state.HasPendingNotifications(), "a notification is dropped, not retried")
}

func TestRollback_DiscardsTheRecordedNotifications(t *testing.T) {
	exec, state, _ := notifying(t, [2]string{"a", "1"})
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	tp := NewTransactionPrimitive(ast.TRANS_STMT_ROLLBACK, "", false, "ROLLBACK", "tg1", nil)
	require.NoError(t, tp.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, noopCallback))
	require.Empty(t, exec.portalSQL, "a rolled back transaction notifies no one")
	require.False(t, state.HasPendingNotifications())
}

func TestCommit_OfAFailedTransactionDiscardsTheRecordedNotifications(t *testing.T) {
	exec, state, _ := notifying(t, [2]string{"a", "1"})
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusFailed)
	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
	require.NoError(t, tp.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, noopCallback))
	require.Empty(t, exec.portalSQL, "COMMIT of a failed transaction is a rollback: nothing is notified")
}

func TestPrepareTransaction_RefusesATransactionThatRanNotify(t *testing.T) {
	exec, state, _ := notifying(t, [2]string{"a", "1"})
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	tp := NewTransactionPrimitive(ast.TRANS_STMT_PREPARE, "", false, "PREPARE TRANSACTION 'x'", "tg1", nil)
	err := tp.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, noopCallback)
	require.ErrorContains(t, err, "cannot PREPARE a transaction that has executed LISTEN, UNLISTEN, or NOTIFY")
	require.Zero(t, exec.streamExecuteCount, "nothing is prepared")
	require.False(t, state.HasPendingNotifications())
}

func TestNotifyPrimitive_RefusesWhatPostgresRefusesWhenItIsRecorded(t *testing.T) {
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	for name, tc := range map[string]struct {
		channel, payload, want string
	}{
		"payload of 8000 bytes":  {"c", strings.Repeat("x", 8000), "payload string too long"},
		"payload just in bounds": {"c", strings.Repeat("x", 7999), ""},
	} {
		t.Run(name, func(t *testing.T) {
			state := handler.NewMultigatewayConnectionState()
			p := NewNotifyPrimitive(ast.NewNotifyStmt(tc.channel, tc.payload), "NOTIFY c", true)
			err := p.StreamExecute(t.Context(), &txMockIExecute{}, conn, state, nil, PlanExecInfo{}, noopCallback)
			if tc.want == "" {
				require.NoError(t, err)
				require.True(t, state.HasPendingNotifications())
				return
			}
			require.ErrorContains(t, err, tc.want)
			require.False(t, state.HasPendingNotifications(), "nothing is recorded for a notification PostgreSQL would refuse")
		})
	}
}

func TestAddPendingNotification_CollapsesRepeatsAtTheFirstOccurrence(t *testing.T) {
	state := handler.NewMultigatewayConnectionState()
	for _, n := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "1"}, {"a", "3"}, {"b", "2"}} {
		require.NoError(t, state.AddPendingNotification(n[0], n[1]))
	}
	got := state.TakePendingNotifications()
	require.Equal(t, []handler.PendingNotification{{Channel: "a", Payload: "1"}, {Channel: "b", Payload: "2"}, {Channel: "a", Payload: "3"}}, got)
}

func TestNotifyPrimitive_RefusesBeyondTheCapPerTransaction(t *testing.T) {
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	notify := func(payload string) error {
		p := NewNotifyPrimitive(ast.NewNotifyStmt("c", payload), "NOTIFY c, '"+payload+"'", true)
		return p.StreamExecute(t.Context(), &txMockIExecute{}, conn, state, nil, PlanExecInfo{}, noopCallback)
	}
	for i := range handler.MaxPendingNotifications {
		require.NoError(t, notify(strconv.Itoa(i)))
	}
	require.NoError(t, notify("0"), "a repeat is collapsed, not counted")
	require.ErrorContains(t, notify("one-more"), "too many NOTIFY statements in one transaction")
}

// TestNotifyPrimitive_RefusesBeyondTheByteCapPerTransaction: the channels and payloads
// together are bounded, not only their number, so the one statement that delivers them
// fits in a gRPC message; a repeat of one already recorded adds nothing.
func TestNotifyPrimitive_RefusesBeyondTheByteCapPerTransaction(t *testing.T) {
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	payload := strings.Repeat("x", 7999)
	notify := func(channel string) error {
		p := NewNotifyPrimitive(ast.NewNotifyStmt(channel, payload), "NOTIFY c, '...'", true)
		return p.StreamExecute(t.Context(), &txMockIExecute{}, conn, state, nil, PlanExecInfo{}, noopCallback)
	}
	var recorded int
	for {
		err := notify("c" + strconv.Itoa(recorded))
		if err != nil {
			require.ErrorContains(t, err, "too large")
			break
		}
		recorded++
		require.Less(t, recorded, 1000, "the cap was never reached")
	}
	require.Less(t, recorded, handler.MaxPendingNotifications, "the byte cap is what stopped it")
	require.InDelta(t, handler.MaxPendingNotificationBytes/(len(payload)+4), recorded, 2)
	require.NoError(t, notify("c0"), "a repeat of a recorded notification is not counted")
	require.Len(t, state.TakePendingNotifications(), recorded)
}

// TestCommit_SendsManyNotificationsInOrderInOneStatement: the statement does not grow with
// the number of notifications, so there is no limit on its select list to run into.
func TestCommit_SendsManyNotificationsInOrderInOneStatement(t *testing.T) {
	var notifies [][2]string
	for i := range 5000 {
		notifies = append(notifies, [2]string{"c", strconv.Itoa(i)})
	}
	exec, _, commit := notifying(t, notifies...)
	result := commit()
	require.Empty(t, result.Notices)
	require.Equal(t, []string{notifyDeliverySQL}, exec.portalSQL)
	payloads := strings.Split(strings.Trim(string(exec.portalParams[0][1]), "{}"), ",")
	require.Len(t, payloads, 5000)
	for i, payload := range payloads {
		require.Equal(t, strconv.Quote(strconv.Itoa(i)), payload, "in the order they were issued")
	}
}

// TestCommit_ALongChannelNameIsTruncatedLikePostgresDoes: NOTIFY truncates a channel name
// to 63 bytes, where pg_notify() refuses it, which would drop every notification of the
// committed transaction. A name is cut at a character boundary, and names that become the
// same channel are one.
func TestCommit_ALongChannelNameIsTruncatedLikePostgresDoes(t *testing.T) {
	long := strings.Repeat("c", 100)
	multibyte := strings.Repeat("é", 40) // 80 bytes; byte 63 is in the middle of a character
	require.Equal(t, strings.Repeat("c", 63), truncateNotifyChannel(long))
	cut := truncateNotifyChannel(multibyte)
	require.Equal(t, strings.Repeat("é", 31), cut, "62 bytes: the 32nd character does not fit")
	require.True(t, utf8.ValidString(cut))
	require.Equal(t, "short", truncateNotifyChannel("short"))

	exec := &txMockIExecute{}
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	state.BeginTransaction()
	p := NewNotifyPrimitive(ast.NewNotifyStmt(long, "x"), "NOTIFY ...", true)
	require.NoError(t, p.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, noopCallback))
	p = NewNotifyPrimitive(ast.NewNotifyStmt(long+"more", "x"), "NOTIFY ...", true)
	require.NoError(t, p.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, noopCallback))
	got := state.TakePendingNotifications()
	require.Len(t, got, 1, "two names that become the same channel are one notification")
	require.Equal(t, strings.Repeat("c", 63), got[0].Channel)
}

// TestNotifyChannelFromTheParserIsCutAtACharacterBoundary: whatever way the channel is
// written (unquoted, quoted, or a Unicode escape), what reaches the delivery is cut to 63
// bytes at a character boundary, so it never carries invalid UTF-8, which pg_notify() would
// refuse for the whole batch. The parser itself cuts only the quoted and escaped forms (and
// assumes UTF-8); for an unquoted name it is the NOTIFY's own cut that applies.
func TestNotifyChannelFromTheParserIsCutAtACharacterBoundary(t *testing.T) {
	multibyte := strings.Repeat("é", 40) // 80 bytes; byte 63 is in the middle of a character
	escaped := strings.Repeat(`\00e9`, 40)
	for name, sql := range map[string]string{
		"unquoted":           "NOTIFY " + multibyte + ", 'x'",
		"quoted":             `NOTIFY "` + multibyte + `", 'x'`,
		"quoted with escape": `NOTIFY U&"` + escaped + `", 'x'`,
	} {
		t.Run(name, func(t *testing.T) {
			stmts, err := parser.ParseSQL(sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			notify, ok := stmts[0].(*ast.NotifyStmt)
			require.True(t, ok, "%T", stmts[0])

			conn := newTxTestConn()
			conn.SetTxnStatus(protocol.TxnStatusInBlock)
			state := handler.NewMultigatewayConnectionState()
			state.BeginTransaction()
			p := NewNotifyPrimitive(notify, sql, true)
			require.NoError(t, p.StreamExecute(t.Context(), &txMockIExecute{}, conn, state, nil, PlanExecInfo{}, noopCallback))
			got := state.TakePendingNotifications()
			require.Len(t, got, 1)
			require.True(t, utf8.ValidString(got[0].Channel), "the channel is valid UTF-8: %q", got[0].Channel)
			require.Equal(t, strings.Repeat("é", 31), got[0].Channel)
		})
	}
}

func TestTextArrayLiteral(t *testing.T) {
	assert.Equal(t, `{"plain"}`, string(textArrayLiteral([]string{"plain"})))
	assert.Equal(t, `{"a","b"}`, string(textArrayLiteral([]string{"a", "b"})))
	assert.Equal(t, `{""}`, string(textArrayLiteral([]string{""})), "an empty string is an element, not a null")
	assert.Equal(t, `{"it's \"quoted\" \\ here"}`, string(textArrayLiteral([]string{`it's "quoted" \ here`})))
	assert.Equal(t, `{"NULL"}`, string(textArrayLiteral([]string{"NULL"})), "quoted, so it is the string")
	assert.Equal(t, `{"a,b","{c}"}`, string(textArrayLiteral([]string{"a,b", "{c}"})))
}

func TestCommit_ADeliveryFailureWarnsWithoutTheErrorText(t *testing.T) {
	exec, _, commit := notifying(t, [2]string{"a", "1"})
	exec.portalErr = errors.New("dial tcp 10.1.2.3:5432: connection refused")
	result := commit()
	require.Len(t, result.Notices, 1)
	require.NotContains(t, result.Notices[0].Detail, "10.1.2.3", "the client is not told where the default cohort is")
}

// TestCommit_AFatalErrorFromTheDefaultBackendIsAWarning: the delivery runs as a pooled
// statement outside the session, so a FATAL error can only be a backend of the default
// cohort dying (a failover, an administrator's terminate). It says nothing about the
// client's session, and the data commit has happened: the COMMIT is reported, with a
// warning, and the session is still usable.
func TestCommit_AFatalErrorFromTheDefaultBackendIsAWarning(t *testing.T) {
	exec, state, commit := notifying(t, [2]string{"a", "1"})
	exec.portalErr = mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", "")
	result := commit()
	require.Equal(t, "COMMIT", result.CommandTag, "the COMMIT is the answer")
	require.Len(t, result.Notices, 1)
	assert.Equal(t, "WARNING", result.Notices[0].Severity)
	require.False(t, state.HasPendingNotifications())
}
