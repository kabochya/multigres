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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
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
		state.AddPendingNotification(n[0], n[1])
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
	require.Equal(t, []string{"SELECT pg_notify('a', '1'), pg_notify('b', '2'), pg_notify('a', '3')"}, exec.streamExecuteSQL,
		"one statement, in order, with the repeat of a channel and payload collapsed")
	require.True(t, exec.streamExecuteResv[0].Control)
	require.True(t, exec.streamExecuteResv[0].Autonomous, "outside the session's transaction")
	require.False(t, state.HasPendingNotifications())
}

func TestCommit_FailedDeliveryIsAWarningNotAnError(t *testing.T) {
	exec, state, commit := notifying(t, [2]string{"a", "1"})
	exec.streamExecuteErr = errors.New("no leader")
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
	require.Zero(t, exec.streamExecuteCount, "a rolled back transaction notifies no one")
	require.False(t, state.HasPendingNotifications())
}

func TestCommit_OfAFailedTransactionDiscardsTheRecordedNotifications(t *testing.T) {
	exec, state, _ := notifying(t, [2]string{"a", "1"})
	conn := newTxTestConn()
	conn.SetTxnStatus(protocol.TxnStatusFailed)
	tp := NewTransactionPrimitive(ast.TRANS_STMT_COMMIT, "", false, "COMMIT", "tg1", nil)
	require.NoError(t, tp.StreamExecute(t.Context(), exec, conn, state, nil, PlanExecInfo{}, noopCallback))
	require.Zero(t, exec.streamExecuteCount, "COMMIT of a failed transaction is a rollback")
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
		require.True(t, state.AddPendingNotification(n[0], n[1]))
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

// TestCommit_SendsManyNotificationsInOrderInStatementsThatAreAccepted: one statement with
// more than 1664 targets is refused by PostgreSQL, which would drop every notification of
// the committed transaction.
func TestCommit_SendsManyNotificationsInOrderInStatementsThatAreAccepted(t *testing.T) {
	var notifies [][2]string
	for i := range 1700 {
		notifies = append(notifies, [2]string{"c", strconv.Itoa(i)})
	}
	exec, _, commit := notifying(t, notifies...)
	result := commit()
	require.Empty(t, result.Notices)
	require.Greater(t, len(exec.streamExecuteSQL), 1, "split into several statements")
	var order []string
	for _, sql := range exec.streamExecuteSQL {
		require.LessOrEqual(t, strings.Count(sql, "pg_notify("), 1664, "within PostgreSQL's select-list limit")
		for _, part := range strings.Split(sql, "pg_notify('c', '")[1:] {
			order = append(order, strings.SplitN(part, "'", 2)[0])
		}
	}
	require.Len(t, order, 1700)
	for i, payload := range order {
		require.Equal(t, strconv.Itoa(i), payload, "in the order they were issued")
	}
}

func TestCommit_ADeliveryFailureWarnsWithoutTheErrorText(t *testing.T) {
	exec, _, commit := notifying(t, [2]string{"a", "1"})
	exec.streamExecuteErr = errors.New("dial tcp 10.1.2.3:5432: connection refused")
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
	exec.streamExecuteErr = mterrors.NewPgError("FATAL", "57P01", "terminating connection due to administrator command", "")
	result := commit()
	require.Equal(t, "COMMIT", result.CommandTag, "the COMMIT is the answer")
	require.Len(t, result.Notices, 1)
	assert.Equal(t, "WARNING", result.Notices[0].Severity)
	require.False(t, state.HasPendingNotifications())
}

// TestCommit_ChunkFailingPartwayDropsTheRestInOrder: a chunk that fails drops itself and
// the rest, so that what comes after never overtakes what did not arrive; the warning
// says how many were delivered.
func TestCommit_ChunkFailingPartwayDropsTheRestInOrder(t *testing.T) {
	var notifies [][2]string
	for i := range 1200 {
		notifies = append(notifies, [2]string{"c", strconv.Itoa(i)})
	}
	exec, _, commit := notifying(t, notifies...)
	exec.streamExecuteErrOnCall = 2 // the second statement fails
	exec.streamExecuteErr = errors.New("default cohort unavailable")
	result := commit()
	require.Len(t, exec.streamExecuteSQL, 2, "nothing is sent after the chunk that failed")
	require.Equal(t, 500, strings.Count(exec.streamExecuteSQL[0], "pg_notify("), "chunks of at most 500")
	require.Len(t, result.Notices, 1)
	require.Contains(t, result.Notices[0].Detail, "500 of 1200 notifications were delivered")
}

func TestQuoteSQLLiteral(t *testing.T) {
	assert.Equal(t, `'plain'`, quoteSQLLiteral("plain"))
	assert.Equal(t, `'it''s'`, quoteSQLLiteral("it's"))
	assert.Equal(t, `E'back\\slash'`, quoteSQLLiteral(`back\slash`), "an escape string reads the same whatever standard_conforming_strings is")
	assert.Equal(t, `E'it''s \\ here'`, quoteSQLLiteral(`it's \ here`))
}
