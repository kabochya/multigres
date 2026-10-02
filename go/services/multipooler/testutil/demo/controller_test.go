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

package demo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	qmock "github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

func testSource() *pb.GatewayRoutingPolicy {
	return &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_SOURCE, SourceConnection: "source", SourceConfigurationBinding: "binding", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}
}

func TestDemoTransitionOwnershipAndBarrier(t *testing.T) {
	r := Request{ID: "enable", Owner: "owner", Phase: Enable, Source: testSource()}
	require.NoError(t, validateNext(r, nil))
	p := &Operation{Request: r, Complete: true}
	next := Request{ID: "open", Owner: r.Owner, Predecessor: r.ID, Phase: OpenSource, Source: r.Source}
	require.NoError(t, validateNext(next, p))
	for _, mutate := range []func(*Request){func(r *Request) { r.Owner = "other" }, func(r *Request) { r.Predecessor = "stale" }, func(r *Request) { r.Phase = OpenTarget }, func(r *Request) {
		r.Source = proto.Clone(r.Source).(*pb.GatewayRoutingPolicy)
		r.Source.SourceConfigurationBinding = "other"
	}} {
		bad := next
		mutate(&bad)
		require.Error(t, validateNext(bad, p))
	}
	p.Complete = false
	require.Error(t, validateNext(next, p))
	h, e := requestHash(next)
	require.NoError(t, e)
	repeat := next
	repeat.ID = "another-open"
	other, e := requestHash(repeat)
	require.NoError(t, e)
	require.NotEqual(t, h, other)
	require.NotEqual(t, ProcessKey(&pb.Multipooler{Id: &pb.ID{Cell: "c", Name: "p"}, ProcessIncarnation: "one"}), ProcessKey(&pb.Multipooler{Id: &pb.ID{Cell: "c", Name: "p"}, ProcessIncarnation: "two"}))
}

// memoryWriter models only serialized confirmed journal transactions. Actual
// PostgreSQL tests cover the WAL and uncertain-commit behavior separately.
type memoryWriter struct {
	mu           sync.Mutex
	ctx          context.Context
	op           *Operation
	read         chan struct{}
	after        func()
	transactions int
	currentID    string
}

func (w *memoryWriter) Context() context.Context { return w.ctx }
func (w *memoryWriter) ShardKey() *pb.ShardKey {
	return &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}
}

func (w *memoryWriter) Transaction(ctx context.Context, f func(context.Context, executor.InternalTx, *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error)) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	w.mu.Lock()
	w.transactions++
	_, e := f(ctx, memoryTx{w: w}, nil)
	after := w.after
	w.after = nil
	w.mu.Unlock()
	if after != nil {
		after()
	}
	if w.read != nil {
		w.read <- struct{}{}
	}
	return e
}

type memoryTx struct {
	executor.InternalTx
	w *memoryWriter
}

func (tx memoryTx) QueryArgs(_ context.Context, q string, args ...any) (*sqltypes.Result, error) {
	switch {
	case strings.HasPrefix(q, "SELECT operation"):
		op := tx.w.op
		if args[1].(string) != op.Request.ID && tx.w.currentID != "" {
			op = &Operation{Request: Request{ID: tx.w.currentID}, Complete: true}
		}
		b, e := json.Marshal(op)
		if e != nil {
			return nil, e
		}
		return qmock.MakeQueryResult([]string{"operation"}, [][]any{{string(b)}}), nil
	case strings.HasPrefix(q, "SELECT request_id"):
		id := tx.w.currentID
		if id == "" {
			id = tx.w.op.Request.ID
		}
		return qmock.MakeQueryResult([]string{"request_id"}, [][]any{{id}}), nil
	case strings.HasPrefix(q, "SELECT table_group,shard"):
		return qmock.MakeQueryResult([]string{"table_group", "shard"}, [][]any{{"default", "0"}}), nil
	case strings.HasPrefix(q, "SELECT s.table_group"):
		subject := pb.AdmissionSubject(args[1].(int32))
		intent := &pb.AdmissionIntent{Owner: "owner", IntentId: "pending", Subject: subject, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED}
		if subject == pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE {
			intent.SourceConnection = "source"
			intent.SourceConfigurationBinding = "binding"
			intent.SourceIdentity = testSource().SourceIdentity
		}
		b, e := proto.Marshal(intent)
		if e != nil {
			return nil, e
		}
		return qmock.MakeQueryResult([]string{"table_group", "shard", "controlled", "owner", "intent"}, [][]any{{"default", "0", true, "owner", hex.EncodeToString(b)}}), nil
	case strings.HasPrefix(q, "INSERT INTO multigres.demo_operations"):
		op := &Operation{}
		if e := json.Unmarshal([]byte(args[2].(string)), op); e != nil {
			return nil, e
		}
		tx.w.op = op
		return &sqltypes.Result{}, nil
	case strings.HasPrefix(q, "UPDATE multigres.admission_scopes"):
		return &sqltypes.Result{}, nil
	default:
		return nil, fmt.Errorf("unexpected test query %s", q)
	}
}

func waitingController(t *testing.T) (*Controller, *memoryWriter, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	w := &memoryWriter{ctx: ctx, op: &Operation{Request: Request{ID: "pending", Owner: "owner"}}, read: make(chan struct{}, 20)}
	c := &Controller{writer: w, changed: make(chan struct{})}
	return c, w, cancel
}

func TestDemoWaitLostWakeupAndCompletedBeforeSubscription(t *testing.T) {
	c, w, cancel := waitingController(t)
	defer cancel()
	w.after = func() { w.mu.Lock(); w.op.Complete = true; w.mu.Unlock(); c.notify() }
	op, e := c.Wait(t.Context(), "pending")
	require.NoError(t, e)
	require.True(t, op.Complete)
	// A new waiter requires no notification when completion already happened.
	op, e = c.Wait(t.Context(), "pending")
	require.NoError(t, e)
	require.True(t, op.Complete)
}

func TestDemoMultipleCanceledWaitersAndReadOnlyConfirmation(t *testing.T) {
	c, w, shutdown := waitingController(t)
	defer shutdown()
	ctx, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	done := make(chan error, 2)
	go func() { _, e := c.Wait(ctx, "pending"); canceled <- e }()
	<-w.read
	for range 2 {
		go func() { _, e := c.Wait(t.Context(), "pending"); done <- e }()
	}
	<-w.read
	<-w.read
	c.mu.Lock()
	subscription := c.changed
	c.mu.Unlock()
	_, e := c.Status(t.Context(), "pending")
	require.NoError(t, e)
	<-w.read
	select {
	case <-subscription:
		t.Fatal("read-only confirmation emitted a change")
	default:
	}
	cancel()
	require.ErrorIs(t, <-canceled, context.Canceled)
	w.mu.Lock()
	require.False(t, w.op.Complete)
	w.op.Complete = true
	w.mu.Unlock()
	c.notify()
	require.NoError(t, <-done)
	require.NoError(t, <-done)
}

func TestDemoWaitLeadershipLoss(t *testing.T) {
	c, w, cancel := waitingController(t)
	done := make(chan error, 1)
	go func() { _, e := c.Wait(t.Context(), "pending"); done <- e }()
	<-w.read
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	w.mu.Lock()
	require.False(t, w.op.Complete)
	w.mu.Unlock()
}

func TestDemoWaitSuperseded(t *testing.T) {
	c, w, cancel := waitingController(t)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.Wait(t.Context(), "pending"); done <- e }()
	<-w.read
	w.mu.Lock()
	w.currentID = "new-operation"
	w.mu.Unlock()
	c.notify()
	require.ErrorContains(t, <-done, "superseded")
}
