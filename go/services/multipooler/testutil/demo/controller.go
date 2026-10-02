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
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

type Writer interface {
	Context() context.Context
	ShardKey() *pb.ShardKey
	Transaction(context.Context, func(context.Context, executor.InternalTx, *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error)) error
}
type Controller struct {
	writer    Writer
	topo      topoclient.Store
	key       []byte
	transport grpc.DialOption
	mu        sync.Mutex
	changed   chan struct{}
	// AfterAcknowledgment is a test hook simulating a lost successful RPC reply.
	// The durable acknowledgment has not been written when this hook runs.
	AfterAcknowledgment func(*pb.Multipooler) error
	// BeforeCompletion injects a test interruption after enforcement and before its journal commit.
	BeforeCompletion func() error
}

func New(ctx context.Context, w Writer, ts topoclient.Store, key []byte, transport grpc.DialOption) (*Controller, error) {
	c := &Controller{writer: w, topo: ts, key: append([]byte(nil), key...), transport: transport, changed: make(chan struct{})}
	if len(key) != 32 {
		return nil, errors.New("32-byte migration key required")
	}
	if err := w.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, _ *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
		return nil, initialize(ctx, tx)
	}); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Controller) notify() {
	c.mu.Lock()
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
}

func (c *Controller) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.writer.Context(), cancel)
	return ctx, func() { stop(); cancel() }
}

func subjects(phase Phase) []pb.AdmissionSubject {
	switch phase {
	case Enable:
		return []pb.AdmissionSubject{pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET}
	case OpenSource, CloseSource:
		return []pb.AdmissionSubject{pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE}
	case OpenTarget:
		return []pb.AdmissionSubject{pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET}
	}
	return nil
}

func (c *Controller) members(ctx context.Context, r Request) ([]*pb.Multipooler, error) {
	list, e := migrationcontrol.Poolers(ctx, c.topo, c.writer.ShardKey().Database)
	if e != nil {
		return nil, e
	}
	authority, e := migrationcontrol.Authority(list)
	if e != nil {
		return nil, e
	}
	if !proto.Equal(authority.ShardKey, c.writer.ShardKey()) {
		return nil, errors.New("migration requires one exact authority cohort")
	}
	var selected []*pb.Multipooler
	for _, p := range list {
		if !proto.Equal(p.ShardKey, c.writer.ShardKey()) {
			return nil, errors.New("cohort mismatch")
		}
		source := p.ManagementMode == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
		match := false
		for _, s := range subjects(r.Phase) {
			match = match || (source == (s == pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE))
		}
		if !match {
			continue
		}
		if p.ProcessIncarnation == "" {
			return nil, errors.New("process incarnation unavailable")
		}
		if source && (p.SourceConnection != r.Source.SourceConnection || p.SourceConfigurationBinding != r.Source.SourceConfigurationBinding) {
			return nil, errors.New("source membership not initialized or identity differs")
		}
		selected = append(selected, proto.Clone(p).(*pb.Multipooler))
	}
	if len(subjects(r.Phase)) > 0 && len(selected) == 0 {
		return nil, errors.New("required admission membership absent")
	}
	return selected, nil
}

// Advance and Recover perform one bounded attempt. A timeout leaves intent and
// journal pending; retry the same request on a newly activated authority.
func (c *Controller) Advance(ctx context.Context, r Request) (*Operation, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	if e := validateRequest(r); e != nil {
		return nil, e
	}
	members, e := c.members(ctx, r)
	if e != nil {
		return nil, e
	}
	var op *Operation
	e = c.writer.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, cat *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
		var route *pb.GatewayRoutingPolicy
		var err error
		op, route, err = begin(ctx, tx, cat, c.writer.ShardKey(), r, members)
		return route, err
	})
	if e != nil {
		return nil, e
	}
	c.notify()
	if op.Complete {
		return op, nil
	}
	// Discovery and drain RPCs never execute inside an authority transaction.
	for range 3 {
		members, e = c.members(ctx, r)
		if e != nil {
			return nil, e
		}
		e = c.writer.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, _ *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
			var err error
			op, err = ownedCurrent(ctx, tx, c.writer.ShardKey(), r.ID)
			if err != nil {
				return nil, err
			}
			known := map[string]bool{}
			for _, p := range op.Required {
				known[ProcessKey(p)] = true
			}
			for _, p := range members {
				if !known[ProcessKey(p)] {
					op.Required = append(op.Required, p)
				}
			}
			return nil, confirm(ctx, tx, c.writer.ShardKey(), op)
		})
		if e != nil {
			return nil, e
		}
		c.notify()
		live := map[string]*pb.Multipooler{}
		for _, p := range members {
			live[ProcessKey(p)] = p
		}
		for _, required := range op.Required {
			k := ProcessKey(required)
			if op.Acknowledged[k] || op.Terminated[k] {
				continue
			}
			p := live[k]
			if p == nil {
				return nil, fmt.Errorf("required process %s absent: actual termination proof required", k)
			}
			var expected *pb.AdmissionIntent
			source := p.ManagementMode == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
			for _, intent := range op.Intents {
				if source == (intent.Subject == pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE) {
					expected = intent
				}
			}
			if expected == nil {
				return nil, errors.New("required process has no admission intent")
			}
			conn, err := migrationcontrol.Dial(p, c.transport)
			if err != nil {
				return nil, err
			}
			callCtx, stop := context.WithTimeout(migrationcontrol.AuthorizedContext(ctx, c.key), 15*time.Second)
			ack, err := rpc.NewMultipoolerServiceClient(conn).RefreshAdmission(callCtx, &rpc.RefreshAdmissionRequest{Database: c.writer.ShardKey().Database, Expected: expected, ExpectedProcessIncarnation: p.ProcessIncarnation})
			stop()
			_ = conn.Close()
			if err != nil {
				return nil, err
			}
			if !proto.Equal(ack.Observed, expected) || !proto.Equal(ack.ProcessId, p.Id) || ack.ProcessIncarnation != p.ProcessIncarnation || ack.AdmissionOpen != (expected.Permission == pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN) {
				return nil, errors.New("inexact process acknowledgment")
			}
			if c.AfterAcknowledgment != nil {
				if err = c.AfterAcknowledgment(p); err != nil {
					return nil, err
				}
			}
			e = c.writer.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, _ *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
				var err error
				op, err = ownedCurrent(ctx, tx, c.writer.ShardKey(), r.ID)
				if err != nil {
					return nil, err
				}
				op.Acknowledged[k] = true
				return nil, save(ctx, tx, c.writer.ShardKey().Database, op)
			})
			if e != nil {
				return nil, e
			}
			c.notify()
		}
		// Re-enumerate all cells before completion. Joins expand the durable required
		// set; disappearance never silently drops an old process acknowledgment.
		latest, err := c.members(ctx, r)
		if err != nil {
			return nil, err
		}
		known := map[string]bool{}
		for _, p := range op.Required {
			known[ProcessKey(p)] = true
		}
		joined := false
		for _, p := range latest {
			joined = joined || !known[ProcessKey(p)]
		}
		if joined {
			continue
		}
		if c.BeforeCompletion != nil {
			if e = c.BeforeCompletion(); e != nil {
				return nil, e
			}
		}
		e = c.writer.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, _ *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
			var err error
			op, err = ownedCurrent(ctx, tx, c.writer.ShardKey(), r.ID)
			if err != nil {
				return nil, err
			}
			for _, p := range op.Required {
				if !op.Acknowledged[ProcessKey(p)] && !op.Terminated[ProcessKey(p)] {
					return nil, errors.New("enforcement incomplete")
				}
			}
			op.Complete = true
			return nil, save(ctx, tx, c.writer.ShardKey().Database, op)
		})
		if e != nil {
			return nil, e
		}
		c.notify()
		return op, nil
	}
	return nil, errors.New("membership changed repeatedly; recover same request")
}

func (c *Controller) Status(ctx context.Context, id string) (*Operation, error) {
	var op *Operation
	e := c.writer.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, _ *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
		var err error
		if err = admission.LockScope(ctx, tx, c.writer.ShardKey()); err != nil {
			return nil, err
		}
		op, err = load(ctx, tx, c.writer.ShardKey().Database, id)
		if err != nil {
			return nil, err
		}
		if op == nil {
			return nil, errors.New("operation not found")
		}
		if !op.Complete {
			active, e := current(ctx, tx, c.writer.ShardKey().Database)
			if e != nil {
				return nil, e
			}
			if active == nil || active.Request.ID != op.Request.ID {
				return nil, errors.New("operation superseded")
			}
		}
		return nil, confirm(ctx, tx, c.writer.ShardKey(), op)
	})
	if e != nil {
		return nil, e
	}
	return op, nil
}

// Subscribe before inspecting durable state. Notifications are hints, and status
// confirmation itself never notifies. Canceling a waiter cannot undo intent.
func (c *Controller) Wait(ctx context.Context, id string) (*Operation, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	for {
		c.mu.Lock()
		changed := c.changed
		c.mu.Unlock()
		op, e := c.Status(ctx, id)
		if e != nil {
			return nil, e
		}
		if op.Complete {
			return op, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// RecordTermination accepts actual process-exit evidence from the test launcher,
// never a topology disappearance or a pooler shutdown advertisement. The caller
// validates that wait belongs to this exact process before supplying it. A
// future production controller needs equivalent operator-provided evidence.
func (c *Controller) RecordTermination(ctx context.Context, id string, p *pb.Multipooler, wait func(context.Context) error) error {
	if wait == nil {
		return errors.New("actual process termination proof required")
	}
	if err := wait(ctx); err != nil {
		return err
	} // No catalog/action lock across a wait.
	key := ProcessKey(p)
	err := c.writer.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx, _ *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error) {
		op, e := ownedCurrent(ctx, tx, c.writer.ShardKey(), id)
		if e != nil {
			return nil, e
		}
		found := false
		for _, required := range op.Required {
			found = found || ProcessKey(required) == key
		}
		if !found {
			return nil, errors.New("termination proof does not name a required incarnation")
		}
		if op.Terminated == nil {
			op.Terminated = map[string]bool{}
		}
		op.Terminated[key] = true
		return nil, save(ctx, tx, c.writer.ShardKey().Database, op)
	})
	if err == nil {
		c.notify()
	}
	return err
}
