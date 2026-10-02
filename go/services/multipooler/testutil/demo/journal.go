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

// Package demo implements a test-only migration controller. It is never linked
// into the production multipooler binary. The replication barrier is simulated.
package demo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/routingpolicy"
)

type Phase string

const (
	Enable           Phase = "enable-closed"
	OpenSource       Phase = "open-source"
	RouteSource      Phase = "route-source"
	CloseSource      Phase = "close-source"
	SimulatedBarrier Phase = "SIMULATED-replication-barrier"
	OpenTarget       Phase = "open-target"
	RouteTarget      Phase = "route-target"
	Finish           Phase = "finish"
	Retire           Phase = "retire-source"
)

var phases = []Phase{Enable, OpenSource, RouteSource, CloseSource, SimulatedBarrier, OpenTarget, RouteTarget, Finish, Retire}

type (
	Request struct {
		ID, Owner, Predecessor string
		Phase                  Phase
		Source                 *pb.GatewayRoutingPolicy
	}
	Operation struct {
		Request      Request
		Hash         string
		Complete     bool
		Required     []*pb.Multipooler
		Acknowledged map[string]bool
		Terminated   map[string]bool
		Intents      []*pb.AdmissionIntent
		Projection   []*pb.AdmissionIntent
	}
)

func ProcessKey(p *pb.Multipooler) string {
	return p.GetId().String() + "/" + p.GetProcessIncarnation()
}

func requestHash(r Request) (string, error) {
	b, e := json.Marshal(r)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func validateRequest(r Request) error {
	if r.ID == "" || r.Owner == "" {
		return errors.New("request ID and owner required")
	}
	found := false
	for _, p := range phases {
		found = found || r.Phase == p
	}
	if !found {
		return errors.New("unknown demo phase")
	}
	if r.Source == nil || r.Source.Destination != pb.RoutingDestination_ROUTING_DESTINATION_SOURCE {
		return errors.New("bound source policy required")
	}
	return routingpolicy.Validate(r.Source)
}

func validateNext(r Request, previous *Operation) error {
	if err := validateRequest(r); err != nil {
		return err
	}
	if previous == nil {
		if r.Phase != Enable || r.Predecessor != "" {
			return errors.New("first operation must enable CLOSED")
		}
		return nil
	}
	if !previous.Complete || r.Predecessor != previous.Request.ID || r.Owner != previous.Request.Owner || !proto.Equal(r.Source, previous.Request.Source) {
		return errors.New("conflicting owner or incomplete/stale predecessor")
	}
	for n, p := range phases {
		if previous.Request.Phase == p && n+1 < len(phases) && r.Phase == phases[n+1] {
			return nil
		}
	}
	return errors.New("invalid phase advancement")
}

func initialize(ctx context.Context, tx executor.InternalTx) error {
	for _, sql := range []string{`CREATE TABLE IF NOT EXISTS multigres.demo_operations(database TEXT NOT NULL,request_id TEXT NOT NULL,operation TEXT NOT NULL,PRIMARY KEY(database,request_id))`, `CREATE TABLE IF NOT EXISTS multigres.demo_current(database TEXT PRIMARY KEY,request_id TEXT NOT NULL)`, `REVOKE ALL ON multigres.demo_operations,multigres.demo_current FROM PUBLIC`} {
		if _, e := tx.Query(ctx, sql); e != nil {
			return e
		}
	}
	return nil
}

func load(ctx context.Context, tx executor.InternalTx, database, id string) (*Operation, error) {
	result, e := tx.QueryArgs(ctx, `SELECT operation FROM multigres.demo_operations WHERE database=$1 AND request_id=$2 FOR UPDATE`, database, id)
	if e != nil {
		return nil, e
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	var data string
	if e = executor.ScanSingleRow(result, &data); e != nil {
		return nil, e
	}
	op := &Operation{}
	if e = json.Unmarshal([]byte(data), op); e != nil {
		return nil, e
	}
	return op, nil
}

func current(ctx context.Context, tx executor.InternalTx, database string) (*Operation, error) {
	result, e := tx.QueryArgs(ctx, `SELECT request_id FROM multigres.demo_current WHERE database=$1 FOR UPDATE`, database)
	if e != nil {
		return nil, e
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	var id string
	if e = executor.ScanSingleRow(result, &id); e != nil {
		return nil, e
	}
	return load(ctx, tx, database, id)
}

func save(ctx context.Context, tx executor.InternalTx, database string, op *Operation) error {
	data, e := json.Marshal(op)
	if e != nil {
		return e
	}
	_, e = tx.QueryArgs(ctx, `INSERT INTO multigres.demo_operations(database,request_id,operation) VALUES($1,$2,$3) ON CONFLICT(database,request_id) DO UPDATE SET operation=EXCLUDED.operation`, database, op.Request.ID, string(data))
	return e
}

func confirm(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, op *Operation) error {
	// A completed-request shortcut and pooler-only restart still require a new
	// synchronous WAL-generating write; local journal visibility is insufficient.
	for _, subject := range []pb.AdmissionSubject{pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET} {
		if _, e := admission.Confirm(ctx, tx, sk, subject); e != nil {
			return e
		}
	}
	return save(ctx, tx, sk.Database, op)
}

func begin(ctx context.Context, tx executor.InternalTx, c *connectioncatalog.Catalog, sk *pb.ShardKey, r Request, required []*pb.Multipooler) (*Operation, *pb.GatewayRoutingPolicy, error) {
	if e := admission.LockScope(ctx, tx, sk); e != nil {
		return nil, nil, e
	}
	h, e := requestHash(r)
	if e != nil {
		return nil, nil, e
	}
	old, e := load(ctx, tx, sk.Database, r.ID)
	if e != nil {
		return nil, nil, e
	}
	if old != nil {
		if old.Hash != h {
			return nil, nil, errors.New("request ID payload conflict")
		}
		if e = confirm(ctx, tx, sk, old); e != nil {
			return nil, nil, e
		}
		return old, nil, nil
	}
	previous, e := current(ctx, tx, sk.Database)
	if e != nil {
		return nil, nil, e
	}
	if e = validateNext(r, previous); e != nil {
		return nil, nil, e
	}
	op := &Operation{Request: r, Hash: h, Required: required, Acknowledged: map[string]bool{}}
	if previous != nil {
		if e = validateProjection(ctx, tx, sk, previous); e != nil {
			return nil, nil, e
		}
		for _, i := range previous.Projection {
			op.Projection = append(op.Projection, proto.Clone(i).(*pb.AdmissionIntent))
		}
	} else {
		scope, err := admission.Read(ctx, tx, sk, pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET)
		if err != nil {
			return nil, nil, err
		}
		if scope.Controlled {
			return nil, nil, errors.New("controlled scope has no recoverable controller journal")
		}
	}
	source := &pb.AdmissionIntent{Owner: r.Owner, IntentId: r.ID + "/source", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED, SourceConnection: r.Source.SourceConnection, SourceConfigurationBinding: r.Source.SourceConfigurationBinding, SourceIdentity: proto.Clone(r.Source.SourceIdentity).(*pb.ExternalBackendIdentity)}
	target := &pb.AdmissionIntent{Owner: r.Owner, IntentId: r.ID + "/target", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED}
	var route *pb.GatewayRoutingPolicy
	switch r.Phase {
	case Enable:
		e = admission.Enable(ctx, tx, sk, source, target)
		op.Intents = []*pb.AdmissionIntent{source, target}
	case OpenSource:
		source.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN
		e = admission.Write(ctx, tx, sk, source)
		op.Intents = []*pb.AdmissionIntent{source}
	case CloseSource:
		e = admission.Write(ctx, tx, sk, source)
		op.Intents = []*pb.AdmissionIntent{source}
	case OpenTarget:
		s, err := admission.Read(ctx, tx, sk, source.Subject)
		if err != nil {
			return nil, nil, err
		}
		if s.Intent.GetPermission() != pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED {
			return nil, nil, errors.New("source must remain CLOSED")
		}
		target.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN
		e = admission.Write(ctx, tx, sk, target)
		op.Intents = []*pb.AdmissionIntent{target}
	case RouteSource:
		route = proto.Clone(r.Source).(*pb.GatewayRoutingPolicy)
	case RouteTarget:
		route = &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_MANAGED}
	case Retire:
		s, err := admission.Read(ctx, tx, sk, source.Subject)
		if err != nil {
			return nil, nil, err
		}
		e = admission.InitializeLifecycle(ctx, tx)
		if e == nil {
			e = admission.WriteLifecycle(ctx, tx, sk, &pb.SourceLifecycleAuthorization{Owner: r.Owner, ClosedIntentId: s.Intent.GetIntentId(), SourceConnection: source.SourceConnection, SourceConfigurationBinding: source.SourceConfigurationBinding, RetireSource: true})
		}
	}
	if e != nil {
		return nil, nil, e
	}
	if route != nil {
		if e = routingpolicy.Write(ctx, tx, sk.Database, route, c); e != nil {
			return nil, nil, e
		}
	}
	if r.Phase == Enable {
		op.Projection = op.Intents
	} else {
		for _, newIntent := range op.Intents {
			for n, oldIntent := range op.Projection {
				if oldIntent.Subject == newIntent.Subject {
					op.Projection[n] = newIntent
				}
			}
		}
	}
	if e = save(ctx, tx, sk.Database, op); e != nil {
		return nil, nil, e
	}
	_, e = tx.QueryArgs(ctx, `INSERT INTO multigres.demo_current(database,request_id) VALUES($1,$2) ON CONFLICT(database) DO UPDATE SET request_id=EXCLUDED.request_id`, sk.Database, r.ID)
	return op, route, e
}

func ownedCurrent(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, id string) (*Operation, error) {
	if e := admission.LockScope(ctx, tx, sk); e != nil {
		return nil, e
	}
	op, e := current(ctx, tx, sk.Database)
	if e != nil {
		return nil, e
	}
	if op == nil || op.Request.ID != id {
		return nil, fmt.Errorf("operation %s superseded", id)
	}
	for _, subject := range []pb.AdmissionSubject{pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET} {
		s, e := admission.Read(ctx, tx, sk, subject)
		if e != nil {
			return nil, e
		}
		if !s.Controlled || s.Owner != op.Request.Owner {
			return nil, errors.New("controller ownership lost")
		}
	}
	return op, validateProjection(ctx, tx, sk, op)
}

func validateProjection(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, op *Operation) error {
	if len(op.Projection) != 2 {
		return errors.New("both admission subjects required in journal")
	}
	seen := map[pb.AdmissionSubject]bool{}
	for _, expected := range op.Projection {
		if expected == nil || seen[expected.Subject] {
			return errors.New("inexact journal projection")
		}
		seen[expected.Subject] = true
		s, err := admission.Read(ctx, tx, sk, expected.Subject)
		if err != nil {
			return err
		}
		if !s.Controlled || s.Owner != op.Request.Owner || !proto.Equal(s.Intent, expected) {
			return errors.New("admission projection changed outside current operation")
		}
	}
	return nil
}
