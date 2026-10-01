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

package manager

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

type admissionGate interface {
	SetApplicationAdmission(bool)
	DrainApplication(context.Context) error
	AdmissionGeneration() uint64
	ApplyAdmissionGeneration(uint64, bool) bool
}

// Serial serializes read/apply/drain, independently of actionLock. Lifecycle
// callbacks only withdraw authority and never wait for this serial/drain lock.
type admissionRuntime struct {
	serial      chan struct{}
	mu          sync.Mutex
	initialized bool
	routing     *pb.RoutingState
	serving     bool
	events      chan struct{}
	cancel      context.CancelFunc
	reader      func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error)
}

func (pm *MultipoolerManager) admissionGate() (admissionGate, error) {
	g, ok := pm.qsc.(admissionGate)
	if !ok {
		return nil, errors.New("admission gate unavailable")
	}
	return g, nil
}

func (pm *MultipoolerManager) admissionSubject() pb.AdmissionSubject {
	if pm.IsUnmanaged() {
		return pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE
	}
	return pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET
}

func (pm *MultipoolerManager) signalAdmission() {
	a := &pm.admissionRuntime
	a.mu.Lock()
	if a.events == nil {
		a.events = make(chan struct{}, 1)
	}
	ch := a.events
	a.mu.Unlock()
	select {
	case ch <- struct{}{}:
	default:
	}
}

type admissionLifecycle struct{ pm *MultipoolerManager }

func (l admissionLifecycle) OnStateChange(_ context.Context, s servingstate.State) error {
	pm := l.pm
	a := &pm.admissionRuntime
	r := s.Routing.ToProto()
	serving := s.ServingStatus == pb.PoolerServingStatus_SERVING
	a.mu.Lock()
	changed := !proto.Equal(a.routing, r) || a.serving != serving
	if changed {
		a.initialized = false
		a.routing = r
		a.serving = serving
		if a.cancel != nil {
			a.cancel()
		}
	}
	a.mu.Unlock()
	if changed {
		if g, err := pm.admissionGate(); err == nil {
			g.SetApplicationAdmission(false)
		}
		pm.signalAdmission()
	}
	return nil
}

func (pm *MultipoolerManager) startAdmission() {
	if len(pm.config.MigrationKey) != 32 {
		return
	}
	pm.signalAdmission()
	_ = pm.stateManager.RegisterAndSync(pm.shutdownCtx, admissionLifecycle{pm})
	go pm.runAdmission(pm.shutdownCtx)
}

func (pm *MultipoolerManager) runAdmission(ctx context.Context) {
	a := &pm.admissionRuntime
	a.mu.Lock()
	events := a.events
	a.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-events:
		}
		burst, cancel := context.WithCancel(ctx)
		a.mu.Lock()
		a.cancel = cancel
		serving := a.serving
		a.mu.Unlock()
		if serving {
			for n := range 5 {
				readCtx, stop := context.WithTimeout(burst, 3*time.Second)
				_, err := pm.enforceAdmission(readCtx, nil)
				stop()
				if err == nil || n == 4 || burst.Err() != nil {
					break
				}
				timer := time.NewTimer((100 * time.Millisecond) << n)
				select {
				case <-burst.Done():
					timer.Stop()
				case <-timer.C:
				}
			}
		}
		cancel()
	}
}

// ReadAdmissionIntent is non-secret but requires protected control transport.
// It returns no snapshot on uncertain commit, including ordinary applicability.
func (pm *MultipoolerManager) ReadAdmissionIntent(ctx context.Context, r *rpc.ReadAdmissionIntentRequest) (*rpc.ReadAdmissionIntentResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	s, err := pm.confirmAdmission(ctx, r.Database, r.Subject)
	if err != nil {
		return nil, err
	}
	return &rpc.ReadAdmissionIntentResponse{Snapshot: s}, nil
}

func (pm *MultipoolerManager) confirmAdmission(ctx context.Context, database string, subject pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
	lockCtx, err := pm.actionLock.Acquire(ctx, "ConfirmAdmission")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(database); err != nil {
		return nil, err
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return nil, err
	}
	var s *pb.AdmissionSnapshot
	err = c.Transaction(lockCtx, func(ctx context.Context, tx executor.InternalTx) error {
		if err := admission.InitializeOrdinary(ctx, tx, pm.record.ShardKey()); err != nil {
			return err
		}
		var err error
		s, err = admission.Confirm(ctx, tx, pm.record.ShardKey(), subject)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (pm *MultipoolerManager) authorityAdmission(ctx context.Context, subject pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
	if pm.admissionRuntime.reader != nil {
		return pm.admissionRuntime.reader(ctx, subject)
	}
	var s *pb.AdmissionSnapshot
	err := migrationcontrol.WithAuthority(ctx, pm.config.TopoClient, pm.record.ShardKey().Database, pm.config.ControlTransport, func(c rpc.MultipoolerServiceClient) error {
		r, err := c.ReadAdmissionIntent(migrationcontrol.AuthorizedContext(ctx, pm.config.MigrationKey), &rpc.ReadAdmissionIntentRequest{Database: pm.record.ShardKey().Database, Subject: subject})
		if err != nil {
			return err
		}
		s = r.Snapshot
		if s == nil || !proto.Equal(s.AuthorityShardKey, pm.record.ShardKey()) {
			return errors.New("admission authority scope mismatch")
		}
		return nil
	})
	return s, err
}

type localAdmissionReader struct{ q executor.InternalQueryService }

func (q localAdmissionReader) QueryArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	return q.q.QueryAdminArgs(ctx, sql, args...)
}

func (pm *MultipoolerManager) readAdmission(ctx context.Context, expected *pb.AdmissionIntent, initialized bool) (*pb.AdmissionSnapshot, error) {
	subject := pm.admissionSubject()
	if pm.admissionRuntime.reader != nil {
		return pm.admissionRuntime.reader(ctx, subject)
	}
	if pm.IsUnmanaged() {
		return pm.authorityAdmission(ctx, subject)
	}
	if pm.servingAuthority(pm.record.ShardKey().Database) == nil {
		return pm.confirmAdmission(ctx, pm.record.ShardKey().Database, subject)
	}
	// Cold follower first establishes current authority, then waits for exactly
	// that confirmed applicability/intent to appear in its local replay.
	var boundary *pb.AdmissionSnapshot
	if !initialized {
		var err error
		boundary, err = pm.authorityAdmission(ctx, subject)
		if err != nil {
			return nil, err
		}
	}
	wait, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for n := range 7 {
		s, err := admission.Read(wait, localAdmissionReader{pm.qsc.InternalQueryService()}, pm.record.ShardKey(), subject)
		if err == nil && (boundary == nil || proto.Equal(s, boundary)) && (expected == nil || proto.Equal(s.Intent, expected)) {
			return s, nil
		}
		if n == 6 {
			return nil, errors.New("admission replay has not reached expected intent")
		}
		timer := time.NewTimer((20 * time.Millisecond) << n)
		select {
		case <-wait.Done():
			timer.Stop()
			return nil, wait.Err()
		case <-timer.C:
		}
	}
	return nil, errors.New("admission replay unavailable")
}

func (pm *MultipoolerManager) RefreshAdmission(ctx context.Context, r *rpc.RefreshAdmissionRequest) (*rpc.RefreshAdmissionResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	if r.GetDatabase() != pm.record.ShardKey().Database {
		return nil, errors.New("admission database mismatch")
	}
	if err := admission.Validate(r.GetExpected()); err != nil {
		return nil, err
	}
	if r.Expected.Subject != pm.admissionSubject() {
		return nil, errors.New("admission subject mismatch")
	}
	return pm.enforceAdmission(ctx, r.Expected)
}

func (pm *MultipoolerManager) enforceAdmission(ctx context.Context, expected *pb.AdmissionIntent) (*rpc.RefreshAdmissionResponse, error) {
	a := &pm.admissionRuntime
	a.mu.Lock()
	if a.serial == nil {
		a.serial = make(chan struct{}, 1)
		a.serial <- struct{}{}
	}
	serial := a.serial
	a.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-serial:
	}
	defer func() { serial <- struct{}{} }()
	g, err := pm.admissionGate()
	if err != nil {
		return nil, err
	}
	generation := g.AdmissionGeneration()
	a.mu.Lock()
	initialized := a.initialized
	a.mu.Unlock()
	s, err := pm.readAdmission(ctx, expected, initialized)
	if err != nil {
		return nil, err
	}
	if s == nil || !proto.Equal(s.AuthorityShardKey, pm.record.ShardKey()) {
		return nil, errors.New("admission scope mismatch")
	}
	if s.Controlled && (s.Intent == nil || s.Owner != s.Intent.Owner) {
		g.SetApplicationAdmission(false)
		return nil, errors.New("controlled admission intent unavailable")
	}
	if s.Intent != nil {
		if err = admission.Validate(s.Intent); err != nil {
			g.SetApplicationAdmission(false)
			return nil, err
		}
	}
	if expected != nil && (!s.Controlled || !proto.Equal(expected, s.Intent)) {
		return nil, errors.New("admission intent mismatch; recover the same operation")
	}
	allow := !s.Controlled && !pm.IsUnmanaged() || s.Controlled && s.Intent.GetPermission() == pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN
	lockCtx, err := pm.actionLock.Acquire(ctx, "ApplyAdmission")
	if err != nil {
		return nil, err
	}
	if allow {
		h := pm.healthStreamer.getState()
		if pm.IsUnmanaged() {
			if !h.BackendReady || !proto.Equal(h.BackendIdentity, s.Intent.SourceIdentity) || s.Intent.SourceConnection != pm.config.SourceConfiguration.GetName() || s.Intent.SourceConfigurationBinding != pm.config.SourceConfigurationBinding {
				pm.actionLock.Release(lockCtx)
				return nil, errors.New("source not prepared for admission intent")
			}
		}
		role := pm.stateManager.RoutingRole()
		if role != pb.RoutingRole_ROUTING_ROLE_PRIMARY && role != pb.RoutingRole_ROUTING_ROLE_REPLICA {
			pm.actionLock.Release(lockCtx)
			return nil, errors.New("backend routing role unavailable")
		}
		if !g.ApplyAdmissionGeneration(generation, true) {
			pm.actionLock.Release(lockCtx)
			return nil, errors.New("admission lifecycle changed during validation")
		}
	} else {
		g.SetApplicationAdmission(false)
	}
	appliedGeneration := g.AdmissionGeneration()
	pm.actionLock.Release(lockCtx)
	// No catalog transaction or callback-required action lock during remote/drain waits.
	if !allow {
		if err = g.DrainApplication(ctx); err != nil {
			return nil, err
		}
	}
	lockCtx, err = pm.actionLock.Acquire(ctx, "AdmissionEnforced")
	if err != nil {
		return nil, err
	}
	if g.AdmissionGeneration() != appliedGeneration {
		pm.actionLock.Release(lockCtx)
		return nil, errors.New("admission lifecycle changed before acknowledgment")
	}
	a.mu.Lock()
	a.initialized = true
	a.mu.Unlock()
	pm.actionLock.Release(lockCtx)
	return &rpc.RefreshAdmissionResponse{Observed: s.Intent, ProcessId: proto.Clone(pm.record.desired.Load().Id).(*pb.ID), AdmissionOpen: allow}, nil
}
