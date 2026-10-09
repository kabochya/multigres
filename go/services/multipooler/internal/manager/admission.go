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
	"fmt"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// Local application admission.
//
// A pooler whose admission is controlled starts with its application gate
// closed and opens it only after reading an UNFENCED admission state from the
// default primary. FENCING, FENCED, a missing row and an unreadable row all keep
// it closed. A role change, serving change or backend reconnect closes the gate
// again until the state has been re-read. The coordinator on the default primary
// asks poolers to refresh through RefreshAdmission; the pooler never treats the
// coordinator's expectation as permission, only what it reads back.

const (
	defaultAdmissionDrainTimeout = 30 * time.Second
	admissionReadTimeout         = 5 * time.Second
	admissionRetryInterval       = time.Second
)

// appliedAdmission is the decision the pooler currently enforces.
type appliedAdmission struct {
	// state is FENCED or UNFENCED: the settled state the gate is in.
	state     multipoolerservicepb.AdmissionState
	requestID string
	// generation is the gate generation the decision is valid for. A close since
	// then invalidates it.
	generation uint64
}

// admissionRuntime is the manager's admission state. serial serializes
// read/apply/drain across competing refreshes and the startup loop, independent
// of the action lock: lifecycle callbacks only withdraw authority and never wait
// for it.
type admissionRuntime struct {
	mu      sync.Mutex
	serial  chan struct{}
	events  chan struct{}
	applied *appliedAdmission
	// lastRole and lastStatus are the serving state the decision was made for.
	lastRole   servingstate.RoutingRole
	lastStatus clustermetadatapb.PoolerServingStatus
	// reader, when set, replaces the read from the default primary (tests).
	reader func(ctx context.Context) (*multipoolerservicepb.TablegroupServingState, error)
}

// admissionControlled reports whether this pooler's application admission
// follows the persisted admission state.
func (pm *MultipoolerManager) admissionControlled() bool {
	return pm.IsUnmanaged() || (pm.config != nil && pm.config.AdmissionControl)
}

func (pm *MultipoolerManager) applicationGate() (poolerserver.ApplicationGate, error) {
	g, ok := pm.qsc.(poolerserver.ApplicationGate)
	if !ok {
		return nil, errors.New("application admission gate unavailable")
	}
	return g, nil
}

func (pm *MultipoolerManager) admissionDrainTimeout() time.Duration {
	if pm.config != nil && pm.config.AdmissionDrainTimeout > 0 {
		return pm.config.AdmissionDrainTimeout
	}
	return defaultAdmissionDrainTimeout
}

func (pm *MultipoolerManager) backingConnectionName() string {
	if pm.config == nil {
		return ""
	}
	return pm.config.BackingConnectionName
}

func (pm *MultipoolerManager) signalAdmission() {
	a := &pm.admission
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

// admissionLifecycle withdraws the admission decision whenever the pooler's
// routing role or serving status changes, including a backend reconnect. It is a
// pure sink: it closes the gate and wakes the admission loop, nothing more.
type admissionLifecycle struct{ pm *MultipoolerManager }

func (l admissionLifecycle) OnStateChange(_ context.Context, s servingstate.State) error {
	pm := l.pm
	a := &pm.admission
	a.mu.Lock()
	changed := a.lastRole != s.Routing.Role || a.lastStatus != s.ServingStatus
	if changed {
		a.lastRole, a.lastStatus = s.Routing.Role, s.ServingStatus
		a.applied = nil
	}
	a.mu.Unlock()
	if changed {
		if g, err := pm.applicationGate(); err == nil {
			g.CloseApplication()
		}
		pm.signalAdmission()
	}
	return nil
}

// StartAdmission starts the admission loop of a controlled pooler. It must run
// after Start. The gate itself was closed at construction, so nothing is
// admitted in between.
func (pm *MultipoolerManager) StartAdmission() {
	if !pm.admissionControlled() {
		return
	}
	pm.signalAdmission()
	_ = pm.stateManager.RegisterAndSync(pm.shutdownCtx, admissionLifecycle{pm})
	go pm.runAdmission(pm.shutdownCtx)
}

// runAdmission opens a controlled pooler once it has read its admission state.
// It retries until the decision is made, and again after every invalidation.
func (pm *MultipoolerManager) runAdmission(ctx context.Context) {
	ticker := time.NewTicker(admissionRetryInterval)
	defer ticker.Stop()
	a := &pm.admission
	a.mu.Lock()
	if a.events == nil {
		a.events = make(chan struct{}, 1)
	}
	events := a.events
	a.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-events:
		case <-ticker.C:
		}
		a.mu.Lock()
		decided := a.applied != nil
		a.mu.Unlock()
		if decided || pm.healthStreamer.getState().ServingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
			continue
		}
		attemptCtx, cancel := context.WithTimeout(ctx, admissionReadTimeout)
		_, err := pm.enforceAdmission(attemptCtx, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, "")
		cancel()
		if err != nil {
			pm.logger.DebugContext(ctx, "admission not decided yet; staying closed", "error", err)
		}
	}
}

// RefreshAdmission re-reads the admission state of the pooler's tablegroup from
// the default primary and applies it. The coordinator calls it on every pooler of
// a tablegroup; expected_state and request_id are expectations, never permission.
func (pm *MultipoolerManager) RefreshAdmission(ctx context.Context, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	if !pm.admissionControlled() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "admission control is not enabled on this pooler")
	}
	key := pm.record.ShardKey()
	if req.GetDatabase() != key.GetDatabase() || req.GetTablegroup() != key.GetTableGroup() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "database or tablegroup does not match this pooler")
	}
	switch req.GetExpectedState() {
	case multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING,
		multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED,
		multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING,
		multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED:
	default:
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_state must be FENCING, FENCED, UNFENCING or UNFENCED")
	}
	if req.GetRequestId() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "request_id is required")
	}
	applied, err := pm.enforceAdmission(ctx, req.GetExpectedState(), req.GetRequestId())
	if err != nil {
		return nil, err
	}
	rec := pm.record.Snapshot()
	return &multipoolerservicepb.RefreshAdmissionResponse{
		PoolerId:           rec.GetId(),
		ProcessIncarnation: rec.GetProcessIncarnation(),
		AppliedState:       applied.state,
		RequestId:          applied.requestID,
	}, nil
}

// settledState maps a persisted state to the state the gate settles in.
func settledState(s multipoolerservicepb.AdmissionState) multipoolerservicepb.AdmissionState {
	switch s {
	case multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED:
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	default:
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	}
}

// enforceAdmission reads the authoritative admission state and makes the gate
// match it. expected is the state the caller is driving to, or UNSPECIFIED for
// the startup read; if the authoritative state differs the call fails without
// applying anything.
func (pm *MultipoolerManager) enforceAdmission(ctx context.Context, expected multipoolerservicepb.AdmissionState, expectedRequestID string) (*appliedAdmission, error) {
	g, err := pm.applicationGate()
	if err != nil {
		return nil, err
	}
	a := &pm.admission
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

	generation := g.AdmissionGeneration()

	// Already enforcing exactly this operation: acknowledge without another read.
	if expected != multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED {
		a.mu.Lock()
		cur := a.applied
		a.mu.Unlock()
		if cur != nil && cur.generation == generation && cur.requestID == expectedRequestID && cur.state == settledState(expected) {
			return cur, nil
		}
	}

	row, err := pm.readTablegroupServing(ctx)
	if err != nil {
		return nil, err
	}
	if row.GetBackingConnection() != pm.backingConnectionName() {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup is backed by connection %q, this pooler by %q", row.GetBackingConnection(), pm.backingConnectionName())
	}
	if expected != multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED &&
		(row.GetAdmissionState() != expected || row.GetRequestId() != expectedRequestID) {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"persisted admission is %s for request %q, not the expected %s for %q; recover the same operation",
			row.GetAdmissionState(), row.GetRequestId(), expected, expectedRequestID)
	}

	settled := settledState(row.GetAdmissionState())
	if row.GetAdmissionState() == multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED {
		settled = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	}

	if settled == multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED {
		// Close, drain, and only then acknowledge. A failed drain leaves the gate
		// closed and the call failing, never a false acknowledgment.
		if _, err := g.FenceApplication(ctx, pm.admissionDrainTimeout()); err != nil {
			return nil, err
		}
		generation = g.AdmissionGeneration()
	} else {
		if err := pm.admissionBackendReady(); err != nil {
			return nil, err
		}
		if !g.OpenApplication(generation) {
			return nil, mterrors.New(mtrpcpb.Code_ABORTED, "pooler lifecycle changed while reading admission; retry")
		}
	}

	applied := &appliedAdmission{state: settled, requestID: row.GetRequestId(), generation: generation}
	a.mu.Lock()
	a.applied = applied
	a.mu.Unlock()
	return applied, nil
}

// admissionBackendReady checks the pooler may admit application work at all: its
// backend is usable, which for an unmanaged pooler means it passed identity
// validation.
func (pm *MultipoolerManager) admissionBackendReady() error {
	h := pm.healthStreamer.getState()
	if h.ServingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "backend is not serving")
	}
	if pm.IsUnmanaged() {
		if !h.BackendReady {
			return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "external backend is not ready")
		}
		return nil
	}
	switch pm.stateManager.RoutingRole() {
	case clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA:
		return nil
	default:
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "backend routing role is unavailable")
	}
}

// readTablegroupServing reads this pooler's tablegroup row from the
// authoritative copy: the default primary, or this pooler itself when it is the
// default primary. A follower's copy is never a source of truth.
func (pm *MultipoolerManager) readTablegroupServing(ctx context.Context) (*multipoolerservicepb.TablegroupServingState, error) {
	if pm.admission.reader != nil {
		return pm.admission.reader(ctx)
	}
	key := pm.record.ShardKey()
	req := &multipoolerservicepb.GetServingStateRequest{Database: key.GetDatabase(), Tablegroups: []string{key.GetTableGroup()}}
	if pm.requireDefaultPrimary(key.GetDatabase()) == nil {
		resp, err := pm.GetServingState(ctx, req)
		if err != nil {
			return nil, err
		}
		return resp.GetTablegroups()[0], nil
	}
	if pm.config == nil || pm.config.TopoClient == nil {
		return nil, errors.New("default primary discovery unavailable")
	}
	transport := pm.config.DefaultPrimaryTransport
	if transport == nil {
		transport = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	var row *multipoolerservicepb.TablegroupServingState
	err := metadataclient.WithDefaultPrimary(ctx, pm.config.TopoClient, key.GetDatabase(), transport, func(c multipoolerservicepb.MultipoolerServiceClient) error {
		resp, err := c.GetServingState(ctx, req)
		if err != nil {
			return err
		}
		if len(resp.GetTablegroups()) != 1 {
			return errors.New("default primary returned an unexpected number of tablegroups")
		}
		row = resp.GetTablegroups()[0]
		return nil
	})
	return row, err
}

// GetServingState returns the authoritative routing and admission metadata. Only
// the default primary serves it.
//
// PROTOTYPE STUB: reads the prototype metadata tables.
func (pm *MultipoolerManager) GetServingState(ctx context.Context, req *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error) {
	if err := pm.requireDefaultPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	if err := pm.ensureMetadataSchema(ctx); err != nil {
		return nil, mterrors.Wrap(err, "metadata unavailable")
	}
	resp := &multipoolerservicepb.GetServingStateResponse{}
	for _, name := range req.GetTablegroups() {
		result, err := pm.adminQueryArgs(ctx,
			"SELECT COALESCE(connection, ''), admission_state, request_id, extract(epoch FROM updated_at)::text FROM multigres.proto_tablegroup_serving WHERE database = $1 AND tablegroup = $2",
			req.GetDatabase(), name)
		if err != nil {
			return nil, mterrors.Wrap(err, "read tablegroup serving state")
		}
		if len(result.Rows) == 0 {
			return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "tablegroup %q has no serving row", name)
		}
		var connection, state, requestID, epoch string
		if err := executor.ScanSingleRow(result, &connection, &state, &requestID, &epoch); err != nil {
			return nil, errors.Join(errors.New("decode tablegroup serving state"), err)
		}
		admissionState, err := parseAdmissionState(state)
		if err != nil {
			return nil, err
		}
		row := &multipoolerservicepb.TablegroupServingState{
			Tablegroup:        name,
			BackingConnection: connection,
			AdmissionState:    admissionState,
			RequestId:         requestID,
		}
		if secs, err := strconv.ParseFloat(epoch, 64); err == nil {
			row.UpdatedAt = timestamppb.New(time.Unix(0, int64(secs*float64(time.Second))))
		}
		resp.Tablegroups = append(resp.Tablegroups, row)
	}

	result, err := pm.adminQueryArgs(ctx, "SELECT app_tablegroup, version FROM multigres.proto_routing WHERE database = $1", req.GetDatabase())
	if err != nil {
		return nil, mterrors.Wrap(err, "read routing")
	}
	if len(result.Rows) > 0 {
		var app string
		var version int64
		if err := executor.ScanSingleRow(result, &app, &version); err != nil {
			return nil, errors.Join(errors.New("decode routing"), err)
		}
		resp.AppTablegroup, resp.RoutingVersion = app, version
	}
	return resp, nil
}

func parseAdmissionState(s string) (multipoolerservicepb.AdmissionState, error) {
	switch s {
	case "UNFENCED":
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, nil
	case "FENCING":
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, nil
	case "FENCED":
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, nil
	case "UNFENCING":
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING, nil
	default:
		return multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNSPECIFIED, fmt.Errorf("unknown admission state %q", s)
	}
}
