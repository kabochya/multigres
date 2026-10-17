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
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// Local application admission.
//
// Every pooler starts with its application gate closed and opens it only after
// reading, from the default primary, the admission state of its tablegroup and finding
// it unfenced. A tablegroup that has never been fenced has no row, and the metadata
// tables may not exist; a missing row, a missing table and an old default primary that
// answers Unimplemented all read as unfenced. A fenced row, and any other failure to
// read, keep the gate closed. A pooler reads its row when it starts, when its role or
// serving status changes (a managed pooler; an unmanaged one has no promotion to guard
// against), and when the coordinator asks it to through RefreshAdmission. It does not
// read it periodically: a pooler that could not read keeps trying while it is closed, and
// one that has decided waits for the next event, which is why the migrator repeats an
// unfence until none of the members is unreached. A pooler never treats the
// coordinator's expectation as permission, only what it reads back. A pooler that is told
// to fence closes its gate and answers at once, and it says what is still running or held
// when it is asked to report: waiting is the migrator's, which asks again.

const admissionReadTimeout = 5 * time.Second

// admissionRetryInterval is how often a pooler that has not decided yet tries to read its
// row. A variable so tests can shorten it.
var admissionRetryInterval = time.Second

// appliedAdmission is the decision the pooler currently enforces.
type appliedAdmission struct {
	// state and version are the row the decision was made from.
	state   multipoolerservicepb.AdmissionState
	version int64
	// generation is the gate generation the decision is valid for. A close since
	// then invalidates it.
	generation uint64
	// busy is what the gate still ran or held when a fence closed it for a refresh, nil when
	// nothing (and for every decision that is not a fence the coordinator asked for).
	busy *poolerserver.FenceBusy
}

// admissionExpectation is what a refresh says the row now is.
type admissionExpectation struct {
	state   multipoolerservicepb.AdmissionState
	version int64
}

// admissionRuntime is the manager's admission state. serial serializes
// read/apply across competing refreshes and the startup loop, independent
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
	// stopped is set when the pooler announces shutdown: the loop ends and no
	// decision may open the gate again.
	stopped atomic.Bool
	// reader, when set, replaces the read from the default primary (tests).
	reader func(ctx context.Context) (*multipoolerservicepb.TablegroupServingState, error)
}

func (pm *MultipoolerManager) applicationGate() (poolerserver.ApplicationGate, error) {
	g, ok := pm.qsc.(poolerserver.ApplicationGate)
	if !ok {
		return nil, errors.New("application admission gate unavailable")
	}
	return g, nil
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

// admissionLifecycle withdraws the admission decision whenever a managed pooler's
// routing role or serving status changes, including a backend reconnect, so that a
// replica promoted while a fence runs reads its row before it opens. It is a pure sink:
// it closes the gate and wakes the admission loop, nothing more. An unmanaged pooler's
// source is writable whatever role it reports and there is no promotion to guard
// against, so its decision stands across a reconnect: the gate stays as the row left it,
// and requests are refused by the serving status while the backend is away.
type admissionLifecycle struct{ pm *MultipoolerManager }

func (l admissionLifecycle) OnStateChange(_ context.Context, s servingstate.State) error {
	pm := l.pm
	if pm.IsUnmanaged() {
		return nil
	}
	a := &pm.admission
	// The gate is closed while holding a.mu so that it is ordered against
	// enforceAdmission's open-and-record step: a close either lands before that
	// step (and the open is refused by the generation check) or after it (and
	// withdraws the decision it recorded).
	a.mu.Lock()
	changed := a.lastRole != s.Routing.Role || a.lastStatus != s.ServingStatus
	if changed {
		a.lastRole, a.lastStatus = s.Routing.Role, s.ServingStatus
		a.applied = nil
		if g, err := pm.applicationGate(); err == nil {
			g.CloseApplication()
		}
	}
	a.mu.Unlock()
	if changed {
		pm.signalAdmission()
	}
	return nil
}

// StartAdmission starts the admission loop. It must run after Start. The gate itself
// was closed at construction, so nothing is admitted in between.
func (pm *MultipoolerManager) StartAdmission() {
	pm.signalAdmission()
	_ = pm.stateManager.RegisterAndSync(pm.shutdownCtx, admissionLifecycle{pm})
	go pm.runAdmission(pm.shutdownCtx)
}

// runAdmission opens a pooler once it has read its row, and decides again after every
// invalidation. It tries again every second while the pooler has not decided; a pooler
// that has decided does not read until something changes.
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
		if a.stopped.Load() {
			return
		}
		// A managed pooler must be visible in topology before it reads anything:
		// the coordinator fences the poolers it finds there, and one that opens
		// from a read taken before it is listed could stay open past a fence
		// committed without it. Unmanaged poolers register before they start.
		if !pm.IsUnmanaged() && !pm.record.IsRegistered() {
			continue
		}
		g, err := pm.applicationGate()
		if err != nil {
			continue
		}
		a.mu.Lock()
		decided := a.applied != nil && a.applied.generation == g.AdmissionGeneration()
		a.mu.Unlock()
		if decided || pm.healthStreamer.getState().ServingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
			continue
		}
		attemptCtx, cancel := context.WithTimeout(ctx, admissionReadTimeout)
		_, err = pm.enforceAdmission(attemptCtx, nil)
		cancel()
		if err != nil {
			pm.logger.DebugContext(ctx, "admission not decided yet; staying closed", "error", err)
		}
	}
}

// RefreshAdmission re-reads the admission row of the pooler's tablegroup from the default
// primary, applies it, and says what the pooler holds. The coordinator calls it on every member of
// a tablegroup, for a start and for a commit alike, so a commit also closes a member that missed
// the start; expected_state and expected_version are expectations, never permission. It waits for
// nothing: a pooler told to fence closes its gate and answers at once with what is still running
// or held.
func (pm *MultipoolerManager) RefreshAdmission(ctx context.Context, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	key := pm.record.ShardKey()
	if req.GetDatabase() != key.GetDatabase() || req.GetTablegroup() != key.GetTableGroup() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "database or tablegroup does not match this pooler")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	applied, err := pm.enforceAdmission(ctx, &admissionExpectation{state: req.GetExpectedState(), version: req.GetExpectedVersion()})
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.RefreshAdmissionResponse{
		PoolerId: pm.record.Snapshot().GetId(),
		State:    applied.state,
		Version:  applied.version,
	}
	if applied.busy != nil {
		resp.Busy = applied.busy.Details()
	}
	return resp, nil
}

// enforceAdmission reads the authoritative row and makes the gate match it. expected
// is what the caller has just committed and is driving to, or nil for the startup and
// event reads; if the authoritative row differs the call fails without applying
// anything. A fenced row closes the gate, however often the caller asks, which is what
// makes a repeated fence a resume; closing is the safe direction, and nothing here waits
// for work to finish.
func (pm *MultipoolerManager) enforceAdmission(ctx context.Context, expected *admissionExpectation) (*appliedAdmission, error) {
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

	// An unfence of what is already enforced: acknowledge without another read.
	if expected != nil && expected.state == stateUnfenced {
		a.mu.Lock()
		cur := a.applied
		a.mu.Unlock()
		if cur != nil && cur.generation == generation && cur.state == stateUnfenced && cur.version == expected.version {
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
	if expected != nil && (row.GetState() != expected.state || row.GetVersion() != expected.version) {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"persisted admission is %s at version %d, not the expected %s at version %d",
			row.GetState(), row.GetVersion(), expected.state, expected.version)
	}

	// FENCING and FENCED both keep the gate closed; only the coordinator tells them apart.
	closed := row.GetState() != stateUnfenced
	var busy *poolerserver.FenceBusy
	if closed {
		if expected != nil {
			// The close and the report in one: what a coordinator's refresh gets back is what
			// is still running or held with the gate closed.
			busy = g.FenceNow(ctx)
		} else {
			g.CloseApplication()
		}
	} else if err := pm.admissionBackendReady(); err != nil && !pm.mayOpenLater() {
		return nil, err
	}

	// Open (or record the fence) and store the decision in one step against
	// the lifecycle sink, which closes the gate under the same lock. A replica whose
	// backend is not ready opens too: nothing is admitted while it is not serving, and the
	// change of its serving status closes the gate and reads the row again.
	a.mu.Lock()
	defer a.mu.Unlock()
	if closed {
		// The gate is closed, and stays closed; whatever closed it again since
		// the close only reinforces the decision.
		generation = g.AdmissionGeneration()
	} else if !g.OpenApplication(generation) {
		return nil, mterrors.New(mtrpcpb.Code_ABORTED, "pooler lifecycle changed while reading admission; retry")
	}
	applied := &appliedAdmission{state: row.GetState(), version: row.GetVersion(), generation: generation, busy: busy}
	a.applied = applied
	return applied, nil
}

// mayOpenLater reports whether the pooler may open before its backend is ready: a managed
// pooler that is not the writable leader, which cannot commit and is refused by the
// serving status until its backend is. A leader, and an unmanaged pooler, whose external
// database is the writable one, must be ready.
func (pm *MultipoolerManager) mayOpenLater() bool {
	return !pm.IsUnmanaged() && pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
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
	// With no store of serving state built in, nothing can have fenced this tablegroup, on
	// this pooler or on the default primary it would ask, which is the same build: it is
	// unfenced, and there is nothing to read.
	//
	// This is a rule of the build, and an upgrade constraint: every pooler of a cluster that
	// can fence must run a build that has the store. A member restarted onto one without it,
	// after a fence completed, opens without asking, as a pooler of a cluster that never
	// fenced anything does.
	if newServingStore == nil {
		return pm.unfencedRowOfThisPooler(), nil
	}
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
	var row *multipoolerservicepb.TablegroupServingState
	err := pm.defaultPrimaryClient().with(ctx, func(c multipoolerservicepb.MultipoolerServiceClient) error {
		resp, err := c.GetServingState(ctx, req)
		if status.Code(err) == codes.Unimplemented {
			// A default primary that does not serve admission state has never fenced
			// anything: this tablegroup is unfenced. That holds only under the upgrade
			// constraint above (every pooler of a cluster that can fence runs a build that
			// has the store): a failover onto an older default primary after a fence reopens
			// the tablegroup.
			row = pm.unfencedRowOfThisPooler()
			return nil
		}
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

// The admission states, as the manager names them.
const (
	stateUnfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	stateFencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	stateFenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
)

// unfencedRowOfThisPooler is the unfenced row that stands for a tablegroup with no persisted
// state because there is no store to ask (no store built in, or a default primary that answers
// Unimplemented): there is no connection to disagree with, so the pooler's own is the one it is
// "backed by". This is not what a store with no row for the tablegroup says: that row names no
// connection, and an unmanaged pooler needs a row that names its own before it opens, so that
// where a store exists the connection a tablegroup is backed by is always a recorded fact.
func (pm *MultipoolerManager) unfencedRowOfThisPooler() *multipoolerservicepb.TablegroupServingState {
	row := unfencedRow(pm.record.ShardKey().GetTableGroup())
	row.BackingConnection = pm.backingConnectionName()
	return row
}

// unfencedRow is what a tablegroup that has no persisted admission state reads as:
// unfenced, at version zero.
func unfencedRow(tablegroup string) *multipoolerservicepb.TablegroupServingState {
	return &multipoolerservicepb.TablegroupServingState{Tablegroup: tablegroup, State: stateUnfenced}
}

// GetServingState returns the authoritative routing and admission metadata. Only
// the default primary serves it. A tablegroup with no row, or no metadata tables at
// all, is unfenced at version zero: reading never creates anything, and a cluster
// that has never migrated anything has nothing to read.
//
// The tablegroup rows and the routing pointer are read by separate statements, so
// the response is not a single snapshot. That is harmless: a caller treats it as
// advice and every decision it leads to is re-checked where it is made
// (MoveRouting re-checks the admission states in the statement that moves the
// pointer; a pooler's admission read is of its own row only).
func (pm *MultipoolerManager) GetServingState(ctx context.Context, req *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error) {
	if err := pm.requireDefaultPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	store, err := pm.servingStore()
	if err != nil {
		return nil, err
	}
	return readServingState(ctx, store, req)
}

// readServingState reads the serving metadata from store. A tablegroup that has no row
// reads as unfenced at version zero, and so does everything when the tables do not exist
// yet: the store reports both as no row, and reading creates nothing.
func readServingState(ctx context.Context, store servingReader, req *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error) {
	resp := &multipoolerservicepb.GetServingStateResponse{}
	for _, name := range req.GetTablegroups() {
		row, err := store.GetRow(ctx, req.GetDatabase(), name)
		if err != nil {
			return nil, err
		}
		if row == nil {
			resp.Tablegroups = append(resp.Tablegroups, unfencedRow(name))
			continue
		}
		resp.Tablegroups = append(resp.Tablegroups, row.proto(name))
	}
	app, version, _, err := store.GetRouting(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	resp.AppTablegroup, resp.RoutingVersion = app, version
	return resp, nil
}

func (pm *MultipoolerManager) newAdmissionCoordinator() (*admissionCoordinator, error) {
	transport, err := pm.config.DefaultPrimaryDialOption()
	if err != nil {
		return nil, mterrors.Wrap(err, "default primary transport")
	}
	store, err := pm.servingStore()
	if err != nil {
		return nil, err
	}
	database := pm.record.ShardKey().GetDatabase()
	members := topologyMembership{ts: pm.config.TopoClient}
	return &admissionCoordinator{
		store:     store,
		members:   members,
		refresher: grpcRefresher{transport: transport},
		logger:    pm.logger,
		isLeader:  func() error { return pm.requireDefaultPrimary(database) },
		requiredNodes: func(ctx context.Context, db, tg string) (int, bool, error) {
			return pm.requiredNodes(ctx, members, db, tg)
		},
	}, nil
}

// requiredNodes returns how many nodes the durability policy of the tablegroup needs to
// acknowledge a write, and whether the tablegroup is managed. The default tablegroup's
// is the committed rule of this pooler, its leader. Another tablegroup's cohort is not
// local, and the policy the database was bootstrapped with stands in for it.
//
// Known limit: a managed tablegroup other than the default one is not asked for its
// current rule.
func (pm *MultipoolerManager) requiredNodes(ctx context.Context, members membership, db, tg string) (int, bool, error) {
	if tg == pm.record.ShardKey().GetTableGroup() {
		var policy *clustermetadatapb.DurabilityPolicy
		if pm.consensusMgr != nil {
			policy = pm.consensusMgr.CachedConsensusStatus().GetCurrentPosition().GetPosition().GetDecision().GetDurabilityPolicy()
		}
		return int(policy.GetRequiredCount()), true, nil
	}
	list, err := members.Snapshot(ctx, db, tg)
	if err != nil {
		return 0, false, err
	}
	managed := false
	for _, p := range list {
		if p.record.GetManagementMode() != clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
			managed = true
		}
	}
	if !managed {
		return 0, false, nil
	}
	policy, err := pm.loadDurabilityPolicy(ctx)
	if err != nil {
		return 0, true, err
	}
	return int(policy.GetRequiredCount()), true, nil
}

// StartFence starts the fence of a tablegroup. Only the default primary serves it. The
// default tablegroup can be fenced too, as the destination of a migration: its
// poolers then refuse application work, and the default primary, which is in the
// fan-out like any other member, keeps serving this control plane, since admission
// applies to application queries only.
func (pm *MultipoolerManager) StartFence(ctx context.Context, req *multipoolerservicepb.StartFenceRequest) (*multipoolerservicepb.StartFenceResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.StartFence(ctx, req)
}

// CommitFence completes the fence of a tablegroup that was started. Only the default
// primary serves it.
func (pm *MultipoolerManager) CommitFence(ctx context.Context, req *multipoolerservicepb.CommitFenceRequest) (*multipoolerservicepb.CommitFenceResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.CommitFence(ctx, req)
}

// UnfenceTablegroup unfences a tablegroup. Only the default primary serves it.
func (pm *MultipoolerManager) UnfenceTablegroup(ctx context.Context, req *multipoolerservicepb.UnfenceTablegroupRequest) (*multipoolerservicepb.UnfenceTablegroupResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.UnfenceTablegroup(ctx, req)
}

// FlipRouting moves application traffic between two fenced tablegroups. Only the
// default primary serves it.
func (pm *MultipoolerManager) FlipRouting(ctx context.Context, req *multipoolerservicepb.FlipRoutingRequest) (*multipoolerservicepb.FlipRoutingResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.FlipRouting(ctx, req)
}

// coordinatorFor returns the admission coordinator, after checking that this pooler is
// the default primary of the database.
func (pm *MultipoolerManager) coordinatorFor(ctx context.Context, database string) (*admissionCoordinator, error) {
	if err := pm.requireDefaultPrimary(database); err != nil {
		return nil, err
	}
	if pm.config == nil || pm.config.TopoClient == nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pooler discovery unavailable")
	}
	return pm.newAdmissionCoordinator()
}
