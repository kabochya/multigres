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
// reading, from the default primary, the admission fence of its tablegroup and finding
// it unfenced. A tablegroup that has never been fenced has no fence, and the metadata
// tables may not exist; no fence, a missing table and an old default primary that
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
	operationID string
	state       multipoolerservicepb.AdmissionState
	version     int64
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
	reader func(ctx context.Context) (*multipoolerservicepb.AdmissionFence, error)
	// binding, when set, replaces the read of the connection the tablegroup is bound to
	// (tests).
	binding func(ctx context.Context) (name string, known bool, err error)
}

func (pm *MultipoolerManager) applicationGate() (poolerserver.ApplicationGate, error) {
	g, ok := pm.qsc.(poolerserver.ApplicationGate)
	if !ok {
		return nil, errors.New("application admission gate unavailable")
	}
	return g, nil
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

// admissionLifecycle withdraws the admission decision whenever the routing role or serving
// status of a pooler whose backend asks for it changes, including a backend reconnect, so that
// a replica promoted while a fence runs reads its fence before it opens. It is a pure sink:
// it closes the gate and wakes the admission loop, nothing more. A backend that does not ask
// for it (see admissionBackend) keeps its decision across a reconnect.
type admissionLifecycle struct{ pm *MultipoolerManager }

func (l admissionLifecycle) OnStateChange(_ context.Context, s servingstate.State) error {
	pm := l.pm
	if !pm.admissionBackend.ClosesOnRoleChange() {
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
		// Gateways are told through the health stream that the gate is closed, so that
		// they do not select the pooler, or release the requests they buffered for the
		// failover, into a closed gate.
		pm.healthStreamer.setAdmissionClosed(true)
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
		// A pooler must be visible in topology before it reads anything: the coordinator
		// fences the poolers it finds there, and one that opens from a read taken before
		// it is listed could stay open past a fence committed without it.
		if !pm.admissionBackend.RegisteredBeforeStart() && !pm.record.IsRegistered() {
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

// RefreshAdmission re-reads the admission fence of the pooler's tablegroup from the default
// primary, applies it, and says what the pooler holds. The coordinator calls it on every member of
// a tablegroup, for a start and for a commit alike, so a commit also closes a member that missed
// the start; expected_state and expected_version are expectations, never permission. It waits for
// nothing: a pooler told to fence closes its gate and answers at once with what is still running
// or held.
func (pm *MultipoolerManager) RefreshAdmission(ctx context.Context, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	key := pm.record.ShardKey()
	if req.GetDatabase() != key.GetDatabase() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "database does not match this pooler")
	}
	// A shard or a table is not a scope yet, whichever pooler is asked.
	operationID, tablegroup, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if tablegroup != key.GetTableGroup() {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup does not match this pooler")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	applied, err := pm.enforceAdmission(ctx, &admissionExpectation{operationID: operationID, state: req.GetExpectedState(), version: req.GetExpectedVersion()})
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

// enforceAdmission reads the authoritative fence and makes the gate match it. expected
// is what the caller has just committed and is driving to, or nil for the startup and
// event reads; if the authoritative fence differs the call fails without applying
// anything. A fenced scope closes the gate, however often the caller asks, which is what
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

	if err := pm.checkTablegroupBinding(ctx); err != nil {
		return nil, err
	}
	row, err := pm.readOwnFence(ctx)
	if err != nil {
		return nil, err
	}
	if expected != nil && (row.GetState() != expected.state || row.GetVersion() != expected.version || row.GetOperationId() != expected.operationID) {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"persisted admission is %s at version %d of operation %q, not the expected %s at version %d of operation %q",
			row.GetState(), row.GetVersion(), row.GetOperationId(), expected.state, expected.version, expected.operationID)
	}

	// FENCING and FENCED both keep the gate closed; only the coordinator tells them apart.
	closed := row.GetState() != stateUnfenced
	var busy *poolerserver.FenceBusy
	if closed {
		pm.announceClosingGate()
		if expected != nil {
			// The close and the report in one: what a coordinator's refresh gets back is what
			// is still running or held with the gate closed.
			busy = g.FenceNow(ctx)
		} else {
			g.CloseApplication()
		}
	} else if err := pm.admissionBackend.ReadyToAdmit(); err != nil {
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
	// Reported to gateways under the same lock, so a lifecycle close that follows
	// cannot be overwritten by this open.
	pm.healthStreamer.setAdmissionClosed(closed)
	applied := &appliedAdmission{state: row.GetState(), version: row.GetVersion(), generation: generation, busy: busy}
	a.applied = applied
	return applied, nil
}

// announceClosingGate tells gateways, through the health stream, that the gate is
// closing.
func (pm *MultipoolerManager) announceClosingGate() {
	pm.healthStreamer.setAdmissionClosed(true)
}

// readOwnFence reads this pooler's tablegroup's fence from the authoritative copy: the default
// primary, or this pooler itself when it is the default primary. A follower's copy is never a
// source of truth. A tablegroup with no fence reads as unfenced at version zero.
func (pm *MultipoolerManager) readOwnFence(ctx context.Context) (*multipoolerservicepb.AdmissionFence, error) {
	if pm.admission.reader != nil {
		return pm.admission.reader(ctx)
	}
	key := pm.record.ShardKey()
	tablegroup := key.GetTableGroup()
	// With no store built in, nothing can have fenced this tablegroup, on this pooler or on the
	// default primary it would ask, which is the same build: it is unfenced, and there is nothing
	// to read.
	//
	// This is a rule of the build, and an upgrade constraint: every pooler of a cluster that can
	// fence must run a build that has the store. A member restarted onto one without it, after a
	// fence completed, opens without asking, as a pooler of a cluster that never fenced anything
	// does.
	if newRoutingStore == nil {
		return unfencedFence(tablegroup), nil
	}
	req := &multipoolerservicepb.GetRoutingStateRequest{Database: key.GetDatabase()}
	if pm.requireDefaultPrimary(key.GetDatabase()) == nil {
		resp, err := pm.GetRoutingState(ctx, req)
		if err != nil {
			return nil, err
		}
		return ownFenceOf(resp, tablegroup), nil
	}
	if pm.config == nil || pm.config.TopoClient == nil {
		return nil, errors.New("default primary discovery unavailable")
	}
	var fence *multipoolerservicepb.AdmissionFence
	err := pm.defaultPrimaryClient().with(ctx, func(c multipoolerservicepb.MultipoolerServiceClient) error {
		resp, err := c.GetRoutingState(ctx, req)
		if status.Code(err) == codes.Unimplemented {
			// A default primary that does not serve admission state has never fenced anything:
			// this tablegroup is unfenced. That holds only under the upgrade constraint above
			// (every pooler of a cluster that can fence runs a build that has the store): a
			// failover onto an older default primary after a fence reopens the tablegroup.
			fence = unfencedFence(tablegroup)
			return nil
		}
		if err != nil {
			return err
		}
		fence = ownFenceOf(resp, tablegroup)
		return nil
	})
	return fence, err
}

// ownFenceOf picks the fence of a tablegroup scope out of the routing state, and a tablegroup
// that has none is unfenced.
func ownFenceOf(resp *multipoolerservicepb.GetRoutingStateResponse, tablegroup string) *multipoolerservicepb.AdmissionFence {
	for _, f := range resp.GetFences() {
		if f.GetScope().GetTablegroup() == tablegroup && f.GetScope().GetShard() == "" && f.GetScope().GetTable() == "" {
			return f
		}
	}
	return unfencedFence(tablegroup)
}

// The admission states, as the manager names them.
const (
	stateUnfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	stateFencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	stateFenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
)

// unfencedFence is what a tablegroup that has no fence reads as: unfenced, at version zero, held
// by no operation.
func unfencedFence(tablegroup string) *multipoolerservicepb.AdmissionFence {
	return &multipoolerservicepb.AdmissionFence{Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: tablegroup}, State: stateUnfenced}
}

// GetRoutingState returns the authoritative routing and admission metadata. Only the default
// primary serves it. A database with no fences and no rules, or no metadata tables at all, reads
// as empty: reading never creates anything, and a cluster that has never migrated anything has
// nothing to read.
func (pm *MultipoolerManager) GetRoutingState(ctx context.Context, req *multipoolerservicepb.GetRoutingStateRequest) (*multipoolerservicepb.GetRoutingStateResponse, error) {
	if err := pm.requireDefaultPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	store, err := pm.routingStore()
	if err != nil {
		return nil, err
	}
	return readRoutingState(ctx, store, req)
}

// readRoutingState reads the routing state from store, in one snapshot. The fences are always
// returned: they change without a change of the routing_version.
func readRoutingState(ctx context.Context, store routingReader, req *multipoolerservicepb.GetRoutingStateRequest) (*multipoolerservicepb.GetRoutingStateResponse, error) {
	snap, err := store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.GetRoutingStateResponse{RoutingVersion: snap.routingVersion}
	if req.GetSinceVersion() > 0 && req.GetSinceVersion() == snap.routingVersion {
		resp.NotModified = true
	} else {
		resp.HomeTablegroup = snap.home
		for _, r := range snap.rules {
			resp.Rules = append(resp.Rules, r.proto())
		}
	}
	for i := range snap.fences {
		resp.Fences = append(resp.Fences, snap.fences[i].proto())
	}
	return resp, nil
}

func (pm *MultipoolerManager) newAdmissionCoordinator() (*admissionCoordinator, error) {
	transport, err := pm.config.DefaultPrimaryDialOption()
	if err != nil {
		return nil, mterrors.Wrap(err, "default primary transport")
	}
	store, err := pm.routingStore()
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
		if !p.unmanaged() {
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

// StartFence starts the fence of a scope. Only the default primary serves it. The default
// tablegroup can be fenced too, as the destination of a migration: its poolers then refuse
// application work, and the default primary, which is in the fan-out like any other member, keeps
// serving this control plane, since admission applies to application queries only.
func (pm *MultipoolerManager) StartFence(ctx context.Context, req *multipoolerservicepb.StartFenceRequest) (*multipoolerservicepb.StartFenceResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.StartFence(ctx, req)
}

// CommitFence completes the fence of a scope that was started. Only the default primary serves
// it.
func (pm *MultipoolerManager) CommitFence(ctx context.Context, req *multipoolerservicepb.CommitFenceRequest) (*multipoolerservicepb.CommitFenceResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.CommitFence(ctx, req)
}

// UnfenceScope unfences a scope. Only the default primary serves it.
func (pm *MultipoolerManager) UnfenceScope(ctx context.Context, req *multipoolerservicepb.UnfenceScopeRequest) (*multipoolerservicepb.UnfenceScopeResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.UnfenceScope(ctx, req)
}

// SwitchRouting moves application traffic between two fenced tablegroups. Only the default
// primary serves it.
func (pm *MultipoolerManager) SwitchRouting(ctx context.Context, req *multipoolerservicepb.SwitchRoutingRequest) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.SwitchRouting(ctx, req)
}

// RevertRouting puts application traffic back on the home tablegroup. Only the default primary
// serves it.
func (pm *MultipoolerManager) RevertRouting(ctx context.Context, req *multipoolerservicepb.RevertRoutingRequest) (*multipoolerservicepb.RevertRoutingResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.RevertRouting(ctx, req)
}

// FoldRouting completes a switched operation. Only the default primary serves it.
func (pm *MultipoolerManager) FoldRouting(ctx context.Context, req *multipoolerservicepb.FoldRoutingRequest) (*multipoolerservicepb.FoldRoutingResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.FoldRouting(ctx, req)
}

// CleanupOperation deletes the fences of an operation. Only the default primary serves it.
func (pm *MultipoolerManager) CleanupOperation(ctx context.Context, req *multipoolerservicepb.CleanupOperationRequest) (*multipoolerservicepb.CleanupOperationResponse, error) {
	c, err := pm.coordinatorFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	return c.CleanupOperation(ctx, req)
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
