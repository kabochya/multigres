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
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// The admission coordinator runs on the default primary. It owns the decisions
// "fence this tablegroup", "unfence it" and "move application traffic", commits
// them durably, and drives every pooler of the tablegroup to enforce them.
//
// The persisted state is the source of truth: the coordinator commits
// FENCING/UNFENCING first, asks poolers to refresh, and commits the terminal
// state only once every required pooler has acknowledged. Poolers re-read the
// persisted state from the default primary, so a stale or restarted coordinator
// cannot make a pooler apply anything.
//
// A fence that cannot complete is aborted, not left in FENCING: a cutover can be
// deferred, a failover cannot, so a tablegroup must not stay closed because a
// fence found work still running (a member reports busy), a member could not be
// reached, or the time ran out. The coordinator moves FENCING to UNFENCING under
// a request id derived from the fence's, which also records the fence's id in the
// history so that a delayed retry of it cannot fence again, asks every member to
// reopen, and commits UNFENCED. Reopening is the safe direction, and it is safe
// at this point because routing has not changed. Once FENCED is committed nothing
// reopens automatically. An unfence that could not reach every member stays
// UNFENCING, and the caller finishes it by retrying with the abort's request id.

const (
	defaultAdmissionTimeout = 60 * time.Second
	// fanOutAttempts bounds the retries per pooler within one call.
	fanOutAttempts = 3
	// maxMembershipRounds bounds how often the required set may grow (poolers
	// joining mid-operation) before the call gives up and asks to be retried.
	maxMembershipRounds = 4
)

// servingRow is one tablegroup's persisted serving row.
type servingRow struct {
	connection string
	state      multipoolerservicepb.AdmissionState
	requestID  string
	updatedAt  time.Time
	// history holds the most recent request ids this row has moved on from,
	// oldest first, so a delayed retry of an operation that has long since been
	// superseded is recognized rather than applied again.
	history []string
}

func (r *servingRow) proto(tablegroup string) *multipoolerservicepb.TablegroupServingState {
	return &multipoolerservicepb.TablegroupServingState{
		Tablegroup:        tablegroup,
		BackingConnection: r.connection,
		AdmissionState:    r.state,
		RequestId:         r.requestID,
		UpdatedAt:         timestampOrNil(r.updatedAt),
	}
}

// servingStore is the durable store of serving state. Every mutation is a
// conditional update on the expected prior state, so concurrent or stale writers
// cannot overwrite each other.
type servingStore interface {
	// GetRow returns the row, or a NOT_FOUND error.
	GetRow(ctx context.Context, database, tablegroup string) (*servingRow, error)
	// CompareAndSet moves a row from (prevState, prevRequestID) to (nextState,
	// nextRequestID) and reports whether it did. An error means the outcome is
	// uncertain; the caller must re-read.
	CompareAndSet(ctx context.Context, database, tablegroup string, prevState multipoolerservicepb.AdmissionState, prevRequestID string, nextState multipoolerservicepb.AdmissionState, nextRequestID string) (bool, error)
	// GetRouting returns the routing pointer; found is false when none exists.
	GetRouting(ctx context.Context, database string) (appTablegroup string, version int64, found bool, err error)
	// MoveRouting atomically moves the pointer from one tablegroup to another and
	// bumps its version, only if the pointer is at from and both tablegroups are
	// FENCED. It reports whether it did.
	MoveRouting(ctx context.Context, database, from, to string) (appTablegroup string, version int64, moved bool, err error)
}

// poolerRef identifies one pooler in a membership snapshot.
type poolerRef struct {
	id     *clustermetadatapb.ID
	record *clustermetadatapb.Multipooler
}

func (p poolerRef) key() string {
	return string(topoclient.ComponentIDString(p.id))
}

func (p poolerRef) name() string { return p.id.GetName() }

// membership enumerates the poolers that must acknowledge an admission change.
type membership interface {
	// Snapshot lists every pooler of the tablegroup in every cell, managed and
	// unmanaged, excluding poolers that have announced shutdown. A partial listing
	// is an error.
	Snapshot(ctx context.Context, database, tablegroup string) ([]poolerRef, error)
}

// poolerRefresher asks one pooler to re-read and apply its admission.
type poolerRefresher interface {
	Refresh(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

type admissionCoordinator struct {
	store     servingStore
	members   membership
	refresher poolerRefresher
	logger    *slog.Logger
	// isLeader is checked before every durable write. It is a LOCAL check of the
	// pooler's own routing role, not of the consensus term, so it narrows the
	// window in which a deposed default primary could write but does not close it:
	// between the check and the write the role can change. What keeps a stale
	// coordinator from corrupting state is the conditional update of every write,
	// and what keeps its decisions from taking effect is that poolers re-read the
	// persisted state themselves. A term-based fence is future work.
	isLeader func() error
	// retryBackoff is the pause between attempts against one pooler.
	retryBackoff time.Duration
}

func (c *admissionCoordinator) backoff() time.Duration {
	if c.retryBackoff > 0 {
		return c.retryBackoff
	}
	return 200 * time.Millisecond
}

// planAdmission decides how a request relates to the persisted row. It is pure.
type admissionPlan int

const (
	// planDone: the operation already completed under this request id.
	planDone admissionPlan = iota
	// planResume: the operation is in progress under this request id.
	planResume
	// planStart: begin a new operation: persist the transitional state first.
	planStart
)

func planAdmission(row *servingRow, req *multipoolerservicepb.UpdatePoolerAdmissionRequest) (admissionPlan, error) {
	fenced := multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	fencing := multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	unfenced := multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	unfencing := multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING

	target := req.GetTarget()
	transitional, terminal := fencing, fenced
	if target == unfenced {
		transitional, terminal = unfencing, unfenced
	}

	// A retry of a fence that was aborted: it must not fence again.
	if target == fenced && row.requestID == abortRequestID(req.GetRequestId()) {
		return 0, mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"fence %q was aborted and the tablegroup reopened (as %q); fence again with a new request_id", req.GetRequestId(), row.requestID)
	}

	// A retry of an operation that was superseded: applying it again would undo
	// whatever came after it.
	if row.requestID != req.GetRequestId() && slices.Contains(row.history, req.GetRequestId()) {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"request %q already ran and was superseded by %q; use a new request_id", req.GetRequestId(), row.requestID)
	}

	// A retry of this very operation: finished, or resume the fan-out.
	if row.requestID == req.GetRequestId() {
		switch row.state {
		case terminal:
			return planDone, nil
		case transitional:
			return planResume, nil
		default:
			return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"request %q already ran with a different target", req.GetRequestId())
		}
	}

	// A different operation: guard against deciding from a stale view.
	if exp := req.GetExpectedRequestId(); exp != "" && exp != row.requestID {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"persisted request_id is %q, not the expected %q", row.requestID, exp)
	}
	// An unfence undoes a completed fence, or aborts one in progress, naming it.
	if target == unfenced && row.state != fenced && !(row.state == fencing && req.GetExpectedRequestId() == row.requestID) {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "cannot unfence from %s", row.state)
	}
	// A fence may start from any state, preempting an unfence in progress.
	return planStart, nil
}

// UpdatePoolerAdmission fences or unfences one tablegroup and returns only when
// every required pooler has acknowledged.
func (c *admissionCoordinator) UpdatePoolerAdmission(ctx context.Context, req *multipoolerservicepb.UpdatePoolerAdmissionRequest) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	fenced := multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	unfenced := multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	target := req.GetTarget()
	if target != fenced && target != unfenced {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "target must be FENCED or UNFENCED")
	}
	if req.GetRequestId() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "request_id is required")
	}
	if target == unfenced && req.GetExpectedRequestId() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_request_id is required to unfence")
	}
	if req.GetTablegroup() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup is required")
	}

	timeout := defaultAdmissionTimeout
	if d := req.GetTimeout().AsDuration(); d > 0 {
		timeout = d
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db, tg := req.GetDatabase(), req.GetTablegroup()
	row, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, err
	}
	plan, err := planAdmission(row, req)
	if err != nil {
		return nil, err
	}

	transitional := multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	if target == unfenced {
		transitional = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING
	}

	switch plan {
	case planDone:
		// Already committed under this request id. Nothing is rewritten, but the
		// poolers are asked again: a caller that retries a completed operation is
		// checking that it holds, and a pooler that missed it must not be skipped.
		// The cost is that the retry fails while a pooler of the tablegroup that
		// cannot answer (a killed one that never announced shutdown) is in the
		// membership, even though the operation itself is committed.
		acks, err := c.fanOut(ctx, db, tg, target, req.GetRequestId())
		if err != nil {
			return nil, err
		}
		return &multipoolerservicepb.UpdatePoolerAdmissionResponse{State: row.proto(tg), Acks: acks}, nil
	case planStart:
		if err := c.isLeader(); err != nil {
			return nil, err
		}
		ok, err := c.store.CompareAndSet(ctx, db, tg, row.state, row.requestID, transitional, req.GetRequestId())
		if err != nil {
			return nil, mterrors.Wrap(err, "persist admission state; outcome uncertain, re-read with GetServingState")
		}
		if !ok {
			return nil, mterrors.New(mtrpcpb.Code_ABORTED, "serving state changed concurrently; re-read and retry")
		}
	case planResume:
	}

	acks, err := c.fanOut(ctx, db, tg, transitional, req.GetRequestId())
	if err != nil {
		if target == fenced {
			// A fence that cannot complete is aborted, not left in FENCING.
			return nil, c.abortFence(ctx, db, tg, req.GetRequestId(), err)
		}
		// An unfence that is incomplete stays UNFENCING: the caller retries with the
		// same request id.
		return nil, err
	}

	if err := c.isLeader(); err != nil {
		return nil, err
	}
	ok, err := c.store.CompareAndSet(ctx, db, tg, transitional, req.GetRequestId(), target, req.GetRequestId())
	if err != nil {
		return nil, mterrors.Wrap(err, "persist admission state; outcome uncertain, re-read with GetServingState")
	}
	final, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, err
	}
	if !ok {
		// A concurrent call with the same request id may have committed first.
		if final.requestID == req.GetRequestId() && final.state == target {
			return &multipoolerservicepb.UpdatePoolerAdmissionResponse{State: final.proto(tg), Acks: acks}, nil
		}
		// Another request superseded this one while it was fanning out.
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "request was superseded by a newer admission decision")
	}
	return &multipoolerservicepb.UpdatePoolerAdmissionResponse{State: final.proto(tg), Acks: acks}, nil
}

// abortRequestID is the request id under which the abort of a fence reopens its
// tablegroup.
func abortRequestID(fenceRequestID string) string { return "abort-" + fenceRequestID }

// abortTimeout bounds the abort of a fence, which runs even when the caller's own
// context has ended: leaving a tablegroup closed is the one thing an abort exists to
// prevent.
const abortTimeout = 30 * time.Second

// abortFence reopens the tablegroup of a fence that could not complete, and returns the
// error that tells the caller so. cause is why the fence could not complete.
func (c *admissionCoordinator) abortFence(ctx context.Context, db, tg, fenceRequestID string, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	abortID := abortRequestID(fenceRequestID)
	fencing := multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	unfencing := multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING
	unfenced := multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	if c.logger != nil {
		c.logger.WarnContext(ctx, "aborting a fence that could not complete", "tablegroup", tg, "request_id", fenceRequestID, "cause", cause)
	}
	failed := func(why string, err error) error {
		return mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"fence aborted: %v; %s: %v. The tablegroup is not left fenced by this fence; read GetServingState and, if it is UNFENCING, finish with UpdatePoolerAdmission(UNFENCED, request_id=%q, expected_request_id=%q)",
			cause, why, err, abortID, fenceRequestID)
	}

	if err := c.isLeader(); err != nil {
		return failed("this pooler is no longer the default primary, so it cannot reopen the tablegroup", err)
	}
	// The row moves from this fence to its abort, which records the fence's request id in
	// the history. A row that has already moved on was aborted or superseded by someone
	// else: do not touch it.
	ok, err := c.store.CompareAndSet(ctx, db, tg, fencing, fenceRequestID, unfencing, abortID)
	if err != nil {
		return failed("the abort could not be recorded", err)
	}
	if !ok {
		row, rerr := c.store.GetRow(ctx, db, tg)
		if rerr != nil || row.requestID != abortID {
			return mterrors.Errorf(mtrpcpb.Code_ABORTED,
				"fence failed: %v; the serving state changed concurrently, so it was not aborted here; re-read with GetServingState", cause)
		}
		// Another call of this fence is aborting it: join it.
	}

	if _, err := c.fanOut(ctx, db, tg, unfencing, abortID); err != nil {
		return failed("reopening is incomplete (the state stays UNFENCING)", err)
	}
	if err := c.isLeader(); err != nil {
		return failed("this pooler is no longer the default primary", err)
	}
	if _, err := c.store.CompareAndSet(ctx, db, tg, unfencing, abortID, unfenced, abortID); err != nil {
		return failed("the reopening could not be recorded", err)
	}
	return mterrors.Errorf(mtrpcpb.Code_ABORTED,
		"fence aborted: %v; the tablegroup is serving again (request %q). Fence again later with a new request_id", cause, abortID)
}

// fanOut drives every required pooler to acknowledge the transitional state.
// Membership is re-enumerated until it is stable, so a pooler that joins while
// the operation runs is included. Acknowledgments are keyed by pooler id.
//
// An acknowledgment from a pooler that then restarted under the same id is still
// safe to accept, because fence safety does not rest on telling processes apart.
// It rests on these invariants, each enforced elsewhere:
//
//  1. FENCING is committed before the membership snapshot is taken, so any
//     process that starts afterwards reads FENCING (or later) and stays closed.
//  2. A pooler under admission control starts with its gate closed.
//  3. A pooler registers in topology before it reads admission state, so a
//     process that read an older state is either in the snapshot or has not yet
//     been able to open.
//  4. A closed pooler reopens only by reading UNFENCED or UNFENCING from the
//     default primary, or by receiving an unfence refresh.
//
// The one thing this does not cover is two live processes with the same pooler
// id (a rolling update with surge): topology holds one record per id, so the
// snapshot would not show the other process either. Running at most one live
// process per service id is an operational requirement.
func (c *admissionCoordinator) fanOut(ctx context.Context, db, tg string, transitional multipoolerservicepb.AdmissionState, requestID string) ([]*multipoolerservicepb.PoolerAdmissionAck, error) {
	settled := settledState(transitional)
	required := map[string]poolerRef{}
	acked := map[string]*multipoolerservicepb.PoolerAdmissionAck{}

	for range maxMembershipRounds {
		snapshot, err := c.members.Snapshot(ctx, db, tg)
		if err != nil {
			return nil, mterrors.Wrap(err, "enumerate poolers")
		}
		for _, p := range snapshot {
			required[p.key()] = p
		}
		var pending []poolerRef
		for k, p := range required {
			if _, done := acked[k]; !done {
				pending = append(pending, p)
			}
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].key() < pending[j].key() })
		if len(pending) == 0 {
			return orderedAcks(acked), nil
		}

		var (
			mu     sync.Mutex
			failed = map[string]error{}
			wg     sync.WaitGroup
		)
		for _, p := range pending {
			wg.Go(func() {
				ack, err := c.refreshWithRetry(ctx, p, db, tg, transitional, settled, requestID)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					failed[p.key()] = err
					return
				}
				acked[p.key()] = ack
			})
		}
		wg.Wait()

		if len(failed) > 0 {
			return nil, c.incompleteError(transitional, required, acked, failed)
		}
		// Loop: re-enumerate to catch poolers that joined during this round, and
		// poolers that disappeared (they stay required and unacknowledged).
		fresh, err := c.members.Snapshot(ctx, db, tg)
		if err != nil {
			return nil, mterrors.Wrap(err, "enumerate poolers")
		}
		stable := true
		freshKeys := map[string]bool{}
		for _, p := range fresh {
			freshKeys[p.key()] = true
			if _, done := acked[p.key()]; !done {
				// Required from now on, even if it is gone by the next round.
				required[p.key()] = p
				stable = false
			}
		}
		for k := range required {
			if _, done := acked[k]; !done && !freshKeys[k] {
				// Required but gone without acknowledging: not proof it stopped.
				return nil, c.incompleteError(transitional, required, acked, map[string]error{k: errors.New("pooler disappeared before acknowledging")})
			}
		}
		if stable {
			return orderedAcks(acked), nil
		}
	}
	return nil, mterrors.New(mtrpcpb.Code_ABORTED, "pooler membership kept changing; retry with the same request_id")
}

func (c *admissionCoordinator) refreshWithRetry(ctx context.Context, p poolerRef, db, tg string, expected, settled multipoolerservicepb.AdmissionState, requestID string) (*multipoolerservicepb.PoolerAdmissionAck, error) {
	var lastErr error
	for attempt := range fanOutAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, errors.Join(lastErr, ctx.Err())
			case <-time.After(c.backoff()):
			}
		}
		resp, err := c.refresher.Refresh(ctx, p, &multipoolerservicepb.RefreshAdmissionRequest{
			Database: db, Tablegroup: tg, ExpectedState: expected, RequestId: requestID,
		})
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, errors.Join(lastErr, ctx.Err())
			}
			if isBusy(err) {
				// The pooler waited out its drain deadline and still has work running:
				// asking again would only wait another deadline, and the fence is going
				// to be aborted.
				return nil, lastErr
			}
			continue
		}
		// The acknowledgment must be exactly the pooler we asked, for exactly the
		// operation we are running.
		switch {
		case !proto.Equal(resp.GetPoolerId(), p.id):
			lastErr = errors.New("acknowledgment from a different pooler")
		case resp.GetRequestId() != requestID || resp.GetAppliedState() != settled:
			lastErr = fmt.Errorf("inexact acknowledgment: %s for %q", resp.GetAppliedState(), resp.GetRequestId())
		default:
			return &multipoolerservicepb.PoolerAdmissionAck{
				PoolerId:     resp.GetPoolerId(),
				AppliedState: resp.GetAppliedState(),
			}, nil
		}
		return nil, lastErr
	}
	return nil, lastErr
}

// isBusy reports whether a pooler's answer to a fence refresh says it is busy: work is
// still running at its drain deadline, or it cannot rule out a backend it lost track of.
func isBusy(err error) bool {
	return mterrors.Code(err) == mtrpcpb.Code_RESOURCE_EXHAUSTED || status.Code(err) == codes.ResourceExhausted
}

func (c *admissionCoordinator) incompleteError(state multipoolerservicepb.AdmissionState, required map[string]poolerRef, acked map[string]*multipoolerservicepb.PoolerAdmissionAck, failed map[string]error) error {
	var parts []string
	for k, err := range failed {
		name := k
		if p, ok := required[k]; ok {
			name = p.name()
		}
		parts = append(parts, fmt.Sprintf("%s: %v", name, err))
	}
	sort.Strings(parts)
	if c.logger != nil {
		c.logger.Warn("admission change incomplete", "state", state.String(), "acknowledged", len(acked), "missing", len(failed))
	}
	return mterrors.Errorf(mtrpcpb.Code_ABORTED,
		"admission change incomplete: %d of %d poolers did not acknowledge (%s); state remains %s",
		len(failed), len(required), strings.Join(parts, "; "), state)
}

func orderedAcks(acked map[string]*multipoolerservicepb.PoolerAdmissionAck) []*multipoolerservicepb.PoolerAdmissionAck {
	keys := make([]string, 0, len(acked))
	for k := range acked {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*multipoolerservicepb.PoolerAdmissionAck, 0, len(keys))
	for _, k := range keys {
		out = append(out, acked[k])
	}
	return out
}

// UpdateMigrationRouting atomically moves application traffic between two
// FENCED tablegroups. Success means the routing metadata committed; it does not
// unfence anything.
func (c *admissionCoordinator) UpdateMigrationRouting(ctx context.Context, req *multipoolerservicepb.UpdateMigrationRoutingRequest) (*multipoolerservicepb.UpdateMigrationRoutingResponse, error) {
	from, to := req.GetFromTablegroup(), req.GetToTablegroup()
	if req.GetRequestId() == "" || from == "" || to == "" || from == to {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "request_id and distinct from/to tablegroups are required")
	}
	db := req.GetDatabase()
	app, version, found, err := c.store.GetRouting(ctx, db)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "database has no routing pointer")
	}
	// A retry after the pointer already moved is success, with no version bump.
	if app == to {
		return &multipoolerservicepb.UpdateMigrationRoutingResponse{AppTablegroup: app, RoutingVersion: version}, nil
	}
	if app != from {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "application tablegroup is %q, not %q", app, from)
	}
	for _, name := range []string{from, to} {
		row, err := c.store.GetRow(ctx, db, name)
		if err != nil {
			return nil, err
		}
		if row.state != multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED {
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is %s, routing requires FENCED", name, row.state)
		}
	}
	if err := c.isLeader(); err != nil {
		return nil, err
	}
	app, version, moved, err := c.store.MoveRouting(ctx, db, from, to)
	if err != nil {
		return nil, mterrors.Wrap(err, "commit routing; outcome uncertain, re-read with GetServingState")
	}
	if !moved {
		return nil, mterrors.New(mtrpcpb.Code_ABORTED, "routing or admission state changed concurrently; re-read and retry")
	}
	return &multipoolerservicepb.UpdateMigrationRoutingResponse{AppTablegroup: app, RoutingVersion: version}, nil
}
