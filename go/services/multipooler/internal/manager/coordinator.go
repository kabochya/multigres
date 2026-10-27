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
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// The admission coordinator runs on the default primary. It owns the decisions "fence this
// scope", "unfence it" and "move application traffic", commits them durably, and drives every
// member of the tablegroup to enforce them. It provides primitives that are safe by construction
// and reports facts; the migrator owns policy (how long to wait, whether to retry or give up, when
// to reopen).
//
// A scope's fence is a state, UNFENCED, FENCING or FENCED, and a version that goes up by one every
// time the scope is fenced or unfenced, and it is held by one operation. Fence and unfence are
// compare-and-sets on the version the caller read, so a delayed call cannot undo what came after
// it. Each call also recognizes the exact row its own success produced, so a retry of it succeeds
// without writing again, while anything else is rejected: only the coordinator's compare-and-set
// changes the version, callers only pass it back. The persisted fence is the source of truth:
// poolers re-read it themselves, so a stale or restarted coordinator cannot make a pooler apply
// anything. A scope with no fence is unfenced at version zero, and an unfence leaves an UNFENCED
// fence, at the next version, until CleanupOperation deletes the operation's fences.
//
// A fence has two calls, and each asks every member the same thing: to apply the fence (which closes
// its gate) and say what it holds. StartFence begins an attempt: it commits FENCING, asks the
// members, and returns at once, whatever they hold. CommitFence, called as often as the migrator
// likes, asks them again from a fresh listing, again without waiting, which also closes a member
// that missed the start, and writes FENCED, at the same version, only when every member holds
// nothing. The version is the attempt: the start and an unfence begin a new one, the commit
// completes the current one, and every response returns the fence, so the caller passes back what
// it got. Everything between the two calls is the migrator's: waiting, deciding what to do about
// the connections that hold the fence, giving up. What Multigres keeps is the safety check inside
// CommitFence. FENCING is a fence that is started, whose commit has not succeeded, or whose
// coordinator was lost; a routing switch refuses it, so a migrator that takes it for a completed
// fence still cannot move the traffic. Every member is asked, not only the writable one: a replica
// can be promoted while the fence runs, and one that read UNFENCED before FENCING was written would
// otherwise be left open. That is closed by invariants enforced elsewhere (see fanOut). Every
// member is asked even when one fails; a start succeeds only if all closed, and a commit only if
// all are clean. A fence never undoes itself: when it cannot complete, the fence stays FENCING and
// the per-member results tell the caller why, and the caller asks again or unfences. An unfence has
// no acknowledgement barrier: a pooler opens once it reads UNFENCED.
//
// A coordinator that dies, or stops being the default primary, in the middle of a fence leaves the
// scope FENCING. Nothing in the system undoes that: the migrator is the one waiting on the call,
// gets the error, reads GetRoutingState and, if the scope is fenced and the traffic has not
// switched, unfences it at the version it read (see the migrator's on-error policy in
// proto/multipoolerservice.proto).

const (
	// defaultMemberCallTimeout is what a member is given to answer a close or a report, both
	// of which are quick: neither waits for work.
	defaultMemberCallTimeout = 5 * time.Second
	// reopenCallTimeout bounds one member's answer to a best-effort reopen. A member that
	// does not answer reopens by itself when it next reads its row.
	reopenCallTimeout = 2 * time.Second
	// maxMembershipRounds bounds how often the required set may grow (poolers
	// joining mid-operation) before the call gives up.
	maxMembershipRounds = 4
)

// poolerRef identifies one pooler in a membership snapshot.
type poolerRef struct {
	id     *clustermetadatapb.ID
	record *clustermetadatapb.Multipooler
}

func (p poolerRef) key() string {
	return string(topoclient.ComponentIDString(p.id))
}

func (p poolerRef) name() string { return p.id.GetName() }

// membership enumerates the poolers that must acknowledge a fence.
type membership interface {
	// Snapshot lists every pooler of the tablegroup in every cell, managed and
	// unmanaged, excluding poolers that have announced shutdown. A partial listing
	// is an error.
	Snapshot(ctx context.Context, database, tablegroup string) ([]poolerRef, error)
}

// poolerRefresher asks one pooler to re-read and enforce its admission.
type poolerRefresher interface {
	Refresh(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

type admissionCoordinator struct {
	store     routingStore
	members   membership
	refresher poolerRefresher
	logger    *slog.Logger
	// isLeader is checked before every durable write. It is a LOCAL check of the
	// pooler's own routing role, not of the consensus term, so it narrows the
	// window in which a deposed default primary could write but does not close it:
	// between the check and the write the role can change. What keeps a stale
	// coordinator from corrupting state is the conditional update of every write,
	// and what keeps its decisions from taking effect is that poolers re-read the
	// persisted row themselves. A term-based fence is future work.
	isLeader func() error
	// requiredNodes returns how many nodes the durability policy of the tablegroup's
	// cohort needs to acknowledge a write, and whether the tablegroup is managed (an
	// unmanaged one has no cohort). Nil skips the check (tests).
	requiredNodes func(ctx context.Context, database, tablegroup string) (nodes int, managed bool, err error)
	// callTimeout replaces a member's call timeout, and reopenTimeout the reopen's (tests).
	callTimeout, reopenTimeout time.Duration
}

// memberCallTimeout is what one member is given to answer a close or a report.
func (c *admissionCoordinator) memberCallTimeout() time.Duration {
	if c.callTimeout > 0 {
		return c.callTimeout
	}
	return defaultMemberCallTimeout
}

func (c *admissionCoordinator) reopenCallTimeout() time.Duration {
	if c.reopenTimeout > 0 {
		return c.reopenTimeout
	}
	return reopenCallTimeout
}

// FenceIncompleteError is the error of a start or a commit some member did not complete:
// ABORTED, with the per-member results (a StartFenceResponse or a CommitFenceResponse) as
// the details of its status. The row stays FENCING.
type FenceIncompleteError struct {
	message string
	Details protoadapt.MessageV1
}

func (e *FenceIncompleteError) Error() string { return e.message }

// ErrorCode is ABORTED.
func (e *FenceIncompleteError) ErrorCode() mtrpcpb.Code { return mtrpcpb.Code_ABORTED }

// GRPCStatus carries the per-member results to the caller.
func (e *FenceIncompleteError) GRPCStatus() *status.Status {
	st := status.New(codes.Aborted, e.message)
	if withDetails, err := st.WithDetails(e.Details); err == nil {
		return withDetails
	}
	return st
}

// requestScope checks the operation and the scope of a request and returns them: the operation
// as a canonical UUID, and the tablegroup the scope names. Only a whole tablegroup is a scope
// today.
func requestScope(operationID string, scope *multipoolerservicepb.AdmissionScope) (op, tablegroup string, err error) {
	op, err = canonicalOperationID(operationID)
	if err != nil {
		return "", "", err
	}
	if scope.GetTablegroup() == "" {
		return "", "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "the tablegroup of the scope is required")
	}
	if scope.GetShard() != "" || scope.GetTable() != "" {
		return "", "", mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "only a tablegroup scope is supported: shard and table scopes are not implemented")
	}
	return op, scope.GetTablegroup(), nil
}

// canonicalOperationID returns the operation id in the form PostgreSQL gives a UUID back, so that
// what a caller passes compares equal to what is stored.
func canonicalOperationID(operationID string) (string, error) {
	id, err := uuid.Parse(operationID)
	if err != nil {
		return "", mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "operation_id must be a UUID: %v", err)
	}
	return id.String(), nil
}

// describeFence says what a scope's fence is, for a message.
func describeFence(f *fenceRow) string {
	if f == nil {
		return "unfenced with no fence (version 0)"
	}
	return fmt.Sprintf("%s at version %d, held by operation %s", f.state, f.version, f.operationID)
}

// unknownTablegroup is the NOT_FOUND of a tablegroup the catalog does not have, or nil when it has
// it. It is checked only when the scope has no fence: a fence is proof that the tablegroup exists.
func (c *admissionCoordinator) unknownTablegroup(ctx context.Context, tablegroup string) error {
	ok, err := c.store.TablegroupExists(ctx, tablegroup)
	if err != nil {
		return err
	}
	if !ok {
		return mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "tablegroup %q is not in the catalog", tablegroup)
	}
	return nil
}

// StartFence begins a fence of one scope for an operation: it writes FENCING and asks every member
// to close and say what it holds, and succeeds when every member answered, whatever it holds. It
// does not wait for work and it does not end anything. A start that some member did not answer
// leaves the fence FENCING, and the error says what each member answered; asking again reaches the
// members that missed it.
//
// A tablegroup whose durability policy, or the control home's, needs fewer than two nodes is
// refused before anything is written: with one node there is nothing that keeps the tablegroup
// available while its only node is replaced. So is a tablegroup that has more than one unmanaged
// pooler: exactly one is supported per tablegroup.
func (c *admissionCoordinator) StartFence(ctx context.Context, req *multipoolerservicepb.StartFenceRequest) (*multipoolerservicepb.StartFenceResponse, error) {
	op, tg, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	db, expected := req.GetDatabase(), req.GetExpectedVersion()
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireLive(ctx, op); err != nil {
		return nil, err
	}
	cur := snap.fenceOf(tg)
	if cur == nil {
		if err := c.unknownTablegroup(ctx, tg); err != nil {
			return nil, err
		}
	}
	curVersion := int64(0)
	if cur != nil {
		curVersion = cur.version
	}
	// The version is the attempt, and the caller passes back the one it read. The fences this call
	// accepts are the ones its own success, or a repeat of it, leaves:
	//   none or UNFENCED at v:    begin a new attempt: write FENCING at v+1, then ask the members. An
	//                             UNFENCED fence that another operation left is taken over, in the
	//                             same write;
	//   FENCING at v or at v+1:   the attempt, repeated (the caller read it, or its first reply was
	//                             lost): ask the members again;
	//   FENCED at v or at v+1:    already done: succeed, and ask nobody.
	// The last two only for this operation's own fence. Anything else is rejected, so a delayed
	// repeat of a fence that was undone cannot fence again, and an operation cannot touch a scope
	// that another one holds.
	own := cur != nil && cur.operationID == op
	var row fenceRow
	switch {
	case (cur == nil || cur.state == stateUnfenced) && curVersion == expected:
		if err := c.checkDurability(ctx, db, tg); err != nil {
			return nil, err
		}
		if _, err := c.snapshotMembers(ctx, db, tg); err != nil {
			return nil, err
		}
		if err := c.isLeader(); err != nil {
			return nil, err
		}
		ok, err := c.store.WriteFence(ctx, fenceWrite{tablegroup: tg, operationID: op, prev: cur, state: stateFencing, version: expected + 1})
		if err != nil {
			return nil, mterrors.Wrap(err, "persist the fence; outcome uncertain, re-read with GetRoutingState")
		}
		if !ok {
			return nil, mterrors.New(mtrpcpb.Code_ABORTED, "the fence changed concurrently; re-read and retry")
		}
		row = fenceRow{operationID: op, tablegroup: tg, state: stateFencing, version: expected + 1, updatedAt: time.Now()}
	case own && cur.state == stateFencing && (cur.version == expected || cur.version == expected+1):
		// The attempt, repeated.
		row = *cur
	case own && cur.state == stateFenced && (cur.version == expected || cur.version == expected+1):
		return &multipoolerservicepb.StartFenceResponse{Fence: cur.proto()}, nil
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"scope %q is %s for operation %s, not unfenced at the expected version %d, nor FENCING or FENCED for the operation at it or the version after; read GetRoutingState and decide",
			tg, describeFence(cur), op, expected)
	}

	results, err := c.fanOut(ctx, db, tg, func(ctx context.Context, p poolerRef) *multipoolerservicepb.MemberFenceResult {
		return c.askOne(ctx, p, db, op, tg, row.version)
	})
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.StartFenceResponse{Fence: row.proto(), Members: results}
	if notOK := unreachable(results); len(notOK) > 0 {
		if c.logger != nil {
			c.logger.WarnContext(ctx, "fence started; a member did not answer", "tablegroup", tg, "version", row.version, "unreachable", len(notOK))
		}
		return nil, &FenceIncompleteError{
			message: fmt.Sprintf("scope %q is FENCING at version %d but %d of %d members did not answer (%s); call StartFence again to reach them, or unfence at that version",
				tg, row.version, len(notOK), len(results), strings.Join(notOK, "; ")),
			Details: resp,
		}
	}
	return resp, nil
}

// CommitFence completes the current fence attempt, when every member holds nothing: it asks every
// member, from a fresh listing, to apply the fence and say what it holds (which closes a member
// that missed the start), and writes FENCED, at the same version, only if all are clean. It waits
// for nothing. When one is not, it writes nothing and the error says what each member answered; the
// migrator asks again when it likes.
func (c *admissionCoordinator) CommitFence(ctx context.Context, req *multipoolerservicepb.CommitFenceRequest) (*multipoolerservicepb.CommitFenceResponse, error) {
	op, tg, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetVersion() <= 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "version must be the version of a fence")
	}
	db, version := req.GetDatabase(), req.GetVersion()
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireLive(ctx, op); err != nil {
		return nil, err
	}
	cur := snap.fenceOf(tg)
	if cur == nil {
		if err := c.unknownTablegroup(ctx, tg); err != nil {
			return nil, err
		}
	}
	// The fences this call accepts: FENCING for the operation at the version, which it commits,
	// and FENCED at it, which is success. Nothing else is a fence of that version.
	own := cur != nil && cur.operationID == op
	switch {
	case own && cur.state == stateFenced && cur.version == version:
		return &multipoolerservicepb.CommitFenceResponse{Fence: cur.proto()}, nil
	case own && cur.state == stateFencing && cur.version == version:
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"scope %q is %s for operation %s, not FENCING or FENCED for the operation at the version %d to commit; read GetRoutingState and decide",
			tg, describeFence(cur), op, version)
	}

	results, err := c.fanOut(ctx, db, tg, func(ctx context.Context, p poolerRef) *multipoolerservicepb.MemberFenceResult {
		return c.askOne(ctx, p, db, op, tg, version)
	})
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.CommitFenceResponse{Fence: cur.proto(), Members: results}
	if notOK := notCompleted(results, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN); len(notOK) > 0 {
		// Another commit of this same fence may have completed it while the members were asked:
		// a member that already sees FENCED refuses the refresh of this one. If the fence says
		// FENCED at the version, it is done, which is what this call was after.
		if now, rerr := c.store.Snapshot(ctx); rerr == nil {
			if f := now.fenceOf(tg); f != nil && f.operationID == op && f.state == stateFenced && f.version == version {
				resp.Fence = f.proto()
				return resp, nil
			}
		}
		if c.logger != nil {
			c.logger.InfoContext(ctx, "fence not complete; the scope stays FENCING", "tablegroup", tg, "version", version, "not_clean", len(notOK))
		}
		return nil, &FenceIncompleteError{
			message: fmt.Sprintf("scope %q is FENCING at version %d but %d of %d members are not clean (%s); call CommitFence again, or unfence at that version",
				tg, version, len(notOK), len(results), strings.Join(notOK, "; ")),
			Details: resp,
		}
	}

	// Every member is clean: the fence is complete, and only now is it FENCED. The version does
	// not change.
	if err := c.isLeader(); err != nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"every member is clean, but this pooler is no longer the default primary, so it cannot complete the fence (%v); the fence stays FENCING at version %d: call CommitFence through the new default primary", err, version)
	}
	// A false result means the fence is no longer FENCING at this version. Another commit may
	// have completed this very fence first, which is success; the read below tells.
	if _, err := c.store.WriteFence(ctx, fenceWrite{tablegroup: tg, operationID: op, prev: cur, state: stateFenced, version: version}); err != nil {
		return nil, mterrors.Wrap(err, "persist the completed fence; outcome uncertain, re-read with GetRoutingState")
	}
	// The fence is read again before success is reported: success needs FENCED at the version the
	// members closed for, whoever wrote it.
	after, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "read back the fence; outcome uncertain, re-read with GetRoutingState")
	}
	current := after.fenceOf(tg)
	if current == nil || current.operationID != op || current.state != stateFenced || current.version != version {
		return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"scope %q is not FENCED at version %d for operation %s; the state changed during the commit; re-read with GetRoutingState", tg, version, op)
	}
	resp.Fence = current.proto()
	return resp, nil
}

// notCompleted lists, for a message, the members whose answer is not the one that completes the
// call.
func notCompleted(results []*multipoolerservicepb.MemberFenceResult, want multipoolerservicepb.MemberFenceResult_Status) []string {
	var out []string
	for _, m := range results {
		if m.GetStatus() != want {
			out = append(out, memberLine(m))
		}
	}
	return out
}

// unreachable lists, for a message, the members that did not answer.
func unreachable(results []*multipoolerservicepb.MemberFenceResult) []string {
	var out []string
	for _, m := range results {
		if m.GetStatus() == multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE {
			out = append(out, memberLine(m))
		}
	}
	return out
}

func memberLine(m *multipoolerservicepb.MemberFenceResult) string {
	return fmt.Sprintf("%s: %s: %s", m.GetPoolerId().GetName(), statusText(m.GetStatus()), m.GetDetail())
}

func statusText(s multipoolerservicepb.MemberFenceResult_Status) string {
	switch s {
	case multipoolerservicepb.MemberFenceResult_STATUS_CLEAN:
		return "clean"
	case multipoolerservicepb.MemberFenceResult_STATUS_BUSY:
		return "busy"
	default:
		return "unreachable"
	}
}

// checkDurability refuses a fence whose tablegroup, or the control home, has a managed
// durability policy that needs fewer than two nodes.
func (c *admissionCoordinator) checkDurability(ctx context.Context, db, tg string) error {
	if c.requiredNodes == nil {
		return nil
	}
	check := []string{tg}
	if tg != constants.DefaultTableGroup {
		check = append(check, constants.DefaultTableGroup)
	}
	for _, name := range check {
		nodes, managed, err := c.requiredNodes(ctx, db, name)
		if err != nil {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "cannot fence tablegroup %q: the durability policy of %q could not be read: %v", tg, name, err)
		}
		if managed && nodes < 2 {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"cannot fence tablegroup %q: the durability policy of %q needs %d node(s) to acknowledge a write, and a fence needs at least 2 so that the tablegroup keeps a leader while it is fenced", tg, name, nodes)
		}
	}
	return nil
}

// UnfenceScope writes UNFENCED over the operation's fence of the scope, at the next version, and
// asks the members to reopen on a best-effort basis. expected_version is the version the fence has,
// and it may be FENCING or FENCED. A retry is recognized by the fence its own success leaves,
// UNFENCED at the version after: it asks the members again and writes nothing. The UNFENCED fence
// stays until CleanupOperation.
func (c *admissionCoordinator) UnfenceScope(ctx context.Context, req *multipoolerservicepb.UnfenceScopeRequest) (*multipoolerservicepb.UnfenceScopeResponse, error) {
	op, tg, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	db, expected := req.GetDatabase(), req.GetExpectedVersion()
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireLive(ctx, op); err != nil {
		return nil, err
	}
	cur := snap.fenceOf(tg)
	if cur == nil {
		if err := c.unknownTablegroup(ctx, tg); err != nil {
			return nil, err
		}
	}
	own := cur != nil && cur.operationID == op
	var row fenceRow
	switch {
	case own && cur.closed() && cur.version == expected:
		if err := c.isLeader(); err != nil {
			return nil, err
		}
		ok, err := c.store.WriteFence(ctx, fenceWrite{tablegroup: tg, operationID: op, prev: cur, state: stateUnfenced, version: expected + 1})
		if err != nil {
			return nil, mterrors.Wrap(err, "persist the unfence; outcome uncertain, re-read with GetRoutingState")
		}
		if !ok {
			return nil, mterrors.New(mtrpcpb.Code_ABORTED, "the fence changed concurrently; re-read and retry")
		}
		row = fenceRow{operationID: op, tablegroup: tg, state: stateUnfenced, version: expected + 1, updatedAt: time.Now()}
	case own && cur.state == stateUnfenced && cur.version == expected+1:
		// Already UNFENCED at the version this unfence would have produced: done. Write
		// nothing; the members are asked again, since the reply that was lost may have
		// been the one that told them.
		row = *cur
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"scope %q is %s for operation %s, not FENCING or FENCED for the operation at the expected version %d, nor UNFENCED at the version after; read GetRoutingState and decide",
			tg, describeFence(cur), op, expected)
	}
	resp := &multipoolerservicepb.UnfenceScopeResponse{Fence: row.proto()}
	for _, p := range c.reopen(ctx, db, op, tg, row.version) {
		resp.Unreached = append(resp.Unreached, p.id)
	}
	return resp, nil
}

// reopen asks every member of the tablegroup to re-read its row, which now says
// UNFENCED at version, on a best-effort basis, and returns the members it could not
// reach. Nothing waits for them: a pooler opens once it reads UNFENCED.
func (c *admissionCoordinator) reopen(ctx context.Context, db, op, tg string, version int64) []poolerRef {
	snapshot, err := c.members.Snapshot(ctx, db, tg)
	if err != nil {
		if c.logger != nil {
			c.logger.WarnContext(ctx, "could not list the members to reopen; they reopen when they next read their row", "tablegroup", tg, "error", err)
		}
		return nil
	}
	var (
		mu        sync.Mutex
		unreached []poolerRef
		wg        sync.WaitGroup
	)
	for _, p := range snapshot {
		wg.Go(func() {
			callCtx, cancel := context.WithTimeout(ctx, c.reopenCallTimeout())
			defer cancel()
			_, err := c.refresher.Refresh(callCtx, p, &multipoolerservicepb.RefreshAdmissionRequest{
				Database: db, OperationId: op, Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: tg}, ExpectedState: stateUnfenced, ExpectedVersion: version,
			})
			if err != nil {
				if c.logger != nil {
					c.logger.WarnContext(ctx, "member did not answer the reopen; it reopens when it next reads its row", "pooler", p.name(), "error", err)
				}
				mu.Lock()
				unreached = append(unreached, p)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	sort.Slice(unreached, func(i, j int) bool { return unreached[i].key() < unreached[j].key() })
	return unreached
}

// fanOut asks every member of the tablegroup, with ask, and returns one result per member.
// Nobody is skipped because another member failed. Membership is re-enumerated until it is
// stable, so a pooler that joins while the call runs is asked too. A commit takes a fresh
// listing every time it runs, so a pooler that joined after the start is asked, and is
// reported not closed if it is not.
//
// A pooler that restarted under the same id after answering is still safe to count,
// because fence safety does not rest on telling processes apart. It rests on these
// invariants, each enforced elsewhere:
//
//  1. FENCING is committed before the membership snapshot is taken, so any process
//     that starts afterwards reads a closed row and stays closed.
//  2. A pooler starts with its gate closed.
//  3. A pooler registers in topology before it reads its row, so a process that read an
//     older row is either in the snapshot or has not yet been able to open.
//  4. A pooler closes on a role change, a serving change or a reconnect, and opens again
//     only by reading UNFENCED from the default primary.
//
// The one thing this does not cover is two live processes with the same pooler id (a
// rolling update with surge): topology holds one record per id, so the snapshot would
// not show the other process either. Running at most one live process per service id is
// an operational requirement.
func (c *admissionCoordinator) fanOut(ctx context.Context, db, tg string, ask func(context.Context, poolerRef) *multipoolerservicepb.MemberFenceResult) ([]*multipoolerservicepb.MemberFenceResult, error) {
	required := map[string]poolerRef{}
	answered := map[string]*multipoolerservicepb.MemberFenceResult{}

	for range maxMembershipRounds {
		snapshot, err := c.snapshotMembers(ctx, db, tg)
		if err != nil {
			return nil, err
		}
		for _, p := range snapshot {
			required[p.key()] = p
		}
		// Everyone required and not yet answered is asked, including a pooler that was listed
		// in an earlier round and is not any more: its disappearance is not proof that it
		// stopped, so it is asked, and unreachable if it does not answer.
		var pending []poolerRef
		for k, p := range required {
			if _, done := answered[k]; !done {
				pending = append(pending, p)
			}
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].key() < pending[j].key() })

		var (
			mu sync.Mutex
			wg sync.WaitGroup
		)
		for _, p := range pending {
			wg.Go(func() {
				result := ask(ctx, p)
				mu.Lock()
				answered[p.key()] = result
				mu.Unlock()
			})
		}
		wg.Wait()

		// Loop until a snapshot taken after the round holds nobody unanswered: that catches
		// poolers that joined during it. A pooler that answered and then left the listing
		// was closed when it answered, and closes again only by reading UNFENCED.
		fresh, err := c.snapshotMembers(ctx, db, tg)
		if err != nil {
			return nil, err
		}
		stable := true
		for _, p := range fresh {
			if _, done := answered[p.key()]; !done {
				required[p.key()] = p
				stable = false
			}
		}
		if stable {
			keys := make([]string, 0, len(answered))
			for k := range answered {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := make([]*multipoolerservicepb.MemberFenceResult, 0, len(keys))
			for _, k := range keys {
				out = append(out, answered[k])
			}
			return out, nil
		}
	}
	return nil, mterrors.New(mtrpcpb.Code_ABORTED, "pooler membership kept changing; the tablegroup stays FENCING")
}

// askOne asks one member to apply the fence at the version and say what it holds, which is the same
// for a start and for a commit: the member closes its gate, if it is not closed, waits for nothing,
// and answers with what is still running or held. The answer must be exactly the pooler we asked,
// for exactly the fence we are running; if it is not, or the call failed, the member is
// unreachable. A member that holds something is busy, with the details it gave.
func (c *admissionCoordinator) askOne(ctx context.Context, p poolerRef, db, op, tg string, version int64) *multipoolerservicepb.MemberFenceResult {
	callCtx, cancel := context.WithTimeout(ctx, c.memberCallTimeout())
	defer cancel()
	resp, err := c.refresher.Refresh(callCtx, p, &multipoolerservicepb.RefreshAdmissionRequest{
		Database: db, OperationId: op, Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: tg}, ExpectedState: stateFencing, ExpectedVersion: version,
	})
	switch {
	case err != nil:
		return memberResult(p, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, failureOf(err))
	case !proto.Equal(resp.GetPoolerId(), p.id):
		return memberResult(p, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, "answer from a different pooler")
	case resp.GetState() != stateFencing || resp.GetVersion() != version:
		return memberResult(p, multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE,
			fmt.Sprintf("inexact answer: %s at version %d", resp.GetState(), resp.GetVersion()))
	case resp.GetBusy() != nil:
		busy := memberResult(p, multipoolerservicepb.MemberFenceResult_STATUS_BUSY,
			fmt.Sprintf("%d requests in flight, %d reservations held", resp.GetBusy().GetRequestsInFlight(), resp.GetBusy().GetReservationsHeld()))
		busy.Busy = resp.GetBusy()
		return busy
	}
	return memberResult(p, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, "")
}

func memberResult(p poolerRef, s multipoolerservicepb.MemberFenceResult_Status, detail string) *multipoolerservicepb.MemberFenceResult {
	return &multipoolerservicepb.MemberFenceResult{PoolerId: p.id, Status: s, Detail: detail}
}

// snapshotMembers lists the tablegroup's poolers and refuses a tablegroup with more than one
// unmanaged pooler: exactly one is supported per tablegroup, which is also what bootstrap
// enforces.
func (c *admissionCoordinator) snapshotMembers(ctx context.Context, db, tg string) ([]poolerRef, error) {
	snapshot, err := c.members.Snapshot(ctx, db, tg)
	if err != nil {
		return nil, mterrors.Wrap(err, "enumerate poolers")
	}
	var unmanaged []string
	for _, p := range snapshot {
		if p.unmanaged() {
			unmanaged = append(unmanaged, p.name())
		}
	}
	if len(unmanaged) > 1 {
		sort.Strings(unmanaged)
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q has %d unmanaged poolers (%s); exactly one is supported", tg, len(unmanaged), strings.Join(unmanaged, ", "))
	}
	return snapshot, nil
}

// failureOf is what a failed answer says: the code and the message of its status, without
// the "rpc error: code = ..." text that wraps them.
func failureOf(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Code().String() + ": " + st.Message()
	}
	return err.Error()
}
