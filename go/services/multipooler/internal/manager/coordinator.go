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

// The admission coordinator runs on the default primary. It owns the decisions
// "fence this tablegroup", "unfence it" and "move application traffic", commits
// them durably, and drives every member of the tablegroup to enforce them. It provides
// primitives that are safe by construction and reports facts; the migrator owns policy
// (how long to wait, whether to retry or give up, when to reopen).
//
// A tablegroup's row is a state, UNFENCED, FENCING or FENCED, and a version that goes up
// by one every time the tablegroup is fenced or unfenced. Fence and unfence are
// compare-and-sets on the version the caller read, so a delayed call cannot undo what
// came after it. Each call also recognizes the
// exact row its own success produced, so a retry of it succeeds without writing again,
// while anything else is rejected: only the coordinator's compare-and-set changes the
// version, callers only pass it back. The persisted row is the source of truth: poolers
// re-read it themselves, so a stale or restarted coordinator cannot make a pooler apply
// anything.
//
// A fence has two calls, and each asks every member the same thing: to apply the row (which closes
// its gate) and say what it holds. StartFence begins an attempt: it commits FENCING, asks the
// members, and returns at once, whatever they hold. CommitFence, called as often as the migrator
// likes, asks them again from a fresh listing, again without waiting, which also closes a member
// that missed the start, and writes FENCED, at the same version, only when every member holds
// nothing. The version is the attempt: the start and an unfence begin a new one, the commit
// completes the current one, and every response returns the row, so the caller passes back what it
// got. Everything between the two calls is the migrator's: waiting, deciding what to
// do about the connections that hold the fence, giving up. What Multigres keeps is the
// safety check inside CommitFence. FENCING is a fence that is started, whose commit has
// not succeeded, or whose coordinator was lost; a flip refuses it, so a migrator that
// takes it for a completed fence still cannot move the traffic. Every member is asked,
// not only the writable one: a replica can be promoted while the fence runs, and one that
// read UNFENCED before FENCING was written would otherwise be left open. That is closed
// by invariants enforced elsewhere (see fanOut). Every member is asked even when one
// fails; a start succeeds only if all closed, and a commit only if all are clean. A fence
// never undoes itself: when it cannot complete, the row stays FENCING and the per-member
// results tell the caller why, and the caller asks again or unfences. An unfence has no
// acknowledgement barrier: a pooler opens once it reads UNFENCED.
//
// A coordinator that dies, or stops being the default primary, in the middle of a fence
// leaves the tablegroup FENCING. Nothing in the system undoes that: the migrator is the
// one waiting on the call, gets the error, reads GetServingState and, if the tablegroup is
// fenced and the cutover has not flipped, unfences it at the version it read (see the
// migrator's on-error policy in proto/multipoolerservice.proto).

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

// StartFence begins a fence of one tablegroup: it writes FENCING and asks every member to close
// and say what it holds, and succeeds when every member answered, whatever it holds. It does not
// wait for work and it does not end anything. A start that some member did not answer leaves the
// row FENCING, and the error says what each member answered; asking again reaches the members
// that missed it.
//
// A tablegroup whose durability policy, or the control home's, needs fewer than two nodes
// is refused before anything is written: with one node there is nothing that keeps the
// tablegroup available while its only node is replaced. So is a tablegroup that has more
// than one unmanaged pooler: exactly one is supported per tablegroup.
func (c *admissionCoordinator) StartFence(ctx context.Context, req *multipoolerservicepb.StartFenceRequest) (*multipoolerservicepb.StartFenceResponse, error) {
	if req.GetTablegroup() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup is required")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	db, tg, expected := req.GetDatabase(), req.GetTablegroup(), req.GetExpectedVersion()
	row, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, err
	}
	if row == nil {
		// Never fenced: UNFENCED at version zero.
		row = &servingRow{state: stateUnfenced}
	}
	// The version is the attempt, and the caller passes back the one it read. The rows this call
	// accepts are the ones its own success, or a repeat of it, leaves:
	//   UNFENCED at v:            begin a new attempt: write FENCING at v+1, then ask the members;
	//   FENCING at v or at v+1:   the attempt, repeated (the caller read it, or its first reply was
	//                             lost): ask the members again;
	//   FENCED at v or at v+1:    already done: succeed, and ask nobody.
	// Anything else is rejected, so a delayed repeat of a fence that was undone cannot fence
	// again.
	switch {
	case row.state == stateUnfenced && row.version == expected:
		if err := c.checkDurability(ctx, db, tg); err != nil {
			return nil, err
		}
		if _, err := c.snapshotMembers(ctx, db, tg); err != nil {
			return nil, err
		}
		if err := c.isLeader(); err != nil {
			return nil, err
		}
		ok, err := c.store.CompareAndSet(ctx, db, tg, stateUnfenced, expected, stateFencing, expected+1)
		if err != nil {
			return nil, mterrors.Wrap(err, "persist the fence; outcome uncertain, re-read with GetServingState")
		}
		if !ok {
			return nil, mterrors.New(mtrpcpb.Code_ABORTED, "serving state changed concurrently; re-read and retry")
		}
		row.state, row.version, row.updatedAt = stateFencing, expected+1, time.Now()
	case row.state == stateFencing && (row.version == expected || row.version == expected+1):
		// The attempt, repeated.
	case row.state == stateFenced && (row.version == expected || row.version == expected+1):
		return &multipoolerservicepb.StartFenceResponse{State: row.proto(tg)}, nil
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q is %s at version %d, not UNFENCED at the expected version %d, nor FENCING or FENCED at it or the version after; read GetServingState and decide",
			tg, row.state, row.version, expected)
	}

	results, err := c.fanOut(ctx, db, tg, func(ctx context.Context, p poolerRef) *multipoolerservicepb.MemberFenceResult {
		return c.askOne(ctx, p, db, tg, row.version)
	})
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.StartFenceResponse{State: row.proto(tg), Members: results}
	if notOK := unreachable(results); len(notOK) > 0 {
		if c.logger != nil {
			c.logger.WarnContext(ctx, "fence started; a member did not answer", "tablegroup", tg, "version", row.version, "unreachable", len(notOK))
		}
		return nil, &FenceIncompleteError{
			message: fmt.Sprintf("tablegroup %q is FENCING at version %d but %d of %d members did not answer (%s); call StartFence again to reach them, or unfence at that version",
				tg, row.version, len(notOK), len(results), strings.Join(notOK, "; ")),
			Details: resp,
		}
	}
	return resp, nil
}

// CommitFence completes the current fence attempt, when every member holds nothing: it asks
// every member, from a fresh listing, to apply the row and say what it holds (which closes a
// member that missed the start), and writes FENCED, at the same version, only if all are clean. It
// waits for nothing. When one is not, it writes nothing and the error says what each member
// answered; the migrator asks again when it likes.
func (c *admissionCoordinator) CommitFence(ctx context.Context, req *multipoolerservicepb.CommitFenceRequest) (*multipoolerservicepb.CommitFenceResponse, error) {
	if req.GetTablegroup() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup is required")
	}
	if req.GetVersion() <= 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "version must be the version of a fence")
	}
	db, tg, version := req.GetDatabase(), req.GetTablegroup(), req.GetVersion()
	row, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, err
	}
	if row == nil {
		row = &servingRow{state: stateUnfenced}
	}
	// The rows this call accepts: FENCING at the version, which it commits, and FENCED at
	// it, which is success. Nothing else is a fence of that version.
	switch {
	case row.state == stateFenced && row.version == version:
		return &multipoolerservicepb.CommitFenceResponse{State: row.proto(tg)}, nil
	case row.state == stateFencing && row.version == version:
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q is %s at version %d, not FENCING or FENCED at the version %d to commit; read GetServingState and decide",
			tg, row.state, row.version, version)
	}

	results, err := c.fanOut(ctx, db, tg, func(ctx context.Context, p poolerRef) *multipoolerservicepb.MemberFenceResult {
		return c.askOne(ctx, p, db, tg, version)
	})
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.CommitFenceResponse{State: row.proto(tg), Members: results}
	if notOK := notCompleted(results, multipoolerservicepb.MemberFenceResult_STATUS_CLEAN); len(notOK) > 0 {
		// Another commit of this same fence may have completed it while the members were asked:
		// a member that already sees FENCED refuses the refresh of this one. If the row says
		// FENCED at the version, the fence is done, which is what this call was after.
		if current, rerr := c.store.GetRow(ctx, db, tg); rerr == nil && current != nil && current.state == stateFenced && current.version == version {
			resp.State = current.proto(tg)
			return resp, nil
		}
		if c.logger != nil {
			c.logger.InfoContext(ctx, "fence not complete; the tablegroup stays FENCING", "tablegroup", tg, "version", version, "not_clean", len(notOK))
		}
		return nil, &FenceIncompleteError{
			message: fmt.Sprintf("tablegroup %q is FENCING at version %d but %d of %d members are not clean (%s); call CommitFence again, or unfence at that version",
				tg, version, len(notOK), len(results), strings.Join(notOK, "; ")),
			Details: resp,
		}
	}

	// Every member is clean: the fence is complete, and only now is it FENCED. The version
	// does not change.
	if err := c.isLeader(); err != nil {
		return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"every member is clean, but this pooler is no longer the default primary, so it cannot complete the fence (%v); the row stays FENCING at version %d: call CommitFence through the new default primary", err, version)
	}
	// A false result means the row is no longer FENCING at this version. Another commit may
	// have completed this very fence first, which is success; the read below tells.
	if _, err := c.store.CompareAndSet(ctx, db, tg, stateFencing, version, stateFenced, version); err != nil {
		return nil, mterrors.Wrap(err, "persist the completed fence; outcome uncertain, re-read with GetServingState")
	}
	// The row is read again before success is reported: success needs FENCED at the version
	// the members closed for, whoever wrote it.
	current, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, mterrors.Wrap(err, "read back the fence; outcome uncertain, re-read with GetServingState")
	}
	if current == nil || current.state != stateFenced || current.version != version {
		return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"tablegroup %q is not FENCED at version %d; the serving state changed during the commit; re-read with GetServingState", tg, version)
	}
	resp.State = current.proto(tg)
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

// UnfenceTablegroup sets the row back to UNFENCED and asks the members to reopen on a
// best-effort basis. expected_version is the version the fence has, and the row may be
// FENCING or FENCED. A retry is recognized by the row its own success leaves, UNFENCED at
// the version after: it asks the members again and writes nothing.
func (c *admissionCoordinator) UnfenceTablegroup(ctx context.Context, req *multipoolerservicepb.UnfenceTablegroupRequest) (*multipoolerservicepb.UnfenceTablegroupResponse, error) {
	if req.GetTablegroup() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup is required")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	db, tg, expected := req.GetDatabase(), req.GetTablegroup(), req.GetExpectedVersion()
	row, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, err
	}
	if row == nil {
		row = &servingRow{state: stateUnfenced}
	}
	switch {
	case row.closed() && row.version == expected:
		if err := c.isLeader(); err != nil {
			return nil, err
		}
		ok, err := c.store.CompareAndSet(ctx, db, tg, row.state, expected, stateUnfenced, expected+1)
		if err != nil {
			return nil, mterrors.Wrap(err, "persist the unfence; outcome uncertain, re-read with GetServingState")
		}
		if !ok {
			return nil, mterrors.New(mtrpcpb.Code_ABORTED, "serving state changed concurrently; re-read and retry")
		}
		row.state, row.version, row.updatedAt = stateUnfenced, expected+1, time.Now()
	case row.state == stateUnfenced && row.version == expected+1:
		// Already UNFENCED at the version this unfence would have produced: done. Write
		// nothing; the members are asked again, since the reply that was lost may have
		// been the one that told them.
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q is %s at version %d, not FENCING or FENCED at the expected version %d, nor UNFENCED at the version after; read GetServingState and decide",
			tg, row.state, row.version, expected)
	}
	resp := &multipoolerservicepb.UnfenceTablegroupResponse{State: row.proto(tg)}
	for _, p := range c.reopen(ctx, db, tg, row.version) {
		resp.Unreached = append(resp.Unreached, p.id)
	}
	return resp, nil
}

// reopen asks every member of the tablegroup to re-read its row, which now says
// UNFENCED at version, on a best-effort basis, and returns the members it could not
// reach. Nothing waits for them: a pooler opens once it reads UNFENCED.
func (c *admissionCoordinator) reopen(ctx context.Context, db, tg string, version int64) []poolerRef {
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
				Database: db, Tablegroup: tg, ExpectedState: stateUnfenced, ExpectedVersion: version,
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
func (c *admissionCoordinator) askOne(ctx context.Context, p poolerRef, db, tg string, version int64) *multipoolerservicepb.MemberFenceResult {
	callCtx, cancel := context.WithTimeout(ctx, c.memberCallTimeout())
	defer cancel()
	resp, err := c.refresher.Refresh(callCtx, p, &multipoolerservicepb.RefreshAdmissionRequest{
		Database: db, Tablegroup: tg, ExpectedState: stateFencing, ExpectedVersion: version,
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
		if p.record.GetManagementMode() == clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
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

// FlipRouting atomically moves application traffic between two tablegroups whose fences
// are complete (FENCED); FENCING does not qualify.
// Success means the routing metadata committed; it does not unfence anything.
func (c *admissionCoordinator) FlipRouting(ctx context.Context, req *multipoolerservicepb.FlipRoutingRequest) (*multipoolerservicepb.FlipRoutingResponse, error) {
	from, to := req.GetFromTablegroup(), req.GetToTablegroup()
	if from == "" || to == "" || from == to {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "distinct from and to tablegroups are required")
	}
	if req.GetExpectedRoutingVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_routing_version must not be negative")
	}
	db := req.GetDatabase()
	app, version, found, err := c.store.GetRouting(ctx, db)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "database has no routing pointer")
	}
	if app == to && version == req.GetExpectedRoutingVersion()+1 {
		// A retry of a flip that committed: the traffic is already on to_tablegroup at the
		// version this flip produced. Nothing to write.
		return &multipoolerservicepb.FlipRoutingResponse{AppTablegroup: app, RoutingVersion: version}, nil
	}
	if app != from || version != req.GetExpectedRoutingVersion() {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing is %q at version %d, not %q at the expected version %d; read GetServingState and decide",
			app, version, from, req.GetExpectedRoutingVersion())
	}
	for _, name := range []string{from, to} {
		row, err := c.store.GetRow(ctx, db, name)
		if err != nil {
			return nil, err
		}
		switch {
		case row == nil || row.state == stateUnfenced:
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %q is UNFENCED, a flip requires both tablegroups FENCED", name)
		case row.state != stateFenced:
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"tablegroup %q is %s at version %d: its fence is not complete, a flip requires both tablegroups FENCED; retry the fence, or unfence it", name, row.state, row.version)
		}
	}
	if err := c.isLeader(); err != nil {
		return nil, err
	}
	app, version, moved, err := c.store.MoveRouting(ctx, db, from, to, req.GetExpectedRoutingVersion())
	if err != nil {
		return nil, mterrors.Wrap(err, "commit routing; outcome uncertain, re-read with GetServingState")
	}
	if !moved {
		return nil, mterrors.New(mtrpcpb.Code_ABORTED, "routing or admission state changed concurrently; re-read and retry")
	}
	return &multipoolerservicepb.FlipRoutingResponse{AppTablegroup: app, RoutingVersion: version}, nil
}
