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
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
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
// A fence commits FENCING first and then asks every member to close, drain and
// answer, and writes FENCED, at the same version, only when every member was clean.
// FENCING is therefore a fence that is in progress, that failed, or whose coordinator
// was lost; a flip refuses it, so a migrator that takes it for a completed fence still
// cannot move the traffic. Every member is asked, not only the writable one: a replica can be promoted
// while the fence runs, and one that read UNFENCED before FENCING was written would
// otherwise be left open. That is closed by invariants enforced elsewhere (see
// fanOutFence). Every member is asked even when one fails, and each answers clean, busy
// or unreachable; the fence succeeds only if all are clean. A fence never undoes
// itself: when it cannot complete, the row stays FENCING and the per-member results tell
// the caller why, and the caller retries the same fence or unfences it. An unfence has
// no acknowledgement barrier: a pooler opens once it reads UNFENCED.
//
// A coordinator that dies, or stops being the default primary, in the middle of a fence
// leaves the tablegroup FENCING. Nothing in the system undoes that: the migrator is the
// one waiting on the call, gets the error, reads GetServingState and, if the tablegroup is
// fenced and the cutover has not flipped, unfences it at the version it read (see the
// migrator's on-error policy in proto/multipoolerservice.proto).

const (
	// defaultFenceOverhead is added to the drain timeout to bound a whole fence when the
	// request names no timeout.
	defaultFenceOverhead = 30 * time.Second
	// memberCallMargin is what a member is given on top of the drain timeout and its
	// look at the server, to answer in.
	memberCallMargin = 3 * time.Second
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
	// callTimeout replaces a member's call timeout of drain + look + margin, and
	// reopenTimeout the reopen's (tests).
	callTimeout, reopenTimeout time.Duration
}

// memberCallTimeout is what one member is given to answer a fence: its drain, its look at
// the server, and a margin.
func (c *admissionCoordinator) memberCallTimeout(drain time.Duration) time.Duration {
	if c.callTimeout > 0 {
		return c.callTimeout
	}
	return drain + poolerserver.BackendCheckBudget + memberCallMargin
}

func (c *admissionCoordinator) reopenCallTimeout() time.Duration {
	if c.reopenTimeout > 0 {
		return c.reopenTimeout
	}
	return reopenCallTimeout
}

// FenceIncompleteError is the error of a fence some member did not complete: ABORTED, with
// the per-member results as the details of its status. The row stays FENCING.
type FenceIncompleteError struct {
	message  string
	Response *multipoolerservicepb.FenceTablegroupResponse
}

func (e *FenceIncompleteError) Error() string { return e.message }

// ErrorCode is ABORTED.
func (e *FenceIncompleteError) ErrorCode() mtrpcpb.Code { return mtrpcpb.Code_ABORTED }

// GRPCStatus carries the per-member results to the caller.
func (e *FenceIncompleteError) GRPCStatus() *status.Status {
	st := status.New(codes.Aborted, e.message)
	if withDetails, err := st.WithDetails(e.Response); err == nil {
		return withDetails
	}
	return st
}

// FenceTablegroup fences one tablegroup and returns OK only when every member has closed
// and drained and the row says FENCED. A fence that cannot complete never undoes itself:
// the row stays FENCING, and the error says what each member answered.
//
// A tablegroup whose durability policy, or the control home's, needs fewer than two nodes
// is refused before anything is written: with one node there is nothing that keeps the
// tablegroup available while its only node is replaced. So is a tablegroup that has more
// than one unmanaged pooler: exactly one is supported per tablegroup.
func (c *admissionCoordinator) FenceTablegroup(ctx context.Context, req *multipoolerservicepb.FenceTablegroupRequest) (*multipoolerservicepb.FenceTablegroupResponse, error) {
	if req.GetTablegroup() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "tablegroup is required")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_version must not be negative")
	}
	drain := defaultAdmissionDrainTimeout
	if d := req.GetDrainTimeout().AsDuration(); d > 0 {
		drain = d
	}
	timeout := drain + defaultFenceOverhead
	if d := req.GetTimeout().AsDuration(); d > 0 {
		timeout = d
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db, tg, expected := req.GetDatabase(), req.GetTablegroup(), req.GetExpectedVersion()
	row, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, err
	}
	if row == nil {
		// Never fenced: UNFENCED at version zero.
		row = &servingRow{state: stateUnfenced}
	}
	// The rows this call accepts are the ones its own success, or a retry of it, leaves:
	//   UNFENCED at v:            write FENCING at v+1, then ask the members;
	//   FENCING at v+1 or at v:   a retry, a resume, or a caller that re-read: ask the members
	//                             again, and write FENCED if all are clean;
	//   FENCED at v+1 or at v:    already done: succeed. The members are asked again on a
	//                             best-effort basis, and one that is busy or unreachable
	//                             now is reported in the response, not made an error.
	// Anything else is rejected, so a delayed retry of a fence that was undone cannot fence
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
		row.state, row.version = stateFencing, expected+1
	case (row.state == stateFencing || row.state == stateFenced) && (row.version == expected || row.version == expected+1):
		// A resume, or a retry of a call whose reply was lost, or a fence already done.
	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"tablegroup %q is %s at version %d, not UNFENCED at the expected version %d, nor FENCING or FENCED at it or the version after; read GetServingState and decide",
			tg, row.state, row.version, expected)
	}

	results, err := c.fanOutFence(ctx, db, tg, row.state, row.version, drain)
	if err != nil {
		return nil, err
	}
	resp := &multipoolerservicepb.FenceTablegroupResponse{Members: results}

	var notClean []string
	for _, m := range results {
		if m.GetStatus() != multipoolerservicepb.MemberFenceResult_STATUS_CLEAN {
			notClean = append(notClean, fmt.Sprintf("%s: %s: %s", m.GetPoolerId().GetName(), statusText(m.GetStatus()), m.GetDetail()))
		}
	}
	if len(notClean) > 0 && row.state == stateFenced {
		// The fence was committed by a call that saw every member clean; a member that is
		// busy or unreachable now does not undo it. The response says who.
		if c.logger != nil {
			c.logger.WarnContext(ctx, "fence already complete; a member is not clean now", "tablegroup", tg, "version", row.version, "not_clean", len(notClean))
		}
	} else if len(notClean) > 0 {
		if c.logger != nil {
			c.logger.WarnContext(ctx, "fence incomplete; the tablegroup stays FENCING", "tablegroup", tg, "version", row.version, "not_clean", len(notClean))
		}
		row.updatedAt = time.Now()
		resp.State = row.proto(tg)
		return nil, &FenceIncompleteError{
			message: fmt.Sprintf("tablegroup %q is %s at version %d but %d of %d members are not clean (%s); retry the same fence, or unfence it at that version",
				tg, row.state, row.version, len(notClean), len(results), strings.Join(notClean, "; ")),
			Response: resp,
		}
	}

	// Every member is clean: the fence is complete, and only now is it FENCED. The version
	// does not change.
	if row.state == stateFencing {
		if err := c.isLeader(); err != nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED,
				"every member is clean, but this pooler is no longer the default primary, so it cannot complete the fence (%v); the row stays FENCING at version %d: retry the same fence through the new default primary", err, row.version)
		}
		// A false result means the row is no longer FENCING at this version. Another resume
		// may have completed this very fence first, which is success; the read below tells.
		if _, err := c.store.CompareAndSet(ctx, db, tg, stateFencing, row.version, stateFenced, row.version); err != nil {
			return nil, mterrors.Wrap(err, "persist the completed fence; outcome uncertain, re-read with GetServingState")
		}
	}

	// The row is read again before success is reported: success needs FENCED at the version
	// the members closed for, whoever wrote it.
	current, err := c.store.GetRow(ctx, db, tg)
	if err != nil {
		return nil, mterrors.Wrap(err, "read back the fence; outcome uncertain, re-read with GetServingState")
	}
	if current == nil || current.state != stateFenced || current.version != row.version {
		return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED,
			"tablegroup %q is not FENCED at version %d; the serving state changed during the fence; re-read with GetServingState", tg, row.version)
	}
	resp.State = current.proto(tg)
	return resp, nil
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

// fanOutFence asks every member of the tablegroup to close at the given version and
// returns one result per member, clean, busy or unreachable. Nobody is skipped because
// another member failed. Membership is re-enumerated until it is stable, so a pooler that
// joins while the fence runs is asked too.
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
func (c *admissionCoordinator) fanOutFence(ctx context.Context, db, tg string, state multipoolerservicepb.AdmissionState, version int64, drain time.Duration) ([]*multipoolerservicepb.MemberFenceResult, error) {
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
				result := c.fenceOne(ctx, p, db, tg, state, version, drain)
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

// fenceOne asks one member to close at the version and says what it answered.
func (c *admissionCoordinator) fenceOne(ctx context.Context, p poolerRef, db, tg string, state multipoolerservicepb.AdmissionState, version int64, drain time.Duration) *multipoolerservicepb.MemberFenceResult {
	result := func(s multipoolerservicepb.MemberFenceResult_Status, detail string) *multipoolerservicepb.MemberFenceResult {
		return &multipoolerservicepb.MemberFenceResult{PoolerId: p.id, Status: s, Detail: detail}
	}
	callCtx, cancel := context.WithTimeout(ctx, c.memberCallTimeout(drain))
	defer cancel()
	resp, err := c.refresher.Refresh(callCtx, p, &multipoolerservicepb.RefreshAdmissionRequest{
		Database: db, Tablegroup: tg, ExpectedState: state, ExpectedVersion: version,
		DrainTimeout: durationpb.New(drain),
	})
	if err != nil {
		if isBusy(err) {
			return result(multipoolerservicepb.MemberFenceResult_STATUS_BUSY, err.Error())
		}
		return result(multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, err.Error())
	}
	// The answer must be exactly the pooler we asked, for exactly the fence we are running.
	switch {
	case !proto.Equal(resp.GetPoolerId(), p.id):
		return result(multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, "answer from a different pooler")
	case resp.GetState() != state || resp.GetVersion() != version:
		return result(multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE,
			fmt.Sprintf("inexact answer: %s at version %d", resp.GetState(), resp.GetVersion()))
	}
	return result(multipoolerservicepb.MemberFenceResult_STATUS_CLEAN, "")
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

// isBusy reports whether a pooler's answer to a fence refresh says it is busy: work is
// still running at its drain deadline, or it cannot rule out a backend it lost track of.
func isBusy(err error) bool {
	return mterrors.Code(err) == mtrpcpb.Code_RESOURCE_EXHAUSTED || status.Code(err) == codes.ResourceExhausted
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
