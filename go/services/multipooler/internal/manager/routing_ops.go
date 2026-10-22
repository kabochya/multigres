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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// The routing side of the admission coordinator: the operations that add, delete and fold the
// routing rule of an operation, and that delete its fences. Each decides on one snapshot, with
// messages that say what the state is, and the write re-checks the same conditions in the
// transaction that makes it, so that a change in between is ABORTED, never a wrong write.

// requireLive refuses a call that names an operation that was cleaned up. A cleaned operation is
// finished for good: a scope with no fence starts again at version zero, so a delayed call of the
// operation, replayed after the cleanup, would otherwise fence it again. The statements that write
// refuse it too, where it cannot be raced; this is for the message.
func (c *admissionCoordinator) requireLive(ctx context.Context, op string) error {
	cleaned, err := c.store.OperationCleaned(ctx, op)
	if err != nil {
		return err
	}
	if cleaned {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "operation %s was cleaned up; a new operation needs a new operation_id", op)
	}
	return nil
}

// describeOperation says where an operation is, for a message.
func describeOperation(o *operationRow) string {
	if o == nil {
		return "nothing is recorded of it"
	}
	return fmt.Sprintf("it is %s at routing version %d", o.state, o.version)
}

// requireFenced checks that the fence of the tablegroup is FENCED for the operation, a completed
// fence.
func requireFenced(snap *routingSnapshot, op, tablegroup string) error {
	f := snap.fenceOf(tablegroup)
	switch {
	case f == nil || f.state == stateUnfenced:
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"scope %q is unfenced, a routing change requires both tablegroups FENCED for the operation", tablegroup)
	case f.operationID != op:
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"scope %q is held by operation %s, not by %s", tablegroup, f.operationID, op)
	case f.state != stateFenced:
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"scope %q is %s at version %d: its fence is not complete, a routing change requires both tablegroups FENCED; retry the fence, or unfence it", tablegroup, f.state, f.version)
	}
	return nil
}

// SwitchRouting adds the routing rule of an operation, in one transaction that checks the
// preconditions (see the contract in proto/multipoolerservice.proto).
//
// Not checked yet, because the catalog has no such thing: that the operation exists as a whole
// (here it is the fences and the rule that carry its id), and that the rules cover the keyspace
// exactly, which a rule of a whole tablegroup does by construction.
func (c *admissionCoordinator) SwitchRouting(ctx context.Context, req *multipoolerservicepb.SwitchRoutingRequest) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	op, err := canonicalOperationID(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	rule := ruleRow{operationID: op, from: req.GetRule().GetFromTablegroup(), to: req.GetRule().GetToTablegroup(), toShard: req.GetRule().GetToShard()}
	if rule.from == "" || rule.to == "" || rule.toShard == "" || rule.from == rule.to {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "a rule with distinct from and to tablegroups and a to_shard is required")
	}
	expected := req.GetExpectedRoutingVersion()
	if expected < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_routing_version must not be negative")
	}
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireLive(ctx, op); err != nil {
		return nil, err
	}
	o := snap.operationOf(op)
	if cur := snap.ruleOf(op); cur != nil {
		if o != nil && o.state == operationSwitched && o.version == expected+1 &&
			cur.from == rule.from && cur.to == rule.to && cur.toShard == rule.toShard && cur.createdVersion == expected+1 {
			// A retry of a switch that committed: the operation was switched at the version this
			// switch produced, and the rule is the one it asked for. Nothing to write.
			return &multipoolerservicepb.SwitchRoutingResponse{RoutingVersion: o.version, EffectiveTablegroup: o.effective}, nil
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"operation %s already has the rule %q to %q, switched at routing version %d; read GetRoutingState and decide", op, cur.from, cur.to, cur.createdVersion)
	}
	if o != nil && o.state == operationFolded {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "operation %s was folded: its rule is gone for good", op)
	}
	for _, name := range []string{rule.from, rule.to} {
		if err := c.unknownTablegroup(ctx, name); err != nil {
			return nil, err
		}
	}
	ok, err := c.store.ShardExists(ctx, rule.to, rule.toShard)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "tablegroup %q has no shard %q", rule.to, rule.toShard)
	}
	switch {
	case snap.home == "":
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "database has no catalog state: there is no home tablegroup to route from")
	case snap.routingVersion != expected:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing is at version %d, not the expected version %d; read GetRoutingState and decide", snap.routingVersion, expected)
	case rule.from != snap.home:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"the rule moves the traffic of %q, but the home tablegroup is %q; read GetRoutingState and decide", rule.from, snap.home)
	case rule.from == constants.DefaultTableGroup:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "the traffic of the default tablegroup cannot be routed away")
	case len(snap.rules) > 0:
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing already has the rule of operation %s (%q to %q): at most one rule applies at a time", snap.rules[0].operationID, snap.rules[0].from, snap.rules[0].to)
	}
	for _, name := range []string{rule.from, rule.to} {
		if err := requireFenced(snap, op, name); err != nil {
			return nil, err
		}
	}
	if err := c.isLeader(); err != nil {
		return nil, err
	}
	version, ok, err := c.store.SwitchRouting(ctx, op, rule, expected)
	if err != nil {
		return nil, mterrors.Wrap(err, "commit routing; outcome uncertain, re-read with GetRoutingState")
	}
	if !ok {
		return nil, mterrors.New(mtrpcpb.Code_ABORTED, "routing or an admission fence changed concurrently; re-read and retry")
	}
	return &multipoolerservicepb.SwitchRoutingResponse{RoutingVersion: version, EffectiveTablegroup: rule.to}, nil
}

// RevertRouting deletes the routing rule of an operation, so that the traffic goes back to the
// home tablegroup, under the same fence preconditions as SwitchRouting, checked against the rule.
func (c *admissionCoordinator) RevertRouting(ctx context.Context, req *multipoolerservicepb.RevertRoutingRequest) (*multipoolerservicepb.RevertRoutingResponse, error) {
	op, err := canonicalOperationID(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	expected := req.GetExpectedRoutingVersion()
	if expected < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_routing_version must not be negative")
	}
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireLive(ctx, op); err != nil {
		return nil, err
	}
	rule := snap.ruleOf(op)
	if rule == nil {
		if o := snap.operationOf(op); o != nil && o.state == operationReverted && o.version == expected+1 {
			// A retry of a revert that committed: the operation was reverted at the version this
			// revert produced, and by nothing else (a fold leaves it FOLDED, and a version that
			// something else bumped is another one).
			return &multipoolerservicepb.RevertRoutingResponse{RoutingVersion: o.version, EffectiveTablegroup: o.effective}, nil
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"operation %s has no routing rule to revert (%s); read GetRoutingState and decide", op, describeOperation(snap.operationOf(op)))
	}
	if snap.routingVersion != expected {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing is at version %d, not the expected version %d; read GetRoutingState and decide", snap.routingVersion, expected)
	}
	for _, name := range []string{rule.from, rule.to} {
		if err := requireFenced(snap, op, name); err != nil {
			return nil, err
		}
	}
	if err := c.isLeader(); err != nil {
		return nil, err
	}
	version, ok, err := c.store.RevertRouting(ctx, op, expected)
	if err != nil {
		return nil, mterrors.Wrap(err, "commit routing; outcome uncertain, re-read with GetRoutingState")
	}
	if !ok {
		return nil, mterrors.New(mtrpcpb.Code_ABORTED, "routing or an admission fence changed concurrently; re-read and retry")
	}
	return &multipoolerservicepb.RevertRoutingResponse{RoutingVersion: version, EffectiveTablegroup: snap.home}, nil
}

// FoldRouting completes a switched operation: the rule's destination becomes the home tablegroup
// and the rule is deleted. It needs no fence, because where the traffic goes is the same before and
// after, which it checks: it resolves the traffic as it is, and as it will be, and refuses if they
// differ.
//
// PROTOTYPE STUB: this moves the home pointer. The routing-rule implementation that replaces it
// moves the catalog membership of the tablegroup.
func (c *admissionCoordinator) FoldRouting(ctx context.Context, req *multipoolerservicepb.FoldRoutingRequest) (*multipoolerservicepb.FoldRoutingResponse, error) {
	op, err := canonicalOperationID(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	expected := req.GetExpectedRoutingVersion()
	if expected < 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "expected_routing_version must not be negative")
	}
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireLive(ctx, op); err != nil {
		return nil, err
	}
	rule := snap.ruleOf(op)
	if rule == nil {
		if o := snap.operationOf(op); o != nil && o.state == operationFolded && o.version == expected+1 {
			// A retry of a fold that committed: the operation was folded at the version this fold
			// produced, and by nothing else.
			return &multipoolerservicepb.FoldRoutingResponse{RoutingVersion: o.version, HomeTablegroup: o.effective}, nil
		}
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"operation %s has no routing rule to fold (%s); read GetRoutingState and decide", op, describeOperation(snap.operationOf(op)))
	}
	if snap.routingVersion != expected {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"routing is at version %d, not the expected version %d; read GetRoutingState and decide", snap.routingVersion, expected)
	}
	if rule.from != snap.home {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"the rule of operation %s moves %q, which is not the home tablegroup %q", op, rule.from, snap.home)
	}
	folded := &routingSnapshot{home: rule.to}
	for _, other := range snap.rules {
		if other.operationID != op {
			folded.rules = append(folded.rules, other)
		}
	}
	if before, after := snap.effective(), folded.effective(); before != after {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"folding the rule of operation %s would move the traffic from %q to %q", op, before, after)
	}
	if err := c.isLeader(); err != nil {
		return nil, err
	}
	version, ok, err := c.store.FoldRouting(ctx, op, expected)
	if err != nil {
		return nil, mterrors.Wrap(err, "commit the fold; outcome uncertain, re-read with GetRoutingState")
	}
	if !ok {
		return nil, mterrors.New(mtrpcpb.Code_ABORTED, "routing changed concurrently; re-read and retry")
	}
	return &multipoolerservicepb.FoldRoutingResponse{RoutingVersion: version, HomeTablegroup: rule.to}, nil
}

// CleanupOperation deletes the fences of an operation and ends it for good: later calls that name
// it are refused. It is refused while the operation has a routing rule, or a fence that is
// FENCING, and it deletes nothing if a switch committed since the state was read.
//
// Deleting a fence does not tell a pooler anything: a pooler that decided to refuse application
// work keeps that decision until its gate generation changes (a restart, a change of role or of
// serving status) or a refresh names a fence, and nothing refreshes it here. So cleaning up an
// UNFENCED fence changes nothing for serving, and cleaning up a FENCED one is safe only once the
// poolers of its scope are gone: they would otherwise stay closed with no fence to say why, and a
// pooler that restarts would open. After a fold the source stays fenced until it is
// decommissioned, and is cleaned up after.
func (c *admissionCoordinator) CleanupOperation(ctx context.Context, req *multipoolerservicepb.CleanupOperationRequest) (*multipoolerservicepb.CleanupOperationResponse, error) {
	op, err := canonicalOperationID(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	snap, err := c.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	// An operation that was cleaned up has nothing to wait for: what is left of it is deleted, which
	// finishes a cleanup that stopped half way.
	cleaned, err := c.store.OperationCleaned(ctx, op)
	if err != nil {
		return nil, err
	}
	if rule := snap.ruleOf(op); rule != nil && !cleaned {
		return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"operation %s still has the routing rule %q to %q: revert or fold it first", op, rule.from, rule.to)
	}
	for _, f := range snap.fences {
		if f.operationID == op && f.state == stateFencing && !cleaned {
			return nil, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"operation %s has scope %q FENCING at version %d: complete or unfence it first", op, f.tablegroup, f.version)
		}
	}
	if err := c.isLeader(); err != nil {
		return nil, err
	}
	deleted, blocked, err := c.store.CleanupOperation(ctx, op, snap.routingVersion)
	if err != nil {
		return nil, mterrors.Wrap(err, "clean up the operation; outcome uncertain, call it again")
	}
	if blocked {
		return nil, mterrors.Errorf(mtrpcpb.Code_ABORTED, "operation %s changed concurrently; re-read and retry", op)
	}
	return &multipoolerservicepb.CleanupOperationResponse{FencesDeleted: int32(deleted)}, nil
}
