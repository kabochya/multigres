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

// Package fakecontrol is an in-memory implementation of the routing and admission RPCs of
// MultipoolerService (GetRoutingState, StartFence, CommitFence, UnfenceScope, SwitchRouting,
// RevertRouting, FoldRouting, CleanupOperation). It follows the contract documented in
// proto/multipoolerservice.proto, and the semantics of the coordinator on the default primary,
// closely enough for a migration controller to develop and test its cutover, rollback, fold and
// cleanup sequences without a cluster.
//
// PROTOTYPE STUB: delete this package when a controller can test against the real implementation.
//
// A cutover from tablegroup "migrateTG" to "destTG", then its fold and cleanup. Every call names the
// operation, a UUID the controller chooses:
//
//	fake := fakecontrol.New()
//	fake.AddTablegroup("db", "migrateTG", "unmanaged-1")
//	fake.AddTablegroup("db", "destTG", "dest-1", "dest-2", "dest-3")
//	fake.SetHome("db", "migrateTG")
//	addr := fakecontrol.Start(t, fake)
//	client := multipoolerservicepb.NewMultipoolerServiceClient(dial(addr))
//
//	op := uuid.NewString()
//	scope := func(tg string) *AdmissionScope { return &AdmissionScope{Tablegroup: tg} }
//	state, _ := client.GetRoutingState(ctx, &GetRoutingStateRequest{Database: "db"})
//	// A scope that is not in state.Fences has no fence: version 0. Start the fence of the source
//	// and of the destination, each naming the version read, and commit each at the version the
//	// start returned.
//	src, _ := client.StartFence(ctx, &StartFenceRequest{Database: "db", OperationId: op, Scope: scope("migrateTG")})
//	dst, _ := client.StartFence(ctx, &StartFenceRequest{Database: "db", OperationId: op, Scope: scope("destTG")})
//	_, _ = client.CommitFence(ctx, &CommitFenceRequest{Database: "db", OperationId: op, Scope: scope("migrateTG"), Version: src.Fence.Version})
//	_, _ = client.CommitFence(ctx, &CommitFenceRequest{Database: "db", OperationId: op, Scope: scope("destTG"), Version: dst.Fence.Version})
//	// ... read the source's WAL position from the source itself and catch the destination up ...
//	_, _ = client.SwitchRouting(ctx, &SwitchRoutingRequest{Database: "db", OperationId: op, ExpectedRoutingVersion: state.RoutingVersion,
//		Rule: &RoutingRule{FromTablegroup: "migrateTG", ToTablegroup: "destTG", ToShard: "0-inf"}})
//	// Unfence names the version the fence has. The fence stays, UNFENCED, at the next version.
//	_, _ = client.UnfenceScope(ctx, &UnfenceScopeRequest{Database: "db", OperationId: op, Scope: scope("destTG"), ExpectedVersion: dst.Fence.Version})
//	// When the migration is complete, fold the rule into the catalog. The source stays FENCED.
//	_, _ = client.FoldRouting(ctx, &FoldRoutingRequest{Database: "db", OperationId: op, ExpectedRoutingVersion: state.RoutingVersion + 1})
//	// Once the source's poolers are decommissioned, delete the operation's fences. This reopens
//	// a fenced scope whose poolers are still running.
//	_, _ = client.CleanupOperation(ctx, &CleanupOperationRequest{Database: "db", OperationId: op})
//
// A scope has at most one fence, held by one operation. Another operation can take a scope over
// only when the holder's fence is UNFENCED, by starting a fence at the version it read; a scope
// that is FENCING or FENCED for another operation answers FAILED_PRECONDITION.
//
// To exercise a fence that cannot complete, make a pooler unreachable with
// FailRefresh("unmanaged-1", true) or busy with BusyMember("unmanaged-1", "..."): the start (for an
// unreachable member) or the commit (for an unreachable or a busy one) asks every member, returns
// ABORTED with the per-member results in the error's details, and the tablegroup stays FENCING (a
// switch refuses it), for the caller to ask again or unfence it. A start does not fail for a busy
// member, it reports it. Clear the fault and repeat the call.
//
// Not reproduced: the durability-policy and one-unmanaged-pooler checks of StartFence, the
// existence of the destination shard of a rule (it must be named, not found), and the membership
// of a tablegroup changing while a call runs: members are the fixed list given to AddTablegroup.
package fakecontrol

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

const (
	stateUnfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	stateFencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	stateFenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
)

// fenceRow is the persisted fence of one scope: held by one operation, at one state and version.
type fenceRow struct {
	operationID string
	state       multipoolerservicepb.AdmissionState
	version     int64
	updatedAt   time.Time
}

func (f *fenceRow) proto(tablegroup string) *multipoolerservicepb.AdmissionFence {
	return &multipoolerservicepb.AdmissionFence{
		OperationId: f.operationID,
		Scope:       &multipoolerservicepb.AdmissionScope{Tablegroup: tablegroup},
		State:       f.state,
		Version:     f.version,
		UpdatedAt:   timestamppb.New(f.updatedAt),
	}
}

// closed reports whether the scope refuses application work. A scope with no fence is open.
func (f *fenceRow) closed() bool { return f != nil && f.state != stateUnfenced }

// ruleRow is a persisted routing rule.
type ruleRow struct {
	operationID    string
	from, to       string
	toShard        string
	createdVersion int64
}

func (r *ruleRow) proto() *multipoolerservicepb.RoutingRuleState {
	return &multipoolerservicepb.RoutingRuleState{
		OperationId:    r.operationID,
		Rule:           &multipoolerservicepb.RoutingRule{FromTablegroup: r.from, ToTablegroup: r.to, ToShard: r.toShard},
		CreatedVersion: r.createdVersion,
	}
}

type database struct {
	// tablegroups are the catalog's tablegroups, with the names of their members.
	tablegroups map[string][]string
	// fences are the fences by tablegroup. A tablegroup with no entry has no fence.
	fences     map[string]*fenceRow
	home       string
	routingVer int64
	rules      []*ruleRow
}

// effective resolves where the traffic goes: the home tablegroup, or, when a rule applies to it,
// the rule's destination, applied once.
func (d *database) effective() string {
	for _, r := range d.rules {
		if r.from == d.home {
			return r.to
		}
	}
	return d.home
}

func (d *database) ruleOf(op string) *ruleRow {
	for _, r := range d.rules {
		if r.operationID == op {
			return r
		}
	}
	return nil
}

func (d *database) deleteRule(op string) {
	kept := d.rules[:0]
	for _, r := range d.rules {
		if r.operationID != op {
			kept = append(kept, r)
		}
	}
	d.rules = kept
}

// Server is the in-memory fake. Its zero value is not usable; call New.
type Server struct {
	multipoolerservicepb.UnimplementedMultipoolerServiceServer

	mu          sync.Mutex
	databases   map[string]*database
	refreshFail map[string]bool
	busy        map[string]string
	busyDetails map[string]*multipoolerservicepb.FenceBusy
}

// New returns an empty fake.
func New() *Server {
	return &Server{databases: map[string]*database{}, refreshFail: map[string]bool{}, busy: map[string]string{}, busyDetails: map[string]*multipoolerservicepb.FenceBusy{}}
}

// AddTablegroup registers a tablegroup in the catalog, with no fence. poolers are the names of the
// members that must acknowledge a fence.
func (s *Server) AddTablegroup(db, tablegroup string, poolers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db(db).tablegroups[tablegroup] = poolers
}

// SetHome sets the home tablegroup, the one that receives the application traffic when no rule
// applies to it, without bumping the routing version, as initial seeding.
func (s *Server) SetHome(db, tablegroup string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db(db).home = tablegroup
}

// FailRefresh makes the named pooler stop acknowledging admission changes, as if
// it were down or partitioned. Clear it with fail=false.
func (s *Server) FailRefresh(pooler string, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshFail[pooler] = fail
}

// BusyMember makes the named pooler answer a commit with busy, saying detail. Clear it with an
// empty detail.
func (s *Server) BusyMember(pooler, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if detail == "" {
		delete(s.busy, pooler)
		return
	}
	s.busy[pooler] = detail
}

// BusyMemberHeldBy makes the named pooler answer a commit with busy, saying detail and what
// holds it. Clear it with an empty detail.
func (s *Server) BusyMemberHeldBy(pooler, detail string, held *multipoolerservicepb.FenceBusy) {
	s.BusyMember(pooler, detail)
	s.mu.Lock()
	defer s.mu.Unlock()
	if detail == "" {
		delete(s.busyDetails, pooler)
		return
	}
	s.busyDetails[pooler] = held
}

func (s *Server) db(name string) *database {
	d, ok := s.databases[name]
	if !ok {
		d = &database{tablegroups: map[string][]string{}, fences: map[string]*fenceRow{}}
		s.databases[name] = d
	}
	return d
}

func poolerID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "fake", Name: name}
}

// operation checks the operation id and returns it in canonical form.
func operation(operationID string) (string, error) {
	id, err := uuid.Parse(operationID)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "operation_id must be a UUID: %v", err)
	}
	return id.String(), nil
}

// requestScope checks the operation and the scope of a request and returns the operation and the
// tablegroup the scope names. Only a whole tablegroup is a scope.
func requestScope(operationID string, scope *multipoolerservicepb.AdmissionScope) (op, tablegroup string, err error) {
	op, err = operation(operationID)
	if err != nil {
		return "", "", err
	}
	if scope.GetTablegroup() == "" {
		return "", "", status.Error(codes.InvalidArgument, "the tablegroup of the scope is required")
	}
	if scope.GetShard() != "" || scope.GetTable() != "" {
		return "", "", status.Error(codes.Unimplemented, "only a tablegroup scope is supported: shard and table scopes are not implemented")
	}
	return op, scope.GetTablegroup(), nil
}

// describe says what a scope's fence is, for a message.
func describe(f *fenceRow) string {
	if f == nil {
		return "unfenced with no fence (version 0)"
	}
	return fmt.Sprintf("%s at version %d, held by operation %s", f.state, f.version, f.operationID)
}

// checkKnown is the NOT_FOUND of a tablegroup the catalog does not have. It is checked only when the
// scope has no fence: a fence is proof that the tablegroup exists.
func (d *database) checkKnown(tablegroup string) error {
	if _, ok := d.tablegroups[tablegroup]; !ok {
		return status.Errorf(codes.NotFound, "tablegroup %q is not in the catalog", tablegroup)
	}
	return nil
}

// GetRoutingState implements the RPC: the home tablegroup, the rules and the fences, in one
// snapshot. When since_version is the current routing version the home and the rules are
// omitted; the fences are always returned. A scope with no fence is not listed.
func (s *Server) GetRoutingState(_ context.Context, req *multipoolerservicepb.GetRoutingStateRequest) (*multipoolerservicepb.GetRoutingStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.databases[req.GetDatabase()]
	if !ok {
		return &multipoolerservicepb.GetRoutingStateResponse{}, nil
	}
	resp := &multipoolerservicepb.GetRoutingStateResponse{RoutingVersion: d.routingVer}
	if req.GetSinceVersion() > 0 && req.GetSinceVersion() == d.routingVer {
		resp.NotModified = true
	} else {
		resp.HomeTablegroup = d.home
		for _, r := range d.rules {
			resp.Rules = append(resp.Rules, r.proto())
		}
	}
	names := make([]string, 0, len(d.fences))
	for name := range d.fences {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		resp.Fences = append(resp.Fences, d.fences[name].proto(name))
	}
	return resp, nil
}

// incomplete is the ABORTED error of a start or a commit that some member did not complete,
// with the per-member results as the details of its status.
func incomplete(message string, details *multipoolerservicepb.StartFenceResponse, commit *multipoolerservicepb.CommitFenceResponse) error {
	var st *status.Status
	var err error
	if commit != nil {
		st, err = status.New(codes.Aborted, message).WithDetails(commit)
	} else {
		st, err = status.New(codes.Aborted, message).WithDetails(details)
	}
	if err != nil {
		return status.Error(codes.Aborted, message)
	}
	return st.Err()
}

// StartFence implements the RPC, including the version and ownership rules. Unfenced at the
// version (no fence, or UNFENCED, even another operation's) it writes FENCING at the next version for
// the operation and asks every member, which it does not wait on; a member answers clean or busy,
// and when one is unreachable the call fails with ABORTED and leaves the row FENCING. For the
// operation's own FENCING fence, starting again asks the members once more and writes nothing; its
// own fence that is already complete (FENCED at the version or the one after) succeeds and asks
// nobody. Anything else, including a fence that is FENCING or FENCED for another operation, is
// FAILED_PRECONDITION.
func (s *Server) StartFence(_ context.Context, req *multipoolerservicepb.StartFenceRequest) (*multipoolerservicepb.StartFenceResponse, error) {
	op, tg, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetExpectedVersion() < 0 {
		return nil, status.Error(codes.InvalidArgument, "expected_version must not be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	cur := d.fences[tg]
	if cur == nil {
		if err := d.checkKnown(tg); err != nil {
			return nil, err
		}
	}
	curVersion := int64(0)
	if cur != nil {
		curVersion = cur.version
	}
	expected := req.GetExpectedVersion()
	own := cur != nil && cur.operationID == op
	switch {
	case (cur == nil || cur.state == stateUnfenced) && curVersion == expected:
		cur = &fenceRow{operationID: op, state: stateFencing, version: expected + 1, updatedAt: time.Now()}
		d.fences[tg] = cur
	case own && cur.state == stateFencing && (cur.version == expected || cur.version == expected+1):
		// The attempt, repeated: ask every member again.
	case own && cur.state == stateFenced && (cur.version == expected || cur.version == expected+1):
		return &multipoolerservicepb.StartFenceResponse{Fence: cur.proto(tg)}, nil
	default:
		return nil, status.Errorf(codes.FailedPrecondition,
			"scope %q is %s for operation %s, not unfenced at the expected version %d, nor FENCING or FENCED for the operation at it or the version after",
			tg, describe(cur), op, expected)
	}
	resp := &multipoolerservicepb.StartFenceResponse{Fence: cur.proto(tg), Members: s.ask(d.tablegroups[tg])}
	for _, m := range resp.Members {
		if m.Status == multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE {
			return nil, incomplete(fmt.Sprintf("scope %q is %s at version %d but not every member answered", tg, cur.state, cur.version), resp, nil)
		}
	}
	return resp, nil
}

// ask is what every member answers to a fence: clean, busy with what the fake was told it holds, or
// unreachable.
func (s *Server) ask(poolers []string) []*multipoolerservicepb.MemberFenceResult {
	var members []*multipoolerservicepb.MemberFenceResult
	for _, p := range poolers {
		m := &multipoolerservicepb.MemberFenceResult{PoolerId: poolerID(p), Status: multipoolerservicepb.MemberFenceResult_STATUS_CLEAN}
		switch {
		case s.refreshFail[p]:
			m.Status, m.Detail = multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE, "did not answer"
		case s.busy[p] != "":
			m.Status, m.Detail = multipoolerservicepb.MemberFenceResult_STATUS_BUSY, s.busy[p]
			m.Busy = s.busyDetails[p]
		}
		members = append(members, m)
	}
	return members
}

// CommitFence implements the RPC. It asks every member what it holds and writes FENCED, at the same
// version, only when all are clean; when one is not it fails with ABORTED and leaves the row FENCING.
// The operation's own fence that is already FENCED at the version succeeds and asks nobody.
func (s *Server) CommitFence(_ context.Context, req *multipoolerservicepb.CommitFenceRequest) (*multipoolerservicepb.CommitFenceResponse, error) {
	op, tg, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetVersion() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "version must be the version of a fence")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	cur := d.fences[tg]
	if cur == nil {
		if err := d.checkKnown(tg); err != nil {
			return nil, err
		}
	}
	own := cur != nil && cur.operationID == op
	switch {
	case own && cur.state == stateFenced && cur.version == req.GetVersion():
		return &multipoolerservicepb.CommitFenceResponse{Fence: cur.proto(tg)}, nil
	case own && cur.state == stateFencing && cur.version == req.GetVersion():
	default:
		return nil, status.Errorf(codes.FailedPrecondition,
			"scope %q is %s for operation %s, not FENCING or FENCED for the operation at the version %d to commit",
			tg, describe(cur), op, req.GetVersion())
	}
	resp := &multipoolerservicepb.CommitFenceResponse{Members: s.ask(d.tablegroups[tg])}
	clean := true
	for _, m := range resp.Members {
		clean = clean && m.Status == multipoolerservicepb.MemberFenceResult_STATUS_CLEAN
	}
	if clean {
		cur.state, cur.updatedAt = stateFenced, time.Now()
	}
	resp.Fence = cur.proto(tg)
	if !clean {
		return nil, incomplete(fmt.Sprintf("scope %q is %s at version %d but not every member is clean", tg, cur.state, cur.version), nil, resp)
	}
	return resp, nil
}

// UnfenceScope implements the RPC: the operation's own FENCING or FENCED fence at the expected
// version becomes UNFENCED at the next version, and stays until CleanupOperation. Its own UNFENCED
// fence at the version after is a retry: nothing is written.
func (s *Server) UnfenceScope(_ context.Context, req *multipoolerservicepb.UnfenceScopeRequest) (*multipoolerservicepb.UnfenceScopeResponse, error) {
	op, tg, err := requestScope(req.GetOperationId(), req.GetScope())
	if err != nil {
		return nil, err
	}
	if req.GetExpectedVersion() < 0 {
		return nil, status.Error(codes.InvalidArgument, "expected_version must not be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	cur := d.fences[tg]
	if cur == nil {
		if err := d.checkKnown(tg); err != nil {
			return nil, err
		}
	}
	own := cur != nil && cur.operationID == op
	expected := req.GetExpectedVersion()
	switch {
	case own && cur.closed() && cur.version == expected:
		cur.state, cur.updatedAt = stateUnfenced, time.Now()
		cur.version++
	case own && cur.state == stateUnfenced && cur.version == expected+1:
		// Already unfenced at the version this unfence would have produced: done, write nothing.
	default:
		return nil, status.Errorf(codes.FailedPrecondition,
			"scope %q is %s for operation %s, not FENCING or FENCED for the operation at the expected version %d, nor UNFENCED at the version after",
			tg, describe(cur), op, expected)
	}
	resp := &multipoolerservicepb.UnfenceScopeResponse{Fence: cur.proto(tg)}
	for _, p := range d.tablegroups[tg] {
		if s.refreshFail[p] {
			resp.Unreached = append(resp.Unreached, poolerID(p))
		}
	}
	return resp, nil
}

// requireFenced checks that the fence of the tablegroup is FENCED for the operation.
func (d *database) requireFenced(op, tablegroup string) error {
	f := d.fences[tablegroup]
	switch {
	case f == nil || f.state == stateUnfenced:
		return status.Errorf(codes.FailedPrecondition, "scope %q is unfenced, a routing change requires both tablegroups FENCED for the operation", tablegroup)
	case f.operationID != op:
		return status.Errorf(codes.FailedPrecondition, "scope %q is held by operation %s, not by %s", tablegroup, f.operationID, op)
	case f.state != stateFenced:
		return status.Errorf(codes.FailedPrecondition, "scope %q is %s at version %d: its fence is not complete, a routing change requires both tablegroups FENCED", tablegroup, f.state, f.version)
	}
	return nil
}

func (d *database) checkRoutingVersion(expected int64) error {
	if d.routingVer != expected {
		return status.Errorf(codes.FailedPrecondition, "routing is at version %d, not the expected version %d", d.routingVer, expected)
	}
	return nil
}

// SwitchRouting implements the RPC: the rule is added, and the routing version bumped, only when
// the routing is at the expected version, the rule moves the home tablegroup (not the default one),
// no rule exists and both tablegroups are FENCED for the operation. A retry that finds the
// operation's rule at the version after the expected one succeeds and writes nothing.
func (s *Server) SwitchRouting(_ context.Context, req *multipoolerservicepb.SwitchRoutingRequest) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	op, err := operation(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	want := &ruleRow{operationID: op, from: req.GetRule().GetFromTablegroup(), to: req.GetRule().GetToTablegroup(), toShard: req.GetRule().GetToShard()}
	if want.from == "" || want.to == "" || want.toShard == "" || want.from == want.to {
		return nil, status.Error(codes.InvalidArgument, "a rule with distinct from and to tablegroups and a to_shard is required")
	}
	expected := req.GetExpectedRoutingVersion()
	if expected < 0 {
		return nil, status.Error(codes.InvalidArgument, "expected_routing_version must not be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	if cur := d.ruleOf(op); cur != nil {
		if cur.from == want.from && cur.to == want.to && cur.toShard == want.toShard && cur.createdVersion == expected+1 && d.routingVer == expected+1 {
			return &multipoolerservicepb.SwitchRoutingResponse{RoutingVersion: d.routingVer, EffectiveTablegroup: d.effective()}, nil
		}
		return nil, status.Errorf(codes.FailedPrecondition, "operation %s already has the rule %q to %q, switched at routing version %d", op, cur.from, cur.to, cur.createdVersion)
	}
	for _, name := range []string{want.from, want.to} {
		if err := d.checkKnown(name); err != nil {
			return nil, err
		}
	}
	switch {
	case d.home == "":
		return nil, status.Error(codes.FailedPrecondition, "database has no catalog state: there is no home tablegroup to route from")
	case d.routingVer != expected:
		return nil, d.checkRoutingVersion(expected)
	case want.from != d.home:
		return nil, status.Errorf(codes.FailedPrecondition, "the rule moves the traffic of %q, but the home tablegroup is %q", want.from, d.home)
	case want.from == constants.DefaultTableGroup:
		return nil, status.Error(codes.FailedPrecondition, "the traffic of the default tablegroup cannot be routed away")
	case len(d.rules) > 0:
		return nil, status.Errorf(codes.FailedPrecondition, "routing already has the rule of operation %s (%q to %q): at most one rule applies at a time", d.rules[0].operationID, d.rules[0].from, d.rules[0].to)
	}
	for _, name := range []string{want.from, want.to} {
		if err := d.requireFenced(op, name); err != nil {
			return nil, err
		}
	}
	d.routingVer++
	want.createdVersion = d.routingVer
	d.rules = append(d.rules, want)
	return &multipoolerservicepb.SwitchRoutingResponse{RoutingVersion: d.routingVer, EffectiveTablegroup: want.to}, nil
}

// RevertRouting implements the RPC: the operation's rule is deleted, and the routing version bumped,
// only when the routing is at the expected version and both tablegroups of the rule are FENCED for
// the operation. A retry that finds no rule of the operation at the version after the expected one
// succeeds and writes nothing.
func (s *Server) RevertRouting(_ context.Context, req *multipoolerservicepb.RevertRoutingRequest) (*multipoolerservicepb.RevertRoutingResponse, error) {
	op, err := operation(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	expected := req.GetExpectedRoutingVersion()
	if expected < 0 {
		return nil, status.Error(codes.InvalidArgument, "expected_routing_version must not be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	r := d.ruleOf(op)
	if r == nil {
		if d.routingVer == expected+1 {
			return &multipoolerservicepb.RevertRoutingResponse{RoutingVersion: d.routingVer, EffectiveTablegroup: d.effective()}, nil
		}
		return nil, status.Errorf(codes.FailedPrecondition, "operation %s has no routing rule to revert", op)
	}
	if err := d.checkRoutingVersion(expected); err != nil {
		return nil, err
	}
	for _, name := range []string{r.from, r.to} {
		if err := d.requireFenced(op, name); err != nil {
			return nil, err
		}
	}
	d.deleteRule(op)
	d.routingVer++
	return &multipoolerservicepb.RevertRoutingResponse{RoutingVersion: d.routingVer, EffectiveTablegroup: d.effective()}, nil
}

// FoldRouting implements the RPC: the rule's destination becomes the home tablegroup, the rule is
// deleted and the routing version bumped, with no fence needed, only when the routing is at the
// expected version, the rule moves the home tablegroup, and the traffic goes where it went before.
// A retry that finds no rule of the operation at the version after the expected one succeeds and
// writes nothing.
func (s *Server) FoldRouting(_ context.Context, req *multipoolerservicepb.FoldRoutingRequest) (*multipoolerservicepb.FoldRoutingResponse, error) {
	op, err := operation(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	expected := req.GetExpectedRoutingVersion()
	if expected < 0 {
		return nil, status.Error(codes.InvalidArgument, "expected_routing_version must not be negative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	r := d.ruleOf(op)
	if r == nil {
		if d.routingVer == expected+1 {
			return &multipoolerservicepb.FoldRoutingResponse{RoutingVersion: d.routingVer, HomeTablegroup: d.home}, nil
		}
		return nil, status.Errorf(codes.FailedPrecondition, "operation %s has no routing rule to fold", op)
	}
	if err := d.checkRoutingVersion(expected); err != nil {
		return nil, err
	}
	if r.from != d.home {
		return nil, status.Errorf(codes.FailedPrecondition, "the rule of operation %s moves %q, which is not the home tablegroup %q", op, r.from, d.home)
	}
	folded := &database{home: r.to}
	for _, other := range d.rules {
		if other.operationID != op {
			folded.rules = append(folded.rules, other)
		}
	}
	if before, after := d.effective(), folded.effective(); before != after {
		return nil, status.Errorf(codes.FailedPrecondition, "folding the rule of operation %s would move the traffic from %q to %q", op, before, after)
	}
	d.deleteRule(op)
	d.home = r.to
	d.routingVer++
	return &multipoolerservicepb.FoldRoutingResponse{RoutingVersion: d.routingVer, HomeTablegroup: d.home}, nil
}

// CleanupOperation implements the RPC: it deletes every fence of the operation, FENCED ones too, and
// says how many. It is refused while the operation has a routing rule or a FENCING fence.
func (s *Server) CleanupOperation(_ context.Context, req *multipoolerservicepb.CleanupOperationRequest) (*multipoolerservicepb.CleanupOperationResponse, error) {
	op, err := operation(req.GetOperationId())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	if r := d.ruleOf(op); r != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "operation %s still has the routing rule %q to %q: revert or fold it first", op, r.from, r.to)
	}
	for name, f := range d.fences {
		if f.operationID == op && f.state == stateFencing {
			return nil, status.Errorf(codes.FailedPrecondition, "operation %s has scope %q FENCING at version %d: complete or unfence it first", op, name, f.version)
		}
	}
	var deleted int32
	for name, f := range d.fences {
		if f.operationID == op {
			delete(d.fences, name)
			deleted++
		}
	}
	return &multipoolerservicepb.CleanupOperationResponse{FencesDeleted: deleted}, nil
}

// Start serves the fake on a loopback port for the life of the test and returns
// its address.
func Start(t testing.TB, s *Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	multipoolerservicepb.RegisterMultipoolerServiceServer(srv, s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}
