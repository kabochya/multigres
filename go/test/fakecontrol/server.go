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

// Package fakecontrol is an in-memory implementation of the serving-control
// RPCs of MultipoolerService (GetServingState, StartFence, CommitFence, UnfenceTablegroup,
// FlipRouting). It follows the contract documented in proto/multipoolerservice.proto
// closely enough for a migration controller to develop and test its cutover and
// rollback sequences before the real coordinator on the default primary exists.
//
// PROTOTYPE STUB: delete this package when a controller can test against the
// real implementation.
//
// A cutover from tablegroup "migrateTG" to "destTG":
//
//	fake := fakecontrol.New()
//	fake.AddTablegroup("db", "migrateTG", "srcDbConn", "unmanaged-1", "unmanaged-2")
//	fake.AddTablegroup("db", "destTG", "", "dest-1", "dest-2", "dest-3")
//	fake.SetAppTablegroup("db", "migrateTG")
//	addr := fakecontrol.Start(t, fake)
//	client := multipoolerservicepb.NewMultipoolerServiceClient(dial(addr))
//
//	state, _ := client.GetServingState(ctx, &GetServingStateRequest{Database: "db", Tablegroups: []string{"migrateTG", "destTG"}})
//	// Start the fence of the source and of the destination, each naming the version just read,
//	// and commit each at the version the start returned.
//	src, _ := client.StartFence(ctx, &StartFenceRequest{Database: "db", Tablegroup: "migrateTG", ExpectedVersion: state.Tablegroups[0].Version})
//	dst, _ := client.StartFence(ctx, &StartFenceRequest{Database: "db", Tablegroup: "destTG", ExpectedVersion: state.Tablegroups[1].Version})
//	_, _ = client.CommitFence(ctx, &CommitFenceRequest{Database: "db", Tablegroup: "migrateTG", Version: src.State.Version})
//	_, _ = client.CommitFence(ctx, &CommitFenceRequest{Database: "db", Tablegroup: "destTG", Version: dst.State.Version})
//	// ... read the source's WAL position from the source itself and catch the destination up ...
//	_, _ = client.FlipRouting(ctx, &FlipRoutingRequest{Database: "db", FromTablegroup: "migrateTG", ToTablegroup: "destTG", ExpectedRoutingVersion: state.RoutingVersion})
//	// Unfence names the version the fence has.
//	_, _ = client.UnfenceTablegroup(ctx, &UnfenceTablegroupRequest{Database: "db", Tablegroup: "destTG", ExpectedVersion: dst.State.Version})
//
// To exercise a fence that cannot complete, make a pooler unreachable with
// FailRefresh("unmanaged-2", true) or busy with BusyMember("unmanaged-2", "..."): the start (for an
// unreachable member) or the commit (for an unreachable or a busy one) asks every member, returns
// ABORTED with the per-member results in the error's details, and the tablegroup stays FENCING (a
// flip refuses it), for the caller to ask again or unfence it. A start does not fail for a busy
// member, it reports it. Clear the fault and repeat the call.
package fakecontrol

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

type row struct {
	backing   string
	state     multipoolerservicepb.AdmissionState
	version   int64
	updatedAt time.Time
	poolers   []string
}

type database struct {
	rows       map[string]*row
	app        string
	routingVer int64
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

// AddTablegroup registers a tablegroup, unfenced at version 0. backing names the
// external connection that backs it, empty for a managed cohort. poolers are the
// names of the members that must acknowledge a fence; the first is the writable
// one.
func (s *Server) AddTablegroup(db, tablegroup, backing string, poolers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(db)
	d.rows[tablegroup] = &row{backing: backing, state: stateUnfenced, updatedAt: time.Now(), poolers: poolers}
}

// SetAppTablegroup sets the tablegroup that receives application traffic
// without bumping the routing version, as initial seeding.
func (s *Server) SetAppTablegroup(db, tablegroup string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db(db).app = tablegroup
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
		d = &database{rows: map[string]*row{}}
		s.databases[name] = d
	}
	return d
}

func (r *row) proto(tablegroup string) *multipoolerservicepb.TablegroupServingState {
	return &multipoolerservicepb.TablegroupServingState{
		Tablegroup: tablegroup,
		State:      r.state,
		Version:    r.version,
		UpdatedAt:  timestamppb.New(r.updatedAt),
	}
}

func poolerID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "fake", Name: name}
}

// GetServingState implements the RPC. Like the real default primary, a tablegroup with no
// row is unfenced at version zero.
func (s *Server) GetServingState(_ context.Context, req *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &multipoolerservicepb.GetServingStateResponse{}
	d, known := s.databases[req.GetDatabase()]
	if known {
		resp.AppTablegroup, resp.RoutingVersion = d.app, d.routingVer
	}
	for _, name := range req.GetTablegroups() {
		var r *row
		if known {
			r = d.rows[name]
		}
		if r == nil {
			r = &row{state: multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, updatedAt: time.Now()}
		}
		resp.Tablegroups = append(resp.Tablegroups, r.proto(name))
	}
	return resp, nil
}

// row returns the tablegroup's row, creating the unfenced version-zero row a tablegroup
// that was never fenced has.
func (s *Server) row(db, tablegroup string) *row {
	d := s.db(db)
	r, ok := d.rows[tablegroup]
	if !ok {
		r = &row{state: stateUnfenced, updatedAt: time.Now()}
		d.rows[tablegroup] = r
	}
	return r
}

const (
	stateUnfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	stateFencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	stateFenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
)

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

// StartFence implements the RPC, including the version rules. It writes FENCING and asks every
// member, which it does not wait on; a member answers clean or busy, and when one is unreachable
// the call fails with ABORTED and leaves the row FENCING. Starting again asks the members once more
// and writes nothing; a fence that is already complete (FENCED at the version or the one after)
// succeeds and asks nobody.
func (s *Server) StartFence(_ context.Context, req *multipoolerservicepb.StartFenceRequest) (*multipoolerservicepb.StartFenceResponse, error) {
	if req.GetTablegroup() == "" {
		return nil, status.Error(codes.InvalidArgument, "tablegroup is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(req.GetDatabase(), req.GetTablegroup())
	v := req.GetExpectedVersion()
	switch {
	case r.state == stateUnfenced && r.version == v:
		r.state, r.updatedAt = stateFencing, time.Now()
		r.version++
	case r.state == stateFencing && (r.version == v || r.version == v+1):
		// The attempt, repeated: ask every member again.
	case r.state == stateFenced && (r.version == v || r.version == v+1):
		return &multipoolerservicepb.StartFenceResponse{State: r.proto(req.GetTablegroup())}, nil
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "tablegroup %q is %s at version %d, not UNFENCED at the expected version %d, nor FENCING or FENCED at it or the version after",
			req.GetTablegroup(), r.state, r.version, v)
	}
	resp := &multipoolerservicepb.StartFenceResponse{Members: s.ask(r)}
	resp.State = r.proto(req.GetTablegroup())
	for _, m := range resp.Members {
		if m.Status == multipoolerservicepb.MemberFenceResult_STATUS_UNREACHABLE {
			return nil, incomplete(fmt.Sprintf("tablegroup %q is %s at version %d but not every member answered", req.GetTablegroup(), r.state, r.version), resp, nil)
		}
	}
	return resp, nil
}

// ask is what every member of the row answers to a fence: clean, busy with what the fake was told
// it holds, or unreachable.
func (s *Server) ask(r *row) []*multipoolerservicepb.MemberFenceResult {
	var members []*multipoolerservicepb.MemberFenceResult
	for _, p := range r.poolers {
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

// CommitFence implements the RPC. It asks every member what it holds and writes FENCED only
// when all are clean; when one is not it fails with ABORTED and leaves the row FENCING. A
// fence that is already complete (FENCED at the version) succeeds and asks nobody.
func (s *Server) CommitFence(_ context.Context, req *multipoolerservicepb.CommitFenceRequest) (*multipoolerservicepb.CommitFenceResponse, error) {
	if req.GetTablegroup() == "" || req.GetVersion() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "tablegroup and the version of a fence are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(req.GetDatabase(), req.GetTablegroup())
	switch {
	case r.state == stateFenced && r.version == req.GetVersion():
		return &multipoolerservicepb.CommitFenceResponse{State: r.proto(req.GetTablegroup())}, nil
	case r.state == stateFencing && r.version == req.GetVersion():
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "tablegroup %q is %s at version %d, not FENCING or FENCED at the version %d to commit",
			req.GetTablegroup(), r.state, r.version, req.GetVersion())
	}
	resp := &multipoolerservicepb.CommitFenceResponse{Members: s.ask(r)}
	clean := true
	for _, m := range resp.Members {
		clean = clean && m.Status == multipoolerservicepb.MemberFenceResult_STATUS_CLEAN
	}
	if clean {
		r.state, r.updatedAt = stateFenced, time.Now()
	}
	resp.State = r.proto(req.GetTablegroup())
	if !clean {
		return nil, incomplete(fmt.Sprintf("tablegroup %q is %s at version %d but not every member is clean", req.GetTablegroup(), r.state, r.version), nil, resp)
	}
	return resp, nil
}

// UnfenceTablegroup implements the RPC.
func (s *Server) UnfenceTablegroup(_ context.Context, req *multipoolerservicepb.UnfenceTablegroupRequest) (*multipoolerservicepb.UnfenceTablegroupResponse, error) {
	if req.GetTablegroup() == "" {
		return nil, status.Error(codes.InvalidArgument, "tablegroup is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.row(req.GetDatabase(), req.GetTablegroup())
	switch {
	case r.state != stateUnfenced && r.version == req.GetExpectedVersion():
		r.state, r.updatedAt = stateUnfenced, time.Now()
		r.version++
	case r.state == stateUnfenced && r.version == req.GetExpectedVersion()+1:
		// Already unfenced at the version this unfence would have produced: done, write nothing.
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "tablegroup %q is %s at version %d, not FENCING or FENCED at the expected version %d, nor UNFENCED at the version after",
			req.GetTablegroup(), r.state, r.version, req.GetExpectedVersion())
	}
	resp := &multipoolerservicepb.UnfenceTablegroupResponse{State: r.proto(req.GetTablegroup())}
	for _, p := range r.poolers {
		if s.refreshFail[p] {
			resp.Unreached = append(resp.Unreached, poolerID(p))
		}
	}
	return resp, nil
}

// FlipRouting implements the RPC.
func (s *Server) FlipRouting(_ context.Context, req *multipoolerservicepb.FlipRoutingRequest) (*multipoolerservicepb.FlipRoutingResponse, error) {
	if req.GetFromTablegroup() == "" || req.GetToTablegroup() == "" || req.GetFromTablegroup() == req.GetToTablegroup() {
		return nil, status.Error(codes.InvalidArgument, "distinct from/to tablegroups are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(req.GetDatabase())
	if d.app == req.GetToTablegroup() && d.routingVer == req.GetExpectedRoutingVersion()+1 {
		// A retry after the move: the traffic is already on to_tablegroup at the version
		// the flip produced.
		return &multipoolerservicepb.FlipRoutingResponse{AppTablegroup: d.app, RoutingVersion: d.routingVer}, nil
	}
	if d.app != req.GetFromTablegroup() || d.routingVer != req.GetExpectedRoutingVersion() {
		return nil, status.Errorf(codes.FailedPrecondition, "routing is %q at version %d, not %q at the expected version %d",
			d.app, d.routingVer, req.GetFromTablegroup(), req.GetExpectedRoutingVersion())
	}
	for _, name := range []string{req.GetFromTablegroup(), req.GetToTablegroup()} {
		if r, ok := d.rows[name]; !ok || r.state != stateFenced {
			return nil, status.Errorf(codes.FailedPrecondition, "tablegroup %q is not FENCED, a flip requires both tablegroups FENCED (a completed fence)", name)
		}
	}
	d.app = req.GetToTablegroup()
	d.routingVer++
	return &multipoolerservicepb.FlipRoutingResponse{AppTablegroup: d.app, RoutingVersion: d.routingVer}, nil
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
