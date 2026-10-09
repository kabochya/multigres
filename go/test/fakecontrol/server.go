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
// RPCs of MultipoolerService (GetServingState, UpdatePoolerAdmission,
// UpdateMigrationRouting). It follows the contract documented in
// proto/multipoolerservice.proto closely enough for a migration controller to
// develop and test its cutover and rollback sequences before the real
// coordinator on the default primary exists.
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
//	// Fence the destination, then the source, under distinct request ids.
//	_, _ = client.UpdatePoolerAdmission(ctx, &UpdatePoolerAdmissionRequest{Database: "db", Tablegroup: "destTG", Target: FENCED, RequestId: "fence-dest"})
//	_, _ = client.UpdatePoolerAdmission(ctx, &UpdatePoolerAdmissionRequest{Database: "db", Tablegroup: "migrateTG", Target: FENCED, RequestId: "fence-src"})
//	// ... copy data ...
//	_, _ = client.UpdateMigrationRouting(ctx, &UpdateMigrationRoutingRequest{Database: "db", FromTablegroup: "migrateTG", ToTablegroup: "destTG", RequestId: "route"})
//	// Unfence undoes the fence it names via expected_request_id.
//	_, _ = client.UpdatePoolerAdmission(ctx, &UpdatePoolerAdmissionRequest{Database: "db", Tablegroup: "destTG", Target: UNFENCED, RequestId: "unfence-dest", ExpectedRequestId: "fence-dest"})
//
// To exercise recovery, make a pooler fail to acknowledge with
// FailRefresh("unmanaged-2", true): the fence returns ABORTED and
// GetServingState reports FENCING. Clear the fault and retry with the same
// request id to complete it.
package fakecontrol

import (
	"context"
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
	requestID string
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
}

// New returns an empty fake.
func New() *Server {
	return &Server{databases: map[string]*database{}, refreshFail: map[string]bool{}}
}

// AddTablegroup registers a tablegroup in the UNFENCED state. backing names the
// external connection that backs it, empty for a managed cohort. poolers are the
// names of the poolers that must acknowledge admission changes.
func (s *Server) AddTablegroup(db, tablegroup, backing string, poolers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.db(db)
	d.rows[tablegroup] = &row{backing: backing, state: multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, updatedAt: time.Now(), poolers: poolers}
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
		Tablegroup:        tablegroup,
		BackingConnection: r.backing,
		AdmissionState:    r.state,
		RequestId:         r.requestID,
		UpdatedAt:         timestamppb.New(r.updatedAt),
	}
}

// GetServingState implements the RPC.
func (s *Server) GetServingState(_ context.Context, req *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.databases[req.GetDatabase()]
	if !ok {
		// Like the real default primary, a database with no routing row reads as
		// an empty pointer at version 0; asking for its tablegroups is NOT_FOUND.
		if len(req.GetTablegroups()) == 0 {
			return &multipoolerservicepb.GetServingStateResponse{}, nil
		}
		return nil, status.Error(codes.NotFound, "database has no serving state")
	}
	resp := &multipoolerservicepb.GetServingStateResponse{AppTablegroup: d.app, RoutingVersion: d.routingVer}
	for _, name := range req.GetTablegroups() {
		r, ok := d.rows[name]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "tablegroup %q has no serving row", name)
		}
		resp.Tablegroups = append(resp.Tablegroups, r.proto(name))
	}
	return resp, nil
}

// UpdatePoolerAdmission implements the RPC, including its request-id rules.
func (s *Server) UpdatePoolerAdmission(_ context.Context, req *multipoolerservicepb.UpdatePoolerAdmissionRequest) (*multipoolerservicepb.UpdatePoolerAdmissionResponse, error) {
	const (
		fenced    = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
		fencing   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
		unfenced  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
		unfencing = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCING
	)
	target := req.GetTarget()
	if target != fenced && target != unfenced {
		return nil, status.Error(codes.InvalidArgument, "target must be FENCED or UNFENCED")
	}
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	if target == unfenced && req.GetExpectedRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "expected_request_id is required to unfence")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.databases[req.GetDatabase()]
	if !ok {
		return nil, status.Error(codes.NotFound, "database has no serving state")
	}
	r, ok := d.rows[req.GetTablegroup()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tablegroup %q has no serving row", req.GetTablegroup())
	}

	transitional, terminal := fencing, fenced
	if target == unfenced {
		transitional, terminal = unfencing, unfenced
	}

	if r.requestID == req.GetRequestId() {
		// A retry of this very operation: finished, or resume the fan-out.
		switch r.state {
		case terminal:
			return s.respond(r, req.GetTablegroup()), nil
		case transitional:
		default:
			return nil, status.Errorf(codes.FailedPrecondition, "request %q already ran with a different target", req.GetRequestId())
		}
	} else {
		if exp := req.GetExpectedRequestId(); exp != "" && exp != r.requestID {
			return nil, status.Errorf(codes.FailedPrecondition, "persisted request_id is %q, not the expected %q", r.requestID, exp)
		}
		if target == unfenced && r.state != fenced {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot unfence from %s", r.state)
		}
		// A fence may start from any state, preempting an unfence in progress.
		r.state, r.requestID, r.updatedAt = transitional, req.GetRequestId(), time.Now()
	}

	// Fan out: every pooler of the tablegroup must acknowledge.
	for _, p := range r.poolers {
		if s.refreshFail[p] {
			return nil, status.Errorf(codes.Aborted, "pooler %q did not acknowledge; state remains %s", p, r.state)
		}
	}
	r.state, r.updatedAt = terminal, time.Now()
	return s.respond(r, req.GetTablegroup()), nil
}

func (s *Server) respond(r *row, tablegroup string) *multipoolerservicepb.UpdatePoolerAdmissionResponse {
	resp := &multipoolerservicepb.UpdatePoolerAdmissionResponse{State: r.proto(tablegroup)}
	for _, p := range r.poolers {
		resp.Acks = append(resp.Acks, &multipoolerservicepb.PoolerAdmissionAck{
			PoolerId:           &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "fake", Name: p},
			ProcessIncarnation: "fake-incarnation",
			AppliedState:       r.state,
		})
	}
	return resp
}

// UpdateMigrationRouting implements the RPC.
func (s *Server) UpdateMigrationRouting(_ context.Context, req *multipoolerservicepb.UpdateMigrationRoutingRequest) (*multipoolerservicepb.UpdateMigrationRoutingResponse, error) {
	if req.GetRequestId() == "" || req.GetFromTablegroup() == "" || req.GetToTablegroup() == "" || req.GetFromTablegroup() == req.GetToTablegroup() {
		return nil, status.Error(codes.InvalidArgument, "request_id and distinct from/to tablegroups are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.databases[req.GetDatabase()]
	if !ok {
		return nil, status.Error(codes.NotFound, "database has no serving state")
	}
	// A retry after the pointer already moved is success, without a version bump.
	if d.app == req.GetToTablegroup() {
		return &multipoolerservicepb.UpdateMigrationRoutingResponse{AppTablegroup: d.app, RoutingVersion: d.routingVer}, nil
	}
	if d.app != req.GetFromTablegroup() {
		return nil, status.Errorf(codes.FailedPrecondition, "application tablegroup is %q, not %q", d.app, req.GetFromTablegroup())
	}
	for _, name := range []string{req.GetFromTablegroup(), req.GetToTablegroup()} {
		r, ok := d.rows[name]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "tablegroup %q has no serving row", name)
		}
		if r.state != multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED {
			return nil, status.Errorf(codes.FailedPrecondition, "tablegroup %q is %s, routing requires FENCED", name, r.state)
		}
	}
	d.app = req.GetToTablegroup()
	d.routingVer++
	return &multipoolerservicepb.UpdateMigrationRoutingResponse{AppTablegroup: d.app, RoutingVersion: d.routingVer}, nil
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
