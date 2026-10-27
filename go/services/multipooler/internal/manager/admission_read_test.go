// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// defaultLeaderStub is a default primary whose answer to GetServingState a test chooses.
type defaultLeaderStub struct {
	multipoolerservicepb.UnimplementedMultipoolerServiceServer
	answer   func() (*multipoolerservicepb.GetRoutingStateResponse, error)
	binding  func() (*multipoolerservicepb.GetTablegroupBindingResponse, error)
	accepted *atomic.Int32
}

// countingListener counts the connections it accepts.
type countingListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

func (s *defaultLeaderStub) GetRoutingState(context.Context, *multipoolerservicepb.GetRoutingStateRequest) (*multipoolerservicepb.GetRoutingStateResponse, error) {
	return s.answer()
}

// GetTablegroupBinding answers as the stub's binding says; a stub with none is a default
// primary that predates the RPC.
func (s *defaultLeaderStub) GetTablegroupBinding(ctx context.Context, req *multipoolerservicepb.GetTablegroupBindingRequest) (*multipoolerservicepb.GetTablegroupBindingResponse, error) {
	if s.binding == nil {
		return s.UnimplementedMultipoolerServiceServer.GetTablegroupBinding(ctx, req)
	}
	return s.binding()
}

// pointAtDefaultLeader makes the harness's pooler read its admission state over gRPC from a
// default primary the test controls, the way every pooler but the default primary itself does.
func pointAtDefaultLeader(t *testing.T, h *admissionHarness, stub *defaultLeaderStub) {
	t.Helper()
	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	lis := &countingListener{Listener: rawLis}
	stub.accepted = &lis.accepted
	srv := grpc.NewServer()
	multipoolerservicepb.RegisterMultipoolerServiceServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	ts := memorytopo.NewServer(t.Context(), "test-cell")
	t.Cleanup(func() { _ = ts.Close() })
	require.NoError(t, ts.CreateMultipooler(t.Context(), &clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "test-cell", Name: "default-leader"},
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0-inf"},
		Hostname: "127.0.0.1",
		PortMap:  map[string]int32{"grpc": int32(lis.Addr().(*net.TCPAddr).Port)},
		RoutingState: &clustermetadatapb.RoutingState{
			Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
			Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		},
	}))
	h.pm.config.TopoClient = ts
	h.pm.admission.reader = nil
	h.pm.bindingSource = nil
	withARoutingStore(t)
}

// withNoStores makes the build one that has neither a store of serving state nor a directory
// of backing connections.
func withNoStores(t *testing.T) {
	t.Helper()
	oldServing, oldDirectory := newRoutingStore, newConnectionDirectory
	newRoutingStore, newConnectionDirectory = nil, nil
	t.Cleanup(func() { newRoutingStore, newConnectionDirectory = oldServing, oldDirectory })
}

// withARoutingStore makes the build one that has a store of serving state, as the
// default primary of a cluster that can fence does; without one no pooler reads admission
// state at all.
func withARoutingStore(t *testing.T) {
	t.Helper()
	old := newRoutingStore
	newRoutingStore = func(*MultipoolerManager) routingStore { return nil }
	t.Cleanup(func() { newRoutingStore = old })
}

// TestWithNoStoreOfServingStateEveryPoolerOpensWithoutReading: nothing can have fenced a
// tablegroup when the build has no store of serving state, so a pooler opens on its own
// without asking anyone, the default primary included (which would otherwise read its own
// answer, UNIMPLEMENTED, as a failure and stay closed forever).
func TestWithNoStoreOfServingStateEveryPoolerOpensWithoutReading(t *testing.T) {
	withNoStores(t) // whatever a build registers, this test is about one that has none
	for name, unmanaged := range map[string]bool{"default primary or replica": false, "unmanaged": true} {
		t.Run(name, func(t *testing.T) {
			h := newAdmissionHarness(t, unmanaged)
			h.pm.admission.reader = nil // the real read path, with no topology to ask either
			h.pm.config.TopoClient = nil
			require.False(t, h.gate.ApplicationOpen(), "a pooler starts closed")
			_, err := h.pm.enforceAdmission(t.Context(), nil)
			require.NoError(t, err)
			require.True(t, h.gate.ApplicationOpen())
		})
	}

	t.Run("the default primary reading its own state", func(t *testing.T) {
		h := newAdmissionHarness(t, false)
		h.pm.admission.reader = nil
		row, err := h.pm.readOwnFence(t.Context())
		require.NoError(t, err)
		require.Equal(t, stateUnfenced, row.GetState())
	})
}

// TestAdmissionReadOfAnOldOrUnreachableDefaultLeader: a default primary that does not serve
// admission state has never fenced anything, so its tablegroup reads as unfenced and the
// pooler opens, as it does when no fence names its tablegroup; a default primary that fails any other way, or says fenced, leaves the gate
// closed. A pooler never opens on a failure it cannot read as "never fenced".
func TestAdmissionReadOfAnOldOrUnreachableDefaultLeader(t *testing.T) {
	for name, tc := range map[string]struct {
		answer func() (*multipoolerservicepb.GetRoutingStateResponse, error)
		opens  bool
	}{
		"unimplemented": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return nil, status.Error(codes.Unimplemented, "unknown method GetRoutingState")
			},
			opens: true,
		},
		"unfenced row": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return &multipoolerservicepb.GetRoutingStateResponse{Fences: []*multipoolerservicepb.AdmissionFence{{Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: "default"}, State: stateUnfenced, Version: 4}}}, nil
			},
			opens: true,
		},
		"fenced row": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return &multipoolerservicepb.GetRoutingStateResponse{Fences: []*multipoolerservicepb.AdmissionFence{{Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: "default"}, State: stateFencing, Version: 5}}}, nil
			},
		},
		"unavailable": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return nil, status.Error(codes.Unavailable, "the default primary is failing over")
			},
		},
		"internal": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return nil, status.Error(codes.Internal, "metadata unavailable")
			},
		},
		"failed precondition": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return nil, status.Error(codes.FailedPrecondition, "not the default primary")
			},
		},
		"permission denied": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return nil, status.Error(codes.PermissionDenied, "no")
			},
		},
		"no fence at all": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return &multipoolerservicepb.GetRoutingStateResponse{}, nil
			},
			opens: true,
		},
		"a fence of another tablegroup": {
			answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
				return &multipoolerservicepb.GetRoutingStateResponse{Fences: []*multipoolerservicepb.AdmissionFence{
					{Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: "other"}, State: stateFenced, Version: 2},
				}}, nil
			},
			opens: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newAdmissionHarness(t, false)
			pointAtDefaultLeader(t, h, &defaultLeaderStub{answer: tc.answer})
			_, err := h.pm.enforceAdmission(t.Context(), nil)
			if tc.opens {
				require.NoError(t, err)
				require.True(t, h.gate.ApplicationOpen())
				return
			}
			require.False(t, h.gate.ApplicationOpen(), "the gate stays closed")
			if name == "fenced row" {
				require.NoError(t, err, "a fenced row is a decision, not a failure")
				return
			}
			require.Error(t, err)
		})
	}
}

// TestTheConnectionToTheDefaultPrimaryIsReused: every read goes over one cached
// connection, not a new one with a listing of the database's poolers each time, and a read
// that is answered as by a pooler that is not the default primary any more is tried once
// more on a fresh one.
func TestTheConnectionToTheDefaultPrimaryIsReused(t *testing.T) {
	h := newAdmissionHarness(t, false)
	var stale atomic.Bool
	stub := &defaultLeaderStub{answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
		if stale.CompareAndSwap(true, false) {
			return nil, status.Error(codes.FailedPrecondition, "not the default primary")
		}
		return &multipoolerservicepb.GetRoutingStateResponse{Fences: []*multipoolerservicepb.AdmissionFence{{Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: "default"}}}}, nil
	}}
	pointAtDefaultLeader(t, h, stub)

	for range 3 {
		_, err := h.pm.readOwnFence(t.Context())
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, stub.accepted.Load(), "one connection for every read")

	stale.Store(true)
	_, err := h.pm.readOwnFence(t.Context())
	require.NoError(t, err, "the stale answer was retried once on a fresh connection")
	require.EqualValues(t, 2, stub.accepted.Load())
}

// TestAPoolerOpensOnlyOnATablegroupBoundToItsOwnConnection: where a default primary answers
// the binding of the tablegroup, a pooler opens only if the connection it fronts is the one the
// tablegroup is bound to, and a managed pooler only if the tablegroup is bound to none (a
// tablegroup that is not in the catalog is bound to nothing). Where nothing can answer (no
// directory built in, or a default primary that predates the RPC and says Unimplemented)
// nothing can disagree.
func TestAPoolerOpensOnlyOnATablegroupBoundToItsOwnConnection(t *testing.T) {
	bound := func(connection string) func() (*multipoolerservicepb.GetTablegroupBindingResponse, error) {
		return func() (*multipoolerservicepb.GetTablegroupBindingResponse, error) {
			return &multipoolerservicepb.GetTablegroupBindingResponse{BackingConnection: connection}, nil
		}
	}
	unfenced := func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
		return &multipoolerservicepb.GetRoutingStateResponse{Fences: []*multipoolerservicepb.AdmissionFence{
			{Scope: &multipoolerservicepb.AdmissionScope{Tablegroup: "migrateTG"}, State: stateUnfenced},
		}}, nil
	}
	for name, tc := range map[string]struct {
		unmanaged bool
		binding   func() (*multipoolerservicepb.GetTablegroupBindingResponse, error)
		opens     bool
	}{
		"a default primary that predates the RPC":       {unmanaged: true},
		"an unmanaged pooler, tablegroup bound to none": {unmanaged: true, binding: bound("")},
		"an unmanaged pooler, tablegroup not in the catalog": {unmanaged: true, binding: func() (*multipoolerservicepb.GetTablegroupBindingResponse, error) {
			return nil, status.Error(codes.NotFound, "tablegroup not found")
		}},
		"an unmanaged pooler, another connection":            {unmanaged: true, binding: bound("other")},
		"an unmanaged pooler, its own connection":            {unmanaged: true, binding: bound("src"), opens: true},
		"a managed pooler, tablegroup bound to none":         {binding: bound(""), opens: true},
		"a managed pooler, tablegroup bound to a connection": {binding: bound("src")},
		"a binding that cannot be read": {unmanaged: true, binding: func() (*multipoolerservicepb.GetTablegroupBindingResponse, error) {
			return nil, status.Error(codes.Unavailable, "failing over")
		}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newAdmissionHarness(t, tc.unmanaged)
			pointAtDefaultLeader(t, h, &defaultLeaderStub{answer: unfenced, binding: tc.binding})
			_, err := h.pm.enforceAdmission(t.Context(), nil)
			if tc.opens {
				require.NoError(t, err)
				require.True(t, h.gate.ApplicationOpen())
				return
			}
			require.Error(t, err)
			require.False(t, h.gate.ApplicationOpen())
		})
	}

	t.Run("no directory built in", func(t *testing.T) {
		withNoStores(t)
		h := newAdmissionHarness(t, true)
		h.pm.admission.reader = nil
		h.pm.config.TopoClient = nil
		_, err := h.pm.enforceAdmission(t.Context(), nil)
		require.NoError(t, err)
		require.True(t, h.gate.ApplicationOpen())
	})
}

// TestAReplicaWithNoStoreDoesNotAskTheDefaultPrimary: with no store built in there is nothing
// the default primary could say, so a replica that restarts opens without a single call to it,
// even when the default primary is failing.
func TestAReplicaWithNoStoreDoesNotAskTheDefaultPrimary(t *testing.T) {
	h := newAdmissionHarness(t, false)
	stub := &defaultLeaderStub{answer: func() (*multipoolerservicepb.GetRoutingStateResponse, error) {
		return nil, status.Error(codes.Unavailable, "the default primary is failing over")
	}}
	pointAtDefaultLeader(t, h, stub)
	withNoStores(t)

	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())
	require.Zero(t, stub.accepted.Load(), "no connection was made to the default primary")
}
