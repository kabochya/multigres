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
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// defaultLeaderStub is a default primary whose answer to GetServingState a test chooses.
type defaultLeaderStub struct {
	multipoolerservicepb.UnimplementedMultipoolerServiceServer
	answer   func() (*multipoolerservicepb.GetServingStateResponse, error)
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

func (s *defaultLeaderStub) GetServingState(context.Context, *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error) {
	return s.answer()
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
	withAStoreOfServingState(t)
}

// withAStoreOfServingState makes the build one that has a store of serving state, as the
// default primary of a cluster that can fence does; without one no pooler reads admission
// state at all.
func withAStoreOfServingState(t *testing.T) {
	t.Helper()
	old := newServingStore
	newServingStore = func(*MultipoolerManager) servingStore { return nil }
	t.Cleanup(func() { newServingStore = old })
}

// TestWithNoStoreOfServingStateEveryPoolerOpensWithoutReading: nothing can have fenced a
// tablegroup when the build has no store of serving state, so a pooler opens on its own
// without asking anyone, the default primary included (which would otherwise read its own
// answer, UNIMPLEMENTED, as a failure and stay closed forever).
func TestWithNoStoreOfServingStateEveryPoolerOpensWithoutReading(t *testing.T) {
	old := newServingStore
	newServingStore = nil // whatever a build registers, this test is about one that has none
	t.Cleanup(func() { newServingStore = old })
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
		row, err := h.pm.readTablegroupServing(t.Context())
		require.NoError(t, err)
		require.Equal(t, stateUnfenced, row.GetState())
	})
}

// TestAdmissionReadOfAnOldOrUnreachableDefaultLeader: a default primary that does not serve
// admission state has never fenced anything, so its tablegroup reads as unfenced and the
// pooler opens; a default primary that fails any other way, or says fenced, leaves the gate
// closed. A pooler never opens on a failure it cannot read as "never fenced".
func TestAdmissionReadOfAnOldOrUnreachableDefaultLeader(t *testing.T) {
	for name, tc := range map[string]struct {
		answer func() (*multipoolerservicepb.GetServingStateResponse, error)
		opens  bool
	}{
		"unimplemented": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return nil, status.Error(codes.Unimplemented, "unknown method GetServingState")
			},
			opens: true,
		},
		"unfenced row": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return &multipoolerservicepb.GetServingStateResponse{Tablegroups: []*multipoolerservicepb.TablegroupServingState{{Tablegroup: "default", State: stateUnfenced, Version: 4}}}, nil
			},
			opens: true,
		},
		"fenced row": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return &multipoolerservicepb.GetServingStateResponse{Tablegroups: []*multipoolerservicepb.TablegroupServingState{{Tablegroup: "default", State: stateFencing, Version: 5}}}, nil
			},
		},
		"unavailable": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return nil, status.Error(codes.Unavailable, "the default primary is failing over")
			},
		},
		"internal": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return nil, status.Error(codes.Internal, "metadata unavailable")
			},
		},
		"failed precondition": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return nil, status.Error(codes.FailedPrecondition, "not the default primary")
			},
		},
		"permission denied": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return nil, status.Error(codes.PermissionDenied, "no")
			},
		},
		"an answer for another number of tablegroups": {
			answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
				return &multipoolerservicepb.GetServingStateResponse{}, nil
			},
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
	stub := &defaultLeaderStub{answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
		if stale.CompareAndSwap(true, false) {
			return nil, status.Error(codes.FailedPrecondition, "not the default primary")
		}
		return &multipoolerservicepb.GetServingStateResponse{Tablegroups: []*multipoolerservicepb.TablegroupServingState{{Tablegroup: "default"}}}, nil
	}}
	pointAtDefaultLeader(t, h, stub)

	for range 3 {
		_, err := h.pm.readTablegroupServing(t.Context())
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, stub.accepted.Load(), "one connection for every read")

	stale.Store(true)
	_, err := h.pm.readTablegroupServing(t.Context())
	require.NoError(t, err, "the stale answer was retried once on a fresh connection")
	require.EqualValues(t, 2, stub.accepted.Load())
}

// TestAnUnmanagedPoolerNeedsARowNamingItsConnectionOnlyWhereThereIsAStore: where a store of
// serving state answers, an unmanaged pooler opens only on a row that names its own backing
// connection (a store with no row for its tablegroup answers an unfenced row naming none, which
// is a mismatch); where nothing can answer (no store built in, or a default primary that
// predates the RPC and says Unimplemented) its own connection is the one it is backed by.
func TestAnUnmanagedPoolerNeedsARowNamingItsConnectionOnlyWhereThereIsAStore(t *testing.T) {
	row := func(connection string) func() (*multipoolerservicepb.GetServingStateResponse, error) {
		return func() (*multipoolerservicepb.GetServingStateResponse, error) {
			return &multipoolerservicepb.GetServingStateResponse{Tablegroups: []*multipoolerservicepb.TablegroupServingState{
				{Tablegroup: "migrateTG", State: stateUnfenced, BackingConnection: connection},
			}}, nil
		}
	}
	for name, tc := range map[string]struct {
		answer func() (*multipoolerservicepb.GetServingStateResponse, error)
		opens  bool
	}{
		"a default primary that predates the RPC": {answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
			return nil, status.Error(codes.Unimplemented, "unknown method GetServingState")
		}, opens: true},
		"a store with no row for the tablegroup":   {answer: row("")},
		"a row naming another connection":          {answer: row("other")},
		"a row naming the pooler's own connection": {answer: row("src"), opens: true},
	} {
		t.Run(name, func(t *testing.T) {
			h := newAdmissionHarness(t, true)
			pointAtDefaultLeader(t, h, &defaultLeaderStub{answer: tc.answer})
			_, err := h.pm.enforceAdmission(t.Context(), nil)
			if tc.opens {
				require.NoError(t, err)
				require.True(t, h.gate.ApplicationOpen())
				return
			}
			requireCode(t, mtrpcpb.Code_FAILED_PRECONDITION, err)
			require.False(t, h.gate.ApplicationOpen())
		})
	}

	t.Run("no store built in", func(t *testing.T) {
		old := newServingStore
		newServingStore = nil
		t.Cleanup(func() { newServingStore = old })
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
	stub := &defaultLeaderStub{answer: func() (*multipoolerservicepb.GetServingStateResponse, error) {
		return nil, status.Error(codes.Unavailable, "the default primary is failing over")
	}}
	pointAtDefaultLeader(t, h, stub)
	old := newServingStore
	newServingStore = nil
	t.Cleanup(func() { newServingStore = old })

	_, err := h.pm.enforceAdmission(t.Context(), nil)
	require.NoError(t, err)
	require.True(t, h.gate.ApplicationOpen())
	require.Zero(t, stub.accepted.Load(), "no connection was made to the default primary")
}
