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

package multipooler

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// fastRetries makes bootstrap give up quickly so failure paths run in
// milliseconds.
func fastRetries(t *testing.T, attempts int) {
	t.Helper()
	oldA, oldT, oldB, oldM := bootstrapAttempts, bootstrapAttemptTimeout, bootstrapBackoffBase, bootstrapBackoffMax
	bootstrapAttempts, bootstrapAttemptTimeout, bootstrapBackoffBase, bootstrapBackoffMax = attempts, time.Second, time.Millisecond, 2*time.Millisecond
	t.Cleanup(func() {
		bootstrapAttempts, bootstrapAttemptTimeout, bootstrapBackoffBase, bootstrapBackoffMax = oldA, oldT, oldB, oldM
	})
}

func TestBackingConnectionFromProtoRequiresIdentity(t *testing.T) {
	valid := &multipoolerservicepb.BackingConnection{
		Name:                     "src",
		Url:                      "postgres://alice:pw@db.example.com:5432/app",
		ExpectedSystemIdentifier: "123",
	}
	got, err := backingConnectionFromProto("src", valid)
	require.NoError(t, err)
	require.Equal(t, "123", got.ExpectedSystemIdentifier)
	require.Equal(t, "db.example.com", got.Host)

	_, err = backingConnectionFromProto("other", valid)
	require.ErrorContains(t, err, "want \"other\"")

	_, err = backingConnectionFromProto("src", nil)
	require.Error(t, err)

	noIdentity := proto.Clone(valid).(*multipoolerservicepb.BackingConnection)
	noIdentity.ExpectedSystemIdentifier = ""
	_, err = backingConnectionFromProto("src", noIdentity)
	require.ErrorContains(t, err, "expected system identifier")

	badURL := proto.Clone(valid).(*multipoolerservicepb.BackingConnection)
	badURL.Url = "mysql://alice@db/app"
	_, err = backingConnectionFromProto("src", badURL)
	require.Error(t, err)
}

func TestRetryBootstrapIsBoundedAndCancellationAware(t *testing.T) {
	fastRetries(t, 3)

	attempts := 0
	require.NoError(t, retryBootstrap(t.Context(), func(ctx context.Context) error {
		_, ok := ctx.Deadline()
		require.True(t, ok, "each attempt is time-bounded")
		attempts++
		if attempts == 1 {
			return errors.New("default primary temporarily unavailable")
		}
		return nil
	}, nil))
	require.Equal(t, 2, attempts)

	attempts = 0
	err := retryBootstrap(t.Context(), func(context.Context) error { attempts++; return errors.New("down") }, nil)
	require.ErrorContains(t, err, "down")
	require.Equal(t, 3, attempts, "retries stop at the configured bound")

	ctx, cancel := context.WithCancel(t.Context())
	attempts = 0
	err = retryBootstrap(ctx, func(context.Context) error { attempts++; cancel(); return errors.New("down") }, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

// fakeDefaultPrimary serves GetBackingConnection and records what topology
// looked like when each call arrived.
type fakeDefaultPrimary struct {
	multipoolerservicepb.UnimplementedMultipoolerServiceServer
	ts          topoclient.Store
	unmanagedID *clustermetadatapb.ID
	reply       func(req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error)

	mu       sync.Mutex
	requests []*multipoolerservicepb.GetBackingConnectionRequest
	// registeredWhenRead is the unmanaged pooler's topology entry as seen from
	// inside the first request.
	registeredWhenRead *clustermetadatapb.Multipooler
}

func (f *fakeDefaultPrimary) GetBackingConnection(ctx context.Context, req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if f.registeredWhenRead == nil {
		if info, err := f.ts.GetMultipooler(ctx, f.unmanagedID); err == nil {
			f.registeredWhenRead = info.Multipooler
		}
	}
	return f.reply(req)
}

func startFakeDefaultPrimary(t *testing.T, ts topoclient.Store, unmanagedID *clustermetadatapb.ID, reply func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error)) *fakeDefaultPrimary {
	t.Helper()
	fake := &fakeDefaultPrimary{ts: ts, unmanagedID: unmanagedID, reply: reply}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	multipoolerservicepb.RegisterMultipoolerServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	require.NoError(t, ts.CreateMultipooler(t.Context(), &clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "default-primary"},
		Hostname: "127.0.0.1",
		PortMap:  map[string]int32{"grpc": int32(lis.Addr().(*net.TCPAddr).Port)},
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0-inf"},
		RoutingState: &clustermetadatapb.RoutingState{
			Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
			Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		},
	}))
	return fake
}

func unmanagedRecord() *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id:             &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "external-1"},
		Hostname:       "127.0.0.1",
		PortMap:        map[string]int32{"grpc": 1},
		ShardKey:       &clustermetadatapb.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		ManagementMode: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED,
		ServingStatus:  clustermetadatapb.PoolerServingStatus_SERVING, // must be published as DISABLED
	}
}

var insecureTransport = grpc.WithTransportCredentials(insecure.NewCredentials())

func TestBootstrapRegistersClosedBeforeReadingFromDefaultPrimary(t *testing.T) {
	fastRetries(t, 2)
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	initial := unmanagedRecord()
	fake := startFakeDefaultPrimary(t, ts, initial.Id, func(req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
		return &multipoolerservicepb.GetBackingConnectionResponse{Connection: &multipoolerservicepb.BackingConnection{
			Name:                     req.GetConnectionName(),
			Url:                      "postgres://alice:pw@db.example.com:5432/app?sslmode=disable",
			ExpectedSystemIdentifier: "123",
		}}, nil
	})

	got, err := bootstrapBackingConnection(t.Context(), ts, initial, "src", insecureTransport, slog.Default())
	require.NoError(t, err)
	require.Equal(t, "db.example.com", got.Host)
	require.Equal(t, "123", got.ExpectedSystemIdentifier)

	require.Len(t, fake.requests, 1)
	require.Equal(t, "db", fake.requests[0].GetDatabase())
	require.Equal(t, "src", fake.requests[0].GetConnectionName())

	// The pooler was already visible, DISABLED and UNMANAGED, when its first
	// metadata read reached the default primary.
	require.NotNil(t, fake.registeredWhenRead, "must register before the first read")
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, fake.registeredWhenRead.GetServingStatus())
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, fake.registeredWhenRead.GetManagementMode())
}

func TestBootstrapStaysClosedWhenDefaultPrimaryIsUnreachable(t *testing.T) {
	fastRetries(t, 2)
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	defer ts.Close()
	initial := unmanagedRecord()

	// No default primary exists in topology at all.
	_, err := bootstrapBackingConnection(t.Context(), ts, initial, "src", insecureTransport, slog.Default())
	require.ErrorContains(t, err, "pooler stays closed")

	// It still registered first, closed, and then marked itself stopped so no
	// membership snapshot waits for it.
	info, getErr := ts.GetMultipooler(t.Context(), initial.Id)
	require.NoError(t, getErr)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, info.GetServingStatus())
	require.Equal(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN, info.GetLifecycleStatus().GetStatus())
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, info.GetManagementMode())
}

func TestBootstrapFailsOnUnknownOrMismatchedConnection(t *testing.T) {
	fastRetries(t, 2)
	for name, reply := range map[string]func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error){
		"not found": func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return nil, status.Error(codes.NotFound, "no such connection")
		},
		"wrong name": func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return &multipoolerservicepb.GetBackingConnectionResponse{Connection: &multipoolerservicepb.BackingConnection{
				Name: "other", Url: "postgres://u@h/db", ExpectedSystemIdentifier: "1",
			}}, nil
		},
		"no identity": func(req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return &multipoolerservicepb.GetBackingConnectionResponse{Connection: &multipoolerservicepb.BackingConnection{
				Name: req.GetConnectionName(), Url: "postgres://u:p@h/db",
			}}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			defer ts.Close()
			initial := unmanagedRecord()
			startFakeDefaultPrimary(t, ts, initial.Id, reply)
			got, err := bootstrapBackingConnection(t.Context(), ts, initial, "src", insecureTransport, slog.Default())
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
