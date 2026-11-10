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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/backingconn/backingconntest"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// fastRetries makes the wait for the catalog retry quickly so failure paths run in milliseconds.
func fastRetries(t *testing.T) {
	t.Helper()
	oldT, oldB, oldM := bootstrapAttemptTimeout, bootstrapBackoffBase, bootstrapBackoffMax
	bootstrapAttemptTimeout, bootstrapBackoffBase, bootstrapBackoffMax = time.Second, time.Millisecond, 2*time.Millisecond
	t.Cleanup(func() {
		bootstrapAttemptTimeout, bootstrapBackoffBase, bootstrapBackoffMax = oldT, oldB, oldM
	})
}

// protoSource is a source that answers with a fixed reply of the default primary.
type protoSource struct {
	reply *multipoolerservicepb.BackingConnection
}

func (p protoSource) TableGroupBinding(context.Context, int64) (*backingConnection, bool, error) {
	conn, err := backingConnectionFromProto(p.reply)
	return conn, false, err
}

func TestBackingConnectionFromProtoConvertsAndAReplyIsValidatedOnce(t *testing.T) {
	valid := &multipoolerservicepb.BackingConnection{
		Name: "src",
		Host: []string{"db.example.com"}, Port: []int32{5432}, Database: "app", User: "alice", Password: "pw",
		SslMode: "verify-full", SslRootCert: "/ca.pem", SslNegotiation: "direct",
	}
	got, err := backingConnectionFromProto(valid)
	require.NoError(t, err)
	require.Equal(t, &backingConnection{
		Name: "src", Host: "db.example.com", Port: 5432, Database: "app", User: "alice", Password: "pw",
		SSLMode: "verify-full", SSLRootCert: "/ca.pem", SSLNegotiation: "direct",
	}, got)

	// What the default primary replied is used only if it is complete and names one server, by
	// the cases the default primary is tested against, and the verdict is given once.
	for _, tc := range backingconntest.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			reply := &multipoolerservicepb.BackingConnection{
				Name: "src", Host: []string{tc.Host}, Port: []int32{int32(tc.Port)}, Database: tc.Database, User: tc.User,
			}
			_, _, err := fetchBinding(t.Context(), protoSource{reply}, 7)
			if tc.Refused == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errInvalidBackingConnection)
			require.ErrorContains(t, err, tc.Refused)
			require.Equal(t, 1, strings.Count(err.Error(), tc.Refused), "validated once: %v", err)
		})
	}
}

// The host and port of a reply are lists, as in libpq. More than one server is refused when the
// reply is converted, with the reasons a host list or a port list was always refused for.
func TestBackingConnectionFromProtoRefusesMoreThanOneServer(t *testing.T) {
	for name, tc := range map[string]struct {
		host   []string
		port   []int32
		reason string
	}{
		"two hosts":            {[]string{"a.example.com", "b.example.com"}, []int32{5432}, "more than one server"},
		"two ports":            {[]string{"a.example.com"}, []int32{5432, 5433}, "port is not a single valid port number"},
		"two hosts, two ports": {[]string{"a.example.com", "b.example.com"}, []int32{5432, 5433}, "port is not a single valid port number"},
	} {
		t.Run(name, func(t *testing.T) {
			reply := &multipoolerservicepb.BackingConnection{
				Name: "src", Host: tc.host, Port: tc.port, Database: "app", User: "alice",
			}
			_, err := backingConnectionFromProto(reply)
			require.ErrorIs(t, err, errInvalidBackingConnection)
			require.ErrorContains(t, err, tc.reason)
			_, _, err = fetchBinding(t.Context(), protoSource{reply}, 7)
			require.ErrorIs(t, err, errInvalidBackingConnection)
		})
	}
	// A reply with no host or port is left for validation to name.
	got, err := backingConnectionFromProto(&multipoolerservicepb.BackingConnection{Name: "src", Database: "app", User: "alice"})
	require.NoError(t, err)
	require.Empty(t, got.Host)
	require.Zero(t, got.Port)
}

func TestRetryBootstrapWaitsAndIsCancellationAware(t *testing.T) {
	fastRetries(t)

	attempts := 0
	require.NoError(t, retryBootstrap(t.Context(), func(ctx context.Context) error {
		_, ok := ctx.Deadline()
		require.True(t, ok, "each attempt is time-bounded")
		attempts++
		if attempts < 50 {
			return errors.New("default primary temporarily unavailable")
		}
		return nil
	}, nil))
	require.Equal(t, 50, attempts, "it does not give up")

	ctx, cancel := context.WithCancel(t.Context())
	attempts = 0
	err := retryBootstrap(ctx, func(context.Context) error { attempts++; cancel(); return errors.New("down") }, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "down", "the last failure is reported with the cancellation")
	require.Equal(t, 1, attempts)
}

// fakeDefaultPrimary serves GetBackingConnection and records the requests.
type fakeDefaultPrimary struct {
	multipoolerservicepb.UnimplementedMultipoolerServiceServer
	reply func(req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error)

	mu       sync.Mutex
	requests []*multipoolerservicepb.GetBackingConnectionRequest
}

func (f *fakeDefaultPrimary) GetBackingConnection(_ context.Context, req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	return f.reply(req)
}

func startFakeDefaultPrimary(t *testing.T, ts topoclient.Store, reply func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error)) *fakeDefaultPrimary {
	t.Helper()
	fake := &fakeDefaultPrimary{reply: reply}
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
		ShardRef: &clustermetadatapb.ShardRef{Database: "db", TableGroup: "default", Shard: "0-inf"},
		RoutingState: &clustermetadatapb.RoutingState{
			Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY,
			Rule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
		},
	}))
	return fake
}

var insecureTransport = grpc.WithTransportCredentials(insecure.NewCredentials())

type reply = func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error)

// bound answers that the tablegroup is served by a good connection.
func bound(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	return &multipoolerservicepb.GetBackingConnectionResponse{Connection: &multipoolerservicepb.BackingConnection{
		Name: "src", Host: []string{"db.example.com"}, Port: []int32{5432}, Database: "db", User: "alice", Password: "pw", SslMode: "disable",
	}}, nil
}

// poolerOf is a pooler of tablegroup 5 that asks the default primary of ts.
func poolerOf(t *testing.T, ts topoclient.Store) *resolveFixture {
	t.Helper()
	f := newResolveFixture(t, nil, false, "--tablegroup-id=5")
	f.mp.ts = ts
	f.mp.connections = &defaultPrimarySource{ts: ts, database: "db", transport: insecureTransport}
	return f
}

// TestTheDefaultPrimaryIsAskedByTablegroupID: the pooler asks the default primary of its
// database how its tablegroup is served, by the tablegroup's id, and the reply is a connection or
// that the tablegroup is managed.
func TestTheDefaultPrimaryIsAskedByTablegroupID(t *testing.T) {
	for name, tc := range map[string]struct {
		reply       reply
		wantMode    clustermetadatapb.LeadershipMode
		wantBacking bool
	}{
		"a connection": {bound, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, true},
		"managed": {func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return &multipoolerservicepb.GetBackingConnectionResponse{Managed: true}, nil
		}, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS, false},
	} {
		t.Run(name, func(t *testing.T) {
			fastRetries(t)
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			defer ts.Close()
			fake := startFakeDefaultPrimary(t, ts, tc.reply)

			got, err := poolerOf(t, ts).resolve(t.Context(), "migrateTG")
			require.NoError(t, err)
			require.Equal(t, tc.wantMode, got.mode)
			require.Equal(t, tc.wantBacking, got.backing != nil)
			if tc.wantBacking {
				require.Equal(t, "db.example.com", got.backing.Host)
				require.Equal(t, "src", got.backing.Name)
			}

			fake.mu.Lock()
			defer fake.mu.Unlock()
			require.Len(t, fake.requests, 1)
			require.Equal(t, "db", fake.requests[0].GetDatabase())
			require.EqualValues(t, 5, fake.requests[0].GetTablegroupId())
		})
	}
}

// TestThePoolerKeepsWaitingForADefaultPrimaryThatCannotAnswer: whatever the default primary
// answers, a tablegroup that is unknown, a reply that says nothing, one with several servers, or
// a default primary that is unavailable, or none in topology at all, the pooler keeps asking and
// never starts: in particular it is never managed for want of an answer.
func TestThePoolerKeepsWaitingForADefaultPrimaryThatCannotAnswer(t *testing.T) {
	fastRetries(t)
	for name, reply := range map[string]reply{
		"not found": func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return nil, status.Error(codes.NotFound, "no such tablegroup")
		},
		"several servers": func(req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			resp, _ := bound(req)
			resp.Connection.Host = []string{"a.example.com", "b.example.com"}
			return resp, nil
		},
		"nothing in the reply": func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return &multipoolerservicepb.GetBackingConnectionResponse{}, nil
		},
		"unavailable": func(*multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
			return nil, status.Error(codes.Unavailable, "not the default primary pooler")
		},
	} {
		t.Run(name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			defer ts.Close()
			fake := startFakeDefaultPrimary(t, ts, reply)
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			got, err := poolerOf(t, ts).resolve(ctx, "migrateTG")
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Zero(t, got.mode)
			fake.mu.Lock()
			defer fake.mu.Unlock()
			require.Greater(t, len(fake.requests), 2, "it kept asking")
		})
	}

	t.Run("no default primary in topology", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
		defer ts.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		got, err := poolerOf(t, ts).resolve(ctx, "migrateTG")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, got.mode)
	})
}

// TestInsecureCredentialTransportIsWarnedAbout: a password fetched over a gRPC transport without
// TLS is warned about, the password is not logged, and a connection without a password or over
// TLS is not warned about.
func TestInsecureCredentialTransportIsWarnedAbout(t *testing.T) {
	for name, tc := range map[string]struct {
		password string
		tls      bool
		warns    bool
	}{
		"password, no TLS":    {"s3cret", false, true},
		"password, TLS":       {"s3cret", true, false},
		"no password, no TLS": {"", false, false},
		"no password, TLS":    {"", true, false},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))
			warnInsecureCredentialTransport(t.Context(), logger, &backingConnection{Name: "src", Password: tc.password}, tc.tls)
			require.Equal(t, tc.warns, buf.Len() > 0)
			require.NotContains(t, buf.String(), "s3cret")
			if tc.warns {
				require.Contains(t, buf.String(), `"connection":"src"`)
			}
		})
	}
}

// TestInitRegistersTheCompleteRecordOnceItKnowsItIsUnmanaged: a first start learns from the
// catalog that it is unmanaged and then registers, DISABLED and UNMANAGED, with the one record it
// goes on to serve with, so the entry in topology already names its shard, key range, tablegroup
// id and the port of the postgres it fronts. A start with that record already there reads it
// first, so it is visible while it asks.
func TestInitRegistersTheCompleteRecordOnceItKnowsItIsUnmanaged(t *testing.T) {
	fastRetries(t)
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	// A TLS setting the pooler cannot use stops Init after the registration and before the
	// manager starts.
	startFakeDefaultPrimary(t, ts, func(req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
		resp, _ := bound(req)
		resp.Connection.SslMode = "not-a-mode"
		return resp, nil
	})
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ext"}

	mp := newTestMultipooler()
	mp.connections = &defaultPrimarySource{ts: ts, database: "db", transport: insecureTransport}
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	require.NoError(t, flags.Parse([]string{
		"--database=db", "--cell=zone1", "--service-id=ext", "--table-group=migrateTG", "--shard=0-inf", "--tablegroup-id=5",
	}))
	require.ErrorContains(t, mp.Init(t.Context(), ts), "not-a-mode")

	info, err := ts.GetMultipooler(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.PoolerServingStatus_DISABLED, info.GetServingStatus())
	require.Equal(t, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, info.GetLeadershipMode())
	require.Equal(t, "migrateTG", info.GetShardRef().GetTableGroup())
	require.NotNil(t, info.GetKeyRange(), "the record carries the shard's key range")
	require.EqualValues(t, 5, info.GetTablegroupId())
	require.EqualValues(t, 5432, info.GetPortMap()["postgres"])
}
