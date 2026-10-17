// Copyright 2025 Supabase, Inc.
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

package multigateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestThePrototypeRoutingSourceIsRegisteredAndOffUnlessConfigured: it adds its two flags, and
// without --routing-poll-database it starts nothing, so the gateway routes application SQL to
// the default tablegroup as it always did.
func TestThePrototypeRoutingSourceIsRegisteredAndOffUnlessConfigured(t *testing.T) {
	var found bool
	for _, s := range routingSources {
		if _, ok := s.(*prototypeRouting); ok {
			found = true
		}
	}
	require.True(t, found, "registered by its init function")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	own := &prototypeRouting{}
	own.RegisterFlags(fs, viperutil.NewRegistry())
	require.NoError(t, fs.Parse([]string{"--routing-poll-database=app", "--routing-poll-interval=3s"}))
	require.Equal(t, "app", own.database.Get())
	require.Equal(t, 3*time.Second, own.interval.Get())

	off := &prototypeRouting{}
	off.RegisterFlags(pflag.NewFlagSet("off", pflag.ContinueOnError), viperutil.NewRegistry())
	require.NoError(t, off.Start(t.Context(), nil, nil, slog.Default()), "not configured: nothing started, nothing needed")
}

// routingServer answers GetServingState from a scripted pointer, as the default primary does.
type routingServer struct {
	multipoolerservicepb.MultipoolerServiceClient
	mu      sync.Mutex
	app     string
	version int64
	err     error
	lastReq *multipoolerservicepb.GetServingStateRequest
}

func (r *routingServer) GetServingState(_ context.Context, in *multipoolerservicepb.GetServingStateRequest, _ ...grpc.CallOption) (*multipoolerservicepb.GetServingStateResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastReq = in
	if r.err != nil {
		return nil, r.err
	}
	return &multipoolerservicepb.GetServingStateResponse{AppTablegroup: r.app, RoutingVersion: r.version}, nil
}

func (r *routingServer) set(app string, version int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.app, r.version, r.err = app, version, err
}

// fakeControl reaches a default primary the test scripts, or none.
type fakeControl struct {
	server *routingServer
	err    error
}

func (f fakeControl) DefaultPrimary(string) (multipoolerservicepb.MultipoolerServiceClient, error) {
	return f.server, f.err
}

func pollerFixture(t *testing.T) (*routingPoller, *gatewayRouting, *routingServer) {
	t.Helper()
	sink := newTestGatewayRouting()
	require.NoError(t, sink.Follow("db"))
	server := &routingServer{}
	return &routingPoller{sink: sink, control: fakeControl{server: server}, database: "db", interval: time.Hour, logger: slog.New(slog.DiscardHandler)}, sink, server
}

func currentOf(t *testing.T, sink *gatewayRouting) (string, bool) {
	t.Helper()
	return sink.routing.Current()
}

func TestRoutingPollerAppliesTheDefaultPrimarysPointer(t *testing.T) {
	poller, sink, server := pollerFixture(t)
	server.set("migrateTG", 1, nil)

	poller.pollOnce(t.Context())
	tg, known := currentOf(t, sink)
	require.True(t, known)
	assert.Equal(t, "migrateTG", tg)
	assert.Equal(t, "db", server.lastReq.GetDatabase())
	assert.Empty(t, server.lastReq.GetTablegroups(), "the poller reads only the routing pointer")

	server.set("destTG", 2, nil)
	poller.pollOnce(t.Context())
	tg, _ = currentOf(t, sink)
	assert.Equal(t, "destTG", tg)
}

func TestRoutingPollerKeepsLastKnownRoutingOnFailureAndFailsClosedBefore(t *testing.T) {
	poller, sink, server := pollerFixture(t)

	// Cold: a failed read leaves routing unknown, so the gateway keeps failing closed.
	server.set("", 0, errors.New("default primary unreachable"))
	poller.pollOnce(t.Context())
	_, known := currentOf(t, sink)
	assert.False(t, known)

	// An empty pointer (no routing row) is not routing either.
	server.set("", 0, nil)
	poller.pollOnce(t.Context())
	_, known = currentOf(t, sink)
	assert.False(t, known)

	server.set("migrateTG", 1, nil)
	poller.pollOnce(t.Context())
	// A later failure keeps what was learned.
	server.set("", 0, errors.New("unreachable"))
	poller.pollOnce(t.Context())
	tg, known := currentOf(t, sink)
	require.True(t, known)
	assert.Equal(t, "migrateTG", tg)
}

func TestRoutingPollerIgnoresAnOlderVersionArrivingLate(t *testing.T) {
	poller, sink, server := pollerFixture(t)
	server.set("destTG", 5, nil)
	poller.pollOnce(t.Context())
	server.set("migrateTG", 4, nil) // a read from before the move
	poller.pollOnce(t.Context())
	tg, _ := currentOf(t, sink)
	assert.Equal(t, "destTG", tg)
}

func TestRoutingPollerWithoutADefaultPrimaryReadsNothing(t *testing.T) {
	poller, sink, _ := pollerFixture(t)
	poller.control = fakeControl{err: errors.New("no default primary")}
	poller.pollOnce(t.Context())
	_, known := currentOf(t, sink)
	assert.False(t, known)
}
