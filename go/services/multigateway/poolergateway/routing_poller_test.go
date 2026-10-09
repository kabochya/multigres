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

package poolergateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// routingServer answers GetServingState from a scripted pointer.
type routingServer struct {
	mockMultipoolerServiceClient
	mu      sync.Mutex
	app     string
	version int64
	err     error
	calls   int
	lastReq *multipoolerservice.GetServingStateRequest
}

func (r *routingServer) GetServingState(_ context.Context, in *multipoolerservice.GetServingStateRequest, _ ...grpc.CallOption) (*multipoolerservice.GetServingStateResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.lastReq = in
	if r.err != nil {
		return nil, r.err
	}
	return &multipoolerservice.GetServingStateResponse{AppTablegroup: r.app, RoutingVersion: r.version}, nil
}

func (r *routingServer) set(app string, version int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.app, r.version, r.err = app, version, err
}

// pollerFixture is a poller whose read of the default primary is scripted.
func pollerFixture(t *testing.T) (*RoutingPoller, *AppRouting, *routingServer, *int) {
	t.Helper()
	pg := &PoolerGateway{loadBalancer: newTestLB(t, "zone1"), logger: slog.New(slog.DiscardHandler)}
	server := &routingServer{}
	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	changes := 0
	poller := NewRoutingPoller(pg, routing, constants.DefaultPostgresDatabase, time.Hour, slog.New(slog.DiscardHandler), func() { changes++ })
	poller.read = func(ctx context.Context) (*multipoolerservice.GetServingStateResponse, error) {
		return server.GetServingState(ctx, &multipoolerservice.GetServingStateRequest{Database: constants.DefaultPostgresDatabase})
	}
	return poller, routing, server, &changes
}

func TestRoutingPollerAppliesTheDefaultPrimarysPointer(t *testing.T) {
	poller, routing, server, changes := pollerFixture(t)
	server.set("migrateTG", 1, nil)

	poller.PollOnce(t.Context())
	tg, known := routing.Current()
	require.True(t, known)
	assert.Equal(t, "migrateTG", tg)
	assert.Equal(t, 1, *changes)
	assert.Equal(t, constants.DefaultPostgresDatabase, server.lastReq.GetDatabase())
	assert.Empty(t, server.lastReq.GetTablegroups(), "the poller reads only the routing pointer")

	// Unchanged: no change notification.
	poller.PollOnce(t.Context())
	assert.Equal(t, 1, *changes)

	server.set("destTG", 2, nil)
	poller.PollOnce(t.Context())
	tg, _ = routing.Current()
	assert.Equal(t, "destTG", tg)
	assert.Equal(t, 2, *changes)
}

func TestRoutingPollerKeepsLastKnownRoutingOnFailureAndFailsClosedBefore(t *testing.T) {
	poller, routing, server, changes := pollerFixture(t)

	// Cold: a failed read leaves routing unknown, so the gateway keeps failing closed.
	server.set("", 0, errors.New("default primary unreachable"))
	poller.PollOnce(t.Context())
	_, known := routing.Current()
	assert.False(t, known)
	assert.Zero(t, *changes)

	// An empty pointer (no routing row) is not routing either.
	server.set("", 0, nil)
	poller.PollOnce(t.Context())
	_, known = routing.Current()
	assert.False(t, known)

	server.set("migrateTG", 1, nil)
	poller.PollOnce(t.Context())
	// A later failure keeps what was learned.
	server.set("", 0, errors.New("unreachable"))
	poller.PollOnce(t.Context())
	tg, known := routing.Current()
	require.True(t, known)
	assert.Equal(t, "migrateTG", tg)
}

func TestRoutingPollerIgnoresAnOlderVersionArrivingLate(t *testing.T) {
	poller, routing, server, _ := pollerFixture(t)
	server.set("destTG", 5, nil)
	poller.PollOnce(t.Context())
	server.set("migrateTG", 4, nil) // a read from before the move
	poller.PollOnce(t.Context())
	tg, _ := routing.Current()
	assert.Equal(t, "destTG", tg)
}

func TestRoutingPollerWithoutADefaultPrimaryReadsNothing(t *testing.T) {
	lb := newTestLB(t, "zone1")
	pg := &PoolerGateway{loadBalancer: lb, logger: slog.New(slog.DiscardHandler)}
	routing := NewAppRouting(constants.DefaultPostgresDatabase)
	poller := NewRoutingPoller(pg, routing, constants.DefaultPostgresDatabase, time.Hour, slog.New(slog.DiscardHandler), nil)
	poller.PollOnce(t.Context())
	_, known := routing.Current()
	assert.False(t, known)
}
