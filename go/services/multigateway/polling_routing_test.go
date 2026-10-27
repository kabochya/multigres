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

// TestThePollingRoutingSourceIsRegisteredAndOffUnlessConfigured: it adds its two flags, and
// without --routing-poll-database it starts nothing, so the gateway routes application SQL to
// the default tablegroup as it always did.
func TestThePollingRoutingSourceIsRegisteredAndOffUnlessConfigured(t *testing.T) {
	var found bool
	for _, newSource := range routingSourceFactories {
		if _, ok := newSource().(*pollingRouting); ok {
			found = true
		}
	}
	require.True(t, found, "registered by its init function")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	own := &pollingRouting{}
	own.RegisterFlags(fs, viperutil.NewRegistry())
	require.NoError(t, fs.Parse([]string{"--routing-poll-database=app", "--routing-poll-interval=3s"}))
	require.Equal(t, "app", own.database.Get())
	require.Equal(t, 3*time.Second, own.interval.Get())

	off := &pollingRouting{}
	off.RegisterFlags(pflag.NewFlagSet("off", pflag.ContinueOnError), viperutil.NewRegistry())
	require.NoError(t, off.Start(t.Context(), nil, nil, slog.Default()), "not configured: nothing started, nothing needed")
}

// routingServer answers GetRoutingState from a scripted state, as the default primary does.
type routingServer struct {
	multipoolerservicepb.MultipoolerServiceClient
	mu      sync.Mutex
	home    string
	rules   []*multipoolerservicepb.RoutingRuleState
	version int64
	err     error
	lastReq *multipoolerservicepb.GetRoutingStateRequest
}

func (r *routingServer) GetRoutingState(_ context.Context, in *multipoolerservicepb.GetRoutingStateRequest, _ ...grpc.CallOption) (*multipoolerservicepb.GetRoutingStateResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastReq = in
	if r.err != nil {
		return nil, r.err
	}
	if in.GetSinceVersion() > 0 && in.GetSinceVersion() == r.version {
		return &multipoolerservicepb.GetRoutingStateResponse{RoutingVersion: r.version, NotModified: true}, nil
	}
	return &multipoolerservicepb.GetRoutingStateResponse{RoutingVersion: r.version, HomeTablegroup: r.home, Rules: r.rules}, nil
}

// set scripts a state with no rule: the home tablegroup receives the traffic.
func (r *routingServer) set(home string, version int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.home, r.rules, r.version, r.err = home, nil, version, err
}

// setRule scripts a home tablegroup and a rule that moves its traffic.
func (r *routingServer) setRule(home, to string, version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.home, r.version, r.err = home, version, nil
	r.rules = []*multipoolerservicepb.RoutingRuleState{{
		OperationId: "0b0c0d0e-0000-4000-8000-000000000001", CreatedVersion: version,
		Rule: &multipoolerservicepb.RoutingRule{FromTablegroup: home, ToTablegroup: to, ToShard: "0-inf"},
	}}
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

func TestRoutingPollerAppliesTheDefaultPrimarysRouting(t *testing.T) {
	poller, sink, server := pollerFixture(t)
	server.set("migrateTG", 1, nil)

	poller.pollOnce(t.Context())
	tg, known := currentOf(t, sink)
	require.True(t, known)
	assert.Equal(t, "migrateTG", tg)
	assert.Equal(t, "db", server.lastReq.GetDatabase())
	assert.Zero(t, server.lastReq.GetSinceVersion(), "the first read asks for everything")

	// A rule that applies to the home tablegroup sends the traffic to its destination, once.
	server.setRule("migrateTG", "destTG", 2)
	poller.pollOnce(t.Context())
	tg, _ = currentOf(t, sink)
	assert.Equal(t, "destTG", tg)

	// Nothing changed: the default primary answers not_modified and the poller keeps what it has.
	poller.pollOnce(t.Context())
	assert.EqualValues(t, 2, server.lastReq.GetSinceVersion(), "later reads name the version already applied")
	tg, _ = currentOf(t, sink)
	assert.Equal(t, "destTG", tg)

	// A fold: the destination is the home tablegroup, and there is no rule.
	server.set("destTG", 3, nil)
	poller.pollOnce(t.Context())
	tg, _ = currentOf(t, sink)
	assert.Equal(t, "destTG", tg)

	// A revert of a rule that applied to another tablegroup does not move the traffic.
	server.setRule("otherTG", "elsewhere", 4)
	poller.pollOnce(t.Context())
	tg, _ = currentOf(t, sink)
	assert.Equal(t, "elsewhere", tg, "the rule applies to the home tablegroup of that state")
}

func TestEffectiveTablegroupAppliesOnlyARuleOfTheHomeTablegroupAndOnlyOnce(t *testing.T) {
	rule := func(from, to string) *multipoolerservicepb.RoutingRuleState {
		return &multipoolerservicepb.RoutingRuleState{Rule: &multipoolerservicepb.RoutingRule{FromTablegroup: from, ToTablegroup: to}}
	}
	for name, tc := range map[string]struct {
		resp *multipoolerservicepb.GetRoutingStateResponse
		want string
	}{
		"no rule":                   {&multipoolerservicepb.GetRoutingStateResponse{HomeTablegroup: "a"}, "a"},
		"a rule of the home":        {&multipoolerservicepb.GetRoutingStateResponse{HomeTablegroup: "a", Rules: []*multipoolerservicepb.RoutingRuleState{rule("a", "b")}}, "b"},
		"a rule of another":         {&multipoolerservicepb.GetRoutingStateResponse{HomeTablegroup: "a", Rules: []*multipoolerservicepb.RoutingRuleState{rule("c", "d")}}, "a"},
		"a rule is not followed on": {&multipoolerservicepb.GetRoutingStateResponse{HomeTablegroup: "a", Rules: []*multipoolerservicepb.RoutingRuleState{rule("a", "b"), rule("b", "c")}}, "b"},
		"no home":                   {&multipoolerservicepb.GetRoutingStateResponse{}, ""},
	} {
		t.Run(name, func(t *testing.T) { assert.Equal(t, tc.want, effectiveTablegroup(tc.resp)) })
	}
}

func TestRoutingPollerKeepsLastKnownRoutingOnFailureAndFailsClosedBefore(t *testing.T) {
	poller, sink, server := pollerFixture(t)

	// Cold: a failed read leaves routing unknown, so the gateway keeps failing closed.
	server.set("", 0, errors.New("default primary unreachable"))
	poller.pollOnce(t.Context())
	_, known := currentOf(t, sink)
	assert.False(t, known)

	// An empty home tablegroup (no catalog state) is not routing either.
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
