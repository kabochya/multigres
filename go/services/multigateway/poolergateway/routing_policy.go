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
	"math/rand/v2"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// acceptedRoutingPolicy retains the last authoritative snapshot across target
// outages. Rule belongs to existing managed consensus, never to migration mode.
type acceptedRoutingPolicy struct {
	routing   *pb.GatewayRoutingPolicy
	authority topoclient.ComponentID
	rule      *pb.RuleNumber
}

func (lb *loadBalancer) acceptRoutingPolicy(summary *shardSummary, conn *poolerConnection) {
	if conn.ctx != nil && conn.ctx.Err() != nil {
		return
	}
	info := conn.PoolerInfo()
	if info.GetManagementMode() == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
		return
	}
	health := conn.Health()
	if health == nil || health.LastError != nil || health.RoutingPolicy == nil {
		return
	}
	leader, ok := summary.leaderID()
	if !ok || leader != conn.ID() {
		return
	}
	if lb.cache != nil {
		if current, exists := lb.cache.GetRider(conn.ID()); exists && current != conn {
			return
		}
	} // obsolete rider
	if health.RoutingPolicy.Destination < pb.RoutingDestination_ROUTING_DESTINATION_MANAGED || health.RoutingPolicy.Destination > pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED {
		return
	}
	rule := health.RoutingState.GetRule()
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.routingPolicies == nil {
		lb.routingPolicies = make(map[string]*acceptedRoutingPolicy)
	}
	previous := lb.routingPolicies[info.GetShardKey().GetDatabase()]
	if previous != nil {
		cmp := commonconsensus.CompareRuleNumbers(rule, previous.rule)
		if cmp < 0 || cmp == 0 && previous.authority != conn.ID() {
			return
		}
	}
	lb.routingPolicies[info.GetShardKey().GetDatabase()] = &acceptedRoutingPolicy{routing: proto.Clone(health.RoutingPolicy).(*pb.GatewayRoutingPolicy), authority: conn.ID(), rule: rule}
}

func (lb *loadBalancer) routingPolicy(database string) (*acceptedRoutingPolicy, bool) {
	lb.mu.Lock()
	policy, known := lb.routingPolicies[database]
	lb.mu.Unlock()
	return policy, known
}

func (lb *loadBalancer) requiresRoutingPolicy(target *query.Target) bool {
	if lb.cache == nil {
		return false
	}
	for _, entry := range lb.cache.All() {
		conn := entry.Rider
		if conn == nil || !matchesShardTarget(conn, target) {
			continue
		}
		if conn.PoolerInfo().GetManagementMode() == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
			return true
		}
		if h := conn.Health(); h != nil && h.RoutingPolicyRequired {
			return true
		}
	}
	return false
}

func (lb *loadBalancer) routingPolicyConnection(target *query.Target) (*poolerConnection, bool, error) {
	policy, known := lb.routingPolicy(target.GetShardKey().GetDatabase())
	if !known {
		if lb.requiresRoutingPolicy(target) {
			return nil, true, newNoWritablePrimaryError("migration authority unknown")
		}
		return nil, false, nil
	}
	switch policy.routing.Destination {
	case pb.RoutingDestination_ROUTING_DESTINATION_MANAGED:
		return nil, false, nil
	case pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED:
		return nil, true, newNoWritablePrimaryError("migration application traffic is fenced")
	case pb.RoutingDestination_ROUTING_DESTINATION_SOURCE:
		var local, remote []*poolerConnection
		if lb.cache != nil {
			for _, entry := range lb.cache.All() {
				conn := entry.Rider
				if conn == nil || !matchesShardTarget(conn, target) {
					continue
				}
				info := conn.PoolerInfo()
				if info.GetManagementMode() != pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED || (info.GetSourceConnection() != policy.routing.SourceConnection || info.GetSourceConfigurationBinding() != policy.routing.SourceConfigurationBinding) {
					continue
				}
				health := conn.Health()
				if health == nil || health.LastError != nil || !health.isServing() || !health.BackendReady || policy.routing.GetSourceIdentity().GetSystemIdentifier() == "" || !proto.Equal(health.BackendIdentity, policy.routing.SourceIdentity) {
					continue
				}
				if info.GetId().GetCell() == lb.localCell {
					local = append(local, conn)
				} else {
					remote = append(remote, conn)
				}
			}
		}
		if len(local) > 0 {
			return local[rand.IntN(len(local))], true, nil
		}
		if len(remote) > 0 {
			return remote[rand.IntN(len(remote))], true, nil
		}
		return nil, true, newNoWritablePrimaryError("selected migration source has no ready endpoints")
	default:
		return nil, true, newNoWritablePrimaryError("invalid migration routing policy")
	}
}

// Returns true for migration databases even while fenced/unknown, preventing
// a managed readiness broadcast from releasing a buffer awaiting the source.
func (lb *loadBalancer) notifyPolicyDestinationReady(sk *pb.ShardKey) bool {
	policy, known := lb.routingPolicy(sk.Database)
	target := &query.Target{ShardKey: sk, Mode: query.Mode_MODE_WRITABLE}
	if !known {
		return lb.requiresRoutingPolicy(target)
	}
	if policy.routing.Destination == pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED {
		return true
	}
	conn, err := lb.getConnection(target)
	if err == nil && conn != nil {
		health := conn.Health()
		if health != nil && health.LastError == nil && health.isServing() && lb.onLeaderServing != nil {
			lb.onLeaderServing(sk)
		}
	}
	return true
}
