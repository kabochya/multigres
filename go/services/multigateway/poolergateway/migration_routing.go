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
	"math/rand/v2"

	"github.com/multigres/multigres/go/common/mterrors"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// acceptedMigrationPolicy retains the last authoritative snapshot across target
// outages. Rule belongs to existing managed consensus, never to migration mode.
type acceptedMigrationPolicy struct {
	routing   *pb.MigrationRouting
	authority topoclient.ComponentID
	rule      *pb.RuleNumber
}

func (lb *loadBalancer) acceptMigrationPolicy(summary *shardSummary, conn *poolerConnection) {
	if conn.ctx != nil && conn.ctx.Err() != nil {
		return
	}
	info := conn.PoolerInfo()
	if info.GetManagementMode() == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
		return
	}
	health := conn.Health()
	if health == nil || health.LastError != nil || health.MigrationRouting == nil {
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
	if health.MigrationRouting.Mode < pb.MigrationMode_MIGRATION_MODE_UNSET || health.MigrationRouting.Mode > pb.MigrationMode_MIGRATION_MODE_MANAGED {
		return
	}
	rule := health.RoutingState.GetRule()
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.migrationPolicies == nil {
		lb.migrationPolicies = make(map[string]*acceptedMigrationPolicy)
	}
	previous := lb.migrationPolicies[info.GetShardKey().GetDatabase()]
	if previous != nil {
		cmp := commonconsensus.CompareRuleNumbers(rule, previous.rule)
		if cmp < 0 || cmp == 0 && previous.authority != conn.ID() {
			return
		}
	}
	lb.migrationPolicies[info.GetShardKey().GetDatabase()] = &acceptedMigrationPolicy{routing: health.MigrationRouting, authority: conn.ID(), rule: rule}
}

func (lb *loadBalancer) migrationPolicy(database string) (*acceptedMigrationPolicy, bool) {
	lb.mu.Lock()
	policy, known := lb.migrationPolicies[database]
	lb.mu.Unlock()
	return policy, known
}

func (lb *loadBalancer) requiresMigrationPolicy(target *query.Target) bool {
	if lb.cache == nil {
		return false
	}
	for _, entry := range lb.cache.All() {
		if entry.Rider != nil && matchesShardTarget(entry.Rider, target) && entry.Rider.PoolerInfo().GetSourceConnection() != "" {
			return true
		}
	}
	return false
}

func (lb *loadBalancer) migrationConnection(target *query.Target) (*poolerConnection, bool, error) {
	policy, known := lb.migrationPolicy(target.GetShardKey().GetDatabase())
	if !known {
		if lb.requiresMigrationPolicy(target) {
			return nil, true, newNoWritablePrimaryError("migration authority unknown")
		}
		return nil, false, nil
	}
	switch policy.routing.Mode {
	case pb.MigrationMode_MIGRATION_MODE_UNSET, pb.MigrationMode_MIGRATION_MODE_MANAGED:
		return nil, false, nil
	case pb.MigrationMode_MIGRATION_MODE_FENCED:
		return nil, true, newNoWritablePrimaryError("migration application traffic is fenced")
	case pb.MigrationMode_MIGRATION_MODE_UNMANAGED:
		var local, remote []*poolerConnection
		if lb.cache != nil {
			for _, entry := range lb.cache.All() {
				conn := entry.Rider
				if conn == nil || !matchesShardTarget(conn, target) {
					continue
				}
				info := conn.PoolerInfo()
				if info.GetManagementMode() != pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED || info.GetSourceConnection() != policy.routing.SourceConnection {
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
func (lb *loadBalancer) notifyMigrationDestinationReady(sk *pb.ShardKey) bool {
	policy, known := lb.migrationPolicy(sk.Database)
	target := &query.Target{ShardKey: sk, Mode: query.Mode_MODE_WRITABLE}
	if !known {
		return lb.requiresMigrationPolicy(target)
	}
	if policy.routing.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED {
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

// managedControlConnection bypasses application policy only for the narrow
// serving-control API. Ordinary SQL never calls this method.
func (lb *loadBalancer) managedControlConnection(target *query.Target) (*poolerConnection, error) {
	if lb.cache == nil {
		return nil, errors.New("managed control authority unavailable")
	}
	var selected *poolerConnection
	ambiguous := false
	for _, entry := range lb.cache.All() {
		conn := entry.Rider
		if conn == nil || !matchesShardTarget(conn, target) || conn.PoolerInfo().GetManagementMode() == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
			continue
		}
		h := conn.Health()
		if h == nil || h.LastError != nil || h.RoutingState.GetRole() != pb.RoutingRole_ROUTING_ROLE_PRIMARY {
			continue
		}
		if selected == nil {
			selected = conn
			continue
		}
		cmp := commonconsensus.CompareRuleNumbers(h.RoutingState.GetRule(), selected.Health().RoutingState.GetRule())
		if cmp == 0 && selected.ID() != conn.ID() {
			ambiguous = true
		}
		if cmp > 0 {
			selected = conn
			ambiguous = false
		}
	}
	if selected == nil {
		return nil, errors.New("managed control authority unavailable")
	}
	if ambiguous {
		return nil, errors.New("managed control authority is ambiguous")
	}
	if policy, known := lb.migrationPolicy(target.GetShardKey().Database); known && commonconsensus.CompareRuleNumbers(selected.Health().RoutingState.GetRule(), policy.rule) < 0 {
		return nil, errors.New("managed control authority is obsolete")
	}
	return selected, nil
}

// ServingControl routes only the explicit serving-control RPC to the managed
// authority. The RPC itself authenticates the service and the target user.
// It deliberately does not retry a transition after an ambiguous response;
// callers can repeat it with the same administrative request ID.
func (pg *PoolerGateway) ServingControl(ctx context.Context, target *query.Target, request *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error) {
	conn, err := pg.loadBalancer.managedControlConnection(target)
	if err != nil {
		return nil, err
	}
	protected, err := pg.servingContext(ctx)
	if err != nil {
		return nil, err
	}
	result, err := conn.ServiceClient().ServingControl(protected, request)
	return result, mterrors.FromGRPC(err)
}

func (pg *PoolerGateway) servingContext(ctx context.Context) (context.Context, error) {
	if pg.servingToken == "" {
		return nil, errors.New("serving control authorization is not configured")
	}
	return metadata.AppendToOutgoingContext(ctx, "x-multigres-serving-key", pg.servingToken), nil
}
