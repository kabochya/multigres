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

package migrationcontrol

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestAuthorityUsesManagedConsensusIndependentOfServing(t *testing.T) {
	managed := &pb.Multipooler{Id: &pb.ID{Name: "target"}, ServingStatus: pb.PoolerServingStatus_DISABLED, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &pb.RuleNumber{CoordinatorTerm: 2}}}
	source := &pb.Multipooler{ManagementMode: pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &pb.RuleNumber{CoordinatorTerm: 99}}}
	leader, err := Authority([]*pb.Multipooler{source, managed})
	require.NoError(t, err)
	require.Same(t, managed, leader)
	newer := &pb.Multipooler{Id: &pb.ID{Name: "new"}, RoutingState: &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY, Rule: &pb.RuleNumber{CoordinatorTerm: 3}}}
	leader, err = Authority([]*pb.Multipooler{managed, newer})
	require.NoError(t, err)
	require.Same(t, newer, leader)
	_, err = Authority([]*pb.Multipooler{source})
	require.Error(t, err)
	_, err = Authority([]*pb.Multipooler{managed, managed})
	require.Error(t, err)
}
