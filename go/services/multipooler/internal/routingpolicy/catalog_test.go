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

package routingpolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestRoutingPolicyValidation(t *testing.T) {
	require.Error(t, Validate(nil))
	require.Error(t, Validate(&pb.GatewayRoutingPolicy{}))
	for _, d := range []pb.RoutingDestination{pb.RoutingDestination_ROUTING_DESTINATION_MANAGED, pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED} {
		require.NoError(t, Validate(&pb.GatewayRoutingPolicy{Destination: d}))
		require.Error(t, Validate(&pb.GatewayRoutingPolicy{Destination: d, SourceConnection: "source"}))
	}
	source := &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination_ROUTING_DESTINATION_SOURCE, SourceConnection: "source", SourceConfigurationBinding: "binding", SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}}
	require.NoError(t, Validate(source))
	source.SourceConfigurationBinding = ""
	require.Error(t, Validate(source))
}
