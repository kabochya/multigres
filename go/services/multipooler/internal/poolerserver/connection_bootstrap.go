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

package poolerserver

import (
	"context"
	"errors"

	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// ConnectionBootstrapProvider is independent of application query admission.
type ConnectionBootstrapProvider interface {
	SetRoutingPolicy(context.Context, *rpc.SetRoutingPolicyRequest) (*rpc.SetRoutingPolicyResponse, error)
	GetRoutingPolicy(context.Context, *rpc.GetRoutingPolicyRequest) (*rpc.GetRoutingPolicyResponse, error)
	CreateSourceConnection(context.Context, *rpc.CreateSourceConnectionRequest) (*rpc.CreateSourceConnectionResponse, error)
	GetSourceConnection(context.Context, *rpc.GetSourceConnectionRequest) (*rpc.GetSourceConnectionResponse, error)
}

func (s *QueryPoolerServer) ConnectionBootstrapProvider() (ConnectionBootstrapProvider, error) {
	p, ok := s.healthProvider.(ConnectionBootstrapProvider)
	if !ok {
		return nil, errors.New("connection bootstrap unavailable")
	}
	return p, nil
}
