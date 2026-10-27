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

package grpcpoolerservice

import (
	"context"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// serveRouting runs one call of the manager's routing service.
func serveRouting[Req, Resp any](ctx context.Context, s *poolerService, req Req, call func(poolerserver.RoutingProvider, context.Context, Req) (Resp, error)) (Resp, error) {
	return serveControl(ctx, s.pooler.RoutingProvider, req, call)
}

// GetRoutingState returns the authoritative routing and admission metadata. Only the default
// primary serves it.
func (s *poolerService) GetRoutingState(ctx context.Context, req *multipoolerpb.GetRoutingStateRequest) (*multipoolerpb.GetRoutingStateResponse, error) {
	return serveRouting(ctx, s, req, poolerserver.RoutingProvider.GetRoutingState)
}

// RefreshAdmission re-reads and applies this pooler's application admission. It
// bypasses the application gate by construction: it is not a query-path request.
func (s *poolerService) RefreshAdmission(ctx context.Context, req *multipoolerpb.RefreshAdmissionRequest) (*multipoolerpb.RefreshAdmissionResponse, error) {
	return serveRouting(ctx, s, req, poolerserver.RoutingProvider.RefreshAdmission)
}
