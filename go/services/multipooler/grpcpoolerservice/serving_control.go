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

	"github.com/multigres/multigres/go/common/mterrors"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
)

func (s *poolerService) ServingControl(ctx context.Context, r *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error) {
	p, err := s.pooler.ServingControlProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	result, err := p.ServingControl(ctx, r)
	return result, mterrors.ToGRPC(err)
}

func (s *poolerService) GetMigrationMode(ctx context.Context, r *rpc.GetMigrationModeRequest) (*rpc.GetMigrationModeResponse, error) {
	p, err := s.pooler.ServingControlProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	result, err := p.GetMigrationMode(ctx, r)
	return result, mterrors.ToGRPC(err)
}

func (s *poolerService) GetSourceConnection(ctx context.Context, r *rpc.GetSourceConnectionRequest) (*rpc.GetSourceConnectionResponse, error) {
	p, err := s.pooler.ServingControlProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	result, err := p.GetSourceConnection(ctx, r)
	return result, mterrors.ToGRPC(err)
}

func (s *poolerService) RefreshRouting(ctx context.Context, r *rpc.RefreshRoutingRequest) (*rpc.RefreshRoutingResponse, error) {
	p, err := s.pooler.ServingControlProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	result, err := p.RefreshRouting(ctx, r)
	return result, mterrors.ToGRPC(err)
}
