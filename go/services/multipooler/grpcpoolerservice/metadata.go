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
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// GetBackingConnection returns a named backing connection. Only the default
// primary serves it; the manager enforces that.
func (s *poolerService) GetBackingConnection(ctx context.Context, req *multipoolerpb.GetBackingConnectionRequest) (*multipoolerpb.GetBackingConnectionResponse, error) {
	p, err := s.pooler.MetadataProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	resp, err := p.GetBackingConnection(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// GetServingState returns the authoritative routing and admission metadata. Only
// the default primary serves it.
func (s *poolerService) GetServingState(ctx context.Context, req *multipoolerpb.GetServingStateRequest) (*multipoolerpb.GetServingStateResponse, error) {
	p, err := s.pooler.MetadataProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	resp, err := p.GetServingState(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// RefreshAdmission re-reads and applies this pooler's application admission. It
// bypasses the application gate by construction: it is not a query-path request.
func (s *poolerService) RefreshAdmission(ctx context.Context, req *multipoolerpb.RefreshAdmissionRequest) (*multipoolerpb.RefreshAdmissionResponse, error) {
	p, err := s.pooler.AdmissionProvider()
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	resp, err := p.RefreshAdmission(ctx, req)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return resp, nil
}
