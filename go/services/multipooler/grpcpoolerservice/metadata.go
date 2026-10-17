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
	"errors"

	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// serveMetadata runs one call of the manager's metadata service and converts its error for
// gRPC. An error that carries a gRPC status with details, such as a fence's per-member
// results, is passed through as it is; any other keeps the message context it was wrapped in.
func serveMetadata[Req, Resp any](
	ctx context.Context,
	s *poolerService,
	req Req,
	call func(poolerserver.MetadataProvider, context.Context, Req) (Resp, error),
) (Resp, error) {
	var zero Resp
	p, err := s.pooler.MetadataProvider()
	if err != nil {
		return zero, mterrors.ToGRPC(err)
	}
	resp, err := call(p, ctx, req)
	if err != nil {
		var withStatus interface{ GRPCStatus() *status.Status }
		if errors.As(err, &withStatus) {
			if st := withStatus.GRPCStatus(); len(st.Details()) > 0 {
				return zero, st.Err()
			}
		}
		return zero, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// GetBackingConnection returns a named backing connection. Only the default
// primary serves it; the manager enforces that.
func (s *poolerService) GetBackingConnection(ctx context.Context, req *multipoolerpb.GetBackingConnectionRequest) (*multipoolerpb.GetBackingConnectionResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.GetBackingConnection)
}

// GetServingState returns the authoritative routing and admission metadata. Only
// the default primary serves it.
func (s *poolerService) GetServingState(ctx context.Context, req *multipoolerpb.GetServingStateRequest) (*multipoolerpb.GetServingStateResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.GetServingState)
}

// RefreshAdmission re-reads and applies this pooler's application admission. It
// bypasses the application gate by construction: it is not a query-path request.
func (s *poolerService) RefreshAdmission(ctx context.Context, req *multipoolerpb.RefreshAdmissionRequest) (*multipoolerpb.RefreshAdmissionResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.RefreshAdmission)
}

// FenceTablegroup fences a tablegroup. Only the default primary serves it. A fence that some
// member did not complete carries the per-member results, which serveMetadata passes through.
func (s *poolerService) FenceTablegroup(ctx context.Context, req *multipoolerpb.FenceTablegroupRequest) (*multipoolerpb.FenceTablegroupResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.FenceTablegroup)
}

// UnfenceTablegroup unfences a tablegroup. Only the default primary serves it.
func (s *poolerService) UnfenceTablegroup(ctx context.Context, req *multipoolerpb.UnfenceTablegroupRequest) (*multipoolerpb.UnfenceTablegroupResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.UnfenceTablegroup)
}

// FlipRouting moves application traffic between two fenced tablegroups. Only the
// default primary serves it.
func (s *poolerService) FlipRouting(ctx context.Context, req *multipoolerpb.FlipRoutingRequest) (*multipoolerpb.FlipRoutingResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.FlipRouting)
}
