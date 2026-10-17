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

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// MetadataProvider serves the manager's control-plane calls: the cluster metadata held on
// the default primary, and the coordination of admission. They are independent of
// application query admission: a closed gate must never block the call that reopens it.
type MetadataProvider interface {
	GetBackingConnection(ctx context.Context, req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error)
	GetServingState(ctx context.Context, req *multipoolerservicepb.GetServingStateRequest) (*multipoolerservicepb.GetServingStateResponse, error)
	FenceTablegroup(ctx context.Context, req *multipoolerservicepb.FenceTablegroupRequest) (*multipoolerservicepb.FenceTablegroupResponse, error)
	UnfenceTablegroup(ctx context.Context, req *multipoolerservicepb.UnfenceTablegroupRequest) (*multipoolerservicepb.UnfenceTablegroupResponse, error)
	FlipRouting(ctx context.Context, req *multipoolerservicepb.FlipRoutingRequest) (*multipoolerservicepb.FlipRoutingResponse, error)
	// RefreshAdmission re-reads and applies this pooler's application admission.
	RefreshAdmission(ctx context.Context, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

// MetadataProvider returns the manager's metadata service, or an error when the
// health provider does not implement one.
func (s *QueryPoolerServer) MetadataProvider() (MetadataProvider, error) {
	p, ok := s.healthProvider.(MetadataProvider)
	if !ok {
		return nil, errors.New("cluster metadata is unavailable")
	}
	return p, nil
}
