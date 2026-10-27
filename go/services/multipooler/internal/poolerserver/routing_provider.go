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

// RoutingProvider serves the manager's control-plane calls on admission fences and routing rules,
// held and coordinated on the default primary. They are independent of application query
// admission: a closed gate must never block the call that reopens it.
type RoutingProvider interface {
	GetRoutingState(ctx context.Context, req *multipoolerservicepb.GetRoutingStateRequest) (*multipoolerservicepb.GetRoutingStateResponse, error)
	// RefreshAdmission re-reads and applies this pooler's application admission.
	RefreshAdmission(ctx context.Context, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error)
}

// RoutingProvider returns the manager's routing service, or an error when the health provider does
// not implement one.
func (s *QueryPoolerServer) RoutingProvider() (RoutingProvider, error) {
	p, ok := s.healthProvider.(RoutingProvider)
	if !ok {
		return nil, errors.New("admission and routing metadata is unavailable")
	}
	return p, nil
}
