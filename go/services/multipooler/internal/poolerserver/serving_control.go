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

// ServingControlProvider is implemented by the owning manager, independently of queries.
type ServingControlProvider interface {
	ServingControl(context.Context, *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error)
	GetMigrationMode(context.Context, *rpc.GetMigrationModeRequest) (*rpc.GetMigrationModeResponse, error)
	GetSourceConnection(context.Context, *rpc.GetSourceConnectionRequest) (*rpc.GetSourceConnectionResponse, error)
	RefreshRouting(context.Context, *rpc.RefreshRoutingRequest) (*rpc.RefreshRoutingResponse, error)
}

func (s *QueryPoolerServer) ServingControlProvider() (ServingControlProvider, error) {
	p, ok := s.healthProvider.(ServingControlProvider)
	if !ok {
		return nil, errors.New("serving control unavailable")
	}
	return p, nil
}
