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

package manager

import (
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// unmanagedAdmissionBackend is the backend of a pooler that fronts an external PostgreSQL.
type unmanagedAdmissionBackend struct{ pm *MultipoolerManager }

// ClosesOnRoleChange is false: the source is writable whatever role the pooler reports, and
// there is no promotion to guard against, so a decision stands across a reconnect. The gate stays
// as the fence left it, and requests are refused by the serving status while the backend is away.
func (unmanagedAdmissionBackend) ClosesOnRoleChange() bool { return false }

// RegisteredBeforeStart is true: an unmanaged pooler registers before it starts.
func (unmanagedAdmissionBackend) RegisteredBeforeStart() bool { return true }

// ReadyToAdmit requires the pooler to be serving and its external backend to have passed identity
// validation: the endpoint is the writable one, so a pooler that is not ready must not open.
func (b unmanagedAdmissionBackend) ReadyToAdmit() error {
	h := b.pm.healthStreamer.getState()
	if h.ServingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "backend is not serving")
	}
	if !h.BackendReady {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "external backend is not ready")
	}
	return nil
}

func (b unmanagedAdmissionBackend) ConnectionName() string {
	if b.pm.config == nil {
		return ""
	}
	return b.pm.config.BackingConnectionName
}
