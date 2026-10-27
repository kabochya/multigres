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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"

	"github.com/multigres/multigres/go/common/mterrors"
)

// admissionBackend is what admission needs to know about the backend of this pooler, so that
// the admission code itself does not ask whether the pooler is managed or not. A managed pooler
// fronts a postgres it runs as a leader or a follower of a cohort; an unmanaged one fronts an
// external endpoint that is writable whatever role it reports.
type admissionBackend interface {
	// ClosesOnRoleChange reports whether a change of the routing role or of the serving
	// status withdraws the admission decision and closes the gate until the fence is read
	// again: true where the pooler may be promoted underneath a decision made as a follower.
	ClosesOnRoleChange() bool
	// RegisteredBeforeStart reports whether the pooler is in topology before it starts, so
	// that it can read its fence at once. A pooler that is not must be listed first: the
	// coordinator fences the poolers it finds there, and one that opened from a read taken
	// before it was listed could stay open past a fence committed without it.
	RegisteredBeforeStart() bool
	// ReadyToAdmit returns nil when the pooler may open its gate now, and otherwise why
	// not. A pooler that cannot yet serve may open anyway when nothing can be admitted until
	// its serving status changes, which closes the gate and reads the fence again.
	ReadyToAdmit() error
	// ConnectionName is the backing connection the pooler fronts, empty for a pooler that is
	// served by a managed cohort. It is what the connection a tablegroup is bound to must be.
	ConnectionName() string
}

// managedAdmissionBackend is the backend of a pooler of a managed cohort.
type managedAdmissionBackend struct{ pm *MultipoolerManager }

func (managedAdmissionBackend) ClosesOnRoleChange() bool { return true }

func (managedAdmissionBackend) RegisteredBeforeStart() bool { return false }

// ReadyToAdmit lets a pooler that is not the writable leader open before its backend is ready:
// it cannot commit, and the serving status refuses requests until its backend is. A leader must
// be serving.
func (b managedAdmissionBackend) ReadyToAdmit() error {
	if b.pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return nil
	}
	if b.pm.healthStreamer.getState().ServingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "backend is not serving")
	}
	return nil
}

func (managedAdmissionBackend) ConnectionName() string { return "" }

// newAdmissionBackend returns the backend of the pooler: the managed one, or the one of an
// external endpoint when the pooler is unmanaged.
func newAdmissionBackend(pm *MultipoolerManager, unmanaged bool) admissionBackend {
	if unmanaged {
		return unmanagedAdmissionBackend{pm: pm}
	}
	return managedAdmissionBackend{pm: pm}
}
