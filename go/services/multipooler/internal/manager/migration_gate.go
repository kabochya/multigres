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
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/migrationcontrol"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"

	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
)

type applicationGate interface {
	SetApplicationAdmission(bool)
	FenceApplication(context.Context) error
}

func (pm *MultipoolerManager) gate() (applicationGate, error) {
	g, ok := pm.qsc.(applicationGate)
	if !ok {
		return nil, errors.New("application gate unavailable")
	}
	return g, nil
}

func (pm *MultipoolerManager) sourceMode(ctx context.Context) (*pb.MigrationRouting, error) {
	if pm.sourceModeReader != nil {
		return pm.sourceModeReader(ctx)
	}
	if pm.config.TopoClient == nil {
		return nil, errors.New("migration authority discovery unavailable")
	}
	var state *pb.MigrationRouting
	err := migrationcontrol.WithAuthority(ctx, pm.config.TopoClient, pm.record.ShardKey().Database, pm.config.ControlTransport, func(c rpc.MultipoolerServiceClient) error {
		r, err := c.GetMigrationMode(ctx, &rpc.GetMigrationModeRequest{Database: pm.record.ShardKey().Database})
		if err != nil {
			return err
		}
		state = r.Routing
		if state == nil {
			return errors.New("migration authority not initialized")
		}
		return nil
	})
	return state, err
}

// One authority discovery and protected callback returns both configuration and
// a confirmed routing snapshot. Bootstrap uses this same protected transport.
func (pm *MultipoolerManager) sourceCommandState(ctx context.Context, checkConfiguration bool) (*pb.MigrationRouting, error) {
	if pm.sourceModeReader != nil {
		return pm.sourceModeReader(ctx)
	}
	if pm.config.TopoClient == nil {
		return nil, errors.New("migration authority discovery unavailable")
	}
	var state *pb.MigrationRouting
	err := migrationcontrol.WithAuthority(ctx, pm.config.TopoClient, pm.record.ShardKey().Database, pm.config.ControlTransport, func(c rpc.MultipoolerServiceClient) error {
		reply, err := c.GetSourceConnection(migrationcontrol.AuthorizedContext(ctx, pm.config.MigrationKey), &rpc.GetSourceConnectionRequest{Database: pm.record.ShardKey().Database, ConnectionName: pm.config.SourceConnection})
		if err != nil {
			return err
		}
		if reply.Routing == nil {
			return errors.New("migration authority not initialized")
		}
		if checkConfiguration && (pm.config.SourceConfiguration == nil || !proto.Equal(reply.Connection, pm.config.SourceConfiguration)) {
			return errors.New("connection changed; restart the prepared source pooler before serving")
		}
		state = reply.Routing
		return nil
	})
	return state, err
}

// The same serialized read/apply/drain path serves both management modes.
// Expected state is correlation only: only the reader supplies admission policy.
// Lifecycle withdrawal advances the existing gate generation, rejecting an open
// based on a read that crossed a backend or consensus transition.
func (pm *MultipoolerManager) enforceRouting(ctx context.Context, expected *pb.MigrationRouting) (*rpc.RefreshRoutingResponse, error) {
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	cold := expected == nil
	g, err := pm.gate()
	if err != nil {
		return nil, err
	}
	guarded, ok := g.(interface {
		AdmissionGeneration() uint64
		ApplyAdmissionGeneration(uint64, bool) bool
	})
	var generation uint64
	if ok {
		generation = guarded.AdmissionGeneration()
	}
	state, err := pm.readEnforcementState(ctx, expected)
	if err != nil {
		return nil, err
	}
	if expected != nil && (state.ActiveRequestId != expected.ActiveRequestId || state.Mode != expected.Mode) {
		return nil, errors.New("routing operation/mode mismatch; recover the same operation")
	}
	allow := routingAdmission(state, pm.record.desired.Load().ManagementMode, pm.config.SourceConnection)
	if pm.IsUnmanaged() {
		if pm.config.SourceConnection == "" {
			return nil, errors.New("migration-enabled source required")
		}
		if allow {
			health, _ := pm.GetHealthState(ctx)
			if state.GetSourceIdentity().GetSystemIdentifier() == "" || health == nil || !health.BackendReady || !proto.Equal(health.BackendIdentity, state.SourceIdentity) {
				return nil, errors.New("source identity not prepared for current attachment")
			}
		}
	}
	lockCtx, err := pm.actionLock.Acquire(ctx, "RefreshRoutingApply")
	if err != nil {
		return nil, err
	}
	if !pm.IsUnmanaged() {
		role := pm.healthStreamer.getState().RoutingState.GetRole()
		if pm.stateManager != nil {
			role = pm.stateManager.RoutingRole()
		}
		if allow && role != pb.RoutingRole_ROUTING_ROLE_PRIMARY && role != pb.RoutingRole_ROUTING_ROLE_REPLICA {
			pm.actionLock.Release(lockCtx)
			return nil, errors.New("managed routing role is not established")
		}
		pm.servingEvents.mu.Lock()
		unavailable := pm.servingEvents.backendKnown && !pm.servingEvents.backendReady
		initialized := pm.servingEvents.confirmed
		pm.servingEvents.mu.Unlock()
		if allow && !cold && !initialized && pm.servingAuthority(pm.record.ShardKey().Database) != nil {
			pm.actionLock.Release(lockCtx)
			return nil, errors.New("managed admission not initialized by current authority")
		}
		if allow && unavailable {
			pm.actionLock.Release(lockCtx)
			return nil, errors.New("managed backend unavailable")
		}
	}
	if ok && allow {
		if !guarded.ApplyAdmissionGeneration(generation, allow) {
			pm.actionLock.Release(lockCtx)
			return nil, errors.New("admission changed during routing validation")
		}
	} else {
		g.SetApplicationAdmission(allow)
	}
	if pm.IsUnmanaged() {
		pm.sourceAdmission.Store(allow)
	}
	pm.actionLock.Release(lockCtx)
	// Never hold the action lock across a drain: completion callbacks need it.
	if !allow {
		if err = g.FenceApplication(ctx); err != nil {
			return nil, err
		}
		if pm.IsUnmanaged() {
			lockCtx, err = pm.actionLock.Acquire(ctx, "RefreshRoutingDrained")
			if err != nil {
				return nil, err
			}
			err = pm.stateManager.Mutate(lockCtx, func(s *servingStateMutation) { s.ServingStatus = pb.PoolerServingStatus_DISABLED })
			pm.actionLock.Release(lockCtx)
			if err != nil {
				return nil, err
			}
		}
	}
	if pm.IsUnmanaged() {
		pm.sourceModeLoaded.Store(true)
	} else if cold {
		pm.servingEvents.mu.Lock()
		pm.servingEvents.confirmed = true
		pm.servingEvents.mu.Unlock()
	}
	return &rpc.RefreshRoutingResponse{OperationId: state.ActiveRequestId, Mode: state.Mode, ProcessId: pm.record.desired.Load().Id, AdmissionOpen: allow}, nil
}

// Source state is a protected, confirmed current-authority callback. Followers
// use local WAL metadata: an exact request is sent only after the controller's
// durable commit. Opaque IDs carry no ordering; mismatch is bounded/retryable.
func (pm *MultipoolerManager) readEnforcementState(ctx context.Context, expected *pb.MigrationRouting) (*pb.MigrationRouting, error) {
	if pm.IsUnmanaged() {
		return pm.sourceCommandState(ctx, expected == nil || expected.Mode == pb.MigrationMode_MIGRATION_MODE_UNMANAGED)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if expected == nil && pm.servingAuthority(pm.record.ShardKey().Database) != nil {
		state, err := pm.sourceMode(ctx)
		if err != nil {
			return nil, err
		}
		if state == nil {
			return nil, errors.New("migration authority not initialized")
		}
		expected = state
	}
	if pm.sourceModeReader != nil { // deterministic authoritative-read test seam
		return pm.sourceModeReader(ctx)
	}
	if pm.servingAuthority(pm.record.ShardKey().Database) == nil {
		pm.servingEvents.mu.Lock()
		confirmed := pm.servingEvents.confirmed
		pm.servingEvents.mu.Unlock()
		if !confirmed {
			return pm.confirmedRoutingLocked(ctx)
		}
		return pm.servingCatalog.Routing(ctx)
	}
	c := pm.servingCatalog
	if c == nil {
		var err error
		c, err = servingcontrol.New(pm.qsc.InternalQueryService(), pm.config.MigrationKey)
		if err != nil {
			return nil, err
		}
	}
	return c.AwaitRouting(ctx, expected.ActiveRequestId, expected.Mode)
}

// Admission policy is shared by local enforcement and remote acknowledgment
// validation; readiness and identity checks remain local enforcement duties.
func routingAdmission(state *pb.MigrationRouting, management pb.PoolerManagementMode, connection string) bool {
	if management == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
		return state.Mode == pb.MigrationMode_MIGRATION_MODE_UNMANAGED && state.SourceConnection == connection
	}
	return state.Mode == pb.MigrationMode_MIGRATION_MODE_UNSET || state.Mode == pb.MigrationMode_MIGRATION_MODE_MANAGED
}

// Initial follower activation still requires fresh authority confirmation. Once
// initialized, operation-time refreshes use only local replay. Startup is not an
// acknowledgment and never substitutes for an explicit operation barrier.
func (pm *MultipoolerManager) initializeManagedAdmission(ctx context.Context) error {
	pm.servingEvents.mu.Lock()
	confirmed := pm.servingEvents.confirmed
	pm.servingEvents.mu.Unlock()
	if confirmed {
		return nil
	}
	_, err := pm.enforceRouting(ctx, nil)
	return err
}
