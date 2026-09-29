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

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/mterrors"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	code "github.com/multigres/multigres/go/pb/mtrpc"
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

// servingMu spans remote reads AND application. An older Unfence completes
// before a newer Fence acquires it. After Fence acknowledges, no earlier check
// can reopen this process. Readiness never writes sourceAdmission.
func (pm *MultipoolerManager) applySourceGate(ctx context.Context, open bool) error {
	if !pm.IsUnmanaged() || pm.config.SourceConnection == "" {
		return mterrors.New(code.Code_FAILED_PRECONDITION, "migration-enabled unmanaged pooler required")
	}
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	state, err := pm.sourceMode(ctx)
	if err != nil {
		return err
	}
	if open && (state.Mode != pb.MigrationMode_MIGRATION_MODE_UNMANAGED || state.SourceConnection != pm.config.SourceConnection) {
		return mterrors.New(code.Code_FAILED_PRECONDITION, "current authority does not permit source serving")
	}
	if !open && state.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED && state.Mode != pb.MigrationMode_MIGRATION_MODE_MANAGED {
		return mterrors.New(code.Code_FAILED_PRECONDITION, "current authority does not permit fencing")
	}
	g, err := pm.gate()
	if err != nil {
		return err
	}
	if open {
		pm.sourceAdmission.Store(true)
		g.SetApplicationAdmission(true)
		pm.sourceModeLoaded.Store(true)
		return nil
	}
	pm.sourceAdmission.Store(false)
	if err = g.FenceApplication(ctx); err != nil {
		return err
	}
	pm.sourceModeLoaded.Store(true)
	lockCtx, err := pm.actionLock.Acquire(ctx, "MigrationFence")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(lockCtx)
	return pm.stateManager.Mutate(lockCtx, func(s *servingStateMutation) { s.ServingStatus = pb.PoolerServingStatus_DISABLED })
}

func (pm *MultipoolerManager) runServingControl(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			pm.refreshServingControl(refreshCtx)
			cancel()
		}
	}
}

func (pm *MultipoolerManager) refreshServingControl(ctx context.Context) {
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	g, err := pm.gate()
	if err != nil {
		return
	}
	if pm.IsUnmanaged() {
		if pm.sourceModeLoaded.Load() {
			return
		} // warm processes keep accepted state on target loss.
		state, err := pm.sourceMode(ctx)
		if err != nil {
			return
		}
		allow := state.Mode == pb.MigrationMode_MIGRATION_MODE_UNMANAGED && state.SourceConnection == pm.config.SourceConnection
		pm.sourceAdmission.Store(allow)
		g.SetApplicationAdmission(allow)
		pm.sourceModeLoaded.Store(true)
		return
	}
	var c *servingcontrol.Catalog
	if pm.servingAuthority(pm.record.ShardKey().Database) == nil {
		c, err = pm.catalogLocked(ctx)
	} else {
		c, err = servingcontrol.New(pm.qsc.InternalQueryService(), pm.config.MigrationKey)
	}
	if err != nil {
		g.SetApplicationAdmission(false)
		return
	}
	state, err := c.Routing(ctx)
	if err != nil {
		g.SetApplicationAdmission(false)
		return
	}
	g.SetApplicationAdmission(state.Mode == pb.MigrationMode_MIGRATION_MODE_UNSET || state.Mode == pb.MigrationMode_MIGRATION_MODE_MANAGED)
}
