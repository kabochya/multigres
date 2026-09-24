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
	"time"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/timer"
)

const externalReadinessQuery = "SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off'"

// startExternalMonitorLocked observes only. It never repairs or reconfigures PG.
func (pm *MultipoolerManager) startExternalMonitorLocked() {
	pm.pgMonitor.StartWithOptions(func(ctx context.Context) { pm.checkExternalReadiness(ctx) }, timer.WithFastStart())
}

func (pm *MultipoolerManager) checkExternalReadiness(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	result, err := pm.adminQuery(probeCtx, externalReadinessQuery)
	var writable bool
	if err == nil {
		err = executor.ScanSingleRow(result, &writable)
	}
	cancel()
	lockCtx, lockErr := pm.actionLock.Acquire(ctx, "ExternalReadiness")
	if lockErr != nil {
		return
	}
	defer pm.actionLock.Release(lockCtx)
	status := pb.PoolerServingStatus_DISABLED
	mode := pgmode.Unknown
	if err == nil && writable {
		status = pb.PoolerServingStatus_SERVING
		mode = pgmode.Primary
	}
	if mutateErr := pm.stateManager.Mutate(lockCtx, func(s *servingStateMutation) { s.ServingStatus = status; s.PostgresMode = mode }); mutateErr != nil {
		pm.logger.WarnContext(ctx, "external readiness transition failed", "error", mutateErr)
	}
}
