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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/timer"
)

const (
	// externalReadinessQuery is true when the endpoint is a writable primary.
	externalReadinessQuery = "SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only') = 'off'"
	// externalIdentityQuery reports the physical identity of the endpoint.
	externalIdentityQuery = "SELECT system_identifier::text, current_database() FROM pg_control_system()"
	externalProbeTimeout  = 2 * time.Second
)

// bindingRecheckInterval is how often a ready pooler asks the default primary again which
// connection its tablegroup is bound to. A variable so that tests can shorten it.
var bindingRecheckInterval = 30 * time.Second

// errExternalIdentityMismatch means the endpoint answered but is not the source
// the backing connection names.
var errExternalIdentityMismatch = errors.New("external backend identity does not match its backing connection")

// startExternalMonitorLocked starts the observe-only monitor of an external
// backend. It never repairs or reconfigures the external database: a failed
// probe withdraws readiness and nothing more.
//
// Caller must hold pm.mu.
func (pm *MultipoolerManager) startExternalMonitorLocked() {
	pm.pgMonitor.StartWithOptions(func(ctx context.Context) { pm.checkExternalReadiness(ctx) }, timer.WithFastStart())
}

// probeExternalBackend reports whether the endpoint is reachable, writable, and
// the intended source. Identity is checked on every probe, so a reconnect that
// lands on a different server (a failover, a repointed DNS name) withdraws
// readiness instead of silently serving from it.
func (pm *MultipoolerManager) probeExternalBackend(ctx context.Context) error {
	result, err := pm.adminQuery(ctx, externalReadinessQuery)
	if err != nil {
		return err
	}
	var writable bool
	if err := executor.ScanSingleRow(result, &writable); err != nil {
		return err
	}
	if !writable {
		return errors.New("external backend is not writable")
	}
	result, err = pm.adminQuery(ctx, externalIdentityQuery)
	if err != nil {
		return err
	}
	var sysID, database string
	if err := executor.ScanSingleRow(result, &sysID, &database); err != nil {
		return err
	}
	want := pm.config.ExternalBackend
	if want == nil || want.ExpectedSystemIdentifier == "" || sysID != want.ExpectedSystemIdentifier || database != want.Database {
		return errExternalIdentityMismatch
	}
	return nil
}

// confirmBinding applies the binding policy of tablegroup_binding.go: the tablegroup has to be
// bound to this pooler's connection, because a pooler that fronts one connection never serves a
// tablegroup that is bound to another, or to none. A pooler that is not ready needs a
// successful confirmation. A ready one asks again every bindingRecheckInterval, not on every
// probe, so that it does not depend on the default primary for each of them: a definitive
// contradiction withdraws its readiness, and no answer keeps it as it is, to be asked again on
// the next probe.
//
// It runs only on the monitor's goroutine, which is what guards bindingConfirmedAt.
func (pm *MultipoolerManager) confirmBinding(ctx context.Context) error {
	wasReady := pm.healthStreamer.getState().BackendReady
	if wasReady && time.Since(pm.bindingConfirmedAt) < bindingRecheckInterval {
		return nil
	}
	err := pm.checkTablegroupBinding(ctx)
	switch {
	case err == nil:
		pm.bindingConfirmedAt = time.Now()
		return nil
	case wasReady && !errors.Is(err, errBindingContradicted):
		pm.logger.WarnContext(ctx, "could not recheck the binding of the tablegroup; keeping the backend ready", "error", err)
		return nil
	}
	return err
}

func (pm *MultipoolerManager) checkExternalReadiness(ctx context.Context) {
	probeCtx, cancel := context.WithTimeout(ctx, externalProbeTimeout)
	err := pm.probeExternalBackend(probeCtx)
	if err == nil {
		err = pm.confirmBinding(probeCtx)
	}
	cancel()
	ready := err == nil
	if err != nil {
		pm.logger.DebugContext(ctx, "external backend not ready", "error", err)
	}
	if !ready {
		// A withdrawal is reported at once, before and whatever becomes of the serving state.
		pm.healthStreamer.setBackendReady(false)
	}

	lockCtx, lockErr := pm.actionLock.Acquire(ctx, "ExternalReadiness")
	if lockErr != nil {
		return
	}
	defer pm.actionLock.Release(lockCtx)

	status := clustermetadatapb.PoolerServingStatus_DISABLED
	mode := pgmode.Unknown
	if ready {
		status = clustermetadatapb.PoolerServingStatus_SERVING
		mode = pgmode.Primary
	}
	if mutateErr := pm.stateManager.Mutate(lockCtx, func(s *servingStateMutation) {
		s.ServingStatus = status
		s.PostgresMode = mode
	}); mutateErr != nil {
		pm.logger.WarnContext(ctx, "external readiness transition failed", "error", mutateErr)
		return
	}
	// Ready is reported only once the pooler serves, so the health stream never says
	// backend_ready while the serving state is still DISABLED.
	if ready {
		pm.healthStreamer.setBackendReady(true)
	}
}
