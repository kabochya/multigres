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
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/manager"

// healthMetrics holds OTel metrics for pooler health/replication observability.
//
// Replication lag is already measured by the manager's heartbeat loop and stored
// in healthStreamer.replicationLagNs for the StreamPoolerHealth gRPC message;
// this publishes the same value as a metric so it can be dashboarded and alerted
// on without subscribing to the health stream. Serving-state transitions give
// failover/recovery visibility that was previously only inferable from logs.
type healthMetrics struct {
	replicationLag metric.Float64ObservableGauge
	transitions    metric.Int64Counter
	failoverSlots  metric.Int64ObservableGauge
}

// newHealthMetrics initialises health metrics. lagNsGetter returns the latest
// replication lag in nanoseconds; failoverSlotsReadyGetter/failoverSlotsTotalGetter
// return the latest logical failover-slot readiness counts (see
// failoverSlotReadiness). All are sampled by the observable-gauge callback at
// metric-collection time (not pushed), so they always reflect the most recent
// measurement. Best-effort: an instrument that fails to initialise is skipped
// and the joined error is returned for logging by the caller.
func newHealthMetrics(lagNsGetter func() int64, failoverSlotsReadyGetter, failoverSlotsTotalGetter func() int64) (*healthMetrics, error) {
	meter := otel.Meter(meterName)
	m := &healthMetrics{transitions: noop.Int64Counter{}}
	var errs []error

	var err error
	m.replicationLag, err = meter.Float64ObservableGauge(
		"mg.pooler.replication.lag",
		metric.WithDescription("PostgreSQL replication lag observed by this pooler (0 on a primary or before the first measurement)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.lag gauge: %w", err))
		m.replicationLag = nil
	}

	m.transitions, err = meter.Int64Counter(
		"mg.pooler.serving.transitions",
		metric.WithDescription("Serving-state transitions by from/to status"),
		metric.WithUnit("{transition}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.serving.transitions counter: %w", err))
		m.transitions = noop.Int64Counter{}
	}

	// Steady-state failover-slot readiness, labelled by status (ready/unready),
	// so an operator can see whether a node is failover-ready before it is ever
	// asked to fail over. Sampled continuously rather than only at promotion
	// time, so the value at any given failover can be read by correlating
	// timestamps against mg.pooler.logical_failover.duration.
	m.failoverSlots, err = meter.Int64ObservableGauge(
		"mg.pooler.logical_failover.slots",
		metric.WithDescription("Logical failover slots on this node, labelled by readiness status"),
		metric.WithUnit("{slot}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.logical_failover.slots gauge: %w", err))
		m.failoverSlots = nil
	}

	var instruments []metric.Observable
	for _, inst := range []metric.Observable{m.replicationLag, m.failoverSlots} {
		if inst != nil {
			instruments = append(instruments, inst)
		}
	}
	if len(instruments) > 0 {
		// The registration lives for the streamer's (i.e. the manager's)
		// lifetime. It is intentionally not torn down on manager close: the
		// streamer is reused across reopen, so unregistering would silently stop
		// the gauges after the first close. Per-test isolation comes from each
		// test shutting down its own meter provider.
		if _, err := meter.RegisterCallback(
			func(_ context.Context, o metric.Observer) error {
				if m.replicationLag != nil {
					// ns → s, matching the seconds unit used across mg.pooler.* durations.
					o.ObserveFloat64(m.replicationLag, float64(lagNsGetter())/1e9)
				}
				if m.failoverSlots != nil {
					ready := failoverSlotsReadyGetter()
					total := failoverSlotsTotalGetter()
					o.ObserveInt64(m.failoverSlots, ready, metric.WithAttributes(attribute.String("status", "ready")))
					o.ObserveInt64(m.failoverSlots, total-ready, metric.WithAttributes(attribute.String("status", "unready")))
				}
				return nil
			},
			instruments...,
		); err != nil {
			errs = append(errs, fmt.Errorf("health gauges callback: %w", err))
		}
	}

	return m, errors.Join(errs...)
}

// recordTransition counts a serving-status transition. Callers should only
// invoke it on an actual change (from != to).
func (m *healthMetrics) recordTransition(ctx context.Context, from, to clustermetadatapb.PoolerServingStatus) {
	if m == nil || m.transitions == nil {
		return
	}
	m.transitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("from", from.String()),
		attribute.String("to", to.String()),
	))
}

// managerMetrics holds the OTel instruments for the pooler manager.
type managerMetrics struct {
	rewindCheckpointWait    metric.Float64Histogram
	rewindExecutionDuration metric.Float64Histogram

	logicalFailoverDuration     metric.Float64Histogram
	logicalFailoverCount        metric.Int64Counter
	logicalFailoverStepDuration metric.Float64Histogram
	logicalFailoverSlotsDropped metric.Int64Counter
}

// logicalFailoverStatus labels the outcome of a logical-replication failover
// (leader promotion) or one of its steps.
type logicalFailoverStatus string

const (
	logicalFailoverStatusSuccess logicalFailoverStatus = "success"
	logicalFailoverStatusFailure logicalFailoverStatus = "failure"
)

// logicalFailoverStep labels a step within promotionHook's slot-management
// sequence, distinct from the failover as a whole.
type logicalFailoverStep string

const (
	logicalFailoverStepEnsureFollowerSlots  logicalFailoverStep = "ensure_follower_physical_slots"
	logicalFailoverStepSetSynchronizedSlots logicalFailoverStep = "set_synchronized_standby_slots"
)

// slotsDroppedReason labels why a logical-replication-related slot was
// dropped during cleanup.
type slotsDroppedReason string

const (
	// slotsDroppedReasonOrphaned is an un-synced failover-slot original left
	// behind on a former primary that has rejoined as a standby.
	slotsDroppedReasonOrphaned slotsDroppedReason = "orphaned"
	// slotsDroppedReasonDepartedFollower is a managed physical slot for a
	// follower no longer in the cohort.
	slotsDroppedReasonDepartedFollower slotsDroppedReason = "departed_follower"
)

// rewindPhase labels a pg_rewind execution-duration sample.
type rewindPhase string

const (
	// rewindPhaseDryRun is the read-only dry-run: connect to source, read the
	// timeline history, find the last common checkpoint, and scan the target WAL
	// (it may also run crash recovery first). No target data is written.
	rewindPhaseDryRun rewindPhase = "dry_run"
	// rewindPhaseRewind is the actual mutating pg_rewind (-R): the phase that
	// copies changed blocks and WAL from the source, whose runtime is dominated by
	// the retained pg_wal it copies.
	rewindPhaseRewind rewindPhase = "rewind"
)

// newManagerMetrics creates and registers the manager's OTel instruments. It
// always returns a non-nil *managerMetrics; a registration error is returned
// alongside, and the affected instrument is left nil (its record helper no-ops).
func newManagerMetrics() (*managerMetrics, error) {
	meter := otel.Meter(meterName)
	// How long a diverged follower's pg_rewind was held off waiting for the new
	// leader to advertise rewind-readiness — i.e. to complete its post-promotion
	// checkpoint onto the current timeline so it is safe to rewind from. Near zero
	// when the leader was already rewind-ready by the time the follower learned of
	// it; seconds when the follower had to wait for the checkpoint. A consistently
	// high distribution would argue for keeping the explicit post-promotion
	// checkpoint over relying on PostgreSQL's lazy one.
	wait, waitErr := meter.Float64Histogram(
		"multipooler.rewind.checkpoint_wait.duration",
		metric.WithDescription("Time a diverged follower's pg_rewind waited for the new leader to become rewind-ready (post-promotion checkpoint completion)"),
		metric.WithUnit("s"),
	)
	// How long pg_rewind itself ran, split by phase (dry_run vs rewind). This is
	// the actual subprocess runtime, distinct from checkpoint_wait above. It
	// matters operationally because pg_rewind runtime scales with retained pg_wal
	// (it copies the whole retained WAL, not just the divergence), so a rewind can
	// take minutes under load — long enough to matter for shutdown grace and for
	// the detached-rewind backstop timeout.
	exec, execErr := meter.Float64Histogram(
		"multipooler.rewind.execution.duration",
		metric.WithDescription("Duration of a pg_rewind invocation, labelled by phase (dry_run vs the mutating rewind)"),
		metric.WithUnit("s"),
	)

	// Overall duration of a logical-replication failover (leader promotion),
	// end to end through promotionHook: clearing any resigned-leader record,
	// slot-management steps, the advisory readiness check, and pg_promote. This
	// is the SLO-relevant "failover time" from the o11y guidelines, specific to
	// the slot-based logical-replication path.
	failoverDuration, failoverDurationErr := meter.Float64Histogram(
		"mg.pooler.logical_failover.duration",
		metric.WithDescription("Duration of a logical-replication failover (leader promotion), labelled by outcome"),
		metric.WithUnit("s"),
	)
	// Count of logical-replication failovers by outcome: the "number of
	// failovers" signal from the o11y guidelines. A rising failure count or an
	// unexpectedly high total both indicate instability worth investigating.
	failoverCount, failoverCountErr := meter.Int64Counter(
		"mg.pooler.logical_failover.count",
		metric.WithDescription("Number of logical-replication failovers (leader promotions), labelled by outcome"),
		metric.WithUnit("{failover}"),
	)
	// Duration of each individual slot-management step inside promotionHook, so
	// a slow or failing failover can be attributed to a specific step instead of
	// only the overall duration above.
	stepDuration, stepDurationErr := meter.Float64Histogram(
		"mg.pooler.logical_failover.step.duration",
		metric.WithDescription("Duration of a single step within a logical-replication failover, labelled by step and outcome"),
		metric.WithUnit("s"),
	)
	// Logical-replication-related slots dropped during cleanup: physical slots
	// for a departed follower during reconcile, and orphaned un-synced
	// failover-slot originals on demote/rejoin. A silent stop in either path
	// would leak slots (retaining WAL) with no obvious symptom until disk
	// pressure shows up.
	slotsDropped, slotsDroppedErr := meter.Int64Counter(
		"mg.pooler.logical_failover.slots_dropped",
		metric.WithDescription("Logical-replication-related slots dropped during cleanup, labelled by reason"),
		metric.WithUnit("{slot}"),
	)

	return &managerMetrics{
		rewindCheckpointWait:        wait,
		rewindExecutionDuration:     exec,
		logicalFailoverDuration:     failoverDuration,
		logicalFailoverCount:        failoverCount,
		logicalFailoverStepDuration: stepDuration,
		logicalFailoverSlotsDropped: slotsDropped,
	}, errors.Join(waitErr, execErr, failoverDurationErr, failoverCountErr, stepDurationErr, slotsDroppedErr)
}

// recordRewindCheckpointWait records how long a pg_rewind waited for the source
// leader to become rewind-ready before proceeding. Nil-receiver safe so manager
// values constructed without metrics (e.g. in unit tests) are no-ops.
func (m *managerMetrics) recordRewindCheckpointWait(ctx context.Context, d time.Duration) {
	if m == nil || m.rewindCheckpointWait == nil {
		return
	}
	m.rewindCheckpointWait.Record(ctx, d.Seconds())
}

// recordRewindExecutionDuration records how long a pg_rewind invocation took, for
// the given phase. Nil-receiver safe so manager values constructed without
// metrics (e.g. in unit tests) are no-ops.
func (m *managerMetrics) recordRewindExecutionDuration(ctx context.Context, phase rewindPhase, d time.Duration) {
	if m == nil || m.rewindExecutionDuration == nil {
		return
	}
	m.rewindExecutionDuration.Record(ctx, d.Seconds(),
		metric.WithAttributes(attribute.String("phase", string(phase))))
}

// recordLogicalFailover records the overall duration and outcome of a
// logical-replication failover (leader promotion). Nil-receiver safe.
func (m *managerMetrics) recordLogicalFailover(ctx context.Context, status logicalFailoverStatus, d time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("status", string(status)))
	if m.logicalFailoverDuration != nil {
		m.logicalFailoverDuration.Record(ctx, d.Seconds(), attrs)
	}
	if m.logicalFailoverCount != nil {
		m.logicalFailoverCount.Add(ctx, 1, attrs)
	}
}

// recordLogicalFailoverStep records the duration and outcome of a single step
// within a logical-replication failover. Nil-receiver safe.
func (m *managerMetrics) recordLogicalFailoverStep(ctx context.Context, step logicalFailoverStep, status logicalFailoverStatus, d time.Duration) {
	if m == nil || m.logicalFailoverStepDuration == nil {
		return
	}
	m.logicalFailoverStepDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("step", string(step)),
		attribute.String("status", string(status)),
	))
}

// recordSlotsDropped counts logical-replication-related slots dropped during
// cleanup, by reason. Nil-receiver safe; a zero count is a no-op so a cleanup
// that dropped nothing doesn't emit an empty data point.
func (m *managerMetrics) recordSlotsDropped(ctx context.Context, reason slotsDroppedReason, count int64) {
	if m == nil || m.logicalFailoverSlotsDropped == nil || count == 0 {
		return
	}
	m.logicalFailoverSlotsDropped.Add(ctx, count, metric.WithAttributes(attribute.String("reason", string(reason))))
}

// timeLogicalFailoverStep runs fn and records its duration and outcome as
// mg.pooler.logical_failover.step.duration, labelled by step.
func (pm *MultipoolerManager) timeLogicalFailoverStep(ctx context.Context, step logicalFailoverStep, fn func() error) error {
	start := time.Now()
	err := fn()
	status := logicalFailoverStatusSuccess
	if err != nil {
		status = logicalFailoverStatusFailure
	}
	pm.metrics.recordLogicalFailoverStep(ctx, step, status, time.Since(start))
	return err
}
