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
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
	"github.com/multigres/multigres/go/tools/telemetry"
)

func findMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			if mm.Name == name {
				return mm
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return metricdata.Metrics{}
}

func attrValue(t *testing.T, set attribute.Set, key string) string {
	t.Helper()
	v, ok := set.Value(attribute.Key(key))
	require.True(t, ok, "attribute %q missing", key)
	return v.AsString()
}

func newTestHealthStreamer(t *testing.T) (*healthStreamer, *sdkmetric.ManualReader) {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "test"}
	return newHealthStreamer(logger, id, "tg1", "0"), setup.MetricReader
}

// TestReplicationLagGauge verifies the observable gauge samples the lag atomic
// and converts nanoseconds to seconds.
func TestReplicationLagGauge(t *testing.T) {
	hs, reader := newTestHealthStreamer(t)

	hs.SetReplicationLag(2_500_000_000) // 2.5s in ns

	g := findMetric(t, reader, "mg.pooler.replication.lag")
	gauge, ok := g.Data.(metricdata.Gauge[float64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	assert.InDelta(t, 2.5, gauge.DataPoints[0].Value, 1e-9)
}

// TestServingTransitions verifies a serving-status change records a transition
// with from/to attributes, and that a no-op change does not.
func TestServingTransitions(t *testing.T) {
	hs, reader := newTestHealthStreamer(t)
	ctx := t.Context()

	// DISABLED (initial) → SERVING records one transition.
	require.NoError(t, hs.OnStateChange(ctx,
		servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRolePrimary}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// SERVING → SERVING is a no-op (role change only, primary → replica):
	// no new transition.
	require.NoError(t, hs.OnStateChange(ctx,
		servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING}))

	// SERVING → DISABLED records a second transition.
	require.NoError(t, hs.OnStateChange(ctx,
		servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED}))

	m := findMetric(t, reader, "mg.pooler.serving.transitions")
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2, "two distinct from/to transitions expected")

	total := int64(0)
	for _, dp := range sum.DataPoints {
		total += dp.Value
		from := attrValue(t, dp.Attributes, "from")
		to := attrValue(t, dp.Attributes, "to")
		assert.NotEqual(t, from, to, "a recorded transition must change status")
	}
	assert.Equal(t, int64(2), total)
}

// TestRecordTransition_NilSafe covers the guards in recordTransition: a nil
// receiver and a zero-value healthMetrics (nil counter) must both be no-ops.
func TestRecordTransition_NilSafe(t *testing.T) {
	from := clustermetadatapb.PoolerServingStatus_DISABLED
	to := clustermetadatapb.PoolerServingStatus_SERVING

	var nilM *healthMetrics
	nilM.recordTransition(t.Context(), from, to)

	(&healthMetrics{}).recordTransition(t.Context(), from, to)
}

// TestRewindExecutionDurationMetric verifies pg_rewind runtime is recorded per
// phase (dry_run vs the mutating rewind) with the durations converted to seconds.
func TestRewindExecutionDurationMetric(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordRewindExecutionDuration(t.Context(), rewindPhaseRewind, 2500*time.Millisecond)
	m.recordRewindExecutionDuration(t.Context(), rewindPhaseDryRun, 500*time.Millisecond)

	hist := findMetric(t, setup.MetricReader, "multipooler.rewind.execution.duration")
	h, ok := hist.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, h.DataPoints, 2, "one data point per phase")

	byPhase := map[string]float64{}
	for _, dp := range h.DataPoints {
		byPhase[attrValue(t, dp.Attributes, "phase")] = dp.Sum
	}
	assert.InDelta(t, 2.5, byPhase[string(rewindPhaseRewind)], 1e-9)
	assert.InDelta(t, 0.5, byPhase[string(rewindPhaseDryRun)], 1e-9)
}

// TestRecordRewindExecutionDuration_NilSafe covers the guards: a nil receiver and
// a zero-value managerMetrics (nil histogram) must both be no-ops.
func TestRecordRewindExecutionDuration_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordRewindExecutionDuration(t.Context(), rewindPhaseRewind, time.Second)

	(&managerMetrics{}).recordRewindExecutionDuration(t.Context(), rewindPhaseDryRun, time.Second)
}

// TestFailoverSlotReadinessGauge verifies the observable gauge samples the
// readiness atomics set via SetFailoverSlotReadiness, split by status label.
func TestFailoverSlotReadinessGauge(t *testing.T) {
	hs, reader := newTestHealthStreamer(t)

	hs.SetFailoverSlotReadiness(2, 3)

	g := findMetric(t, reader, "mg.pooler.logical_failover.slots")
	gauge, ok := g.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 2, "one data point per status")

	byStatus := map[string]int64{}
	for _, dp := range gauge.DataPoints {
		byStatus[attrValue(t, dp.Attributes, "status")] = dp.Value
	}
	assert.Equal(t, int64(2), byStatus["ready"])
	assert.Equal(t, int64(1), byStatus["unready"])
}

// TestRecordLogicalFailover verifies the overall failover duration and count
// are recorded per outcome.
func TestRecordLogicalFailover(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordLogicalFailover(t.Context(), logicalFailoverStatusSuccess, 1500*time.Millisecond)
	m.recordLogicalFailover(t.Context(), logicalFailoverStatusFailure, 500*time.Millisecond)

	hist := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.duration")
	h, ok := hist.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, h.DataPoints, 2, "one data point per status")
	byStatus := map[string]float64{}
	for _, dp := range h.DataPoints {
		byStatus[attrValue(t, dp.Attributes, "status")] = dp.Sum
	}
	assert.InDelta(t, 1.5, byStatus[string(logicalFailoverStatusSuccess)], 1e-9)
	assert.InDelta(t, 0.5, byStatus[string(logicalFailoverStatusFailure)], 1e-9)

	count := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.count")
	sum, ok := count.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2, "one data point per status")
	for _, dp := range sum.DataPoints {
		assert.Equal(t, int64(1), dp.Value)
	}
}

// TestRecordLogicalFailover_NilSafe covers the guards: a nil receiver and a
// zero-value managerMetrics (nil instruments) must both be no-ops.
func TestRecordLogicalFailover_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordLogicalFailover(t.Context(), logicalFailoverStatusSuccess, time.Second)

	(&managerMetrics{}).recordLogicalFailover(t.Context(), logicalFailoverStatusFailure, time.Second)
}

// TestRecordLogicalFailoverStep verifies step duration is recorded with both
// step and outcome labels.
func TestRecordLogicalFailoverStep(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordLogicalFailoverStep(t.Context(), logicalFailoverStepEnsureFollowerSlots, logicalFailoverStatusSuccess, 200*time.Millisecond)
	m.recordLogicalFailoverStep(t.Context(), logicalFailoverStepSetSynchronizedSlots, logicalFailoverStatusFailure, 100*time.Millisecond)

	hist := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.step.duration")
	h, ok := hist.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, h.DataPoints, 2)
	for _, dp := range h.DataPoints {
		step := attrValue(t, dp.Attributes, "step")
		status := attrValue(t, dp.Attributes, "status")
		switch step {
		case string(logicalFailoverStepEnsureFollowerSlots):
			assert.Equal(t, string(logicalFailoverStatusSuccess), status)
			assert.InDelta(t, 0.2, dp.Sum, 1e-9)
		case string(logicalFailoverStepSetSynchronizedSlots):
			assert.Equal(t, string(logicalFailoverStatusFailure), status)
			assert.InDelta(t, 0.1, dp.Sum, 1e-9)
		default:
			t.Fatalf("unexpected step %q", step)
		}
	}
}

// TestRecordLogicalFailoverStep_NilSafe covers the guards: a nil receiver and a
// zero-value managerMetrics (nil histogram) must both be no-ops.
func TestRecordLogicalFailoverStep_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordLogicalFailoverStep(t.Context(), logicalFailoverStepEnsureFollowerSlots, logicalFailoverStatusSuccess, time.Second)

	(&managerMetrics{}).recordLogicalFailoverStep(t.Context(), logicalFailoverStepEnsureFollowerSlots, logicalFailoverStatusSuccess, time.Second)
}

// TestRecordSlotsDropped verifies slots-dropped counts are recorded per reason
// and that a zero count is a no-op (no empty data point emitted).
func TestRecordSlotsDropped(t *testing.T) {
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-multipooler"))

	m, err := newManagerMetrics()
	require.NoError(t, err)

	m.recordSlotsDropped(t.Context(), slotsDroppedReasonOrphaned, 2)
	m.recordSlotsDropped(t.Context(), slotsDroppedReasonDepartedFollower, 1)
	m.recordSlotsDropped(t.Context(), slotsDroppedReasonDepartedFollower, 0) // no-op

	sum, ok := findMetric(t, setup.MetricReader, "mg.pooler.logical_failover.slots_dropped").Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2, "one data point per reason; the zero-count call adds none")

	byReason := map[string]int64{}
	for _, dp := range sum.DataPoints {
		byReason[attrValue(t, dp.Attributes, "reason")] = dp.Value
	}
	assert.Equal(t, int64(2), byReason[string(slotsDroppedReasonOrphaned)])
	assert.Equal(t, int64(1), byReason[string(slotsDroppedReasonDepartedFollower)])
}

// TestRecordSlotsDropped_NilSafe covers the guards: a nil receiver and a
// zero-value managerMetrics (nil counter) must both be no-ops.
func TestRecordSlotsDropped_NilSafe(t *testing.T) {
	var nilM *managerMetrics
	nilM.recordSlotsDropped(t.Context(), slotsDroppedReasonOrphaned, 1)

	(&managerMetrics{}).recordSlotsDropped(t.Context(), slotsDroppedReasonOrphaned, 1)
}
