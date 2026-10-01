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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func admissionTestManager(t *testing.T) (*MultipoolerManager, *poolerserver.QueryPoolerServer, *pb.AdmissionSnapshot) {
	t.Helper()
	record := newRecordFromProto(&pb.Multipooler{Id: &pb.ID{Cell: "cell", Name: "incarnation-a"}, ShardKey: &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}})
	logger := newTestLogger()
	pm := &MultipoolerManager{record: record, config: &Config{}, actionLock: actionlock.NewActionLock(), healthStreamer: newHealthStreamer(logger, nil, "default", "0")}
	g := poolerserver.NewQueryPoolerServer(logger, nil, nil, "default", "0", pm, 0, false)
	pm.qsc = g
	g.EnableAdmissionControl(true)
	pm.healthStreamer.servingStatus = pb.PoolerServingStatus_SERVING
	pm.stateManager = NewStateManager(logger, record, func() *pb.ConsensusStatus { return nil })
	require.NoError(t, g.OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: pb.PoolerServingStatus_SERVING}))
	s := &pb.AdmissionSnapshot{AuthorityShardKey: record.ShardKey(), Controlled: true, Owner: "owner", Intent: &pb.AdmissionIntent{Owner: "owner", IntentId: "open-a", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN}}
	return pm, g, s
}

func TestAdmissionExactIntentAndIndependentRouting(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		return proto.Clone(s).(*pb.AdmissionSnapshot), nil
	}
	expected := proto.Clone(s.Intent).(*pb.AdmissionIntent)
	r, err := pm.enforceAdmission(t.Context(), expected)
	require.NoError(t, err)
	require.True(t, r.AdmissionOpen)
	require.Equal(t, "incarnation-a", r.ProcessId.Name)
	s.Intent.IntentId = "open-b" // matching OPEN alone must not acknowledge.
	_, err = pm.enforceAdmission(t.Context(), expected)
	require.Error(t, err)
	s.Intent.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED
	s.Intent.IntentId = "closed-c"
	closeIntent := proto.Clone(s.Intent).(*pb.AdmissionIntent)
	r, err = pm.enforceAdmission(t.Context(), closeIntent)
	require.NoError(t, err)
	require.False(t, r.AdmissionOpen)
	_, err = pm.enforceAdmission(t.Context(), expected)
	require.Error(t, err)
	_, err = g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.Error(t, err)
	// Duplicate exact closed calls acknowledge the same process and no routing state is changed.
	r, err = pm.enforceAdmission(t.Context(), closeIntent)
	require.NoError(t, err)
	require.True(t, proto.Equal(r.Observed, closeIntent))
	require.Nil(t, pm.healthStreamer.getState().RoutingPolicy)
}

func TestAdmissionLifecycleRejectsDelayedOpen(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	entered, release := make(chan struct{}), make(chan struct{})
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		close(entered)
		<-release
		return s, nil
	}
	done := make(chan error, 1)
	go func() { _, err := pm.enforceAdmission(t.Context(), s.Intent); done <- err }()
	<-entered
	require.NoError(t, (admissionLifecycle{pm}).OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: pb.PoolerServingStatus_DISABLED}))
	close(release)
	require.Error(t, <-done)
	_, err := g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.Error(t, err)
	pm.admissionRuntime.mu.Lock()
	require.False(t, pm.admissionRuntime.initialized)
	pm.admissionRuntime.mu.Unlock()
}

func TestAdmissionDrainTimeoutIsNotCompletion(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	_, err := pm.enforceAdmission(t.Context(), s.Intent)
	require.NoError(t, err)
	release, err := g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.NoError(t, err)
	s.Intent.IntentId = "close"
	s.Intent.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan struct{})
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		close(entered)
		return s, nil
	}
	done := make(chan error, 1)
	go func() {
		r, err := pm.enforceAdmission(ctx, s.Intent)
		if r != nil {
			done <- errors.New("acknowledged incomplete drain")
			return
		}
		done <- err
	}()
	<-entered
	// Wait deterministically until the gate is closed; no scheduler sleep.
	for {
		probe, probeErr := g.BeginRequest(nil, poolerserver.RequestNewReservation)
		if probeErr != nil {
			break
		}
		probe()
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	release()
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	r, err := pm.enforceAdmission(t.Context(), s.Intent)
	require.NoError(t, err)
	require.False(t, r.AdmissionOpen)
}

func TestAdmissionQueuedRequestCancellation(t *testing.T) {
	pm, _, s := admissionTestManager(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		reads.Add(1)
		close(entered)
		<-release
		return s, nil
	}
	done := make(chan error, 1)
	go func() { _, err := pm.enforceAdmission(t.Context(), s.Intent); done <- err }()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := pm.enforceAdmission(ctx, s.Intent)
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 1, reads.Load())
	close(release)
	require.NoError(t, <-done)
}

func TestAdmissionMissingControlledIntentNeverDefaultsOpen(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	s.Intent = nil
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	_, err := pm.enforceAdmission(t.Context(), nil)
	require.Error(t, err)
	_, err = g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.Error(t, err)
}

func TestRefreshAdmissionRequiresAuthentication(t *testing.T) {
	pm, _, s := admissionTestManager(t)
	_, err := pm.RefreshAdmission(t.Context(), &rpc.RefreshAdmissionRequest{Database: "postgres", Expected: s.Intent})
	require.Error(t, err)
}

func TestAdmissionOpenCannotAcknowledgeUnavailableBackend(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	pm.healthStreamer.servingStatus = pb.PoolerServingStatus_DISABLED
	require.NoError(t, g.OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: pb.PoolerServingStatus_DISABLED}))
	reply, err := pm.enforceAdmission(t.Context(), s.Intent)
	require.Error(t, err)
	require.Nil(t, reply, "OPEN cannot complete against an unavailable backend")
}
