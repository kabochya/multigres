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
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

func TestWarmSourceAuthorityOutageAndBackendReconnect(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	p := proto.Clone(pm.record.desired.Load()).(*pb.Multipooler)
	p.ManagementMode = pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	p.ProcessIncarnation = "source-process-a"
	pm.record.desired.Store(p)
	pm.config.SourceConfiguration = &rpc.SourceConnection{Name: "source"}
	pm.config.SourceConfigurationBinding = "binding"
	identity := &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}
	s.Intent.Subject = pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE
	s.Intent.SourceConnection = "source"
	s.Intent.SourceConfigurationBinding = "binding"
	s.Intent.SourceIdentity = identity
	pm.healthStreamer.backendReady = true
	pm.healthStreamer.backendIdentity = identity
	pm.stateManager.pgMode = pgmode.Primary
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	r, err := pm.enforceAdmission(t.Context(), s.Intent)
	require.NoError(t, err)
	require.True(t, r.AdmissionOpen)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		return nil, errors.New("target outage")
	}
	_, err = pm.enforceAdmission(t.Context(), s.Intent)
	require.Error(t, err)
	release, err := g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.NoError(t, err)
	release()
	require.NoError(t, (admissionLifecycle{pm}).OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: pb.PoolerServingStatus_DISABLED}))
	pm.healthStreamer.backendReady = false
	_, err = pm.enforceAdmission(t.Context(), s.Intent)
	require.Error(t, err)
	_, err = g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.Error(t, err, "reconnected backend cannot inherit old warm-source authority")
}

func TestAuthoritativeInvalidProjectionClosesAdmission(t *testing.T) {
	pm, g, s := admissionTestManager(t)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	_, err := pm.enforceAdmission(t.Context(), s.Intent)
	require.NoError(t, err)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		return nil, admission.ErrInvalidProjection
	}
	_, err = pm.enforceAdmission(t.Context(), s.Intent)
	require.Error(t, err)
	_, err = g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.Error(t, err)
}

func TestAdmissionProcessReplacementKeepsConsensusIdentity(t *testing.T) {
	pm, _, s := admissionTestManager(t)
	old := proto.Clone(pm.record.desired.Load()).(*pb.Multipooler)
	old.ProcessIncarnation = "old-process"
	pm.record.desired.Store(old)
	require.NoError(t, pm.validateAdmissionIncarnation("old-process"))
	replacement := proto.Clone(old).(*pb.Multipooler)
	replacement.ProcessIncarnation = "new-process"
	pm.record.desired.Store(replacement)
	require.True(t, proto.Equal(old.Id, replacement.Id))
	require.Error(t, pm.validateAdmissionIncarnation("old-process"))
	require.Error(t, pm.validateAdmissionIncarnation(""))
	require.NoError(t, pm.validateAdmissionIncarnation("new-process"))
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) { return s, nil }
	r, err := pm.enforceAdmission(t.Context(), s.Intent)
	require.NoError(t, err)
	require.Equal(t, "new-process", r.ProcessIncarnation)
}

func TestAdmissionEventsReuseInitializedStateWhileIdle(t *testing.T) {
	pm, _, s := admissionTestManager(t)
	var reads atomic.Int32
	read := make(chan struct{}, 1)
	pm.admissionRuntime.reader = func(context.Context, pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
		reads.Add(1)
		read <- struct{}{}
		return s, nil
	}
	pm.config.MigrationKey = bytes.Repeat([]byte{1}, 32)
	pm.admissionRuntime.events = make(chan struct{}, 1)
	pm.admissionRuntime.serving = true
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { pm.runAdmission(ctx); close(done) }()
	pm.signalAdmission()
	<-read
	// Synchronize with completion using the same local enforcement serial.
	pm.admissionRuntime.mu.Lock()
	serial := pm.admissionRuntime.serial
	pm.admissionRuntime.mu.Unlock()
	<-serial
	serial <- struct{}{}
	for range 10 {
		_ = pm.healthStreamer.getState()
	}
	cancel()
	<-done
	require.EqualValues(t, 1, reads.Load())
}

func TestManagedRestartWithoutEncryptionKeyStartsClosed(t *testing.T) {
	pooler := &pb.Multipooler{Id: &pb.ID{Component: pb.ID_MULTIPOOLER, Cell: "cell", Name: "restart"}, ShardKey: &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"}}
	pm, err := NewMultipoolerManager(newTestLogger(), pooler, &Config{})
	require.NoError(t, err)
	g := pm.qsc.(*poolerserver.QueryPoolerServer)
	require.NoError(t, g.OnStateChange(t.Context(), servingstate.State{Routing: servingstate.RoutingState{Role: servingstate.RoutingRoleReplica}, ServingStatus: pb.PoolerServingStatus_SERVING}))
	_, err = g.BeginRequest(nil, poolerserver.RequestSingleQuery)
	require.Error(t, err, "missing key is not ordinary applicability proof")
}
