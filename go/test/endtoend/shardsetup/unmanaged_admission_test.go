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

package shardsetup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Admission states, as the e2e tests name them.
const (
	admUnfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	admFencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	admFenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
)

// admStateOf is the state a fenced flag stands for in these tests: a completed fence.
func admStateOf(fenced bool) multipoolerservicepb.AdmissionState {
	if fenced {
		return admFenced
	}
	return admUnfenced
}

// refreshAdmission calls RefreshAdmission on one pooler, as the coordinator does.
func refreshAdmission(ctx context.Context, grpcPort int, database, operation, tablegroup string, fenced bool, version int64) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	return refreshAdmissionState(ctx, grpcPort, database, operation, tablegroup, admStateOf(fenced), version)
}

// refreshAdmissionState is refreshAdmission naming the state the coordinator expects.
func refreshAdmissionState(ctx context.Context, grpcPort int, database, operation, tablegroup string, state multipoolerservicepb.AdmissionState, version int64) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return multipoolerservicepb.NewMultipoolerServiceClient(conn).RefreshAdmission(ctx, &multipoolerservicepb.RefreshAdmissionRequest{
		Database: database, OperationId: operation, Scope: scopeOf(tablegroup), ExpectedState: state, ExpectedVersion: version,
	})
}

// admits reports whether the pooler admits application queries.
func admits(t *testing.T, grpcPort int) bool {
	t.Helper()
	return admissionError(t, grpcPort) == nil
}

// admissionError is the error of an application query through the pooler, nil when it
// is admitted.
func admissionError(t *testing.T, grpcPort int) error {
	t.Helper()
	client, err := NewMultipoolerClient(grpcPort)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = client.Pooler.ExecuteQuery(ctx, "SELECT 1", 1)
	return err
}

func requireAdmits(t *testing.T, grpcPort int, msg string) {
	t.Helper()
	var last error
	ok := assert.Eventually(t, func() bool {
		last = admissionError(t, grpcPort)
		return last == nil
	}, 30*time.Second, 200*time.Millisecond, msg)
	require.True(t, ok, "%s: last rejection: %v", msg, last)
}

// waitBackendReady waits until the pooler has validated its external backend.
func waitBackendReady(t *testing.T, p *unmanagedPooler) {
	t.Helper()
	require.Eventually(t, func() bool {
		h, err := p.firstHealth(t.Context())
		return err == nil && h.GetBackendReady()
	}, 60*time.Second, 200*time.Millisecond, "backend never became ready")
}
