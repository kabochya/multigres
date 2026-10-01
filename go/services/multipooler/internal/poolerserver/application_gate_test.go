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

package poolerserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestApplicationFenceWaitsForAdmissionPermit(t *testing.T) {
	s := newStartRequestTestServer()
	s.servingStatus = pb.PoolerServingStatus_SERVING
	release, err := s.BeginRequest(nil, RequestSingleQuery)
	require.NoError(t, err)
	s.SetApplicationAdmission(false)
	_, err = s.BeginRequest(nil, RequestSingleQuery)
	require.Error(t, err)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, s.FenceApplication(canceled), context.Canceled)
	release()
	release() // permit release is idempotent.
	require.NoError(t, s.FenceApplication(t.Context()))
	s.servingStatus = pb.PoolerServingStatus_SERVING // readiness recovery never reopens.
	require.Error(t, s.StartRequest(nil, RequestSingleQuery))
	s.SetApplicationAdmission(true)
	require.NoError(t, s.StartRequest(nil, RequestSingleQuery))
}

func TestApplicationFenceAllowsReservationCleanup(t *testing.T) {
	s := newStartRequestTestServer()
	s.SetApplicationAdmission(false)
	release, err := s.BeginRequest(nil, RequestExistingReserved)
	require.NoError(t, err)
	release()
	require.NoError(t, s.FenceApplication(t.Context()))
	require.Error(t, s.StartRequest(nil, RequestNewReservation))
}
