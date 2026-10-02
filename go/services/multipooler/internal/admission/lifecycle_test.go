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

package admission

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestLifecycleRequiresOwnedClosureAndExactProofRelease(t *testing.T) {
	s := &pb.AdmissionSnapshot{Controlled: true, Owner: "owner", Intent: &pb.AdmissionIntent{Owner: "owner", IntentId: "closed", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED, SourceConnection: "source", SourceConfigurationBinding: "binding"}}
	a := &pb.SourceLifecycleAuthorization{Owner: "owner", ClosedIntentId: "closed", SourceConnection: "source", SourceConfigurationBinding: "binding", RetireSource: true}
	require.NoError(t, ValidateLifecycle(a, s))
	stale := proto.Clone(a).(*pb.SourceLifecycleAuthorization)
	stale.Owner = "old"
	require.Error(t, ValidateLifecycle(stale, s))
	stale = proto.Clone(a).(*pb.SourceLifecycleAuthorization)
	stale.ClosedIntentId = "previous-closed"
	require.Error(t, ValidateLifecycle(stale, s))
	a.ProofReleaseProcesses = []*pb.ID{{Component: pb.ID_MULTIPOOLER, Cell: "cell", Name: "k8s-pod-uid"}}
	require.NoError(t, ValidateLifecycle(a, s))
	a.RetireSource = false
	require.Error(t, ValidateLifecycle(a, s))
	a.RetireSource = true
	a.ProofReleaseProcesses = append(a.ProofReleaseProcesses, a.ProofReleaseProcesses[0])
	require.Error(t, ValidateLifecycle(a, s))
	s.Intent.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN
	require.Error(t, ValidateLifecycle(a, s))
}
