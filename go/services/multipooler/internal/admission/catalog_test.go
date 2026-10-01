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
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	qmock "github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

type projectionReader struct {
	controlled     bool
	owner, encoded string
}

func (q projectionReader) QueryArgs(context.Context, string, ...any) (*sqltypes.Result, error) {
	return qmock.MakeQueryResult([]string{"table_group", "shard", "controlled", "owner", "intent"}, [][]any{{"default", "0", q.controlled, q.owner, q.encoded}}), nil
}

func TestAdmissionProjectionApplicability(t *testing.T) {
	sk := &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0"}
	s, err := Read(t.Context(), projectionReader{}, sk, pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET)
	require.NoError(t, err)
	require.False(t, s.Controlled)
	_, err = Read(t.Context(), projectionReader{controlled: true, owner: "owner"}, sk, pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET)
	require.Error(t, err, "missing controlled intent never means ordinary")
	intent := &pb.AdmissionIntent{Owner: "owner", IntentId: "close", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED}
	data, err := proto.Marshal(intent)
	require.NoError(t, err)
	s, err = Read(t.Context(), projectionReader{controlled: true, owner: "owner", encoded: hex.EncodeToString(data)}, sk, intent.Subject)
	require.NoError(t, err)
	require.True(t, proto.Equal(s.Intent, intent))
	_, err = Read(t.Context(), projectionReader{controlled: true, owner: "other", encoded: hex.EncodeToString(data)}, sk, intent.Subject)
	require.Error(t, err)
	other := proto.Clone(sk).(*pb.ShardKey)
	other.Shard = "other"
	_, err = Read(t.Context(), projectionReader{}, other, intent.Subject)
	require.Error(t, err)
}

func TestAdmissionIntentBindings(t *testing.T) {
	i := &pb.AdmissionIntent{Owner: "owner", IntentId: "open", Subject: pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE, Permission: pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN}
	require.Error(t, Validate(i))
	i.SourceConnection = "source"
	i.SourceConfigurationBinding = "opaque"
	i.SourceIdentity = &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}
	require.NoError(t, Validate(i))
	i.Permission = pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED
	require.NoError(t, Validate(i))
	i.Subject = pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET
	require.Error(t, Validate(i))
	i.SourceConnection = ""
	i.SourceConfigurationBinding = ""
	i.SourceIdentity = nil
	require.NoError(t, Validate(i))
	i.IntentId = ""
	require.Error(t, Validate(i))
}
