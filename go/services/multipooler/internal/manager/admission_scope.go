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
	"github.com/google/uuid"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// requestScope checks the operation and the scope of a request and returns them: the operation
// as a canonical UUID, and the tablegroup the scope names. Only a whole tablegroup is a scope
// today.
func requestScope(operationID string, scope *multipoolerservicepb.AdmissionScope) (op, tablegroup string, err error) {
	op, err = canonicalOperationID(operationID)
	if err != nil {
		return "", "", err
	}
	if scope.GetTablegroup() == "" {
		return "", "", mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "the tablegroup of the scope is required")
	}
	if scope.GetShard() != "" || scope.GetTable() != "" {
		return "", "", mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "only a tablegroup scope is supported: shard and table scopes are not implemented")
	}
	return op, scope.GetTablegroup(), nil
}

// canonicalOperationID returns the operation id in the form PostgreSQL gives a UUID back, so that
// what a caller passes compares equal to what is stored.
func canonicalOperationID(operationID string) (string, error) {
	id, err := uuid.Parse(operationID)
	if err != nil {
		return "", mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "operation_id must be a UUID: %v", err)
	}
	return id.String(), nil
}
