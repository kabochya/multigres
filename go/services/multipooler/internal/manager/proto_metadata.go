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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protometadata"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// PROTOTYPE STUB: the cluster metadata served here lives in the throwaway
// multigres.proto_* tables (see package protometadata). Delete this file with
// them.

// metadataSchemaReady is set once the prototype tables are known to exist.
type metadataSchemaReady struct{ ready atomic.Bool }

// requireDefaultPrimary checks that this pooler is the current writable primary
// of the managed default tablegroup, the only place cluster metadata may be
// read. A follower's copy is never a source of truth.
func (pm *MultipoolerManager) requireDefaultPrimary(database string) error {
	key := pm.record.ShardKey()
	if database != key.GetDatabase() {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "database does not match pooler")
	}
	if pm.IsUnmanaged() || key.GetTableGroup() != constants.DefaultTableGroup || key.GetShard() != constants.DefaultShard {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "not the default primary pooler")
	}
	if pm.stateManager.RoutingRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "not the default primary pooler")
	}
	return nil
}

func (pm *MultipoolerManager) ensureMetadataSchema(ctx context.Context) error {
	if pm.metadataSchema.ready.Load() {
		return nil
	}
	if err := protometadata.EnsureSchema(ctx, pm.adminExec); err != nil {
		return err
	}
	pm.metadataSchema.ready.Store(true)
	return nil
}

// GetBackingConnection returns the named backing connection from the default
// primary's metadata.
func (pm *MultipoolerManager) GetBackingConnection(ctx context.Context, req *multipoolerservicepb.GetBackingConnectionRequest) (*multipoolerservicepb.GetBackingConnectionResponse, error) {
	if err := pm.requireDefaultPrimary(req.GetDatabase()); err != nil {
		return nil, err
	}
	if req.GetConnectionName() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "connection name is required")
	}
	if err := pm.ensureMetadataSchema(ctx); err != nil {
		return nil, mterrors.Wrap(err, "metadata unavailable")
	}
	result, err := pm.adminQueryArgs(ctx, "SELECT dsn, expected_system_identifier FROM multigres.proto_connections WHERE name = $1", req.GetConnectionName())
	if err != nil {
		return nil, mterrors.Wrap(err, "read backing connection")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "backing connection %q not found", req.GetConnectionName())
	}
	var dsn, sysID string
	if err := executor.ScanSingleRow(result, &dsn, &sysID); err != nil {
		return nil, errors.Join(errors.New("decode backing connection"), err)
	}
	return &multipoolerservicepb.GetBackingConnectionResponse{Connection: &multipoolerservicepb.BackingConnection{
		Name:                     req.GetConnectionName(),
		Url:                      dsn,
		ExpectedSystemIdentifier: sysID,
	}}, nil
}
