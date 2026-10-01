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

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/admission"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

func (pm *MultipoolerManager) ReadSourceLifecycle(ctx context.Context, r *rpc.ReadSourceLifecycleRequest) (*rpc.ReadSourceLifecycleResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	lockCtx, err := pm.actionLock.Acquire(ctx, "ReadSourceLifecycle")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(r.GetDatabase()); err != nil {
		return nil, err
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return nil, err
	}
	var a *pb.SourceLifecycleAuthorization
	err = c.Transaction(lockCtx, func(ctx context.Context, tx executor.InternalTx) error {
		if err := admission.InitializeOrdinary(ctx, tx, pm.record.ShardKey()); err != nil {
			return err
		}
		if err := admission.InitializeLifecycle(ctx, tx); err != nil {
			return err
		}
		var err error
		a, err = admission.ConfirmLifecycle(ctx, tx, pm.record.ShardKey())
		return err
	})
	if err != nil {
		return nil, err
	}
	if a != nil && a.SourceConnection != r.GetSourceConnection() {
		return nil, errors.New("lifecycle source association mismatch")
	}
	return &rpc.ReadSourceLifecycleResponse{AuthorityShardKey: proto.Clone(pm.record.ShardKey()).(*pb.ShardKey), Authorization: a}, nil
}
