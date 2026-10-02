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

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// MetadataWriter is a local authority activation handle, not a durable workflow
// or receiver version. A colocated controller owns its schema/operations. Pooler
// code supplies only existing leadership serialization and transaction durability.
type MetadataWriter struct {
	pm       *MultipoolerManager
	database string
	ctx      context.Context
	cancel   context.CancelFunc
	active   atomic.Bool
}
type metadataLifecycle struct{ pm *MultipoolerManager }

func (l metadataLifecycle) OnStateChange(_ context.Context, _ servingstate.State) error {
	if w := l.pm.metadataWriter.Load(); w != nil {
		w.active.Store(false)
		w.cancel()
	}
	return nil
}

// ActivateMetadataWriter invalidates the previous local worker under the same
// action lock used by writes. Cancellation alone is not a write precondition.
func (pm *MultipoolerManager) ActivateMetadataWriter(ctx context.Context, database string) (*MetadataWriter, error) {
	lockCtx, err := pm.actionLock.Acquire(ctx, "ActivateMetadataWriter")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(database); err != nil {
		return nil, err
	}
	if !pm.metadataLifecycleRegistered {
		if pm.stateManager != nil {
			pm.stateManager.Register(metadataLifecycle{pm})
		}
		pm.metadataLifecycleRegistered = true
	}
	if old := pm.metadataWriter.Load(); old != nil {
		old.active.Store(false)
		old.cancel()
	}
	lifetime, cancel := context.WithCancel(pm.shutdownCtx)
	w := &MetadataWriter{pm: pm, database: database, ctx: lifetime, cancel: cancel}
	w.active.Store(true)
	pm.metadataWriter.Store(w)
	return w, nil
}
func (w *MetadataWriter) Context() context.Context { return w.ctx }
func (w *MetadataWriter) ShardKey() *pb.ShardKey {
	return proto.Clone(w.pm.record.ShardKey()).(*pb.ShardKey)
}

// Transaction returns no authoritative result on uncertain commit. Callback
// code must not perform remote calls or drains. An optional routing snapshot is
// published exactly after confirmed commit, independent of admission.
func (w *MetadataWriter) Transaction(ctx context.Context, update func(context.Context, executor.InternalTx, *connectioncatalog.Catalog) (*pb.GatewayRoutingPolicy, error)) error {
	pm := w.pm
	lockCtx, err := pm.actionLock.Acquire(ctx, "ControllerMetadata")
	if err != nil {
		return err
	}
	defer pm.actionLock.Release(lockCtx)
	if !w.active.Load() || pm.metadataWriter.Load() != w || w.ctx.Err() != nil {
		return errors.New("metadata writer activation lost")
	}
	if err = pm.servingAuthority(w.database); err != nil {
		return err
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return err
	}
	var snapshot *pb.GatewayRoutingPolicy
	err = c.Transaction(lockCtx, func(ctx context.Context, tx executor.InternalTx) error {
		var err error
		snapshot, err = update(ctx, tx, c)
		if snapshot != nil {
			snapshot = proto.Clone(snapshot).(*pb.GatewayRoutingPolicy)
		}
		return err
	})
	if err != nil {
		pm.healthStreamer.setRoutingPolicy(nil)
		pm.signalRoutingPublication()
		return err
	}
	if snapshot != nil {
		pm.healthStreamer.setRoutingPolicy(snapshot)
	}
	return nil
}
