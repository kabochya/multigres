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
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/routingpolicy"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// routingPublication tracks local authority lifecycle only. It never interprets
// admission intent, enumerates peers, or advances migration operations.
type routingPublication struct {
	mu        sync.Mutex
	event     chan struct{}
	routing   *pb.RoutingState
	available bool
	cancel    context.CancelFunc
}
type routingLifecycle struct{ pm *MultipoolerManager }

func (l routingLifecycle) OnStateChange(_ context.Context, s servingstate.State) error {
	pm := l.pm
	p := &pm.routingPublication
	r := s.Routing.ToProto()
	available := s.ServingStatus == pb.PoolerServingStatus_SERVING && r.GetRole() == pb.RoutingRole_ROUTING_ROLE_PRIMARY
	p.mu.Lock()
	changed := !proto.Equal(p.routing, r) || p.available != available
	if changed {
		p.routing = r
		p.available = available
		if p.cancel != nil {
			p.cancel()
		}
	}
	p.mu.Unlock()
	if changed {
		pm.healthStreamer.setRoutingPolicy(nil)
		pm.signalRoutingPublication()
	}
	return nil
}

func (pm *MultipoolerManager) signalRoutingPublication() {
	p := &pm.routingPublication
	p.mu.Lock()
	if p.event == nil {
		p.event = make(chan struct{}, 1)
	}
	ch := p.event
	p.mu.Unlock()
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (pm *MultipoolerManager) startRoutingPublication() {
	if pm.IsUnmanaged() || len(pm.config.MigrationKey) != 32 {
		return
	}
	pm.signalRoutingPublication()
	_ = pm.stateManager.RegisterAndSync(pm.shutdownCtx, routingLifecycle{pm})
	go pm.runRoutingPublication(pm.shutdownCtx)
}

func (pm *MultipoolerManager) runRoutingPublication(ctx context.Context) {
	p := &pm.routingPublication
	p.mu.Lock()
	events := p.event
	p.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-events:
		}
		burst, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.cancel = cancel
		available := p.available
		p.mu.Unlock()
		if available {
			for n := range 5 {
				readCtx, stop := context.WithTimeout(burst, 3*time.Second)
				_, err := pm.confirmRoutingPolicy(readCtx)
				stop()
				if err == nil || n == 4 || burst.Err() != nil {
					break
				}
				timer := time.NewTimer((100 * time.Millisecond) << n)
				select {
				case <-burst.Done():
					timer.Stop()
				case <-timer.C:
				}
			}
		}
		cancel()
	}
}

func (pm *MultipoolerManager) confirmRoutingPolicy(ctx context.Context) (*pb.GatewayRoutingPolicy, error) {
	lockCtx, err := pm.actionLock.Acquire(ctx, "ConfirmRoutingPolicy")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(pm.record.ShardKey().Database); err != nil {
		return nil, err
	}
	if existing := pm.healthStreamer.getState().RoutingPolicy; existing != nil {
		return existing, nil
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return nil, err
	}
	var snapshot *pb.GatewayRoutingPolicy
	err = c.Transaction(lockCtx, func(ctx context.Context, tx executor.InternalTx) error {
		if err := routingpolicy.Initialize(ctx, tx, pm.record.ShardKey().Database); err != nil {
			return err
		}
		var err error
		snapshot, err = routingpolicy.Confirm(ctx, tx, pm.record.ShardKey().Database)
		return err
	})
	if err != nil {
		pm.healthStreamer.setRoutingPolicy(nil)
		return nil, err
	}
	if err = pm.servingAuthority(pm.record.ShardKey().Database); err != nil {
		return nil, err
	}
	pm.healthStreamer.setRoutingPolicy(snapshot)
	return snapshot, nil
}

func (pm *MultipoolerManager) GetRoutingPolicy(ctx context.Context, r *rpc.GetRoutingPolicyRequest) (*rpc.GetRoutingPolicyResponse, error) {
	if r.GetDatabase() != pm.record.ShardKey().Database {
		return nil, errors.New("routing database differs from authority")
	}
	snapshot, err := pm.confirmRoutingPolicy(ctx)
	if err != nil {
		return nil, err
	}
	return &rpc.GetRoutingPolicyResponse{Policy: snapshot}, nil
}

func (pm *MultipoolerManager) SetRoutingPolicy(ctx context.Context, r *rpc.SetRoutingPolicyRequest) (*rpc.SetRoutingPolicyResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	if err := routingpolicy.Validate(r.GetPolicy()); err != nil {
		return nil, err
	}
	lockCtx, err := pm.actionLock.Acquire(ctx, "SetRoutingPolicy")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(r.GetDatabase()); err != nil {
		return nil, err
	}
	if err = pm.authorizeServingAdmin(lockCtx, r.GetUsername(), r.GetUserAuth()); err != nil {
		return nil, err
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return nil, err
	}
	snapshot := proto.Clone(r.Policy).(*pb.GatewayRoutingPolicy)
	err = c.Transaction(lockCtx, func(ctx context.Context, tx executor.InternalTx) error {
		if err := routingpolicy.Initialize(ctx, tx, r.Database); err != nil {
			return err
		}
		if _, err := routingpolicy.Read(ctx, tx, r.Database); err != nil {
			return err
		}
		return routingpolicy.Write(ctx, tx, r.Database, snapshot, c)
	})
	if err != nil {
		pm.healthStreamer.setRoutingPolicy(nil)
		pm.signalRoutingPublication()
		return nil, err
	}
	// This exact transaction snapshot is published once commit is confirmed.
	pm.healthStreamer.setRoutingPolicy(snapshot)
	return &rpc.SetRoutingPolicyResponse{Policy: snapshot}, nil
}
