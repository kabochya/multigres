//go:build migration_demo

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
	"net"
	"strings"

	"google.golang.org/grpc/peer"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Test-only controller stub: it simulates a completed replication barrier and
// exercises the same in-process hooks as a future controller. It is excluded
// from normal builds and restricted to loopback, authenticated target admins.
func (pm *MultipoolerManager) demoServingControl(ctx context.Context, r *rpc.ServingControlRequest) (*rpc.ServingControlResponse, bool, error) {
	if !strings.HasPrefix(r.Operation, "demo-") {
		return nil, false, nil
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, true, errors.New("demo controller requires loopback")
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		return nil, true, errors.New("demo controller requires loopback")
	}
	journal := func(ctx context.Context, tx executor.InternalTx) error {
		if _, err := tx.Query(ctx, `CREATE TABLE IF NOT EXISTS multigres.demo_controller_journal (request_id TEXT PRIMARY KEY, operation TEXT NOT NULL)`); err != nil {
			return err
		}
		if _, err := tx.QueryArgs(ctx, `INSERT INTO multigres.demo_controller_journal VALUES($1,$2) ON CONFLICT DO NOTHING`, r.RequestId, r.Operation); err != nil {
			return err
		}
		if r.Operation == "demo-rollback" {
			return errors.New("simulated journal failure")
		}
		return nil
	}
	migrationID, err := pm.MigrationIdentity(ctx)
	if err != nil {
		return nil, true, err
	}
	fenceID := r.ConnectionName // Activation must identify its owning fence, including retries.
	switch r.Operation {
	case "demo-fence", "demo-rollback":
		err = pm.ControllerTransition(ctx, migrationID, r.RequestId, "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal)
	case "demo-managed":
		err = pm.ControllerTransition(ctx, migrationID, r.RequestId, fenceID, pb.MigrationMode_MIGRATION_MODE_MANAGED, journal)
	case "demo-unmanaged":
		err = pm.ControllerTransition(ctx, migrationID, r.RequestId, fenceID, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, journal)
	case "demo-complete":
		err = pm.CompleteMigration(ctx, migrationID, r.RequestId, journal)
	default:
		return nil, true, errors.New("unsupported demo controller operation")
	}
	if err != nil {
		return nil, true, err
	}
	state, err := pm.routingSnapshot(ctx)
	return &rpc.ServingControlResponse{Routing: state}, true, err
}
