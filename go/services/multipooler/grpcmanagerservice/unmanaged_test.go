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

package grpcmanagerservice

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadata "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestManagerServiceRejectsUnmanaged verifies that every management RPC, including ReloadConfig, fail with an
// explicit FAILED_PRECONDITION on an unmanaged pooler instead of reaching
// components (pgctld, consensus, backups) that do not exist for it.
func TestManagerServiceRejectsUnmanaged(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	pm, err := manager.NewMultipoolerManager(slog.New(slog.NewTextHandler(os.Stderr, nil)), &clustermetadata.Multipooler{
		Id:             &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "external"},
		ShardKey:       &clustermetadata.ShardKey{Database: "db", TableGroup: "migrateTG", Shard: "0-inf"},
		ManagementMode: clustermetadata.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED,
	}, &manager.Config{TopoClient: ts, ConnPoolConfig: connpoolmanager.NewConfig(viperutil.NewRegistry())})
	require.NoError(t, err)
	defer pm.ShutdownForTest(ctx)

	svc := &managerService{manager: pm}
	for name, call := range map[string]func() error{
		"WaitForLSN": func() error {
			_, err := svc.WaitForLSN(ctx, &multipoolermanagerdata.WaitForLSNRequest{})
			return err
		},
		"StartReplication": func() error {
			_, err := svc.StartReplication(ctx, &multipoolermanagerdata.StartReplicationRequest{})
			return err
		},
		"StopReplication": func() error {
			_, err := svc.StopReplication(ctx, &multipoolermanagerdata.StopReplicationRequest{})
			return err
		},
		"Status": func() error {
			_, err := svc.Status(ctx, &multipoolermanagerdata.StatusRequest{})
			return err
		},
		"Backup": func() error {
			_, err := svc.Backup(ctx, &multipoolermanagerdata.BackupRequest{})
			return err
		},
		"GetBackups": func() error {
			_, err := svc.GetBackups(ctx, &multipoolermanagerdata.GetBackupsRequest{})
			return err
		},
		"ExpireBackups": func() error {
			_, err := svc.ExpireBackups(ctx, &multipoolermanagerdata.ExpireBackupsRequest{})
			return err
		},
		"ResignLeadership": func() error {
			_, err := svc.ResignLeadership(ctx, &multipoolermanagerdata.ResignLeadershipRequest{})
			return err
		},
		"ReloadConfig": func() error {
			_, err := svc.ReloadConfig(ctx, &multipoolermanagerdata.ReloadConfigRequest{})
			return err
		},
		"SetPostgresRestartsEnabled": func() error {
			_, err := svc.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdata.SetPostgresRestartsEnabledRequest{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.Error(t, err)
			require.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(mterrors.FromGRPC(err)))
			require.ErrorContains(t, err, "not supported on an unmanaged pooler")
		})
	}
}
