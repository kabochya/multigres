// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multipooler

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	mdpb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

const (
	routingE2EDatabase     = "postgres"
	routingImportOp        = "aaaaaaaa-0000-0000-0000-000000000001"
	routingSecondImportOp  = "aaaaaaaa-0000-0000-0000-000000000002"
	routingEnsuredImportOp = "aaaaaaaa-0000-0000-0000-000000000003"
	routingSplitOpA        = "aaaaaaaa-0000-0000-0000-00000000000a"
	routingSplitOpB        = "aaaaaaaa-0000-0000-0000-00000000000b"
	routingCreateOp        = "aaaaaaaa-0000-0000-0000-00000000000c"
)

// routingClients are the routing RPC clients of one pooler.
type routingClients struct {
	manager multipoolermanagerpb.MultipoolerManagerClient
	query   multipoolerpb.MultipoolerServiceClient
}

func newRoutingClients(t *testing.T, grpcPort int) routingClients {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return routingClients{
		manager: multipoolermanagerpb.NewMultipoolerManagerClient(conn),
		query:   multipoolerpb.NewMultipoolerServiceClient(conn),
	}
}

func (c routingClients) snapshot(t *testing.T) *mdpb.GetRoutingSnapshotResponse {
	t.Helper()
	resp, err := c.query.GetRoutingSnapshot(t.Context(), &mdpb.GetRoutingSnapshotRequest{Database: routingE2EDatabase})
	require.NoError(t, err)
	return resp
}

func shardSpec(from, to, shard string) *mdpb.ShardRoutingRuleSpec {
	return &mdpb.ShardRoutingRuleSpec{FromTablegroup: from, ToTablegroup: to, ToShard: shard}
}

// importFixture adds the connection routing-src, the tablegroup srctg bound
// to it with its 0-inf shard, as CREATE TABLEGROUP ... CONNECTION leaves them
// once the tablegroup is active, and an import operation into srctg.
const importFixture = `
INSERT INTO multigres.connections (name, dsn) VALUES ('routing-src', 'host=source.invalid dbname=app');
INSERT INTO multigres.tablegroup (name, state, backing_connection_id)
SELECT 'srctg', 'active', connection_id FROM multigres.connections WHERE name = 'routing-src';
INSERT INTO multigres.shard (tablegroup_id, shard_name, state)
SELECT tablegroup_id, '0-inf', 'active' FROM multigres.tablegroup WHERE name = 'srctg';
INSERT INTO multigres.operation (operation_id, kind, state, request)
SELECT '` + routingImportOp + `', 'import', 'preparing', jsonb_build_object('tablegroup_id', tablegroup_id, 'name', name)
FROM multigres.tablegroup WHERE name = 'srctg'`

// splitFixture adds two operations and two two-shard tablegroups, each with a
// split of its first shard in preparation, so two independent rule changes
// can race on the routing version. Shard ranges are non-NULL bytea.
const splitFixture = `
INSERT INTO multigres.operation (operation_id, kind, state) VALUES
	('` + routingSplitOpA + `', 'reshard', 'preparing'),
	('` + routingSplitOpB + `', 'reshard', 'preparing');
INSERT INTO multigres.tablegroup (name, state) VALUES ('commerce', 'active'), ('orders', 'active');
INSERT INTO multigres.shard (tablegroup_id, shard_name, range_start, range_end, state)
SELECT tg.tablegroup_id, s.name, s.range_start, s.range_end, s.state
FROM multigres.tablegroup tg CROSS JOIN (VALUES
	('0-80',   NULL,          '\x80'::bytea, 'active'),
	('80-inf', '\x80'::bytea, NULL,          'active'),
	('0-40',   NULL,          '\x40'::bytea, 'preparing'),
	('40-80',  '\x40'::bytea, '\x80'::bytea, 'preparing')
) AS s (name, range_start, range_end, state)
WHERE tg.name IN ('commerce', 'orders')`

// TestRoutingRules_RPCs drives the routing RPCs against a real catalog: which
// poolers serve them, EnsureImportTarget, setting, retrying and deleting
// rules, a race of two writers on the routing version, shard ranges in the
// snapshot, an unfinished operation blocking a tablegroup's routing, and the
// default tablegroup guard.
//
// Uses an isolated shard: it writes catalog rows.
func TestRoutingRules_RPCs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()

	primary := setup.GetPrimary(t)
	standbys := setup.GetStandbys()
	require.Len(t, standbys, 1)
	pc := newRoutingClients(t, primary.Multipooler.GrpcPort)
	sc := newRoutingClients(t, standbys[0].Multipooler.GrpcPort)
	db := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer db.Close()
	ctx := t.Context()

	t.Run("empty routing", func(t *testing.T) {
		snap := pc.snapshot(t)
		assert.Zero(t, snap.GetRoutingVersion())
		assert.True(t, snap.GetFull())
		assert.Empty(t, snap.GetRules())
	})

	t.Run("only the catalog primary serves routing", func(t *testing.T) {
		_, err := sc.query.GetRoutingSnapshot(ctx, &mdpb.GetRoutingSnapshotRequest{Database: routingE2EDatabase})
		assert.Equal(t, codes.Unavailable, status.Code(err), "standby snapshot: %v", err)
		_, err = sc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, Rules: []*mdpb.ShardRoutingRuleSpec{shardSpec("default", "srctg", "0-inf")},
		})
		assert.Equal(t, codes.Unavailable, status.Code(err), "standby set: %v", err)
		_, err = pc.query.GetRoutingSnapshot(ctx, &mdpb.GetRoutingSnapshotRequest{Database: "otherdb"})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "another database: %v", err)
	})

	t.Run("operation id must be a uuid", func(t *testing.T) {
		_, err := pc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: "not-a-uuid", Rules: []*mdpb.ShardRoutingRuleSpec{shardSpec("default", "srctg", "0-inf")},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "set: %v", err)
		_, err = pc.manager.EnsureImportTarget(ctx, &mdpb.EnsureImportTargetRequest{
			Database: routingE2EDatabase, OperationId: "not-a-uuid", Tablegroup: "srctg",
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "ensure: %v", err)
	})

	t.Run("set, retry and snapshot the import rule", func(t *testing.T) {
		_, err := db.ExecContext(ctx, importFixture)
		require.NoError(t, err)

		req := &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp,
			Rules: []*mdpb.ShardRoutingRuleSpec{shardSpec("default", "srctg", "0-inf")}, ExpectedRoutingVersion: 0,
		}
		resp, err := pc.manager.SetRoutingRules(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.GetRoutingVersion())

		// A retry with the same, now stale, expected version succeeds and
		// changes nothing.
		resp, err = pc.manager.SetRoutingRules(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, int64(1), resp.GetRoutingVersion())

		snap := pc.snapshot(t)
		assert.Equal(t, int64(1), snap.GetRoutingVersion())
		assert.Zero(t, snap.GetPlanEpoch(), "a rule from one shard to one shard keeps plans valid")
		require.Len(t, snap.GetRules(), 1)
		rule := snap.GetRules()[0]
		assert.Equal(t, routingImportOp, rule.GetOperationId())
		assert.Equal(t, mdpb.RoutingRuleScope_ROUTING_RULE_SCOPE_TABLEGROUP, rule.GetScope())
		assert.Equal(t, "default", rule.GetFromTablegroup())
		assert.Equal(t, "srctg", rule.GetToTablegroup())
		assert.Equal(t, "0-inf", rule.GetToShard())
		assert.Empty(t, rule.GetToKeyRange().GetStart())
		assert.Empty(t, rule.GetToKeyRange().GetEnd())
		assert.Equal(t, int64(1), rule.GetCreatedVersion())
	})

	t.Run("ensure import target", func(t *testing.T) {
		ensure := func(op, tg string) (*mdpb.EnsureImportTargetResponse, error) {
			return pc.manager.EnsureImportTarget(ctx, &mdpb.EnsureImportTargetRequest{
				Database: routingE2EDatabase, OperationId: op, Tablegroup: tg,
			})
		}

		// The target must exist, be active and be bound to a connection.
		_, err := db.ExecContext(ctx, `
INSERT INTO multigres.tablegroup (name, state) VALUES ('unboundtg', 'active');
INSERT INTO multigres.tablegroup (name, state, backing_connection_id)
SELECT 'creatingtg', 'creating', connection_id FROM multigres.connections WHERE name = 'routing-src'`)
		require.NoError(t, err)
		for tg, want := range map[string]string{
			"nosuchtg":   `tablegroup "nosuchtg" does not exist`,
			"unboundtg":  `tablegroup "unboundtg" is not bound to a connection`,
			"creatingtg": `tablegroup "creatingtg" is creating, not active`,
		} {
			_, err := ensure(routingEnsuredImportOp, tg)
			require.Error(t, err, tg)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err), tg)
			assert.Contains(t, err.Error(), want)
		}
		var ops int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM multigres.operation WHERE operation_id = $1`, routingEnsuredImportOp).Scan(&ops))
		assert.Zero(t, ops, "a refused ensure creates no operation")

		// The import's rule is already in place, so ensuring it changes
		// nothing.
		version := pc.snapshot(t).GetRoutingVersion()
		resp, err := ensure(routingImportOp, "srctg")
		require.NoError(t, err)
		assert.Equal(t, version, resp.GetRoutingVersion())
		assert.Equal(t, version, pc.snapshot(t).GetRoutingVersion(), "a rule already in place changes no routing")

		// A second import into the same tablegroup is refused.
		_, err = ensure(routingSecondImportOp, "srctg")
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "is already the target of operation "+routingImportOp)

		// After its rule is deleted, ensuring the import writes it again.
		del, err := pc.manager.DeleteRoutingRules(ctx, &mdpb.DeleteRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version,
		})
		require.NoError(t, err)
		resp, err = ensure(routingImportOp, "srctg")
		require.NoError(t, err)
		assert.Equal(t, del.GetRoutingVersion()+1, resp.GetRoutingVersion())
		snap := pc.snapshot(t)
		assert.Equal(t, resp.GetRoutingVersion(), snap.GetRoutingVersion())
		require.Len(t, snap.GetRules(), 1)
		rule := snap.GetRules()[0]
		assert.Equal(t, routingImportOp, rule.GetOperationId())
		assert.Equal(t, "default", rule.GetFromTablegroup())
		assert.Equal(t, "srctg", rule.GetToTablegroup())
		assert.Equal(t, "0-inf", rule.GetToShard())
	})

	t.Run("two writers race on the routing version", func(t *testing.T) {
		_, err := db.ExecContext(ctx, splitFixture)
		require.NoError(t, err)
		base := pc.snapshot(t).GetRoutingVersion()

		// Hold the catalog_state lock so both writers block on it, then let
		// them go: the first to get it commits version base+1, the second then
		// reads that version and must fail its expected version of base.
		lockTx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = lockTx.Rollback() }()
		_, err = lockTx.ExecContext(ctx, `SELECT 1 FROM multigres.catalog_state FOR UPDATE`)
		require.NoError(t, err)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, tc := range []struct{ op, tg string }{{routingSplitOpA, "commerce"}, {routingSplitOpB, "orders"}} {
			wg.Go(func() {
				_, errs[i] = pc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
					Database: routingE2EDatabase, OperationId: tc.op, ExpectedRoutingVersion: base,
					Rules: []*mdpb.ShardRoutingRuleSpec{shardSpec(tc.tg, tc.tg, "0-40"), shardSpec(tc.tg, tc.tg, "40-80")},
				})
			})
		}
		waitForCatalogStateWaiters(t, db, 2)
		require.NoError(t, lockTx.Rollback())
		wg.Wait()

		var won, lost int
		for _, err := range errs {
			if err == nil {
				won++
				continue
			}
			lost++
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Contains(t, err.Error(), fmt.Sprintf("routing version is %d, expected %d", base+1, base))
		}
		assert.Equal(t, 1, won, "exactly one writer commits: %v", errs)
		assert.Equal(t, 1, lost)

		snap := pc.snapshot(t)
		assert.Equal(t, base+1, snap.GetRoutingVersion())
		require.Len(t, snap.GetRules(), 3)
		ranges := map[string][2][]byte{}
		for _, r := range snap.GetRules()[1:] {
			ranges[r.GetToShard()] = [2][]byte{r.GetToKeyRange().GetStart(), r.GetToKeyRange().GetEnd()}
		}
		assert.Equal(t, map[string][2][]byte{
			"0-40":  {nil, {0x40}},
			"40-80": {{0x40}, {0x80}},
		}, ranges, "shard ranges round-trip through the snapshot")
	})

	t.Run("delete rules", func(t *testing.T) {
		start := pc.snapshot(t).GetRoutingVersion()
		version := start
		for _, op := range []string{routingSplitOpA, routingSplitOpB, routingImportOp} {
			resp, err := pc.manager.DeleteRoutingRules(ctx, &mdpb.DeleteRoutingRulesRequest{
				Database: routingE2EDatabase, OperationId: op, ExpectedRoutingVersion: version,
			})
			require.NoError(t, err, "delete %s", op)
			version = resp.GetRoutingVersion()
		}
		snap := pc.snapshot(t)
		assert.Empty(t, snap.GetRules())
		assert.Equal(t, start+2, snap.GetRoutingVersion(), "two deletes changed routing; the losing writer's had nothing to delete")
	})

	t.Run("an unfinished operation on a tablegroup blocks routing it", func(t *testing.T) {
		// Another operation on srctg, such as its creation, is still running.
		_, err := db.ExecContext(ctx, `INSERT INTO multigres.operation (operation_id, kind, state, request)
			SELECT $1::uuid, 'create_tablegroup', 'creating',
				jsonb_build_object('tablegroup_id', tablegroup_id, 'name', name, 'shards', jsonb_build_array('0-inf'))
			FROM multigres.tablegroup WHERE name = 'srctg'`, routingCreateOp)
		require.NoError(t, err)

		version := pc.snapshot(t).GetRoutingVersion()
		req := &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version,
			Rules: []*mdpb.ShardRoutingRuleSpec{shardSpec("default", "srctg", "0-inf")},
		}
		_, err = pc.manager.SetRoutingRules(ctx, req)
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), `tablegroup "srctg" is the subject of operation `+routingCreateOp)

		// The import sees that operation too.
		_, err = pc.manager.EnsureImportTarget(ctx, &mdpb.EnsureImportTargetRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, Tablegroup: "srctg",
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "is already the target of operation "+routingCreateOp)

		// Once that operation is done, the import may route the tablegroup.
		_, err = db.ExecContext(ctx, `UPDATE multigres.operation SET state = 'done' WHERE operation_id = $1::uuid`, routingCreateOp)
		require.NoError(t, err)
		resp, err := pc.manager.SetRoutingRules(ctx, req)
		require.NoError(t, err)
		resp2, err := pc.manager.DeleteRoutingRules(ctx, &mdpb.DeleteRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: resp.GetRoutingVersion(),
		})
		require.NoError(t, err)
		assert.Equal(t, version+2, resp2.GetRoutingVersion())
	})

	t.Run("default tablegroup with a table cannot be routed", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `INSERT INTO multigres.tablegroup_table (schema_name, table_name, tablegroup_id)
			SELECT 'public', 'routing_e2e', tablegroup_id FROM multigres.tablegroup WHERE name = 'default'`)
		require.NoError(t, err)
		_, err = pc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: pc.snapshot(t).GetRoutingVersion(),
			Rules: []*mdpb.ShardRoutingRuleSpec{shardSpec("default", "srctg", "0-inf")},
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "registered tables")

		// EnsureImportTarget validates its rule the same way.
		_, err = pc.manager.EnsureImportTarget(ctx, &mdpb.EnsureImportTargetRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, Tablegroup: "srctg",
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "registered tables")
	})
}

// waitForCatalogStateWaiters waits until n sessions are blocked taking the
// catalog_state row lock of a routing rule write.
func waitForCatalogStateWaiters(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%FROM multigres.catalog_state FOR UPDATE%'`).Scan(&waiting)
		return err == nil && waiting >= n
	}, 10*time.Second, 50*time.Millisecond, "both writers should wait for the catalog_state lock")
}
