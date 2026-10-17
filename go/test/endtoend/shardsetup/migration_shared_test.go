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

package shardsetup

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/metadataclient"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Running the migration scenarios quickly.
//
// A migration cluster costs half a minute to start: etcd, a default cohort whose
// PostgreSQL instances are initialised, an external PostgreSQL, an unmanaged pooler and
// a gateway. Two things keep the suite short:
//
//   - Scenarios that leave the cluster as they found it, once reset, run one after the
//     other on ONE shared cluster (TestMigrationScenariosOnASharedCluster). The reset is
//     strict and asserts it worked.
//   - Scenarios that break their cluster (a failover, a killed or suspended process, a
//     backend the pooler lost track of) each build their own, and run in parallel, at
//     most MULTIGRES_E2E_PARALLEL clusters at a time (default 3: a cluster is about four
//     PostgreSQL instances).
//
// MULTIGRES_E2E_SLOW=1 adds the scenarios that are slow by nature (CI runs them); see
// slowScenario.

// fixtureSlots bounds how many migration clusters are alive at once.
var fixtureSlots = make(chan struct{}, e2eParallelism())

func e2eParallelism() int {
	if v, err := strconv.Atoi(os.Getenv("MULTIGRES_E2E_PARALLEL")); err == nil && v > 0 {
		return v
	}
	return 3
}

// markedParallel remembers which tests already called t.Parallel, so that a test and
// the fixture it builds can both ask.
var markedParallel sync.Map

// runInParallel makes the test run in parallel with the other parallel tests of the
// package, once the serial ones are done. Asking twice is fine.
func runInParallel(t *testing.T) {
	t.Helper()
	if _, loaded := markedParallel.LoadOrStore(t, true); loaded {
		return
	}
	t.Parallel()
}

// acquireFixtureSlot makes the test parallel and waits for room for one more cluster.
// The slot is given back after everything the test registered for cleanup has run,
// which is when the cluster's processes are gone.
func acquireFixtureSlot(t *testing.T) {
	t.Helper()
	runInParallel(t)
	fixtureSlots <- struct{}{}
	t.Cleanup(func() { <-fixtureSlots })
}

// slowScenario skips a scenario that is slow by nature (it fails a primary over twice
// and waits out a drain that is long on purpose) unless MULTIGRES_E2E_SLOW=1 or the run
// is in CI, where the variable CI is set: CI runs everything. MULTIGRES_E2E_SLOW=0
// skips them there too.
func slowScenario(t *testing.T) {
	t.Helper()
	switch os.Getenv("MULTIGRES_E2E_SLOW") {
	case "1":
		return
	case "0":
	default:
		if os.Getenv("CI") != "" {
			return
		}
	}
	t.Skip("slow scenario: set MULTIGRES_E2E_SLOW=1 to run it (CI runs it)")
}

// sharedScenario is a migration scenario that runs on the shared cluster. It may do
// anything that reset undoes: move traffic, fence, write rows, open and abandon sessions.
type sharedScenario struct {
	name string
	run  func(t *testing.T, f *migrationFixture)
}

// sharedScenarios are run in this order. A scenario that cannot promise reset puts the
// cluster back belongs in its own cluster, not here.
var sharedScenarios = []sharedScenario{
	{name: "FencedDefaultStillServesTheControlPlane", run: scenarioFencedDefaultStillServesTheControlPlane},
	{name: "NotifyInATransactionIsSentAfterTheCommit", run: scenarioNotifyAfterTheCommit},
	{name: "SessionsHoldingStateKeepTheFenceBusy", run: scenarioSessionsHoldingState},
	{name: "DirectClientsOfTheSourceMakeTheFenceBusyOnlyWhileTheyRunSomething", run: scenarioDirectClientsOfTheSource},
	{name: "TheLookForUnexplainedBackends", run: scenarioTheLookForUnexplainedBackends},
	{name: "CoordinatorCallsAreSafeToRepeat", run: scenarioCoordinatorCallsAreSafeToRepeat},
	{name: "FenceIsNotCleanWhenAMemberDoesNotKnowTheProtocol", run: scenarioFenceIsNotCleanWhenAMemberDoesNotKnowTheProtocol},
	{name: "MigrationCutoverAndRollbackUnderLoad", run: scenarioCutoverAndRollbackUnderLoad},
}

// TestMigrationScenariosOnASharedCluster runs the scenarios that do not break their
// cluster on one cluster of three default poolers, resetting it before each. Run one
// with -run 'TestMigrationScenariosOnASharedCluster/<name>'.
func TestMigrationScenariosOnASharedCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("requires built binaries, etcd and PostgreSQL")
	}
	base := newMigrationFixtureWith(t, replicated(migrationOptions{drain: 3 * time.Second, shared: true}))
	var dirty atomic.Value // the scenario after which the reset failed
	previous := "(none: the first scenario)"
	for _, sc := range sharedScenarios {
		before := previous
		ok := t.Run(sc.name, func(t *testing.T) {
			if by, ok := dirty.Load().(string); ok {
				t.Fatalf("the shared cluster was not left clean by %q, and every scenario after it would prove nothing", by)
			}
			sc.run(t, base.forScenario(t, before))
		})
		previous = sc.name
		// A failed scenario may leave anything behind. Find out now whether a reset can
		// still undo it, and say which scenario to look at if it cannot.
		if !ok {
			if err := base.tryReset(t); err != nil {
				dirty.Store(sc.name)
				t.Logf("reset after %s failed: %v", sc.name, err)
			}
		}
	}
}

// forScenario is the fixture as one scenario sees it: the shared cluster, reset to its
// initial state, with a gateway of its own that has just started, so that nothing a
// previous scenario did to a gateway (a buffer cycle, the timing guard, a plan cache,
// a routing it has seen) carries over.
//
// after names the scenario that ran before it: if the reset fails, that is the scenario that
// left the cluster in a state that cannot be reset, whether or not it passed.
func (f *migrationFixture) forScenario(t *testing.T, after string) *migrationFixture {
	t.Helper()
	v := *f
	v.t = t
	require.NoError(t, v.reset(), "resetting the shared cluster before this scenario; the scenario before it was %q", after)
	v.gatewayPort = startRoutedGateway(t, v.def)
	ctx := t.Context()
	require.Eventually(t, func() bool {
		c, err := pgx.Connect(ctx, appDSN(v.gatewayPort))
		if err != nil {
			return false
		}
		defer c.Close(ctx)
		port, err := backendPort(ctx, c)
		return err == nil && port == v.srcPortText
	}, 60*time.Second, 200*time.Millisecond, "the scenario's gateway never served the application role from the source")
	return &v
}

// waitForNoHeldBackends waits for the application's backends on a server to settle, as a client
// that disconnected leaves them until the pooler lets go, and fails if one still holds
// anything: an open transaction, a session lock, a running statement.
func waitForNoHeldBackends(ctx context.Context, conn *pgx.Conn) error {
	var held string
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := conn.QueryRow(ctx, `SELECT coalesce(string_agg(pid || ' ' || state || ' ' || left(query, 60), '; '), '')
FROM pg_stat_activity
WHERE usename = $1 AND backend_type = 'client backend' AND pid <> pg_backend_pid()
  AND (state <> 'idle' OR EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = pg_stat_activity.pid AND l.locktype = 'advisory'))`, appRole).Scan(&held)
		if err != nil {
			return fmt.Errorf("looking at the backends: %w", err)
		}
		if held == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("application backends still hold something: %s", held)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// tryReset is reset for the harness's own use, outside any scenario.
func (f *migrationFixture) tryReset(t *testing.T) error {
	v := *f
	v.t = t
	return v.reset()
}

// reset puts the shared cluster back to what newMigrationFixtureWith built, through
// the same RPCs a migrator uses where there is one, and returns an error if anything
// does not come back clean. A scenario's clients are gone by now (the scenario's
// cleanups closed them); what they left behind is what is checked.
//
//  1. If routing moved, fence both tablegroups (each at the version it reads), flip back
//     to migrateTG, and unfence both. Otherwise unfence whatever is fenced.
//  2. Wait for the application backends of the source and of the default cohort to settle,
//     and require that none holds anything: no open transaction, no session lock, no
//     running statement; and require that topology has no pooler but the cluster's own.
//  3. Empty the ledger on both sides and require that no table of the application is
//     left but the ledger.
//  4. Require every pooler to admit, the routing to name migrateTG, and both
//     tablegroups to be unfenced.
func (f *migrationFixture) reset() error {
	t := f.t
	ctx := t.Context()
	fail := func(format string, args ...any) error {
		return fmt.Errorf("reset: "+format, args...)
	}
	ctl := f.control()
	st, err := ctl.tryState(ctx, "migrateTG", "default")
	if err != nil {
		return fail("reading the serving state: %w", err)
	}
	rowOf := func(resp *multipoolerservicepb.GetServingStateResponse, tg string) *multipoolerservicepb.TablegroupServingState {
		for _, row := range resp.Tablegroups {
			if row.Tablegroup == tg {
				return row
			}
		}
		return nil
	}
	for _, tg := range []string{"migrateTG", "default"} {
		if rowOf(st, tg) == nil {
			return fail("no row for %s", tg)
		}
	}
	if st.AppTablegroup != "migrateTG" {
		for _, tg := range []string{"default", "migrateTG"} {
			if !closedRow(rowOf(st, tg)) {
				if _, err := ctl.fence(ctx, tg); err != nil {
					return fail("fencing %s to flip back: %w", tg, err)
				}
			}
		}
		if _, err := ctl.flip(ctx, st.AppTablegroup, "migrateTG"); err != nil {
			return fail("flipping back to migrateTG: %w", err)
		}
		if st, err = ctl.tryState(ctx, "migrateTG", "default"); err != nil {
			return fail("reading the serving state: %w", err)
		}
	}
	for _, tg := range []string{"migrateTG", "default"} {
		if row := rowOf(st, tg); closedRow(row) {
			if err := f.lib().UnfenceUntilReached(ctx, tg, row.Version); err != nil {
				return fail("unfencing %s: %w", tg, err)
			}
		}
	}

	// The source's application backends settle: a scenario's clients closed, and the
	// pooler's own reserved connections for them with them.
	srcAdmin, err := pgx.Connect(ctx, externalDSN(f.srcPort))
	if err != nil {
		return fail("connecting to the source: %w", err)
	}
	defer srcAdmin.Close(ctx)
	for name, conn := range map[string]*pgx.Conn{"source": srcAdmin, "default cohort": f.adminConn(f.def)} {
		if err := waitForNoHeldBackends(ctx, conn); err != nil {
			return fail("the %s: %w", name, err)
		}
	}

	// The cluster's topology is as built: the default poolers and the source's.
	want := map[string]bool{"mig-src-1": true}
	for name := range f.def.Multipoolers {
		want[name] = true
	}
	poolers, err := metadataclient.Poolers(ctx, f.def.TopoServer, f.def.Database)
	if err != nil {
		return fail("listing the poolers in topology: %w", err)
	}
	for _, p := range poolers {
		if !want[p.GetId().GetName()] {
			return fail("topology still has the pooler %q, which a scenario registered and did not remove", p.GetId().GetName())
		}
	}

	// Data: nothing but an empty ledger.
	for name, conn := range map[string]*pgx.Conn{"source": srcAdmin, "default": f.adminConn(f.def)} {
		tables, err := userTables(ctx, conn)
		if err != nil {
			return fail("listing the tables of the %s: %w", name, err)
		}
		if len(tables) != 1 || tables[0] != "public.ledger" {
			return fail("the %s has tables %v; a shared scenario must clean up what it creates", name, tables)
		}
		if _, err := conn.Exec(ctx, "TRUNCATE public.ledger"); err != nil {
			return fail("emptying the ledger of the %s: %w", name, err)
		}
	}

	// The state is as built.
	if st, err = ctl.tryState(ctx, "migrateTG", "default"); err != nil {
		return fail("reading the serving state: %w", err)
	}
	if st.AppTablegroup != "migrateTG" {
		return fail("routing names %s, not migrateTG", st.AppTablegroup)
	}
	for _, tg := range []string{"migrateTG", "default"} {
		if row := rowOf(st, tg); closedRow(row) {
			return fail("%s is fenced after the reset", tg)
		}
	}
	ports := map[string]int{"source pooler": f.src.grpcPort}
	for name, inst := range f.def.Multipoolers {
		ports["default pooler "+name] = inst.Multipooler.GrpcPort
	}
	for name, port := range ports {
		ok := false
		for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
			if admits(t, port) {
				ok = true
				break
			}
		}
		if !ok {
			return fail("the %s does not admit application queries after the reset", name)
		}
	}
	return nil
}
