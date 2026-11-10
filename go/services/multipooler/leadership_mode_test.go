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

package multipooler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backingconn/backingconntest"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// newTestMultipooler builds a Multipooler with just enough servenv wiring for flag and
// configuration tests; it is never started.
func newTestMultipooler() *Multipooler {
	reg := viperutil.NewRegistry()
	grpcServer := servenv.NewGrpcServer(reg)
	return NewMultipooler(telemetry.NewTelemetry(), reg, servenv.ProcessResources{
		ServEnv:         servenv.NewServEnv(reg),
		GrpcServer:      grpcServer,
		QueryGrpcServer: grpcServer,
	}, "/", false)
}

func TestValidateBackendFlags(t *testing.T) {
	none := func(string) bool { return false }
	only := func(name string) func(string) bool { return func(n string) bool { return n == name } }

	// Managed poolers may set any of the local-postgres flags.
	require.NoError(t, validateBackendFlags(false, none))
	require.NoError(t, validateBackendFlags(false, only("pg-port")))

	// An unmanaged pooler has no pgctld, no local postgres and no backups: every flag that
	// belongs to them is refused, the ones pgctld's Status is adopted into included.
	require.NoError(t, validateBackendFlags(true, none))
	for _, name := range managedPostgresFlags() {
		require.ErrorContains(t, validateBackendFlags(true, only(name)), "--"+name, name)
	}
	require.ElementsMatch(t, []string{
		"socket-file", "pooler-dir", "pg-port", "pgctld-addr", "pgbackrest-port",
		"pgbackrest-cert-file", "pgbackrest-key-file", "pgbackrest-ca-file", "pgbackrest-cipher-key-file",
	}, managedPostgresFlags())
}

// TestManagedPostgresFlagsAreRegisteredFlags: the list is of real flags, so that renaming one
// cannot leave it silently unchecked.
func TestManagedPostgresFlagsAreRegisteredFlags(t *testing.T) {
	mp := newTestMultipooler()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	for _, name := range managedPostgresFlags() {
		require.NotNil(t, flags.Lookup(name), name)
	}
	require.Nil(t, flags.Lookup("backing-connection"), "the connection comes from the catalog, not a flag")
}

func TestBackingConnectionValidate(t *testing.T) {
	for _, tc := range backingconntest.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			c := backingConnection{Host: tc.Host, Port: tc.Port, Database: tc.Database, User: tc.User}
			if tc.Refused == "" {
				require.NoError(t, c.Validate())
				return
			}
			require.ErrorContains(t, c.Validate(), tc.Refused)
		})
	}
}

// fakeSource answers every question the same way, and counts them.
type fakeSource struct {
	conn    *backingConnection
	managed bool
	err     error
	asked   []int64
}

func (f *fakeSource) TableGroupBinding(_ context.Context, tablegroupID int64) (*backingConnection, bool, error) {
	f.asked = append(f.asked, tablegroupID)
	return f.conn, f.managed, f.err
}

// TestAnAnswerIsUsedOnlyIfTheSourceGaveAUsableOne: whatever the source is, nothing, both a
// connection and managed, an incomplete connection and a connection that names several servers
// are refused, and a refusal is not taken for managed.
func TestAnAnswerIsUsedOnlyIfTheSourceGaveAUsableOne(t *testing.T) {
	good := &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "app", User: "u"}
	got, managed, err := fetchBinding(t.Context(), &fakeSource{conn: good}, 7)
	require.NoError(t, err)
	require.False(t, managed)
	require.Same(t, good, got)

	got, managed, err = fetchBinding(t.Context(), &fakeSource{managed: true}, 7)
	require.NoError(t, err)
	require.True(t, managed)
	require.Nil(t, got)

	_, _, err = fetchBinding(t.Context(), &fakeSource{}, 7)
	require.ErrorIs(t, err, errInvalidBackingConnection, "a source that says nothing")

	_, _, err = fetchBinding(t.Context(), &fakeSource{conn: good, managed: true}, 7)
	require.ErrorIs(t, err, errInvalidBackingConnection, "both")

	incomplete := *good
	incomplete.User = ""
	_, _, err = fetchBinding(t.Context(), &fakeSource{conn: &incomplete}, 7)
	require.ErrorIs(t, err, errInvalidBackingConnection)

	hostList := *good
	hostList.Host = "a.example.com,b.example.com"
	_, _, err = fetchBinding(t.Context(), &fakeSource{conn: &hostList}, 7)
	require.ErrorIs(t, err, errInvalidBackingConnection)

	withCredentials := *good
	withCredentials.Host = "alice@db.example.com"
	_, _, err = fetchBinding(t.Context(), &fakeSource{conn: &withCredentials}, 7)
	require.ErrorIs(t, err, errInvalidBackingConnection)

	_, managed, err = fetchBinding(t.Context(), &fakeSource{err: errors.New("unavailable")}, 7)
	require.ErrorContains(t, err, "unavailable")
	require.NotErrorIs(t, err, errInvalidBackingConnection, "a source's own failure is not a verdict on the connection")
	require.False(t, managed, "a failed read is not managed")
}

// TestIPv6HostsAreValidAndKeepTheirColons: a literal IPv6 address is a host, and is dialed as
// one (see the pgprotocol client).
func TestIPv6HostsAreValidAndKeepTheirColons(t *testing.T) {
	c := backingConnection{Host: "::1", Port: 5432, Database: "app", User: "u"}
	require.NoError(t, c.Validate())
}

// resolveFixture is a pooler under test with a topology and a source of bindings.
type resolveFixture struct {
	mp      *Multipooler
	ts      topoclient.Store
	factory *memorytopo.Factory
	id      *clustermetadatapb.ID
}

func newResolveFixture(t *testing.T, source connectionSource, static bool, args ...string) *resolveFixture {
	t.Helper()
	ts, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	mp := newTestMultipooler()
	mp.ts = ts
	mp.staticLeader = static
	mp.connections = source
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	require.NoError(t, flags.Parse(append([]string{"--database=db", "--cell=zone1", "--service-id=ext", "--shard=0-inf"}, args...)))
	return &resolveFixture{mp: mp, ts: ts, factory: factory, id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "ext"}}
}

func (f *resolveFixture) register(t *testing.T, mode clustermetadatapb.LeadershipMode) {
	t.Helper()
	require.NoError(t, f.ts.CreateMultipooler(t.Context(), &clustermetadatapb.Multipooler{Id: f.id, LeadershipMode: mode}))
}

func (f *resolveFixture) resolve(ctx context.Context, tableGroup string) (leadership, error) {
	return f.mp.resolveLeadership(ctx, f.mp.senv.GetLogger(), f.id, tableGroup)
}

// TestResolveLeadership: a pooler learns how it is managed from its own topology record, else
// from its tablegroup's catalog row; a static leader, a pooler of the default tablegroup and one
// with no catalog id are managed without a lookup.
func TestResolveLeadership(t *testing.T) {
	const (
		consensus   = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS
		static      = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
		unmanaged   = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED
		unspecified = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNSPECIFIED
	)
	bound := &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "db", User: "u"}
	for name, tc := range map[string]struct {
		source     *fakeSource
		static     bool
		tableGroup string
		id         string
		record     *clustermetadatapb.LeadershipMode
		want       clustermetadatapb.LeadershipMode
		wantAsked  int
		wantErr    string
	}{
		"the default tablegroup is managed, without a lookup":          {source: &fakeSource{conn: bound}, tableGroup: "default", id: "5", want: consensus},
		"a static leader is managed, without a lookup":                 {source: &fakeSource{conn: bound}, static: true, tableGroup: "migrateTG", id: "5", want: static},
		"a tablegroup with no catalog id is managed, without a lookup": {source: &fakeSource{conn: bound}, tableGroup: "migrateTG", id: "0", want: consensus},
		"a bound tablegroup is unmanaged":                              {source: &fakeSource{conn: bound}, tableGroup: "migrateTG", id: "5", want: unmanaged, wantAsked: 1},
		"a tablegroup with no binding is managed":                      {source: &fakeSource{managed: true}, tableGroup: "migrateTG", id: "5", want: consensus, wantAsked: 1},
		"a managed record is managed, without a lookup":                {source: &fakeSource{conn: bound}, tableGroup: "migrateTG", id: "5", record: new(consensus), want: consensus},
		"a legacy record is managed, without a lookup":                 {source: &fakeSource{conn: bound}, tableGroup: "migrateTG", id: "5", record: new(unspecified), want: consensus},
		"a static record is managed, without a lookup":                 {source: &fakeSource{conn: bound}, static: true, tableGroup: "migrateTG", id: "5", record: new(static), want: static},
		"an unmanaged record reads its connection":                     {source: &fakeSource{conn: bound}, tableGroup: "migrateTG", id: "5", record: new(unmanaged), want: unmanaged, wantAsked: 1},
		"an unmanaged record the catalog calls managed is refused":     {source: &fakeSource{managed: true}, tableGroup: "migrateTG", id: "5", record: new(unmanaged), wantAsked: 1, wantErr: "cannot start as"},
		"an unmanaged record with no catalog id is refused":            {source: &fakeSource{conn: bound}, tableGroup: "migrateTG", id: "0", record: new(unmanaged), wantErr: "with a tablegroup in the catalog"},
		"an unmanaged record of the default tablegroup is refused":     {source: &fakeSource{conn: bound}, tableGroup: "default", id: "5", record: new(unmanaged), wantErr: "with a tablegroup in the catalog"},
		"an unmanaged record of a static leader is refused":            {source: &fakeSource{conn: bound}, static: true, tableGroup: "migrateTG", id: "5", record: new(unmanaged), wantErr: "with a tablegroup in the catalog"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResolveFixture(t, tc.source, tc.static, "--tablegroup-id="+tc.id)
			if tc.record != nil {
				f.register(t, *tc.record)
			}
			got, err := f.resolve(t.Context(), tc.tableGroup)
			require.Len(t, tc.source.asked, tc.wantAsked)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got.mode)
			if tc.want == unmanaged {
				require.Same(t, bound, got.backing)
			} else {
				require.Nil(t, got.backing)
			}
			if tc.wantAsked > 0 {
				require.Equal(t, int64(5), tc.source.asked[0], "asked by the tablegroup id")
			}
		})
	}
}

// TestAnUnreadableRecordDoesNotDecideTheMode: a pooler that cannot read its own record goes by
// its tablegroup, as a pooler with no record does.
func TestAnUnreadableRecordDoesNotDecideTheMode(t *testing.T) {
	f := newResolveFixture(t, &fakeSource{}, false, "--tablegroup-id=0")
	f.factory.SetError(errors.New("topology unreachable"))
	got, err := f.resolve(t.Context(), "default")
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS, got.mode)
}

// TestAPoolerWaitsForTheCatalogAndNeverGuessesManaged: while the catalog cannot be read, the
// pooler keeps asking, with each answer it gets that is not an answer treated as none; it starts
// as soon as the catalog answers, and never as a managed pooler for want of one.
func TestAPoolerWaitsForTheCatalogAndNeverGuessesManaged(t *testing.T) {
	fastRetries(t)
	bound := &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "db", User: "u"}

	t.Run("it answers after a few failures", func(t *testing.T) {
		source := &flakySource{failures: 5, conn: bound}
		f := newResolveFixture(t, source, false, "--tablegroup-id=5")
		got, err := f.resolve(t.Context(), "migrateTG")
		require.NoError(t, err)
		require.Equal(t, clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, got.mode)
		require.Equal(t, 6, source.calls)
	})
	t.Run("it never answers", func(t *testing.T) {
		source := &flakySource{failures: -1}
		f := newResolveFixture(t, source, false, "--tablegroup-id=5")
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		got, err := f.resolve(ctx, "migrateTG")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "did not say how tablegroup 5 is served")
		require.Zero(t, got.mode, "no mode, and in particular not a managed one")
		require.Greater(t, source.calls, 2, "it kept asking")
	})
	t.Run("an answer that is not usable is waited out like a failure", func(t *testing.T) {
		source := &flakySource{failures: -1, invalid: true}
		f := newResolveFixture(t, source, false, "--tablegroup-id=5")
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err := f.resolve(ctx, "migrateTG")
		require.ErrorIs(t, err, errInvalidBackingConnection)
		require.Greater(t, source.calls, 2)
	})
}

// flakySource fails a number of times (forever when negative), then answers with conn.
// invalid makes its answer, when it does answer, one that says nothing.
type flakySource struct {
	failures int
	invalid  bool
	conn     *backingConnection
	calls    int
}

func (f *flakySource) TableGroupBinding(context.Context, int64) (*backingConnection, bool, error) {
	f.calls++
	if f.invalid {
		return nil, false, nil
	}
	if f.failures < 0 || f.calls <= f.failures {
		return nil, false, errors.New("default primary unavailable")
	}
	return f.conn, false, nil
}

// countingSource counts the questions it is asked, and never answers.
type countingSource struct{ fetches int }

func (c *countingSource) TableGroupBinding(context.Context, int64) (*backingConnection, bool, error) {
	c.fetches++
	return nil, false, errors.New("not reached")
}

// TestInitRefusesAShardItCannotServeBeforeAskingTheCatalog: a bad shard name and a missing
// one are refused before the pooler registers anything or asks how its tablegroup is served: a
// pooler that exits on them leaves no topology record behind.
func TestInitRefusesAShardItCannotServeBeforeAskingTheCatalog(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tableGroup string
		shard      string
		tgID       string
		wantErr    string
	}{
		{"bad shard name", "migrateTG", "foo", "5", "foo"},
		{"no shard", "migrateTG", "", "5", "shard is required"},
		{"no tablegroup", "", "0-inf", "5", "table group is required"},
		{"negative tablegroup id", "migrateTG", "0-inf", "-1", "must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
			t.Cleanup(func() { _ = ts.Close() })
			source := &countingSource{}
			mp := newTestMultipooler()
			mp.connections = source
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			mp.RegisterFlags(flags)
			require.NoError(t, flags.Parse([]string{
				"--database=db", "--cell=zone1", "--service-id=ext",
				"--table-group=" + tc.tableGroup, "--shard=" + tc.shard, "--tablegroup-id=" + tc.tgID,
			}))
			err := mp.Init(t.Context(), ts)
			require.ErrorContains(t, err, tc.wantErr)
			require.Zero(t, source.fetches)
			registered, err := ts.GetMultipoolersByCell(t.Context(), "zone1", nil)
			require.NoError(t, err)
			require.Empty(t, registered, "nothing is registered")
		})
	}
}

// initWithBinding runs Init for a pooler of tablegroup 5 whose catalog answers with source.
func initWithBinding(t *testing.T, source connectionSource, ts topoclient.Store, extraArgs ...string) error {
	t.Helper()
	mp := newTestMultipooler()
	mp.connections = source
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	require.NoError(t, flags.Parse(append([]string{
		"--database=db", "--cell=zone1", "--service-id=ext",
		"--table-group=migrateTG", "--shard=0-inf", "--tablegroup-id=5",
	}, extraArgs...)))
	return mp.Init(t.Context(), ts)
}

func registeredPoolers(t *testing.T, ts topoclient.Store) []*topoclient.MultipoolerInfo {
	t.Helper()
	registered, err := ts.GetMultipoolersByCell(t.Context(), "zone1", nil)
	require.NoError(t, err)
	return registered
}

// TestAPoolerRefusesABackingConnectionThatServesAnotherDatabase: the pooler advertises
// --database to the cluster and dials the connection's, so the two must agree, and a pooler
// whose connection serves another database refuses to start, before it registers anywhere.
func TestAPoolerRefusesABackingConnectionThatServesAnotherDatabase(t *testing.T) {
	require.NoError(t, checkBackingDatabase(&backingConnection{Name: "src", Database: "other"}, "other"))
	require.EqualError(t, checkBackingDatabase(&backingConnection{Name: "src", Database: "other"}, "db"),
		`backing connection "src" serves database "other", which does not match --database "db"`)

	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	err := initWithBinding(t, &fakeSource{conn: &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "other", User: "u"}}, ts)
	require.EqualError(t, err, `backing connection "src" serves database "other", which does not match --database "db"`)
	require.Empty(t, registeredPoolers(t, ts), "nothing is registered")
}

// TestTheManagedPostgresFlagsAreRefusedWhenTheCatalogSaysUnmanaged: whether a pooler is
// unmanaged is only known once the catalog has said so, and the flags of a pgctld-managed
// postgres are refused then, before the pooler registers.
func TestTheManagedPostgresFlagsAreRefusedWhenTheCatalogSaysUnmanaged(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })
	bound := &backingConnection{Name: "src", Host: "db.example.com", Port: 5432, Database: "db", User: "u"}
	err := initWithBinding(t, &fakeSource{conn: bound}, ts, "--pg-port=5432")
	require.ErrorContains(t, err, "--pg-port")
	require.ErrorContains(t, err, "unmanaged")
	require.Empty(t, registeredPoolers(t, ts))
}
