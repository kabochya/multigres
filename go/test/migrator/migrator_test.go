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

package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/fakecontrol"
)

const (
	unfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
	fencing  = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING
	fenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED

	// otherOp is an operation other than the migrators' own, which are generated.
	otherOp = "b2d4f6a8-1c3e-4a5b-8c7d-9e0f1a2b3c4d"
)

// fenceWith starts the fence of a tablegroup for an operation from version and commits it, once
// each.
func fenceWith(t *testing.T, ctl Control, op, tg string, version int64) {
	t.Helper()
	started, err := ctl.StartFence(t.Context(), &multipoolerservicepb.StartFenceRequest{Database: "db", OperationId: op, Scope: scope(tg), ExpectedVersion: version})
	require.NoError(t, err)
	_, err = ctl.CommitFence(t.Context(), &multipoolerservicepb.CommitFenceRequest{Database: "db", OperationId: op, Scope: scope(tg), Version: started.Fence.Version})
	require.NoError(t, err)
}

// unfenceWith unfences a tablegroup for an operation at the version it has.
func unfenceWith(t *testing.T, ctl Control, op, tg string, version int64) {
	t.Helper()
	_, err := ctl.UnfenceScope(t.Context(), &multipoolerservicepb.UnfenceScopeRequest{Database: "db", OperationId: op, Scope: scope(tg), ExpectedVersion: version})
	require.NoError(t, err)
}

func fixture(t *testing.T, tune func(*Config)) (*Migrator, *fakecontrol.Server) {
	t.Helper()
	fake := fakecontrol.New()
	fake.AddTablegroup("db", "src", "ext-1")
	fake.AddTablegroup("db", "dst", "dst-1", "dst-2")
	fake.SetHome("db", "src")
	conn, err := grpc.NewClient(fakecontrol.Start(t, fake), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := multipoolerservicepb.NewMultipoolerServiceClient(conn)
	cfg := Config{
		Database: "db", Source: "src", Destination: "dst",
		Control: func() Control { return client }, RetryInterval: 10 * time.Millisecond,
	}
	if tune != nil {
		tune(&cfg)
	}
	return New(cfg), fake
}

func TestCutoverAndRollbackLeaveTheTrafficWhereTheyMoveIt(t *testing.T) {
	m, _ := fixture(t, nil)
	ctx := t.Context()
	caughtUp := 0
	steps := Steps{CatchUp: func(context.Context) error { caughtUp++; return nil }}

	res, err := m.Cutover(ctx, steps)
	require.NoError(t, err)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, "src", st.Home, "the home does not move until Complete")
	require.NotNil(t, st.ruleOf(m.OperationID()), "the operation has switched its rule")
	require.Equal(t, fenced, st.Source.State, "the source stays fenced after a cutover")
	require.Equal(t, unfenced, st.Dest.State)
	require.Equal(t, res.FromFenceVersion, st.Source.Version)
	require.Equal(t, m.OperationID(), st.Source.OperationId)
	require.Equal(t, m.OperationID(), st.Dest.OperationId)

	back, err := m.Rollback(ctx, steps)
	require.NoError(t, err)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "src", st.AppTablegroup)
	require.Nil(t, st.ruleOf(m.OperationID()), "the rollback deleted the rule")
	require.Equal(t, unfenced, st.Source.State, "the rollback unfences the source")
	require.Equal(t, fenced, st.Dest.State, "and leaves the destination fenced")
	require.Equal(t, back.FromFenceVersion, st.Dest.Version)
	require.Equal(t, back.ToFenceVersion, st.Source.Version-1, "the source is unfenced at the version after the fence the result names")
	require.Equal(t, 2, caughtUp)
}

// TestTheOperationIDIsOneUUIDPerMigrator: a migrator has one operation id, generated unless given,
// and every call carries it.
func TestTheOperationIDIsOneUUIDPerMigrator(t *testing.T) {
	a, _ := fixture(t, nil)
	b, _ := fixture(t, nil)
	require.Len(t, a.OperationID(), 36)
	require.NotEqual(t, a.OperationID(), b.OperationID())

	given, _ := fixture(t, func(c *Config) { c.OperationID = strings.ToUpper(otherOp) })
	require.Equal(t, otherOp, given.OperationID(), "the id is kept in the form the coordinator gives it back")
	_, err := given.FenceOrGiveUp(t.Context(), "dst")
	require.NoError(t, err)
	st, err := given.Read(t.Context())
	require.NoError(t, err)
	require.Equal(t, otherOp, st.Dest.OperationId)

	bad, _ := fixture(t, func(c *Config) { c.OperationID = "not-a-uuid" })
	_, err = bad.FenceOrGiveUp(t.Context(), "dst")
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestReadReportsAScopeWithNoFenceAsUnfencedAtVersionZero.
func TestReadReportsAScopeWithNoFenceAsUnfencedAtVersionZero(t *testing.T) {
	m, _ := fixture(t, nil)
	st, err := m.Read(t.Context())
	require.NoError(t, err)
	require.Equal(t, "src", st.Home)
	require.Equal(t, "src", st.AppTablegroup)
	require.Empty(t, st.Fences)
	for _, f := range []*multipoolerservicepb.AdmissionFence{st.Source, st.Dest} {
		require.Equal(t, unfenced, f.State)
		require.Zero(t, f.Version)
		require.Empty(t, f.OperationId)
	}
	require.Equal(t, "dst", st.Dest.Scope.Tablegroup)
}

// TestFenceThatMembersCannotCompleteIsRetriedAndThenGivenUp: a busy member is asked again,
// and the fence completes once it is clean; one that stays busy past GiveUpAfter makes the
// migrator unfence the tablegroup, which is the migrator's decision, not the fence's.
func TestFenceThatMembersCannotCompleteIsRetriedAndThenGivenUp(t *testing.T) {
	m, fake := fixture(t, func(c *Config) { c.GiveUpAfter = 2 * time.Second })
	ctx := t.Context()
	fake.BusyMember("dst-2", "1 reservations held")
	go func() {
		time.Sleep(100 * time.Millisecond)
		fake.BusyMember("dst-2", "")
	}()
	resp, err := m.FenceOrGiveUp(ctx, "dst")
	require.NoError(t, err, "the same fence, retried, completes once the member is clean")
	require.EqualValues(t, 1, resp.Fence.Version, "the retries wrote nothing")

	m, fake = fixture(t, func(c *Config) { c.GiveUpAfter = 100 * time.Millisecond })
	fake.BusyMember("dst-2", "1 reservations held")
	_, err = m.FenceOrGiveUp(ctx, "dst")
	var gaveUp *GaveUpError
	require.ErrorAs(t, err, &gaveUp)
	require.ErrorIs(t, err, ErrGaveUp)
	require.Contains(t, err.Error(), "1 reservations held")
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, unfenced, st.Dest.State, "giving up unfences")
	require.Equal(t, m.OperationID(), st.Dest.OperationId, "and the unfenced row stays, the operation's")
}

// TestAGiveUpNamesWhatHeldTheFence: a busy member that says what holds its fence is described,
// reservation by reservation, in the log of each attempt and in the error the migrator gives
// up with, with nothing but facts: why it is reserved, held and idle durations, and who holds
// it.
func TestAGiveUpNamesWhatHeldTheFence(t *testing.T) {
	var logged []string
	m, fake := fixture(t, func(c *Config) {
		c.GiveUpAfter = 100 * time.Millisecond
		c.Logf = func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) }
	})
	fake.BusyMemberHeldBy("dst-2", "busy: 0 requests in flight, 21 reservations held", &multipoolerservicepb.FenceBusy{
		ReservationsHeld:      21,
		ReservationsTruncated: 1,
		Reservations: []*multipoolerservicepb.HeldReservation{{
			Reasons: []string{"transaction"}, HeldFor: durationpb.New(90 * time.Second), IdleFor: durationpb.New(45 * time.Second),
			User: "app", BackendPid: 4242, ReservedConnectionId: 11, ClientConnectionId: 9,
		}},
	})
	_, err := m.FenceOrGiveUp(t.Context(), "dst")
	var gaveUp *GaveUpError
	require.ErrorAs(t, err, &gaveUp)
	want := "transaction held 1m30s, idle 45s, pid 4242, user app, reservation 11, client 9; and 1 more"
	require.Contains(t, err.Error(), want, "the error the migrator gives up with")
	var warned bool
	for _, line := range logged {
		if strings.HasPrefix(line, "WARN: ") && strings.Contains(line, "dst-2 is busy") && strings.Contains(line, want) {
			warned = true
		}
	}
	require.True(t, warned, "each busy attempt is logged at warn level with the details: %v", logged)
}

func TestUnfenceUntilReachedRepeatsUntilNoMemberIsUnreached(t *testing.T) {
	m, fake := fixture(t, nil)
	ctx := t.Context()
	resp, err := m.FenceOrGiveUp(ctx, "dst")
	require.NoError(t, err)
	fake.FailRefresh("dst-1", true)
	go func() {
		time.Sleep(100 * time.Millisecond)
		fake.FailRefresh("dst-1", false)
	}()
	require.NoError(t, m.UnfenceUntilReached(ctx, "dst", resp.Fence.Version))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, st.Dest.Version, "the repeats wrote nothing")

	fake.FailRefresh("dst-1", true)
	m.cfg.UnfenceAttempts = 3
	err = m.UnfenceUntilReached(ctx, "dst", resp.Fence.Version)
	require.ErrorContains(t, err, "still unreached", "a member that never answers is reported, not forgotten")
	var unreached *UnreachedError
	require.ErrorAs(t, err, &unreached)
	require.Equal(t, 1, unreached.Unreached)

	// A fence given up on, with a member that cannot be reached afterwards either, says both.
	m, fake = fixture(t, func(c *Config) { c.GiveUpAfter = 50 * time.Millisecond; c.UnfenceAttempts = 2 })
	fake.BusyMember("dst-1", "1 reservations held")
	fake.FailRefresh("dst-2", true)
	_, err = m.FenceOrGiveUp(ctx, "dst")
	var gaveUp *GaveUpError
	require.ErrorAs(t, err, &gaveUp)
	require.Equal(t, 1, gaveUp.UnreachedAfterUnfence)
}

// TestOnErrorUnfencesWhatTheMoveHasNotGivenTheTrafficTo: whatever went wrong, the policy reads the
// state: a cutover that had not switched leaves nothing fenced; one that had keeps the
// source fenced and opens the destination.
func TestOnErrorUnfencesWhatTheMoveHasNotGivenTheTrafficTo(t *testing.T) {
	m, _ := fixture(t, nil)
	ctx := t.Context()
	boom := errors.New("the catch-up failed")
	_, err := m.Cutover(ctx, Steps{CatchUp: func(context.Context) error { return boom }})
	require.ErrorIs(t, err, boom)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "src", st.AppTablegroup)
	require.Equal(t, unfenced, st.Source.State)
	require.Equal(t, unfenced, st.Dest.State, "an error before the switch leaves nothing fenced")
	require.Empty(t, st.Rules)

	// After the switch: a failure between the switch and the unfence. The unfenced rows of the
	// failed attempt are this operation's, so the next attempt takes them at the version read.
	_, err = m.Cutover(ctx, Steps{BetweenSwitchAndUnfence: func(context.Context) error { return boom }})
	require.ErrorIs(t, err, boom)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, fenced, st.Dest.State, "the migrator was in the middle of the cutover")
	require.NoError(t, m.OnCutoverError(ctx))
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, unfenced, st.Dest.State, "the policy opens the tablegroup that holds the traffic")
	require.Equal(t, fenced, st.Source.State, "and leaves the source fenced")
}

func TestPreflightReportsTheFirstCheckThatFails(t *testing.T) {
	m, _ := fixture(t, nil)
	ctx := t.Context()
	require.NoError(t, m.Preflight(ctx, m.CheckRoutingAt("src"), m.CheckNeitherFenced()))
	err := m.Preflight(ctx, m.CheckRoutingAt("dst"))
	require.ErrorContains(t, err, `traffic is on "src"`)

	_, err = m.FenceOrGiveUp(ctx, "dst")
	require.NoError(t, err)
	require.ErrorContains(t, m.Preflight(ctx, m.CheckNeitherFenced()), "earlier migration was not finished")
}

// flaky is a Control whose first calls of a routing change fail in a way that does not say whether
// they took effect: commit says whether they commit first (the reply is lost) or not (the request
// never arrived), for as many calls as drops says. The calls of every RPC it covers (switch, revert,
// fold, cleanup) are counted together.
type flaky struct {
	Control
	commit bool
	drops  int
	calls  *int
}

// drop reports the failure of this call, after running it when the reply is to be lost, or nil when
// the call is to go through.
func (c flaky) drop(do func() error) error {
	*c.calls++
	if *c.calls > c.drops {
		return nil
	}
	if c.commit {
		if err := do(); err != nil && status.Code(err) != codes.FailedPrecondition {
			return err
		}
	}
	return status.Error(codes.Unavailable, "connection reset")
}

func (c flaky) SwitchRouting(ctx context.Context, in *multipoolerservicepb.SwitchRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	if err := c.drop(func() error { _, err := c.Control.SwitchRouting(ctx, in, opts...); return err }); err != nil {
		return nil, err
	}
	return c.Control.SwitchRouting(ctx, in, opts...)
}

func (c flaky) RevertRouting(ctx context.Context, in *multipoolerservicepb.RevertRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.RevertRoutingResponse, error) {
	if err := c.drop(func() error { _, err := c.Control.RevertRouting(ctx, in, opts...); return err }); err != nil {
		return nil, err
	}
	return c.Control.RevertRouting(ctx, in, opts...)
}

func (c flaky) FoldRouting(ctx context.Context, in *multipoolerservicepb.FoldRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.FoldRoutingResponse, error) {
	if err := c.drop(func() error { _, err := c.Control.FoldRouting(ctx, in, opts...); return err }); err != nil {
		return nil, err
	}
	return c.Control.FoldRouting(ctx, in, opts...)
}

func (c flaky) CleanupOperation(ctx context.Context, in *multipoolerservicepb.CleanupOperationRequest, opts ...grpc.CallOption) (*multipoolerservicepb.CleanupOperationResponse, error) {
	if err := c.drop(func() error { _, err := c.Control.CleanupOperation(ctx, in, opts...); return err }); err != nil {
		return nil, err
	}
	return c.Control.CleanupOperation(ctx, in, opts...)
}

func withFlaky(t *testing.T, m *Migrator, commit bool, drops int) *int {
	t.Helper()
	calls := new(int)
	real := m.cfg.Control()
	m.cfg.Control = func() Control { return flaky{Control: real, commit: commit, drops: drops, calls: calls} }
	return calls
}

// TestARoutingChangeWhoseOutcomeIsUnknownIsRepeatedUntilItIsDefinitive: a switch or revert that fails
// in a way that does not say whether it committed (a reset after the commit, or before the request
// arrived) is asked again, which is safe, and only then does the migrator decide anything: it
// finishes the move whether the first attempt had committed or not, in a cutover and a
// rollback, and never unfences the side the traffic left.
func TestARoutingChangeWhoseOutcomeIsUnknownIsRepeatedUntilItIsDefinitive(t *testing.T) {
	for name, commit := range map[string]bool{"the change committed and the reply was lost": true, "the change never arrived": false} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			m, _ := fixture(t, nil)
			calls := withFlaky(t, m, commit, 2)
			_, err := m.Cutover(ctx, Steps{})
			require.NoError(t, err)
			require.Equal(t, 3, *calls, "asked again until the answer was definitive")
			st, err := m.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "dst", st.AppTablegroup)
			require.Equal(t, unfenced, st.Dest.State, "the side that holds the traffic serves")
			require.Equal(t, fenced, st.Source.State, "and the side the traffic left stays fenced")

			*calls = 0
			_, err = m.Rollback(ctx, Steps{})
			require.NoError(t, err)
			require.Equal(t, 3, *calls)
			st, err = m.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "src", st.AppTablegroup)
			require.Equal(t, unfenced, st.Source.State)
			require.Equal(t, fenced, st.Dest.State, "the destination the traffic just left stays fenced: a stale gateway cannot write there")
		})
	}
}

// TestACompleteAndACleanupWhoseOutcomeIsUnknownAreRepeated: the fold and the cleanup are idempotent,
// so they are asked again until they answer.
func TestACompleteAndACleanupWhoseOutcomeIsUnknownAreRepeated(t *testing.T) {
	for name, commit := range map[string]bool{"committed, reply lost": true, "never arrived": false} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			m, _ := fixture(t, nil)
			_, err := m.Cutover(ctx, Steps{})
			require.NoError(t, err)
			calls := withFlaky(t, m, commit, 2)
			folded, err := m.Complete(ctx)
			require.NoError(t, err)
			require.Equal(t, 3, *calls)
			require.Equal(t, "dst", folded.HomeTablegroup)

			*calls = 0
			cleaned, err := m.Cleanup(ctx)
			require.NoError(t, err)
			require.Equal(t, 3, *calls)
			if commit {
				require.Zero(t, cleaned.FencesDeleted, "the first attempt deleted them, and the answer that got through found nothing left")
			} else {
				require.EqualValues(t, 2, cleaned.FencesDeleted)
			}
			st, err := m.Read(ctx)
			require.NoError(t, err)
			require.Empty(t, st.Fences)
		})
	}
}

// TestARoutingChangeThatNeverAnswersDefinitivelyLeavesBothSidesFenced: when the outcome stays unknown,
// nothing is unfenced, because the traffic may be on either side and unfencing the one it left
// would let a stale gateway write there once the change lands.
func TestARoutingChangeThatNeverAnswersDefinitivelyLeavesBothSidesFenced(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, func(c *Config) { c.RoutingAttempts = 3 })
	calls := withFlaky(t, m, true, 1000)
	_, err := m.Cutover(ctx, Steps{})
	var unknown *RoutingOutcomeUnknownError
	require.ErrorAs(t, err, &unknown)
	require.Equal(t, 3, unknown.Attempts)
	require.Equal(t, 3, *calls)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fenced, st.Source.State)
	require.Equal(t, fenced, st.Dest.State, "nothing was unfenced")
}

// TestOnErrorPoliciesForACallerThatLostTrack: the policies a crashed migrator's successor
// applies decide relative to the move: traffic on the side it went to, that side serves and
// the other stays fenced, in a cutover and in a rollback.
func TestOnErrorPoliciesForACallerThatLostTrack(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	ctl, op := m.cfg.Control(), m.OperationID()
	fence := func(tg string, v int64) { fenceWith(t, ctl, op, tg, v) }
	// A cutover that switched and then lost its migrator.
	fence("src", 0)
	fence("dst", 0)
	_, err := ctl.SwitchRouting(ctx, &multipoolerservicepb.SwitchRoutingRequest{
		Database: "db", OperationId: op, ExpectedRoutingVersion: 0,
		Rule: &multipoolerservicepb.RoutingRule{FromTablegroup: "src", ToTablegroup: "dst", ToShard: "0-inf"},
	})
	require.NoError(t, err)
	require.NoError(t, m.OnCutoverError(ctx))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, unfenced, st.Dest.State)
	require.Equal(t, fenced, st.Source.State)

	// A rollback that reverted and then lost its migrator.
	fence("dst", st.Dest.Version)
	_, err = ctl.RevertRouting(ctx, &multipoolerservicepb.RevertRoutingRequest{Database: "db", OperationId: op, ExpectedRoutingVersion: 1})
	require.NoError(t, err)
	require.NoError(t, m.OnRollbackError(ctx))
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "src", st.AppTablegroup)
	require.Equal(t, unfenced, st.Source.State)
	require.Equal(t, fenced, st.Dest.State, "the side the traffic left stays fenced")
}

// TestAMoveDoesNotAdoptSomeoneElsesFence: a side that is fenced when a move starts is an
// earlier migration that was not finished, or a concurrent one, and the move refuses it
// (and unfences nothing); the caller can resume it explicitly when it is this operation's own. And
// what a move did not fence, it does not unfence.
func TestAMoveDoesNotAdoptSomeoneElsesFence(t *testing.T) {
	ctx := t.Context()

	// Another operation's fence is never adopted, resumed or not.
	m, _ := fixture(t, nil)
	fenceWith(t, m.cfg.Control(), otherOp, "dst", 0)
	for _, resume := range []bool{false, true} {
		_, err := m.Cutover(ctx, Steps{Resume: resume})
		require.ErrorIs(t, err, ErrHeldByAnotherOperation)
	}
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fenced, st.Dest.State, "the other operation's fence is left alone")
	require.Equal(t, otherOp, st.Dest.OperationId)
	require.Equal(t, "src", st.AppTablegroup)
	require.Equal(t, unfenced, st.Source.State, "and nothing the move touched is left fenced")
	_, err = m.FenceOrGiveUp(ctx, "dst")
	require.ErrorIs(t, err, ErrHeldByAnotherOperation)

	// A fence of this operation that an earlier attempt left is refused, unless it is resumed.
	m, _ = fixture(t, nil)
	fenceWith(t, m.cfg.Control(), m.OperationID(), "dst", 0)
	_, err = m.Cutover(ctx, Steps{})
	require.ErrorContains(t, err, "a fence this move did not make")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fenced, st.Dest.State, "the earlier fence is left alone")
	require.Equal(t, "src", st.AppTablegroup)

	// The policy touches only what the move fenced: here only the source, though the
	// destination is closed too.
	require.NoError(t, m.onError(ctx, "src", "dst", map[string]bool{"src": true}, false))
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fenced, st.Dest.State)

	// Resumed explicitly, the move takes the fence over and completes.
	_, err = m.Cutover(ctx, Steps{Resume: true})
	require.NoError(t, err)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
}

// TestAMoveTakesOverFencesThatAnotherOperationLeftUnfenced: an UNFENCED row is nobody's, whoever
// wrote it last, and a move takes it over at the version it read.
func TestAMoveTakesOverFencesThatAnotherOperationLeftUnfenced(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	ctl := m.cfg.Control()
	for _, tg := range []string{"src", "dst"} {
		fenceWith(t, ctl, otherOp, tg, 0)
		unfenceWith(t, ctl, otherOp, tg, 1)
	}
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, otherOp, st.Source.OperationId)
	require.EqualValues(t, 2, st.Source.Version)
	require.NoError(t, m.Preflight(ctx, m.CheckRoutingAt("src")))

	_, err = m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, m.OperationID(), st.Source.OperationId, "the source is this operation's now")
	require.Equal(t, fenced, st.Source.State)
	require.EqualValues(t, 3, st.Source.Version, "taken over at the version read, and fenced there")
	require.Equal(t, m.OperationID(), st.Dest.OperationId)
	require.Equal(t, unfenced, st.Dest.State)
	require.EqualValues(t, 4, st.Dest.Version)

	// The other operation's cleanup does not touch what it no longer holds.
	cleaned, err := ctl.CleanupOperation(ctx, &multipoolerservicepb.CleanupOperationRequest{Database: "db", OperationId: otherOp})
	require.NoError(t, err)
	require.Zero(t, cleaned.FencesDeleted)
}

// TestARollbackFencesTheSourceAgain: the rollback fences both sides, the source too, which costs
// nothing when the cutover left it FENCED, and closes it when someone opened it.
func TestARollbackFencesTheSourceAgain(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	unfenceWith(t, m.cfg.Control(), m.OperationID(), "src", st.Source.Version)

	sawSourceFenced := false
	_, err = m.Rollback(ctx, Steps{CatchUp: func(ctx context.Context) error {
		st, err := m.Read(ctx)
		require.NoError(t, err)
		sawSourceFenced = st.Source.State == fenced && st.Dest.State == fenced
		return nil
	}})
	require.NoError(t, err)
	require.True(t, sawSourceFenced, "both sides were fenced while the destination caught up")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "src", st.AppTablegroup)
	require.Equal(t, unfenced, st.Source.State)
	require.Equal(t, fenced, st.Dest.State)
}

// TestARollbackNeedsTheOperationsRule: a rollback reverts the rule of its own operation, and refuses
// to start without one, before it fences anything.
func TestARollbackNeedsTheOperationsRule(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Rollback(ctx, Steps{})
	require.Error(t, err)

	_, err = m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	_, err = m.Complete(ctx)
	require.NoError(t, err)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	_, err = m.Rollback(ctx, Steps{})
	require.ErrorContains(t, err, "no routing rule", "a folded operation cannot be rolled back")
	after, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, st.Dest.Version, after.Dest.Version, "and nothing was fenced")
}

// TestCompleteFoldsTheRuleAndLeavesTheSourceFenced.
func TestCompleteFoldsTheRuleAndLeavesTheSourceFenced(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Complete(ctx)
	require.ErrorContains(t, err, "no routing rule to fold", "nothing was cut over")

	_, err = m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	folded, err := m.Complete(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", folded.HomeTablegroup)
	require.EqualValues(t, 2, folded.RoutingVersion)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.Home)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Empty(t, st.Rules)
	require.Equal(t, fenced, st.Source.State, "the source stays fenced until its poolers are decommissioned")
	require.Equal(t, unfenced, st.Dest.State)

	again, err := m.Complete(ctx)
	require.NoError(t, err, "completing twice is safe")
	require.EqualValues(t, 2, again.RoutingVersion)

	// A rule of this operation that is not this migration's is not folded.
	other, _ := fixture(t, nil)
	_, err = other.Cutover(ctx, Steps{})
	require.NoError(t, err)
	other.cfg.Destination = "elsewhere"
	_, err = other.Complete(ctx)
	require.ErrorContains(t, err, `not "src" to "elsewhere"`)
}

// TestCleanupDeletesTheOperationsFences: after the fold, once the source's poolers are
// decommissioned (the caller's business), the cleanup deletes the fences, FENCED ones included.
func TestCleanupDeletesTheOperationsFences(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)

	_, err = m.Cleanup(ctx)
	require.Error(t, err, "refused while the operation has a rule")
	require.Equal(t, codes.FailedPrecondition, status.Code(errors.Unwrap(err)))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Len(t, st.Fences, 2, "and nothing is deleted")

	_, err = m.Complete(ctx)
	require.NoError(t, err)
	cleaned, err := m.Cleanup(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, cleaned.FencesDeleted, "the FENCED source and the unfenced destination")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, st.Fences, "the scopes are open again")
	require.Equal(t, "dst", st.AppTablegroup)

	again, err := m.Cleanup(ctx)
	require.NoError(t, err, "an operation with nothing left cleans up")
	require.Zero(t, again.FencesDeleted)
}

// TestACleanedUpOperationIsFinished: the cleanup ends the operation, so the same Migrator cannot
// fence again (a replay would fence a scope that is at version zero), and a new migration needs a
// new operation id.
func TestACleanedUpOperationIsFinished(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	_, err = m.Complete(ctx)
	require.NoError(t, err)
	_, err = m.Cleanup(ctx)
	require.NoError(t, err)

	_, err = m.FenceOrGiveUp(ctx, "dst")
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "cleaned up")
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, st.Fences, "nothing was fenced")
}

// TestCleanupAfterARollbackReopensTheDestination: the rollback leaves the destination FENCED, and
// the cleanup deletes that row too.
func TestCleanupAfterARollbackReopensTheDestination(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	_, err = m.Rollback(ctx, Steps{})
	require.NoError(t, err)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fenced, st.Dest.State)

	cleaned, err := m.Cleanup(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, cleaned.FencesDeleted, "the unfenced source and the FENCED destination")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Empty(t, st.Fences, "the destination serves application work again")
}

// TestGivingUpAfterResumingAFenceUnfencesAtTheRowsVersion: an earlier attempt left the
// tablegroup FENCING at the version the migrator reads, so the fence it resumes is at that
// version, and giving up unfences at it, not one above.
func TestGivingUpAfterResumingAFenceUnfencesAtTheRowsVersion(t *testing.T) {
	m, fake := fixture(t, func(c *Config) { c.GiveUpAfter = 50 * time.Millisecond })
	ctx := t.Context()
	fake.BusyMember("dst-2", "1 reservations held")
	// An earlier attempt: started, asked once, left FENCING.
	ctl := m.cfg.Control()
	_, err := ctl.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "db", OperationId: m.OperationID(), Scope: scope("dst"), ExpectedVersion: 0})
	require.NoError(t, err)
	_, err = ctl.CommitFence(ctx, &multipoolerservicepb.CommitFenceRequest{Database: "db", OperationId: m.OperationID(), Scope: scope("dst"), Version: 1})
	require.Equal(t, codes.Aborted, status.Code(err))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fencing, st.Dest.State)
	require.EqualValues(t, 1, st.Dest.Version)

	_, err = m.FenceOrGiveUp(ctx, "dst")
	var gaveUp *GaveUpError
	require.ErrorAs(t, err, &gaveUp)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, unfenced, st.Dest.State, "giving up unfenced the resumed fence, which would not at version+1")
	require.EqualValues(t, 2, st.Dest.Version)
}

// TestAMoveThatFailsItsPreflightUndoesNothing: a mistaken Cutover while a rollback is in
// progress (the rollback fenced the destination, which holds the traffic) fails its preflight,
// and must not run the on-error policy, which would unfence the rollback's fence.
func TestAMoveThatFailsItsPreflightUndoesNothing(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	// A rollback, on its way, fenced the destination.
	fenceWith(t, m.cfg.Control(), m.OperationID(), "dst", st.Dest.Version)

	_, err = m.Cutover(ctx, Steps{})
	require.Error(t, err, "the traffic is on the destination, not the source")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, fenced, st.Dest.State, "the rollback's fence is not undone")
	require.Equal(t, fenced, st.Source.State)

	// And a move that never tried to switch does not reopen the side it finds the traffic on.
	require.ErrorContains(t, m.onError(ctx, "src", "dst", map[string]bool{"src": true}, false), "someone else moved it")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, fenced, st.Dest.State)
}

// staleLeaderSwitch is a Control whose switch first commits and loses the reply, then lands on a
// pooler that has just lost its place as the default primary, which answers UNAVAILABLE, and
// only then reaches the real leader.
type staleLeaderSwitch struct {
	Control
	calls *int
}

func (c staleLeaderSwitch) SwitchRouting(ctx context.Context, in *multipoolerservicepb.SwitchRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.SwitchRoutingResponse, error) {
	*c.calls++
	switch *c.calls {
	case 1:
		if _, err := c.Control.SwitchRouting(ctx, in, opts...); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "connection reset")
	case 2:
		return nil, status.Error(codes.Unavailable, "not the default primary pooler")
	}
	return c.Control.SwitchRouting(ctx, in, opts...)
}

// TestASwitchRetryThatLandsOnAPoolerThatIsNotTheLeaderIsNotDefinitive: a not-leader answer is
// UNAVAILABLE, so it is retried, and the migrator does not unfence while the first attempt
// may still be committing on the real leader.
func TestASwitchRetryThatLandsOnAPoolerThatIsNotTheLeaderIsNotDefinitive(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	calls := new(int)
	real := m.cfg.Control()
	m.cfg.Control = func() Control { return staleLeaderSwitch{Control: real, calls: calls} }
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	require.Equal(t, 3, *calls)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, unfenced, st.Dest.State)
	require.Equal(t, fenced, st.Source.State, "the side the traffic left stays fenced")
}

// startCounter is a Control that counts the starts of fences, and runs afterFirst once the first
// has returned.
type startCounter struct {
	Control
	starts     *int
	afterFirst func()
}

func (c startCounter) StartFence(ctx context.Context, in *multipoolerservicepb.StartFenceRequest, opts ...grpc.CallOption) (*multipoolerservicepb.StartFenceResponse, error) {
	*c.starts++
	resp, err := c.Control.StartFence(ctx, in, opts...)
	if *c.starts == 1 && c.afterFirst != nil {
		c.afterFirst()
	}
	return resp, err
}

// TestAStartThatSomeMemberDidNotAnswerIsRepeated: a start that a member did not answer leaves the fence
// FENCING; the migrator starts it again, naming the version it read, until every member answered,
// and then commits it, at the same version: nothing is written by the repeat.
func TestAStartThatSomeMemberDidNotAnswerIsRepeated(t *testing.T) {
	m, fake := fixture(t, func(c *Config) { c.GiveUpAfter = 2 * time.Second })
	starts := new(int)
	real := m.cfg.Control()
	fake.FailRefresh("dst-2", true)
	m.cfg.Control = func() Control {
		return startCounter{Control: real, starts: starts, afterFirst: func() { fake.FailRefresh("dst-2", false) }}
	}

	resp, err := m.FenceOrGiveUp(t.Context(), "dst")
	require.NoError(t, err)
	require.Equal(t, fenced, resp.Fence.State)
	require.EqualValues(t, 1, resp.Fence.Version, "starting again wrote nothing")
	require.Equal(t, 2, *starts, "the fence was started again to reach the member")
}

// recordingTerminator is a BackendTerminator that records what it was asked to end.
type recordingTerminator struct {
	ended []int32
	port  map[int32]int32
}

func (r *recordingTerminator) TerminateIfIdle(_ context.Context, pid, clientPort int32, _ time.Duration) (bool, error) {
	r.ended = append(r.ended, pid)
	r.port[pid] = clientPort
	return true, nil
}

// TestTerminateIdleEndsOnlyReservationsThatAreIdleAndNamedSafely: the optional policy hands a
// reservation to the source only if no request is running on it, it has been idle for longer than
// the threshold by the pooler's account, and the pooler said which port its connection uses; and
// not at all unless it is set with a threshold.
func TestTerminateIdleEndsOnlyReservationsThatAreIdleAndNamedSafely(t *testing.T) {
	reservation := func(pid int32, idle time.Duration, inUse bool, port int32) *multipoolerservicepb.HeldReservation {
		return &multipoolerservicepb.HeldReservation{
			Reasons: []string{"transaction"}, HeldFor: durationpb.New(time.Hour), IdleFor: durationpb.New(idle),
			User: "app", BackendPid: pid, InUse: inUse, ClientPort: port,
		}
	}
	held := &multipoolerservicepb.FenceBusy{ReservationsHeld: 5, Reservations: []*multipoolerservicepb.HeldReservation{
		reservation(101, 45*time.Second, false, 51101), // idle long enough: ended
		reservation(102, 5*time.Second, false, 51102),  // not idle long enough
		reservation(103, 45*time.Second, true, 51103),  // a request is running on it
		reservation(104, 45*time.Second, false, 0),     // the connection cannot be named: not TCP
		reservation(105, 31*time.Second, false, 51105), // idle just long enough: ended
	}}

	terminator := &recordingTerminator{port: map[int32]int32{}}
	m, fake := fixture(t, func(c *Config) {
		c.GiveUpAfter = 50 * time.Millisecond
		c.TerminateIdle = &TerminateIdle{After: 30 * time.Second, Source: terminator}
	})
	fake.BusyMemberHeldBy("dst-2", "busy", held)
	_, err := m.FenceOrGiveUp(t.Context(), "dst")
	require.ErrorIs(t, err, ErrGaveUp)
	require.Contains(t, terminator.ended, int32(101))
	require.Contains(t, terminator.ended, int32(105))
	require.NotContains(t, terminator.ended, int32(102))
	require.NotContains(t, terminator.ended, int32(103), "a reservation with a request running is not ended")
	require.NotContains(t, terminator.ended, int32(104), "nor one whose connection has no port")
	require.EqualValues(t, 51101, terminator.port[101], "the backend is named by pid and port")

	// With no threshold, nothing is ended.
	none := &recordingTerminator{port: map[int32]int32{}}
	m, fake = fixture(t, func(c *Config) {
		c.GiveUpAfter = 50 * time.Millisecond
		c.TerminateIdle = &TerminateIdle{Source: none}
	})
	fake.BusyMemberHeldBy("dst-2", "busy", held)
	_, err = m.FenceOrGiveUp(t.Context(), "dst")
	require.ErrorIs(t, err, ErrGaveUp)
	require.Empty(t, none.ended)
}
