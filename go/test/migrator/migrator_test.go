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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/test/fakecontrol"
)

func fixture(t *testing.T, tune func(*Config)) (*Migrator, *fakecontrol.Server) {
	t.Helper()
	fake := fakecontrol.New()
	fake.AddTablegroup("db", "src", "conn", "ext-1")
	fake.AddTablegroup("db", "dst", "", "dst-1", "dst-2")
	fake.SetAppTablegroup("db", "src")
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
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Source.State, "the source stays fenced after a cutover")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Dest.State)
	require.Equal(t, res.FromFenceVersion, st.Source.Version)

	_, err = m.Rollback(ctx, steps)
	require.NoError(t, err)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "src", st.AppTablegroup)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Source.State, "the rollback unfences the source")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State, "and leaves the destination fenced")
	require.Equal(t, 2, caughtUp)
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
	require.EqualValues(t, 1, resp.State.Version, "the retries wrote nothing")

	m, fake = fixture(t, func(c *Config) { c.GiveUpAfter = 100 * time.Millisecond })
	fake.BusyMember("dst-2", "1 reservations held")
	_, err = m.FenceOrGiveUp(ctx, "dst")
	var gaveUp *GaveUpError
	require.ErrorAs(t, err, &gaveUp)
	require.ErrorIs(t, err, ErrGaveUp)
	require.Contains(t, err.Error(), "1 reservations held")
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Dest.State, "giving up unfences")
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
	require.NoError(t, m.UnfenceUntilReached(ctx, "dst", resp.State.Version))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, st.Dest.Version, "the repeats wrote nothing")

	fake.FailRefresh("dst-1", true)
	m.cfg.UnfenceAttempts = 3
	err = m.UnfenceUntilReached(ctx, "dst", resp.State.Version)
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
// state: a cutover that had not flipped leaves nothing fenced; one that had keeps the
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
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Source.State)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Dest.State, "an error before the flip leaves nothing fenced")

	// After the flip: a failure between the flip and the unfence.
	_, err = m.Cutover(ctx, Steps{BetweenFlipAndUnfence: func(context.Context) error { return boom }})
	require.ErrorIs(t, err, boom)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State, "the migrator was in the middle of the cutover")
	require.NoError(t, m.OnCutoverError(ctx))
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Dest.State, "the policy opens the tablegroup that holds the traffic")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Source.State, "and leaves the source fenced")
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

	require.ErrorContains(t, m.Preflight(ctx, CheckSourceSeesEverySession(fakeQuerier{ok: false})), "pg_read_all_stats")
	require.NoError(t, m.Preflight(ctx, CheckSourceSeesEverySession(fakeQuerier{ok: true})))
}

type fakeQuerier struct{ ok bool }

func (f fakeQuerier) QueryRowScan(_ context.Context, _ string, dest ...any) error {
	*(dest[0].(*bool)) = f.ok
	return nil
}

// flakyFlip is a Control whose first flips fail in a way that does not say whether they
// took effect: commit says whether they commit first (the reply is lost) or not (the request
// never arrived), for as many calls as drops says.
type flakyFlip struct {
	Control
	commit bool
	drops  int
	calls  *int
}

func (c flakyFlip) FlipRouting(ctx context.Context, in *multipoolerservicepb.FlipRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.FlipRoutingResponse, error) {
	*c.calls++
	if *c.calls <= c.drops {
		if c.commit {
			if _, err := c.Control.FlipRouting(ctx, in, opts...); err != nil && status.Code(err) != codes.FailedPrecondition {
				return nil, err
			}
		}
		return nil, status.Error(codes.Unavailable, "connection reset")
	}
	return c.Control.FlipRouting(ctx, in, opts...)
}

func withFlakyFlip(t *testing.T, m *Migrator, commit bool, drops int) *int {
	t.Helper()
	calls := new(int)
	real := m.cfg.Control()
	m.cfg.Control = func() Control { return flakyFlip{Control: real, commit: commit, drops: drops, calls: calls} }
	return calls
}

// TestAFlipWhoseOutcomeIsUnknownIsRepeatedUntilItIsDefinitive: a flip that fails in a way
// that does not say whether it committed (a reset after the commit, or before the request
// arrived) is asked again, which is safe, and only then does the migrator decide anything: it
// finishes the move whether the first attempt had committed or not, in a cutover and a
// rollback, and never unfences the side the traffic left.
func TestAFlipWhoseOutcomeIsUnknownIsRepeatedUntilItIsDefinitive(t *testing.T) {
	const (
		unfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
		fenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	)
	for name, commit := range map[string]bool{"the flip committed and the reply was lost": true, "the flip never arrived": false} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			m, _ := fixture(t, nil)
			calls := withFlakyFlip(t, m, commit, 2)
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
			st, err = m.Read(ctx)
			require.NoError(t, err)
			require.Equal(t, "src", st.AppTablegroup)
			require.Equal(t, unfenced, st.Source.State)
			require.Equal(t, fenced, st.Dest.State, "the destination the traffic just left stays fenced: a stale gateway cannot write there")
		})
	}
}

// TestAFlipThatNeverAnswersDefinitivelyLeavesBothSidesFenced: when the outcome stays unknown,
// nothing is unfenced, because the traffic may be on either side and unfencing the one it left
// would let a stale gateway write there once the flip lands.
func TestAFlipThatNeverAnswersDefinitivelyLeavesBothSidesFenced(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, func(c *Config) { c.FlipAttempts = 3 })
	calls := withFlakyFlip(t, m, true, 1000)
	_, err := m.Cutover(ctx, Steps{})
	var unknown *FlipOutcomeUnknownError
	require.ErrorAs(t, err, &unknown)
	require.Equal(t, 3, unknown.Attempts)
	require.Equal(t, 3, *calls)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Source.State)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State, "nothing was unfenced")
}

// TestOnErrorPoliciesForACallerThatLostTrack: the policies a crashed migrator's successor
// applies decide relative to the move: traffic on the side it went to, that side serves and
// the other stays fenced, in a cutover and in a rollback.
func TestOnErrorPoliciesForACallerThatLostTrack(t *testing.T) {
	const (
		unfenced = multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
		fenced   = multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED
	)
	ctx := t.Context()
	m, _ := fixture(t, nil)
	ctl := m.cfg.Control()
	fence := func(tg string, v int64) {
		_, err := ctl.FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: tg, ExpectedVersion: v})
		require.NoError(t, err)
	}
	flip := func(from, to string, v int64) {
		_, err := ctl.FlipRouting(ctx, &multipoolerservicepb.FlipRoutingRequest{Database: "db", FromTablegroup: from, ToTablegroup: to, ExpectedRoutingVersion: v})
		require.NoError(t, err)
	}
	// A cutover that flipped and then lost its migrator.
	fence("src", 0)
	fence("dst", 0)
	flip("src", "dst", 0)
	require.NoError(t, m.OnCutoverError(ctx))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, unfenced, st.Dest.State)
	require.Equal(t, fenced, st.Source.State)

	// A rollback that flipped and then lost its migrator.
	fence("dst", st.Dest.Version)
	flip("dst", "src", 1)
	require.NoError(t, m.OnRollbackError(ctx))
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "src", st.AppTablegroup)
	require.Equal(t, unfenced, st.Source.State)
	require.Equal(t, fenced, st.Dest.State, "the side the traffic left stays fenced")
}

// TestAMoveDoesNotAdoptSomeoneElsesFence: a side that is fenced when a move starts is an
// earlier migration that was not finished, or a concurrent one, and the move refuses it
// (and unfences nothing); the caller can resume it explicitly. And what a move did not fence,
// it does not unfence.
func TestAMoveDoesNotAdoptSomeoneElsesFence(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	_, err := m.cfg.Control().FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: "dst"})
	require.NoError(t, err)

	_, err = m.Cutover(ctx, Steps{})
	require.ErrorContains(t, err, "a fence this move did not make")
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State, "the other fence is left alone")
	require.Equal(t, "src", st.AppTablegroup)

	// The policy touches only what the move fenced: here only the source, though the
	// destination is closed too.
	require.NoError(t, m.onError(ctx, "src", "dst", map[string]bool{"src": true}, false))
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State)

	// Resumed explicitly, the move takes the fence over and completes.
	_, err = m.Cutover(ctx, Steps{Resume: true})
	require.NoError(t, err)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)

	// A rollback expects the cutover's fence on the source, and refuses to start without it.
	m, _ = fixture(t, nil)
	_, err = m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	_, err = m.cfg.Control().UnfenceTablegroup(ctx, &multipoolerservicepb.UnfenceTablegroupRequest{Database: "db", Tablegroup: "src", ExpectedVersion: st.Source.Version})
	require.NoError(t, err)
	_, err = m.Rollback(ctx, Steps{})
	require.ErrorContains(t, err, "is expected to be FENCED from the cutover")
}

// TestGivingUpAfterResumingAFenceUnfencesAtTheRowsVersion: an earlier attempt left the
// tablegroup FENCING at the version the migrator reads, so the fence it resumes is at that
// version, and giving up unfences at it, not one above.
func TestGivingUpAfterResumingAFenceUnfencesAtTheRowsVersion(t *testing.T) {
	m, fake := fixture(t, func(c *Config) { c.GiveUpAfter = 50 * time.Millisecond })
	ctx := t.Context()
	fake.BusyMember("dst-2", "1 reservations held")
	// An earlier attempt: asked once, left FENCING.
	_, err := m.cfg.Control().FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: "dst", ExpectedVersion: 0})
	require.Equal(t, codes.Aborted, status.Code(err))
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCING, st.Dest.State)
	require.EqualValues(t, 1, st.Dest.Version)

	_, err = m.FenceOrGiveUp(ctx, "dst")
	var gaveUp *GaveUpError
	require.ErrorAs(t, err, &gaveUp)
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Dest.State, "giving up unfenced the resumed fence, which would not at version+1")
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
	_, err = m.cfg.Control().FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{Database: "db", Tablegroup: "dst", ExpectedVersion: st.Dest.Version})
	require.NoError(t, err)

	_, err = m.Cutover(ctx, Steps{})
	require.Error(t, err, "the traffic is on the destination, not the source")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State, "the rollback's fence is not undone")
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Source.State)

	// And a move that never tried to flip does not reopen the side it finds the traffic on.
	require.ErrorContains(t, m.onError(ctx, "src", "dst", map[string]bool{"src": true}, false), "someone else moved it")
	st, err = m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Dest.State)
}

// staleLeaderFlip is a Control whose flip first commits and loses the reply, then lands on a
// pooler that has just lost its place as the default primary, which answers UNAVAILABLE, and
// only then reaches the real leader.
type staleLeaderFlip struct {
	Control
	calls *int
}

func (c staleLeaderFlip) FlipRouting(ctx context.Context, in *multipoolerservicepb.FlipRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.FlipRoutingResponse, error) {
	*c.calls++
	switch *c.calls {
	case 1:
		if _, err := c.Control.FlipRouting(ctx, in, opts...); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "connection reset")
	case 2:
		return nil, status.Error(codes.Unavailable, "not the default primary pooler")
	}
	return c.Control.FlipRouting(ctx, in, opts...)
}

// TestAFlipRetryThatLandsOnAPoolerThatIsNotTheLeaderIsNotDefinitive: a not-leader answer is
// UNAVAILABLE, so it is retried, and the migrator does not unfence while the first attempt
// may still be committing on the real leader.
func TestAFlipRetryThatLandsOnAPoolerThatIsNotTheLeaderIsNotDefinitive(t *testing.T) {
	ctx := t.Context()
	m, _ := fixture(t, nil)
	calls := new(int)
	real := m.cfg.Control()
	m.cfg.Control = func() Control { return staleLeaderFlip{Control: real, calls: calls} }
	_, err := m.Cutover(ctx, Steps{})
	require.NoError(t, err)
	require.Equal(t, 3, *calls)
	st, err := m.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "dst", st.AppTablegroup)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED, st.Dest.State)
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, st.Source.State, "the side the traffic left stays fenced")
}
