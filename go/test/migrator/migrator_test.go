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

// fenceWith starts the fence of a tablegroup from version and commits it, once each.
func fenceWith(t *testing.T, ctl Control, tg string, version int64) {
	t.Helper()
	started, err := ctl.StartFence(t.Context(), &multipoolerservicepb.StartFenceRequest{Database: "db", Tablegroup: tg, ExpectedVersion: version})
	require.NoError(t, err)
	_, err = ctl.CommitFence(t.Context(), &multipoolerservicepb.CommitFenceRequest{Database: "db", Tablegroup: tg, Version: started.State.Version})
	require.NoError(t, err)
}

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
	fence := func(tg string, v int64) { fenceWith(t, ctl, tg, v) }
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
	fenceWith(t, m.cfg.Control(), "dst", 0)

	_, err := m.Cutover(ctx, Steps{})
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
	// An earlier attempt: started, asked once, left FENCING.
	ctl := m.cfg.Control()
	_, err := ctl.StartFence(ctx, &multipoolerservicepb.StartFenceRequest{Database: "db", Tablegroup: "dst", ExpectedVersion: 0})
	require.NoError(t, err)
	_, err = ctl.CommitFence(ctx, &multipoolerservicepb.CommitFenceRequest{Database: "db", Tablegroup: "dst", Version: 1})
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
	fenceWith(t, m.cfg.Control(), "dst", st.Dest.Version)

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
	require.Equal(t, multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED, resp.State.State)
	require.EqualValues(t, 1, resp.State.Version, "starting again wrote nothing")
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
