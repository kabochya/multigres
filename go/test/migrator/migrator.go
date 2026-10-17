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

// Package migrator is a reference client of the serving-control RPCs of the default
// primary (GetServingState, FenceTablegroup, UnfenceTablegroup, FlipRouting). It is
// optional: the RPCs are primitives that are safe by construction and report facts, and
// the policy around them is the migration controller's. This package is one such policy,
// as a set of standard workflows a controller can use or replace:
//
//   - Preflight runs the checks that must hold before anything is fenced.
//   - FenceOrGiveUp fences a tablegroup, retries the same fence while a member is busy or
//     unreachable, and when it gives up unfences the tablegroup.
//   - UnfenceUntilReached unfences, and repeats until every member was reached.
//   - OnCutoverError and OnRollbackError are the on-error policy for a caller that lost
//     track of a move (a crashed migrator's successor): read the serving state, and decide
//     relative to the move: traffic still where it was, unfence the sides the move fenced;
//     traffic on the side it went to, that side serves and the other stays fenced.
//     Cutover and Rollback apply the same policy themselves, to the sides they fenced.
//   - Cutover and Rollback are the sequences, with the data movement left to the caller. They
//     refuse a side that is already fenced when they start (someone else's fence) unless
//     Steps.Resume says to take it over, unfence only what they fenced, and repeat a flip whose
//     outcome is unknown until it answers definitively, which is safe because a flip that
//     already committed succeeds when asked again.
//
// A migrator holds no open transaction and no running statement of its own on the source
// while a fence is in progress: they are application work the fence waits for, or an
// unexplained backend that makes it busy. It reads the catch-up position after the fence
// returns, from the source (pg_current_wal_insert_lsn() on a connection that is closed
// again, or IDENTIFY_SYSTEM on a replication connection), because nothing more is written
// through the fenced poolers from then on and the fence itself returns none. Its logical
// replication connections are walsenders on the source; the pooler tells them from running
// clients only because its role sees every session (pg_read_all_stats).
//
// Everything here only calls the RPCs. How long to wait, whether to retry, and what to
// catch the destination up to (the migrator reads its own target, for instance
// pg_current_wal_insert_lsn() on the fenced source) are its settings and callbacks.
//
// PROTOTYPE STUB: a test client of the prototype RPCs.
package migrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Control is the part of the default primary's service a migration calls.
// multipoolerservicepb.MultipoolerServiceClient satisfies it.
type Control interface {
	GetServingState(ctx context.Context, in *multipoolerservicepb.GetServingStateRequest, opts ...grpc.CallOption) (*multipoolerservicepb.GetServingStateResponse, error)
	FenceTablegroup(ctx context.Context, in *multipoolerservicepb.FenceTablegroupRequest, opts ...grpc.CallOption) (*multipoolerservicepb.FenceTablegroupResponse, error)
	UnfenceTablegroup(ctx context.Context, in *multipoolerservicepb.UnfenceTablegroupRequest, opts ...grpc.CallOption) (*multipoolerservicepb.UnfenceTablegroupResponse, error)
	FlipRouting(ctx context.Context, in *multipoolerservicepb.FlipRoutingRequest, opts ...grpc.CallOption) (*multipoolerservicepb.FlipRoutingResponse, error)
}

// Config is a migration's settings. Every field has a default except Database, Source,
// Destination and Control.
type Config struct {
	Database string
	// Source is the tablegroup that holds the application traffic, Destination the one
	// that is to receive it.
	Source, Destination string
	// Control returns the client of the default primary, called before every RPC so that
	// it follows a failover of the default primary.
	Control func() Control

	// Drain is the drain timeout asked of every member of a fence; zero is the server's
	// default (3 seconds). A short one keeps the pause of an ordinary cutover small, a long
	// one waits out a maintenance window.
	Drain time.Duration
	// FenceTimeout bounds each fence call as a whole; zero is the server's default.
	FenceTimeout time.Duration
	// GiveUpAfter is how long a fence that members cannot complete is retried before the
	// migrator gives up and unfences. Zero gives up after the first attempt.
	GiveUpAfter time.Duration
	// RetryInterval is the wait between attempts of the same fence, and of an unfence
	// that left members unreached. Zero is 500 milliseconds.
	RetryInterval time.Duration
	// UnfenceAttempts bounds how often an unfence is repeated for unreached members; zero
	// is 20.
	UnfenceAttempts int
	// FlipAttempts bounds how often a flip whose outcome is unknown is asked again before
	// the migrator reports that it does not know; zero is 20.
	FlipAttempts int

	Logf func(format string, args ...any)
}

// Migrator runs the standard workflows.
type Migrator struct {
	cfg Config
}

// New returns a migrator for cfg.
func New(cfg Config) *Migrator {
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 500 * time.Millisecond
	}
	if cfg.UnfenceAttempts <= 0 {
		cfg.UnfenceAttempts = 20
	}
	if cfg.FlipAttempts <= 0 {
		cfg.FlipAttempts = 20
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Migrator{cfg: cfg}
}

// ErrGaveUp is returned, wrapped, by FenceOrGiveUp when members could not complete the
// fence within GiveUpAfter. The tablegroup has been unfenced.
var ErrGaveUp = errors.New("the fence could not complete; gave up and unfenced")

// GaveUpError says what each member last answered.
type GaveUpError struct {
	Tablegroup string
	Members    []*multipoolerservicepb.MemberFenceResult
	// UnreachedAfterUnfence is how many members the unfence that followed could not reach
	// within its attempts; the tablegroup is unfenced all the same.
	UnreachedAfterUnfence int
}

func (e *GaveUpError) Error() string {
	var parts []string
	for _, m := range e.Members {
		if m.GetStatus() != multipoolerservicepb.MemberFenceResult_STATUS_CLEAN {
			parts = append(parts, fmt.Sprintf("%s: %s: %s", m.GetPoolerId().GetName(), m.GetStatus(), m.GetDetail()))
		}
	}
	msg := fmt.Sprintf("tablegroup %q: %s (%s)", e.Tablegroup, ErrGaveUp, strings.Join(parts, "; "))
	if e.UnreachedAfterUnfence > 0 {
		msg += fmt.Sprintf("; the unfence could not reach %d members", e.UnreachedAfterUnfence)
	}
	return msg
}

func (e *GaveUpError) Unwrap() error { return ErrGaveUp }

// State is the serving state of the migration's two tablegroups.
type State struct {
	AppTablegroup  string
	RoutingVersion int64
	Source, Dest   *multipoolerservicepb.TablegroupServingState
}

// Read reads the serving state.
func (m *Migrator) Read(ctx context.Context) (*State, error) {
	resp, err := m.cfg.Control().GetServingState(ctx, &multipoolerservicepb.GetServingStateRequest{
		Database: m.cfg.Database, Tablegroups: []string{m.cfg.Source, m.cfg.Destination},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetTablegroups()) != 2 {
		return nil, fmt.Errorf("serving state has %d tablegroups, want 2", len(resp.GetTablegroups()))
	}
	return &State{
		AppTablegroup: resp.GetAppTablegroup(), RoutingVersion: resp.GetRoutingVersion(),
		Source: resp.Tablegroups[0], Dest: resp.Tablegroups[1],
	}, nil
}

// closed reports whether a tablegroup refuses application work: FENCING or FENCED.
func closed(tg *multipoolerservicepb.TablegroupServingState) bool {
	return tg.GetState() != multipoolerservicepb.AdmissionState_ADMISSION_STATE_UNFENCED
}

func (s *State) of(tablegroup string) *multipoolerservicepb.TablegroupServingState {
	if s.Source.GetTablegroup() == tablegroup {
		return s.Source
	}
	return s.Dest
}

// Check is a preflight check: it returns an error saying what is not ready.
type Check func(ctx context.Context) error

// CheckRoutingAt requires that the application's traffic is on the given tablegroup.
func (m *Migrator) CheckRoutingAt(tablegroup string) Check {
	return func(ctx context.Context) error {
		st, err := m.Read(ctx)
		if err != nil {
			return err
		}
		if st.AppTablegroup != tablegroup {
			return fmt.Errorf("the application's traffic is on %q, not on %q", st.AppTablegroup, tablegroup)
		}
		return nil
	}
}

// CheckNeitherFenced requires that neither tablegroup is fenced already, which would be an
// earlier migration that was not finished.
func (m *Migrator) CheckNeitherFenced() Check {
	return func(ctx context.Context) error {
		st, err := m.Read(ctx)
		if err != nil {
			return err
		}
		for _, tg := range []*multipoolerservicepb.TablegroupServingState{st.Source, st.Dest} {
			if closed(tg) {
				return fmt.Errorf("tablegroup %q is fenced at version %d: an earlier migration was not finished", tg.GetTablegroup(), tg.GetVersion())
			}
		}
		return nil
	}
}

// Querier is the part of a database connection the SQL checks use.
type Querier interface {
	QueryRowScan(ctx context.Context, sql string, dest ...any) error
}

// CheckSourceSeesEverySession requires that the role the source is reached as can read the
// state of every session of its database: a superuser, or a role with the privileges of
// pg_read_all_stats (pg_monitor is one). What counts is whether the role inherits the
// privileges, which on PostgreSQL 16 and later is decided per grant: a member through a grant
// WITH INHERIT FALSE (or granted while the role was NOINHERIT) does not have them, and still
// sees NULL states, so mere membership is not enough.
// Without that the pooler cannot tell an idle session of another role from a running one,
// and a fence is busy for as long as any exists.
func CheckSourceSeesEverySession(q Querier) Check {
	return func(ctx context.Context) error {
		var ok bool
		err := q.QueryRowScan(ctx, `SELECT pg_has_role(current_user, 'pg_read_all_stats', 'usage')`, &ok)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("the source's role cannot read other sessions' state (superuser or pg_read_all_stats is needed): a fence will stay busy while any other session exists")
		}
		return nil
	}
}

// Preflight runs the checks and returns the first that fails. The standard ones are
// CheckRoutingAt, CheckNeitherFenced and CheckSourceSeesEverySession; a controller adds
// its own, for instance that every gateway follows routing.
func (m *Migrator) Preflight(ctx context.Context, checks ...Check) error {
	for i, check := range checks {
		if err := check(ctx); err != nil {
			return fmt.Errorf("preflight check %d: %w", i+1, err)
		}
	}
	return nil
}

// FenceOrGiveUp fences a tablegroup at the version it reads. If members are busy or
// unreachable it asks the same fence again every RetryInterval until GiveUpAfter has
// passed, and then unfences the tablegroup (until every member was reached) and returns a
// *GaveUpError. Any other error is returned as it is, for the on-error policy.
func (m *Migrator) FenceOrGiveUp(ctx context.Context, tablegroup string) (*multipoolerservicepb.FenceTablegroupResponse, error) {
	st, err := m.Read(ctx)
	if err != nil {
		return nil, err
	}
	version := st.of(tablegroup).GetVersion()
	deadline := time.Now().Add(m.cfg.GiveUpAfter)
	for {
		resp, err := m.cfg.Control().FenceTablegroup(ctx, &multipoolerservicepb.FenceTablegroupRequest{
			Database: m.cfg.Database, Tablegroup: tablegroup, ExpectedVersion: version,
			Timeout: optionalDuration(m.cfg.FenceTimeout), DrainTimeout: optionalDuration(m.cfg.Drain),
		})
		if err == nil {
			return resp, nil
		}
		incomplete := incompleteFence(err)
		if incomplete == nil {
			return nil, err
		}
		members := incomplete.GetMembers()
		m.cfg.Logf("fence of %q is not complete: %v", tablegroup, err)
		if time.Now().Add(m.cfg.RetryInterval).After(deadline) {
			// What is unfenced is the version the row is at now: the fence's own, which
			// the error carries. An earlier attempt may have left it FENCING at the
			// version read, so version+1 is not always it.
			fencedVersion := incomplete.GetState().GetVersion()
			if fencedVersion == 0 {
				fencedVersion = version + 1
			}
			gaveUp := &GaveUpError{Tablegroup: tablegroup, Members: members}
			if uerr := m.UnfenceUntilReached(ctx, tablegroup, fencedVersion); uerr != nil {
				var unreached *UnreachedError
				if !errors.As(uerr, &unreached) {
					return nil, fmt.Errorf("gave up on the fence of %q, and the unfence failed: %w", tablegroup, uerr)
				}
				gaveUp.UnreachedAfterUnfence = unreached.Unreached
			}
			return nil, gaveUp
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.cfg.RetryInterval):
		}
	}
}

// UnfenceUntilReached unfences a tablegroup fenced at fencedVersion and repeats the same
// call until no member is left unreached. A repeat is a success that writes nothing.
func (m *Migrator) UnfenceUntilReached(ctx context.Context, tablegroup string, fencedVersion int64) error {
	var lastUnreached int
	for attempt := 0; attempt < m.cfg.UnfenceAttempts; attempt++ {
		resp, err := m.cfg.Control().UnfenceTablegroup(ctx, &multipoolerservicepb.UnfenceTablegroupRequest{
			Database: m.cfg.Database, Tablegroup: tablegroup, ExpectedVersion: fencedVersion,
		})
		if err != nil {
			return err
		}
		lastUnreached = len(resp.GetUnreached())
		if lastUnreached == 0 {
			return nil
		}
		m.cfg.Logf("unfence of %q left %d members unreached", tablegroup, lastUnreached)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.cfg.RetryInterval):
		}
	}
	return &UnreachedError{Tablegroup: tablegroup, Unreached: lastUnreached, Attempts: m.cfg.UnfenceAttempts}
}

// UnreachedError is the error of an unfence that wrote UNFENCED but could not reach every
// member within its attempts.
type UnreachedError struct {
	Tablegroup string
	Unreached  int
	Attempts   int
}

func (e *UnreachedError) Error() string {
	return fmt.Sprintf("tablegroup %q is unfenced, but %d members were still unreached after %d attempts", e.Tablegroup, e.Unreached, e.Attempts)
}

// OnCutoverError is the policy for a caller that lost track of a cutover (Source to
// Destination), a crashed migrator for instance: it assumes the cutover fenced both sides.
// Cutover and Rollback themselves apply the same policy to the sides they fenced. See onError.
func (m *Migrator) OnCutoverError(ctx context.Context) error {
	return m.onError(ctx, m.cfg.Source, m.cfg.Destination, map[string]bool{m.cfg.Source: true, m.cfg.Destination: true}, true)
}

// OnRollbackError is the same for a rollback (Destination to Source), which fences only the
// destination: the source was fenced by the cutover.
func (m *Migrator) OnRollbackError(ctx context.Context) error {
	return m.onError(ctx, m.cfg.Destination, m.cfg.Source, map[string]bool{m.cfg.Destination: true}, true)
}

// onError is the policy for an error during a move of the traffic from one tablegroup to
// another. It reads the serving state and decides relative to the move in progress; fenced
// are the sides this move fenced:
//   - if the traffic is still on from (the move did not happen), it unfences the sides it
//     fenced, and nothing else: a side that was fenced before, by an earlier cutover or by
//     someone else, is not this move's to reopen;
//   - if the traffic is on to (the move happened), it unfences to, which holds the traffic
//     and has to serve, and leaves from fenced: reopening it would let a gateway that has
//     not seen the move write there. That is only for a move that flipped, or tried to
//     (flipAttempted): traffic found on to by a move that never flipped was moved by someone
//     else, and nothing is unfenced.
//
// It must not run while the outcome of the flip is unknown (see flip): the state it reads
// could still change under it.
//
// Traffic anywhere else is not this move's to repair, and is an error. What to do next, a
// new attempt, is for later: it needs a fresh expected version.
func (m *Migrator) onError(ctx context.Context, from, to string, fenced map[string]bool, flipAttempted bool) error {
	st, err := m.Read(ctx)
	if err != nil {
		return err
	}
	var toUnfence []*multipoolerservicepb.TablegroupServingState
	switch st.AppTablegroup {
	case from:
		for _, tg := range []string{from, to} {
			if fenced[tg] {
				toUnfence = append(toUnfence, st.of(tg))
			}
		}
	case to:
		if !flipAttempted {
			return fmt.Errorf("the application's traffic is on %q, which this move never tried to flip it to: someone else moved it, and nothing is unfenced", to)
		}
		toUnfence = append(toUnfence, st.of(to))
	default:
		return fmt.Errorf("the application's traffic is on %q, which is neither side of the move from %q to %q", st.AppTablegroup, from, to)
	}
	var errs []error
	for _, tg := range toUnfence {
		if closed(tg) {
			if err := m.UnfenceUntilReached(ctx, tg.GetTablegroup(), tg.GetVersion()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// FlipOutcomeUnknownError is the error of a flip whose outcome could not be learned: it
// failed in a way that does not say whether it committed (a timeout, a lost connection) and
// kept failing so. Nothing is unfenced then, since traffic may be on either side: both
// tablegroups are left as they are, for the caller to settle by repeating the flip, which is
// safe, or by reading the serving state once the control plane answers.
type FlipOutcomeUnknownError struct {
	Attempts int
	Err      error
}

func (e *FlipOutcomeUnknownError) Error() string {
	return fmt.Sprintf("the outcome of the flip is unknown after %d attempts, and both tablegroups are left as they are: %v", e.Attempts, e.Err)
}

func (e *FlipOutcomeUnknownError) Unwrap() error { return e.Err }

// outcomeUncertain reports whether a failed call may nonetheless have taken effect.
func outcomeUncertain(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown, codes.Internal:
		return true
	}
	return false
}

// flip moves the traffic and returns only with a definitive answer. FlipRouting is
// idempotent: a flip that committed and is asked again succeeds, and one that did not either
// commits or fails with FailedPrecondition. So when a flip fails in a way that does not say
// which, it is asked again until it does, and only then does anyone decide what to unfence:
// reading the routing while a flip is still committing on the server could show the old
// side, and unfencing it would leave it open when the flip lands.
func (m *Migrator) flip(ctx context.Context, req *multipoolerservicepb.FlipRoutingRequest) error {
	var err error
	attempts := 0
	for attempts < m.cfg.FlipAttempts {
		attempts++
		_, err = m.cfg.Control().FlipRouting(ctx, req)
		if err == nil || !outcomeUncertain(err) {
			return err
		}
		m.cfg.Logf("flip from %q to %q: outcome unknown: %v", req.GetFromTablegroup(), req.GetToTablegroup(), err)
		select {
		case <-ctx.Done():
			return &FlipOutcomeUnknownError{Attempts: attempts, Err: err}
		case <-time.After(m.cfg.RetryInterval):
		}
	}
	return &FlipOutcomeUnknownError{Attempts: attempts, Err: err}
}

// Steps are the parts of a cutover or rollback that are the migrator's own.
type Steps struct {
	// CatchUp brings the destination up to the source while both are fenced. It reads its
	// own target; the fence returns none.
	CatchUp func(ctx context.Context) error
	// BetweenFlipAndUnfence runs after traffic moved and before the destination serves
	// (optional).
	BetweenFlipAndUnfence func(ctx context.Context) error
	// Resume lets the move start from fences already in place, ones that an earlier attempt
	// of it left, instead of refusing them: without it a side that is fenced when the move
	// starts is someone else's fence, an earlier migration that was not finished or a
	// concurrent one, and the move does not adopt it. Resuming is the caller's explicit
	// statement that the fences found are its own: it adopts a fence at any version and
	// state, and will unfence it if the move fails before the flip.
	Resume bool
}

// Result is what a cutover or rollback leaves.
type Result struct {
	// FromFenceVersion is the version of the fence of the tablegroup traffic left, which
	// stays fenced and is the version to unfence it at to undo.
	FromFenceVersion int64
	// ToFenceVersion is the version of the fence of the tablegroup traffic went to, which
	// has been unfenced.
	ToFenceVersion int64
}

// Cutover moves the traffic from Source to Destination: fence both, catch up, flip,
// unfence the destination. An error before the flip leaves nothing fenced that the cutover
// fenced (the on-error policy runs); the source stays fenced after a flip.
func (m *Migrator) Cutover(ctx context.Context, steps Steps) (*Result, error) {
	return m.move(ctx, m.cfg.Source, m.cfg.Destination, steps, false)
}

// Rollback moves the traffic from Destination back to Source. The source is still fenced
// from the cutover, so only the destination is fenced; the source is unfenced at its
// fence's version at the end.
func (m *Migrator) Rollback(ctx context.Context, steps Steps) (*Result, error) {
	return m.move(ctx, m.cfg.Destination, m.cfg.Source, steps, true)
}

// checkMoveStart requires the tablegroups to be as a move expects to find them: the side the
// traffic leaves open, and the side it goes to open too (a cutover) or fenced by the cutover
// (a rollback). Anything else is a fence this move did not make.
func (m *Migrator) checkMoveStart(from, to string, toIsFenced bool) Check {
	return func(ctx context.Context) error {
		st, err := m.Read(ctx)
		if err != nil {
			return err
		}
		if closed(st.of(from)) {
			return fmt.Errorf("tablegroup %q is %s at version %d: a fence this move did not make (an earlier migration that was not finished, or a concurrent one); resume it explicitly or unfence it", from, st.of(from).GetState(), st.of(from).GetVersion())
		}
		toState := st.of(to).GetState()
		switch {
		case toIsFenced && toState != multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED:
			return fmt.Errorf("tablegroup %q is expected to be FENCED from the cutover, and is %s", to, toState)
		case !toIsFenced && closed(st.of(to)):
			return fmt.Errorf("tablegroup %q is %s at version %d: a fence this move did not make (an earlier migration that was not finished, or a concurrent one); resume it explicitly or unfence it", to, toState, st.of(to).GetVersion())
		}
		return nil
	}
}

func (m *Migrator) move(ctx context.Context, from, to string, steps Steps, toIsFenced bool) (res *Result, err error) {
	flipped, outcomeUnknown, flipAttempted, started := false, false, false, false
	fenced := map[string]bool{}
	defer func() {
		// A move that failed its preflight has done nothing, and undoes nothing: what it
		// found fenced is someone else's.
		if err != nil && started && !flipped && !outcomeUnknown {
			if oerr := m.onError(ctx, from, to, fenced, flipAttempted); oerr != nil {
				err = errors.Join(err, fmt.Errorf("on-error policy: %w", oerr))
			}
		}
	}()
	checks := []Check{m.CheckRoutingAt(from)}
	if !steps.Resume {
		checks = append(checks, m.checkMoveStart(from, to, toIsFenced))
	}
	if err := m.Preflight(ctx, checks...); err != nil {
		return nil, err
	}
	started = true
	if steps.Resume {
		fenced[from] = true
		fenced[to] = !toIsFenced
	}
	// What this move fences is recorded before it asks, since a fence that fails may have
	// written FENCING.
	fenced[from] = true
	fromFence, err := m.FenceOrGiveUp(ctx, from)
	if err != nil {
		return nil, err
	}
	res = &Result{FromFenceVersion: fromFence.GetState().GetVersion()}
	if toIsFenced {
		st, err := m.Read(ctx)
		if err != nil {
			return nil, err
		}
		if st.of(to).GetState() != multipoolerservicepb.AdmissionState_ADMISSION_STATE_FENCED {
			return nil, fmt.Errorf("tablegroup %q is expected to be FENCED from the cutover, and is %s", to, st.of(to).GetState())
		}
		res.ToFenceVersion = st.of(to).GetVersion()
	} else {
		fenced[to] = true
		toFence, err := m.FenceOrGiveUp(ctx, to)
		if err != nil {
			return nil, err
		}
		res.ToFenceVersion = toFence.GetState().GetVersion()
	}
	if steps.CatchUp != nil {
		if err := steps.CatchUp(ctx); err != nil {
			return nil, fmt.Errorf("catch up: %w", err)
		}
	}
	st, err := m.Read(ctx)
	if err != nil {
		return nil, err
	}
	flipAttempted = true
	if err := m.flip(ctx, &multipoolerservicepb.FlipRoutingRequest{
		Database: m.cfg.Database, FromTablegroup: from, ToTablegroup: to, ExpectedRoutingVersion: st.RoutingVersion,
	}); err != nil {
		var unknown *FlipOutcomeUnknownError
		outcomeUnknown = errors.As(err, &unknown)
		return nil, fmt.Errorf("flip: %w", err)
	}
	flipped = true
	// From here the destination holds the traffic: failing to unfence it is retried, not
	// undone.
	if steps.BetweenFlipAndUnfence != nil {
		if err := steps.BetweenFlipAndUnfence(ctx); err != nil {
			return res, err
		}
	}
	if err := m.UnfenceUntilReached(ctx, to, res.ToFenceVersion); err != nil {
		return res, fmt.Errorf("unfence the new home of the traffic: %w", err)
	}
	return res, nil
}

func incompleteFence(err error) *multipoolerservicepb.FenceTablegroupResponse {
	if status.Code(err) != codes.Aborted {
		return nil
	}
	for _, d := range status.Convert(err).Details() {
		if resp, ok := d.(*multipoolerservicepb.FenceTablegroupResponse); ok {
			return resp
		}
	}
	return nil
}

func optionalDuration(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}
	return durationpb.New(d)
}
