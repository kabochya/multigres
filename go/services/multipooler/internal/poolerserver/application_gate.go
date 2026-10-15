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

package poolerserver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
)

// abandonedCheckTimeout bounds the look, on the server, for the backends the pooler
// gave up on. A variable so tests can shorten it.
var abandonedCheckTimeout = 5 * time.Second

// ApplicationGate is the application admission gate of the query server. The
// manager drives it from the persisted admission state; nothing else may reopen
// it.
type ApplicationGate interface {
	// EnableAdmissionControl closes the gate and makes every role or serving
	// change close it again until the admission state is re-read. Call it once,
	// before any application work. externalBackend is true for an unmanaged
	// pooler, whose source is writable whatever role the pooler reports: a probe
	// failure makes it report UNKNOWN, and it must not be taken for a standby.
	EnableAdmissionControl(externalBackend bool)
	// AdmissionGeneration returns a token that changes whenever the gate closes.
	AdmissionGeneration() uint64
	// OpenApplication opens the gate only if no close happened since generation
	// was read, and reports whether it did.
	OpenApplication(generation uint64) bool
	// CloseApplication closes the gate immediately.
	CloseApplication()
	// ShutdownApplication closes the gate for good: OpenApplication refuses from
	// then on, whatever generation it is given. It then waits until no application
	// work admitted before the close is in flight, and returns an error if ctx
	// ends first. The gate stays closed either way.
	ShutdownApplication(ctx context.Context) error
	// ApplicationOpen reports whether the gate is open.
	ApplicationOpen() bool
	// FenceApplication closes the gate and drains the application's work for up to
	// drainTimeout. At the deadline it closes the sessions that have nothing running
	// (idle, idle in a transaction, or pinned) and returns how many. It does not end
	// running work: if a request is in flight, or a backend the pooler gave up on may
	// still be present, it returns a FenceBusyError and leaves the gate closed, and
	// the fence is to be aborted.
	FenceApplication(ctx context.Context, drainTimeout time.Duration) (ended int, err error)
}

var _ ApplicationGate = (*QueryPoolerServer)(nil)

// BeginRequest admits an application query request, like StartRequest, but also
// applies the application admission gate and returns a release function the
// handler must call when it has finished. Until then the request counts as in
// flight, so a fence cannot complete while it runs: the permit covers the gap
// between admission and the acquisition of a backend connection.
func (s *QueryPoolerServer) BeginRequest(target *query.Target, kind RequestKind) (func(), error) {
	return s.BeginRequestOnReservation(target, kind, 0)
}

// BeginRequestOnReservation is BeginRequest for a request that continues the reserved
// connection reservedConnID (zero when it does not). A reservation a fence closed is
// answered with the error that ends the session, in the same step as the admission, so
// that a request cannot be admitted onto a reservation that is being closed.
func (s *QueryPoolerServer) BeginRequestOnReservation(target *query.Target, kind RequestKind, reservedConnID uint64) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reservedConnID != 0 {
		if _, ended := s.endedByFence[int64(reservedConnID)]; ended {
			return nil, mterrors.NewReservationEndedByFence()
		}
	}
	// Control traffic is not application work: it is served while the gate is
	// closed. A request that would reserve a connection is not exempt, or the
	// reservation, whose later requests pass the gate, would be a way in.
	gated := !(target.GetControl() && kind == RequestSingleQuery)
	// While a fence closes the idle sessions it has found nothing in flight on, no
	// request may start on a reservation.
	if s.fenceEnding && gated && kind == RequestExistingReserved {
		return nil, mterrors.NewReservationEndedByFence()
	}
	if err := s.startRequestLocked(target, kind, gated); err != nil {
		return nil, err
	}
	// Only a pooler with admission control ever waits for requests to finish, so
	// every other pooler skips the accounting (and its second lock acquisition).
	// Control traffic is not work a fence waits for either: steady NOTIFY traffic must
	// not keep a fence from ever reaching idle.
	if !s.admissionControlled || !gated {
		return noopRelease, nil
	}
	s.activeRequests++
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.activeRequests--
			s.notifyRequestsLocked()
		})
	}, nil
}

func noopRelease() {}

// notifyRequestsLocked wakes anyone waiting for requests to finish. The channel
// is created by the waiter, so a completion nobody waits for allocates nothing.
func (s *QueryPoolerServer) notifyRequestsLocked() {
	if s.requestsChanged != nil {
		close(s.requestsChanged)
		s.requestsChanged = nil
	}
}

// EnableAdmissionControl implements ApplicationGate.
func (s *QueryPoolerServer) EnableAdmissionControl(externalBackend bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.externalBackend = externalBackend
	s.admissionControlled = true
	s.applicationBlocked = true
	s.admissionGeneration++
}

// AdmissionGeneration implements ApplicationGate.
func (s *QueryPoolerServer) AdmissionGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissionGeneration
}

// OpenApplication implements ApplicationGate.
func (s *QueryPoolerServer) OpenApplication(generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != s.admissionGeneration || s.applicationShutdown {
		return false
	}
	s.applicationBlocked = false
	s.applicationFenced = false
	return true
}

// ShutdownApplication implements ApplicationGate. It waits for the pooler's own
// work only, without ending it: a pooler that shuts down gracefully drains what
// it admitted first, which is what makes a restart safe. The server's other
// clients are not this pooler's to wait for.
func (s *QueryPoolerServer) ShutdownApplication(ctx context.Context) error {
	s.mu.Lock()
	s.applicationShutdown = true
	s.closeApplicationLocked()
	s.mu.Unlock()
	return s.awaitIdle(ctx)
}

// CloseApplication implements ApplicationGate.
func (s *QueryPoolerServer) CloseApplication() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeApplicationLocked()
}

// closeApplicationLocked closes the gate and invalidates every admission read
// that began before it, even if the gate was already closed: a read in flight
// must not open the gate on a decision made for the state before this close.
func (s *QueryPoolerServer) closeApplicationLocked() {
	s.admissionGeneration++
	s.applicationBlocked = true
	s.applicationFenced = false
}

// ApplicationOpen implements ApplicationGate.
func (s *QueryPoolerServer) ApplicationOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.applicationBlocked
}

// FenceBusyError is the answer of a fence that cannot be completed because work is
// not over: the pooler does not end running work, it reports it and leaves the gate
// closed for the coordinator to abort the fence.
type FenceBusyError struct {
	// InFlight is how many requests were still running at the drain deadline.
	InFlight int
	// Abandoned is how many backends the pooler gave up on are still on the server.
	Abandoned int
	// Cause is why the look for abandoned backends could not be made, if it could not.
	Cause error
}

func (e *FenceBusyError) Error() string {
	msg := fmt.Sprintf("busy: %d requests in flight, %d abandoned backends present", e.InFlight, e.Abandoned)
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *FenceBusyError) Unwrap() error { return e.Cause }

// ErrorCode makes the error travel as RESOURCE_EXHAUSTED, which the coordinator reads as
// "busy": a pooler that has already waited out its drain deadline is not asked again.
func (e *FenceBusyError) ErrorCode() mtrpcpb.Code { return mtrpcpb.Code_RESOURCE_EXHAUSTED }

// FenceApplication implements ApplicationGate.
//
// A fence has to see that nothing it admitted can still commit, and it does not end
// running work to make sure: a cutover can wait, so when anything is in doubt the fence
// gives up and the coordinator reopens the tablegroup. It closes the gate and drains
// the admitted requests and the open transactions for up to drainTimeout. If requests
// are still in flight then, or a backend the pooler gave up on (a cancel that did not
// take effect, a connection that failed after a request was written) may still be on
// the server, it returns a FenceBusyError with the gate still closed. Otherwise it
// closes the sessions that have nothing running (idle, idle in a transaction, pinned
// by an advisory lock or a temporary table): the pooler holds their sockets and knows
// no request is in flight, so no COMMIT can be on its way, and an open transaction
// aborts when its socket closes. Their next request is answered with the error that
// ends the session.
//
// Clients that reach the server without going through the pooler are not this
// pooler's to wait for or to end.
func (s *QueryPoolerServer) FenceApplication(ctx context.Context, drainTimeout time.Duration) (int, error) {
	s.mu.Lock()
	s.closeApplicationLocked()
	s.applicationFenced = true
	// Only a managed pooler that is not the writable leader may be taken for a standby:
	// a pooler whose backend is an external source never is, however its readiness
	// probe is faring.
	standby := !s.externalBackend && !s.routingRole.Writable()
	s.mu.Unlock()

	drainCtx := ctx
	if drainTimeout > 0 {
		var cancel context.CancelFunc
		drainCtx, cancel = context.WithTimeout(ctx, drainTimeout)
		defer cancel()
	}
	drainErr := s.awaitIdle(drainCtx)
	if drainErr != nil && ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// What is still running now stays running. From here, until the idle sessions are
	// closed, no request may start on a reservation.
	s.mu.Lock()
	inFlight := s.activeRequests
	if inFlight == 0 {
		s.fenceEnding = true
	}
	s.mu.Unlock()
	if inFlight > 0 {
		if s.logger != nil {
			s.logger.WarnContext(ctx, "application fence deadline passed with work still running; reporting busy",
				"drain_timeout", drainTimeout, "in_flight", inFlight)
		}
		return 0, &FenceBusyError{InFlight: inFlight}
	}
	defer func() {
		s.mu.Lock()
		s.fenceEnding = false
		s.mu.Unlock()
	}()

	if s.poolManager == nil {
		return 0, nil
	}
	checkCtx, cancel := context.WithTimeout(ctx, abandonedCheckTimeout)
	defer cancel()
	abandoned, err := s.poolManager.AbandonedBackends(checkCtx)
	switch {
	case err != nil && standby:
		// A standby cannot commit, and a server it cannot reach is not work: one dead
		// replica does not hold up a fence of its whole cohort.
		if s.logger != nil {
			s.logger.WarnContext(ctx, "could not look for abandoned backends on a standby's server; its gate is closed and it cannot commit", "error", err)
		}
	case err != nil:
		return 0, &FenceBusyError{Abandoned: abandoned, Cause: err}
	case abandoned > 0:
		return 0, &FenceBusyError{Abandoned: abandoned}
	}

	// The reservations are remembered before they are closed: a request that arrives
	// meanwhile must already get the error that ends its session, not one it could
	// retry from.
	ended := s.poolManager.EndApplicationReservations(s.rememberEndedByFence)
	if len(ended) > 0 && s.logger != nil {
		s.logger.WarnContext(ctx, "application fence closed idle sessions", "sessions", len(ended))
	}
	return len(ended), nil
}

// maxRememberedFenceKills bounds the reservations remembered as ended by a fence.
// Reserved connection ids do not repeat, so a forgotten one simply gets the ordinary
// "connection terminated" error, which is the behavior for any other lost reservation.
const maxRememberedFenceKills = 100_000

// rememberEndedByFence records reservations a fence terminated.
func (s *QueryPoolerServer) rememberEndedByFence(ids []int64) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.endedByFence == nil || len(s.endedByFence)+len(ids) > maxRememberedFenceKills {
		s.endedByFence = make(map[int64]struct{}, len(ids))
	}
	for _, id := range ids {
		s.endedByFence[id] = struct{}{}
	}
}

// ReservationEndedByFence returns the error that ends the session of a request on a
// reserved connection that a fence terminated, and nil for any other request. A
// session that held a reservation across a fence must not carry on from an error it
// could retry: the state it held (a transaction, a session advisory lock) is gone,
// whatever the routing has done since, and a gateway that has not yet seen the
// routing move cannot tell.
func (s *QueryPoolerServer) ReservationEndedByFence(reservedConnID uint64) error {
	if reservedConnID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ended := s.endedByFence[int64(reservedConnID)]; ended {
		return mterrors.NewReservationEndedByFence()
	}
	return nil
}

// awaitIdle waits until no admitted request is in flight and every borrowed
// connection has been returned.
func (s *QueryPoolerServer) awaitIdle(ctx context.Context) error {
	if err := s.awaitRequests(ctx); err != nil {
		return err
	}
	if s.poolManager != nil {
		if err := s.poolManager.WaitForApplicationDrain(ctx); err != nil {
			return err
		}
	}
	// A request admitted before the gate closed may have started a new borrow.
	return s.awaitRequests(ctx)
}

func (s *QueryPoolerServer) awaitRequests(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.activeRequests == 0 {
			s.mu.Unlock()
			return nil
		}
		if s.requestsChanged == nil {
			s.requestsChanged = make(chan struct{})
		}
		ch := s.requestsChanged
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return errors.Join(errors.New("requests still in flight"), ctx.Err())
		case <-ch:
		}
	}
}
