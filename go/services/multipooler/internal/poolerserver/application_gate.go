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

	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
)

// BackendCheckBudget bounds the look, on the server, for backends that nothing of the
// pooler's explains. It is not a setting: the check always runs, and a caller that
// gives a fence a drain timeout is given this on top of it.
const BackendCheckBudget = 2 * time.Second

// FenceOptions are the limits of a fence.
type FenceOptions struct {
	// DrainTimeout is how long the fence waits for admitted work to finish.
	DrainTimeout time.Duration
	// BackendCheckTimeout bounds the look for unexplained backends. Zero selects
	// BackendCheckBudget; tests shorten it.
	BackendCheckTimeout time.Duration
}

// ApplicationGate is the application admission gate of the query server. The
// manager drives it from the persisted admission state; nothing else may reopen
// it.
type ApplicationGate interface {
	// EnableAdmissionControl closes the gate and makes every role or serving
	// change close it again until the admission state is read. Call it once,
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
	// FenceApplication closes the gate and waits, for up to opts.DrainTimeout, for the
	// application's requests to finish and its reservations to be released. It never
	// ends a client's state: if a request is in flight, an application reservation is
	// still held at the deadline, or a backend that is not idle is on the server that no
	// connection of the pooler explains, it returns a FenceBusyError and leaves the
	// gate closed, and the migrator decides whether to retry the fence or to unfence.
	FenceApplication(ctx context.Context, opts FenceOptions) error
}

var _ ApplicationGate = (*QueryPoolerServer)(nil)

// BeginRequest admits an application query request, like StartRequest, but also
// applies the application admission gate and returns a release function the
// handler must call when it has finished. Until then the request counts as in
// flight, so a fence cannot complete while it runs: the permit covers the gap
// between admission and the acquisition of a backend connection.
func (s *QueryPoolerServer) BeginRequest(target *query.Target, kind RequestKind) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Control traffic is not application work: it is served while the gate is
	// closed. A request that would reserve a connection is not exempt, or the
	// reservation, whose later requests pass the gate, would be a way in.
	gated := !(target.GetControl() && kind == RequestSingleQuery)
	if err := s.startRequestLocked(target, kind, gated); err != nil {
		return nil, err
	}
	// Control traffic is not work a fence waits for: steady NOTIFY traffic must not keep
	// a fence from ever reaching idle. A server that was never given admission control
	// (tests, a process that does not run one) skips the accounting and its second lock
	// acquisition.
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

// AdmitStream is the admission decision for a request that is not application work
// the fence waits for: a logical replication stream. It applies the gate, so a stream
// is refused while the tablegroup is fenced, but the stream is not counted as a
// request in flight for as long as it lives. Moving the consumers of a replication
// stream is the migration's job, and the pooler ends the streams it has when it shuts
// down.
//
// This assumes that replication connections through the pooler are used only for
// replication. SQL sent over a replication connection is not covered by fencing (a
// replication=database connection accepts SQL, and is a walsender, which the look for
// unexplained backends does not see), and callers must not use replication connections
// for writes.
func (s *QueryPoolerServer) AdmitStream(target *query.Target, kind RequestKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startRequestLocked(target, kind, true)
}

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
}

// ApplicationOpen implements ApplicationGate.
func (s *QueryPoolerServer) ApplicationOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.applicationBlocked
}

// FenceBusyError is the answer of a fence that cannot be completed because work is
// not over: the pooler does not end running work, it reports it and leaves the gate
// closed, for the migrator to retry the fence or to unfence.
type FenceBusyError struct {
	// InFlight is how many requests were still running at the drain deadline.
	InFlight int
	// Reservations is how many application reservations (a transaction, even an idle
	// one, a temporary table, a session advisory lock, a COPY, a portal) were still
	// held at the drain deadline.
	Reservations int
	// Unexplained is how many backends that are not idle were on the server that no
	// connection of the pooler explains.
	Unexplained int
	// Cause is why the look for them could not be made, if it could not.
	Cause error
}

func (e *FenceBusyError) Error() string {
	msg := fmt.Sprintf("busy: %d requests in flight, %d reservations held, %d unexplained backends present", e.InFlight, e.Reservations, e.Unexplained)
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *FenceBusyError) Unwrap() error { return e.Cause }

// ErrorCode makes the error travel as RESOURCE_EXHAUSTED, which the coordinator reads as
// "busy" in the per-member results of the fence.
func (e *FenceBusyError) ErrorCode() mtrpcpb.Code { return mtrpcpb.Code_RESOURCE_EXHAUSTED }

// FenceApplication implements ApplicationGate.
//
// A fence never ends a client's state: a cutover can wait, so when anything is in
// doubt the fence reports busy and the migrator decides. It closes the
// gate and waits, for up to the drain timeout, for the admitted requests to finish
// and the reservations to be released; requests on existing reservations still pass
// the closed gate, so an ordinary transaction can commit inside the deadline. At the
// deadline it returns a FenceBusyError, with the gate still closed, if a request is
// still in flight, if an application reservation is still held (a transaction, even an
// idle one, a temporary table or session advisory lock, a COPY, a portal), or if a
// backend that is not idle is on the server that no connection of the pooler explains
// (a cancel that did not take effect, a connection that failed after a request was
// written, a process that ran before this one, a client that did not come through the
// pooler). Otherwise the pooler holds nothing application work could still commit
// through, and it acknowledges.
//
// Control traffic is not application work: the LISTEN connection, control single
// queries and logical replication streams are not reservations a fence waits for.
//
// The look at the server covers every client role of the database, the pooler's own
// included: a client that reaches the server without going through the pooler is not
// this pooler's to end, but its work is not safe to ignore either, so a fence reports
// busy rather than guess. A managed standby skips the look: it cannot commit, and the
// read-only sessions of a replica would abort every fence of its cohort. Whether it is
// a standby is read when the look would run, so a replica promoted during the fence
// is looked at.
func (s *QueryPoolerServer) FenceApplication(ctx context.Context, opts FenceOptions) error {
	drainTimeout := opts.DrainTimeout
	s.mu.Lock()
	s.closeApplicationLocked()
	s.mu.Unlock()

	drainCtx := ctx
	if drainTimeout > 0 {
		var cancel context.CancelFunc
		drainCtx, cancel = context.WithTimeout(ctx, drainTimeout)
		defer cancel()
	}
	drainErr := s.awaitIdle(drainCtx)
	if drainErr != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	// What is still running, or still held, stays so: the fence reports it and ends
	// nothing.
	s.mu.Lock()
	inFlight := s.activeRequests
	s.mu.Unlock()
	var reservations int
	if s.poolManager != nil {
		reservations = int(s.poolManager.ApplicationReservations())
	}
	if inFlight > 0 || reservations > 0 {
		if s.logger != nil {
			s.logger.WarnContext(ctx, "application fence deadline passed with work still running or held; reporting busy",
				"drain_timeout", drainTimeout, "in_flight", inFlight, "reservations", reservations)
		}
		return &FenceBusyError{InFlight: inFlight, Reservations: reservations}
	}

	if s.poolManager == nil {
		return nil
	}
	// Only a managed pooler that is not the writable leader may be taken for a standby:
	// a pooler whose backend is an external source never is, however its readiness
	// probe is faring.
	s.mu.Lock()
	standby := !s.externalBackend && !s.routingRole.Writable()
	s.mu.Unlock()
	if standby {
		return nil
	}
	checkTimeout := opts.BackendCheckTimeout
	if checkTimeout <= 0 {
		checkTimeout = BackendCheckBudget
	}
	checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	unexplained, err := s.poolManager.UnexplainedBackends(checkCtx)
	switch {
	case err != nil:
		return &FenceBusyError{Cause: err}
	case unexplained > 0:
		return &FenceBusyError{Unexplained: unexplained}
	}
	return nil
}

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
