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

	"github.com/multigres/multigres/go/pb/query"
)

// forceFinalWait bounds how long a fence waits for in-flight work to finish
// after it has terminated the reserved connections. A variable so tests can
// shorten it.
var forceFinalWait = 5 * time.Second

// ApplicationGate is the application admission gate of the query server. The
// manager drives it from the persisted admission state; nothing else may reopen
// it.
type ApplicationGate interface {
	// EnableAdmissionControl closes the gate and makes every role or serving
	// change close it again until the admission state is re-read. Call it once,
	// before any application work. quiescenceExcludedRoles are database roles whose
	// backends a fence does not wait for (a migrator's, for example), in addition
	// to the pooler's own admin role.
	EnableAdmissionControl(quiescenceExcludedRoles []string)
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
	// FenceApplication closes the gate and waits until no application work is in
	// flight. Work still running when drainTimeout expires is terminated. It
	// returns the number of terminated reserved connections.
	FenceApplication(ctx context.Context, drainTimeout time.Duration) (terminated int, err error)
}

// backendQuiescenceChecker is implemented by pool managers that can count the
// client backends of the database that are doing work, other than those of the
// given roles and the pooler's own.
type backendQuiescenceChecker interface {
	BusyBackends(ctx context.Context, excludedRoles []string) (int, error)
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
	// Only a pooler with admission control ever waits for requests to finish, so
	// every other pooler skips the accounting (and its second lock acquisition).
	if !s.admissionControlled {
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
func (s *QueryPoolerServer) EnableAdmissionControl(quiescenceExcludedRoles []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quiescenceExcludedRoles = quiescenceExcludedRoles
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
// work only: the server's other clients are not this pooler's to wait for, and a
// pooler that has stopped admitting leaves the check of the server to the
// poolers that stay.
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

// FenceApplication implements ApplicationGate.
//
// It closes the gate, then waits for admitted requests and borrowed connections
// to finish, including open transactions. If drainTimeout expires first it
// terminates the reserved connections (rolling back their transactions) and waits
// a bounded time more. It never reports success while application work is still
// running: a fence that cannot reach an idle pooler returns an error, and the gate
// stays closed.
func (s *QueryPoolerServer) FenceApplication(ctx context.Context, drainTimeout time.Duration) (int, error) {
	s.CloseApplication()

	drainCtx := ctx
	if drainTimeout > 0 {
		var cancel context.CancelFunc
		drainCtx, cancel = context.WithTimeout(ctx, drainTimeout)
		defer cancel()
	}
	err := s.awaitIdle(drainCtx)
	if err == nil {
		// The pooler is idle; the server may not be (a backend whose connection the
		// pooler closed, a direct client).
		quietCtx, cancel := context.WithTimeout(ctx, forceFinalWait)
		defer cancel()
		return 0, s.awaitBackendQuiescent(quietCtx)
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	terminated := 0
	if s.poolManager != nil {
		terminated = s.poolManager.CloseApplicationConnections(ctx)
	}
	if s.logger != nil {
		s.logger.WarnContext(ctx, "application fence deadline passed, terminated remaining work",
			"drain_timeout", drainTimeout, "terminated_reserved_connections", terminated)
	}
	finalCtx, cancel := context.WithTimeout(ctx, forceFinalWait)
	defer cancel()
	if err := s.awaitIdle(finalCtx); err != nil {
		return terminated, fmt.Errorf("application work still in flight after terminating reserved connections: %w", err)
	}
	if err := s.awaitBackendQuiescent(finalCtx); err != nil {
		return terminated, err
	}
	return terminated, nil
}

// awaitBackendQuiescent waits until the database server shows no application work
// in flight. Closing a connection whose backend is still executing a statement does
// not stop the statement, and a client that reaches the database directly is not
// something the pooler's own accounting sees; looking at the backends themselves
// covers both, along with every way the pooler may have given up on a connection
// without learning that its work ended. A check that cannot be made fails the
// fence, so does work still running at the deadline.
func (s *QueryPoolerServer) awaitBackendQuiescent(ctx context.Context) error {
	checker, ok := s.poolManager.(backendQuiescenceChecker)
	if !ok {
		return nil
	}
	s.mu.Lock()
	excluded := s.quiescenceExcludedRoles
	s.mu.Unlock()
	for {
		busy, err := checker.BusyBackends(ctx, excluded)
		if err != nil {
			return fmt.Errorf("check that the backend is quiescent: %w", err)
		}
		if busy == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d backend(s) of the database are still doing work; the fence cannot be acknowledged", busy)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// awaitIdle waits until no admitted request is in flight and every borrowed
// connection has been returned.
func (s *QueryPoolerServer) awaitIdle(ctx context.Context) error {
	if err := s.awaitRequests(ctx); err != nil {
		return err
	}
	if s.poolManager != nil {
		if err := s.poolManager.WaitForDrain(ctx); err != nil {
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
