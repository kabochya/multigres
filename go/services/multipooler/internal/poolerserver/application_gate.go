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
	// before any application work.
	EnableAdmissionControl()
	// AdmissionGeneration returns a token that changes whenever the gate closes.
	AdmissionGeneration() uint64
	// OpenApplication opens the gate only if no close happened since generation
	// was read, and reports whether it did.
	OpenApplication(generation uint64) bool
	// CloseApplication closes the gate immediately.
	CloseApplication()
	// ApplicationOpen reports whether the gate is open.
	ApplicationOpen() bool
	// FenceApplication closes the gate and waits until no application work is in
	// flight. Work still running when drainTimeout expires is terminated. It
	// returns the number of terminated reserved connections.
	FenceApplication(ctx context.Context, drainTimeout time.Duration) (terminated int, err error)
}

// backendTerminationChecker is implemented by pool managers that remember
// backends they failed to terminate.
type backendTerminationChecker interface {
	PendingTerminations(ctx context.Context) (int, error)
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
	if err := s.startRequestLocked(target, kind, true /* gated */); err != nil {
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
func (s *QueryPoolerServer) EnableAdmissionControl() {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if generation != s.admissionGeneration {
		return false
	}
	s.applicationBlocked = false
	return true
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
		// Idle, but backends that an earlier fence failed to terminate may still
		// be running.
		termCtx, cancel := context.WithTimeout(ctx, forceFinalWait)
		defer cancel()
		return 0, s.awaitTerminations(termCtx)
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	terminated := 0
	if s.poolManager != nil {
		terminated = s.poolManager.CloseReservedConnections(ctx)
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
	if err := s.awaitTerminations(finalCtx); err != nil {
		return terminated, err
	}
	return terminated, nil
}

// awaitTerminations waits until every backend whose termination failed earlier
// is gone from the server. Closing a connection whose backend is still executing
// a statement does not stop the statement, so such a backend could commit after
// the fence was acknowledged; a fence that cannot rule that out fails.
func (s *QueryPoolerServer) awaitTerminations(ctx context.Context) error {
	checker, ok := s.poolManager.(backendTerminationChecker)
	if !ok {
		return nil
	}
	for {
		pending, err := checker.PendingTerminations(ctx)
		if err != nil {
			return fmt.Errorf("check backends that could not be terminated: %w", err)
		}
		if pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d backend(s) that could not be terminated are still running; the fence cannot be acknowledged", pending)
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
