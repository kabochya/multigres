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
	"sync"

	"github.com/multigres/multigres/go/pb/query"
)

// BeginRequest holds an admission permit until the handler has completed. The
// permit covers the gap between admission and acquisition of a backend.
func (s *QueryPoolerServer) BeginRequest(target *query.Target, kind RequestKind) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.startRequestLocked(target, kind); err != nil {
		return nil, err
	}
	s.activeRequests++
	var once sync.Once
	return func() {
		once.Do(func() { s.mu.Lock(); defer s.mu.Unlock(); s.activeRequests--; s.notifyRequestsLocked() })
	}, nil
}

func (s *QueryPoolerServer) notifyRequestsLocked() {
	if s.requestsChanged != nil {
		close(s.requestsChanged)
	}
	s.requestsChanged = make(chan struct{})
}

// SetApplicationAdmission changes permission independently of backend health.
// Only the admission actuator may reopen it. Health recovery cannot clear this flag.
func (s *QueryPoolerServer) SetApplicationAdmission(allow bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !allow {
		s.admissionGeneration++
	}
	s.applicationBlocked = !allow
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
			return ctx.Err()
		case <-ch:
		}
	}
}

// FenceApplication stops new work immediately. Existing reservations can finish
// on their original backend. A timeout leaves admission closed and is an error;
// this path never force-rolls back a transaction and calls that a successful fence.
func (s *QueryPoolerServer) FenceApplication(ctx context.Context) error {
	s.SetApplicationAdmission(false)
	return s.DrainApplication(ctx)
}

// DrainApplication waits with the gate already closed, preserving its generation.
func (s *QueryPoolerServer) DrainApplication(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.awaitRequests(ctx); err != nil {
		return err
	}
	if s.poolManager != nil {
		if err := s.poolManager.WaitForDrain(ctx); err != nil {
			return err
		}
	}
	return s.awaitRequests(ctx)
}

// EnableAdmissionControl is called once before any application work. Local generation
// tokens prevent a read begun before promotion from opening the new leader's gate.
// They are process-lifetime ordering, not persisted migration-mode versions.
func (s *QueryPoolerServer) EnableAdmissionControl(managed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applicationBlocked = true
	s.closeGateOnRoleChange = managed
}

func (s *QueryPoolerServer) AdmissionGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissionGeneration
}

func (s *QueryPoolerServer) ApplyAdmissionGeneration(generation uint64, allow bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != s.admissionGeneration {
		return false
	}
	s.applicationBlocked = !allow
	return true
}
