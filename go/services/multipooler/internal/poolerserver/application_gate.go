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
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/protoutil"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
)

// ApplicationGate is the application admission gate of the query server. The
// manager drives it from the persisted admission state; nothing else may reopen
// it.
type ApplicationGate interface {
	// EnableAdmissionControl closes the gate. When closesOnRoleChange is true it also
	// makes every role or serving change close it again until the admission state is
	// read. Call it once, before any application work. closesOnRoleChange is false for
	// a backend that is writable whatever role the pooler reports (an external source): a
	// probe failure makes it report UNKNOWN, and it must not be taken for a standby.
	EnableAdmissionControl(closesOnRoleChange bool)
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
	// FenceNow closes the gate and says, now and without waiting for anything, what the
	// application still runs or holds: it returns nil when no request is in flight and no
	// application reservation is held, which nothing can change while the gate stays closed.
	// It ends nothing. It covers what goes through the pooler and nothing else.
	FenceNow(ctx context.Context) *FenceBusy
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

// HoldReserved marks a request that names a reserved connection as running on it, until
// the returned function is called, which the handler defers right after BeginRequest. A
// fence that cannot complete reports which reservations have a request running on them,
// so a long-running request is not mistaken for an idle client. A request that does
// not name a reserved connection has nothing to hold.
func (s *QueryPoolerServer) HoldReserved(options *query.ExecuteOptions) func() {
	id := options.GetReservedConnectionId()
	if id == 0 || s.poolManager == nil {
		return noopRelease
	}
	user := options.GetUser()
	if user == "" {
		user = "postgres" // the user the executor serves a request that names none
	}
	return s.poolManager.HoldReservedConn(int64(id), user)
}

// AdmitStream is the admission decision for a request that is not application work
// the fence waits for: a logical replication stream. It applies the gate, so a stream
// is refused while the tablegroup is fenced, but the stream is not counted as a
// request in flight for as long as it lives. Moving the consumers of a replication
// stream is the migration's job, and the pooler ends the streams it has when it shuts
// down.
//
// This assumes that replication connections through the pooler are used only for
// replication. SQL sent over a replication connection is not covered by fencing (a
// replication=database connection accepts SQL), and callers must not use replication
// connections for writes.
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
func (s *QueryPoolerServer) EnableAdmissionControl(closesOnRoleChange bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closesOnRoleChange = closesOnRoleChange
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

// FenceBusy is what a closed gate still holds: work that is not over. The pooler does not
// end running work, it reports it, for the migrator to wait for, to end, or to unfence.
type FenceBusy struct {
	// InFlight is how many requests were still running. The gate closed when the fence
	// began, so every one of them was admitted before it: the count is reported without
	// ages.
	InFlight int
	// Reservations is how many application reservations (a transaction, even an idle
	// one, a temporary table, a session advisory lock, a COPY, a portal) were still
	// held.
	Reservations int
	// Held describes the longest held of them, at most MaxHeldReservationsReported, and
	// HeldNotListed is how many more there were. It carries nothing a client sent.
	Held          []connpoolmanager.HeldReservation
	HeldNotListed int
}

// MaxHeldReservationsReported bounds how many held reservations a busy fence describes, so
// that the answer stays small however many are held.
const MaxHeldReservationsReported = 20

// String describes what holds the gate, in a sentence, for a log line or a message.
func (e *FenceBusy) String() string {
	var msg strings.Builder
	fmt.Fprintf(&msg, "busy: %d requests in flight, %d reservations held", e.InFlight, e.Reservations)
	const inMessage = 3
	for i, h := range e.Held {
		if i == inMessage {
			fmt.Fprintf(&msg, "; and %d more", len(e.Held)-inMessage+e.HeldNotListed)
			break
		}
		idle := "idle " + h.IdleFor.Round(time.Millisecond).String()
		if h.InUse {
			idle = "running a request"
		}
		fmt.Fprintf(&msg, "; %s held %s %s pid %d user %s",
			strings.Join(reasonNames(h.Reasons), "|"), h.HeldFor.Round(time.Millisecond), idle, h.BackendPID, h.User)
	}
	return msg.String()
}

// Details is what the gate says about what holds the fence, as the message that travels
// to the coordinator and on to the controller.
func (e *FenceBusy) Details() *multipoolerservicepb.FenceBusy {
	d := &multipoolerservicepb.FenceBusy{
		RequestsInFlight:      int32(e.InFlight),
		ReservationsHeld:      int32(e.Reservations),
		ReservationsTruncated: int32(e.HeldNotListed),
	}
	for _, h := range e.Held {
		r := &multipoolerservicepb.HeldReservation{
			Reasons:              reasonNames(h.Reasons),
			HeldFor:              durationpb.New(h.HeldFor),
			IdleFor:              durationpb.New(h.IdleFor),
			User:                 h.User,
			BackendPid:           int32(h.BackendPID),
			InUse:                h.InUse,
			ClientPort:           h.ClientPort,
			ReservedConnectionId: h.ConnID,
			ClientConnectionId:   int64(h.ClientConnectionID),
		}
		d.Reservations = append(d.Reservations, r)
	}
	return d
}

// reasonNames lists the labels of the reservation reasons in a bitmask.
func reasonNames(reasons uint32) []string {
	var names []string
	for _, r := range protoutil.ReasonLabels() {
		if protoutil.HasReason(reasons, r.Bit) {
			names = append(names, r.Name)
		}
	}
	return names
}

// FenceNow implements ApplicationGate.
//
// A fence never ends a client's state: a cutover can wait, so what is not over is reported and
// the migrator decides. The order matters, and is this: the gate is closed and the requests in
// flight are read under one hold of s.mu, and only then are the reservations read. With the gate
// closed no request starts, so the requests in flight can only finish; a request that makes or
// resumes a reservation and finishes between the two reads is either still counted in flight (the
// report is busy) or has left its reservation behind (the second read sees it). Reading the
// reservations first would miss that request: it could create a reservation after the read and
// finish before the other, and a report of nothing would be false. A report of nothing is a fact
// that stays true while the gate stays closed. Requests on existing reservations still pass a
// closed gate, so an ordinary transaction can commit; an idle reservation is reaped by the
// reserved pool's own inactivity timeout, so a fence that one held up completes when it is asked
// again after that.
//
// Control traffic is not application work: the LISTEN connection, control single queries and
// logical replication streams are not reservations a fence waits for.
//
// The fence covers what goes through this pooler and nothing else. Clients that reach the server
// by another way (a direct connection, a background worker, a scheduled job) are not fenced, and
// not looked for: keeping every writer behind the pooler, and stopping the ones that cannot be,
// is the precondition of a cutover, and the operator's, or the migration workflow's, to meet.
func (s *QueryPoolerServer) FenceNow(ctx context.Context) *FenceBusy {
	s.mu.Lock()
	s.closeApplicationLocked()
	inFlight := s.activeRequests
	s.mu.Unlock()
	if s.betweenFenceReads != nil {
		s.betweenFenceReads()
	}
	var reservations int
	if s.poolManager != nil {
		reservations = int(s.poolManager.ApplicationReservations())
	}
	if inFlight == 0 && reservations == 0 {
		return nil
	}
	busy := &FenceBusy{InFlight: inFlight, Reservations: reservations}
	if s.poolManager != nil && reservations > 0 {
		busy.Held, _ = s.poolManager.HeldReservations(MaxHeldReservationsReported)
		// A reservation whose connection the server has ended (a controller terminated the
		// session) holds nothing: it is released here, from the sockets alone, so the report
		// that follows an ended session does not wait for the inactivity timeout to find it
		// gone.
		before := len(busy.Held)
		busy.Held = s.poolManager.ReleaseGone(busy.Held)
		if len(busy.Held) < before {
			reservations = int(s.poolManager.ApplicationReservations())
			busy.Reservations = reservations
			if inFlight == 0 && reservations == 0 {
				return nil
			}
		}
		// The count and the scan are taken apart, so the rest is what the count says is
		// held and the list does not show: the numbers add up.
		busy.HeldNotListed = max(reservations-len(busy.Held), 0)
	}
	return busy
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
