// Copyright 2025 Supabase, Inc.
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

package reserved

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/protoutil"
)

// TestHeldReadsReasonsWhileTheRequestChangesThem: a fence reads the reasons of a
// reservation from outside the request that holds it, as that request adds and removes
// them (removing the last one clears the reservation properties). Run under -race: the read
// must not touch what the request goroutine mutates, and must never see a half-made state.
func TestHeldReadsReasonsWhileTheRequestChangesThem(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)

	const rounds = 5000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Go(func() {
		defer close(stop)
		for range rounds {
			conn.AddReservationReason(protoutil.ReasonTransaction)
			conn.AddReservationReason(protoutil.ReasonTempTable)
			conn.RemoveReservationReason(protoutil.ReasonTransaction)
			conn.RemoveReservationReason(protoutil.ReasonTempTable) // the last one: props are cleared
		}
	})
	wg.Go(func() {
		valid := protoutil.ReasonTransaction | protoutil.ReasonTempTable
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, h := range pool.HeldReservations(time.Now()) {
				require.Zero(t, h.Reasons&^valid, "only the reasons the request set can be seen")
			}
		}
	})
	wg.Wait()

	conn.Release(ReleaseCommit, nil)
}

// TestHeldSaysWhetherARequestIsRunningAndSinceWhenItIsIdle: a reservation with a request
// running on it is in use and not idle, however long the request has run; once the request
// ends the connection has been idle since then, not since the request began.
func TestHeldSaysWhetherARequestIsRunningAndSinceWhenItIsIdle(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	conn.AddReservationReason(protoutil.ReasonTransaction)

	held := func() Held {
		all := pool.HeldReservations(time.Now())
		require.Len(t, all, 1)
		return all[0]
	}

	time.Sleep(60 * time.Millisecond)
	h := held()
	require.False(t, h.InUse)
	require.GreaterOrEqual(t, h.IdleFor, 50*time.Millisecond)

	end := pool.Hold(conn.ConnID())
	time.Sleep(60 * time.Millisecond)
	h = held()
	require.True(t, h.InUse, "a request is running on it")
	require.Zero(t, h.IdleFor, "a connection with a running request is not idle")
	require.GreaterOrEqual(t, h.HeldFor, 100*time.Millisecond)

	end()
	h = held()
	require.False(t, h.InUse)
	require.Less(t, h.IdleFor, 50*time.Millisecond, "idle counts from the end of the request")

	// A connection that is not there has nothing to hold.
	pool.Hold(conn.ConnID() + 1000)()

	conn.Release(ReleaseCommit, nil)
}

// TestHeldNamesTheConnectionByItsLocalPort: the port of the pooler's connection to the backend is
// reported, which is the client_port the server shows for it.
func TestHeldNamesTheConnectionByItsLocalPort(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()
	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	conn.AddReservationReason(protoutil.ReasonTransaction)

	all := pool.HeldReservations(time.Now())
	require.Len(t, all, 1)
	require.Positive(t, all[0].ClientPort)
	require.Equal(t, conn.Conn().RawConn().LocalPort(), all[0].ClientPort)
	conn.Release(ReleaseCommit, nil)
}

// TestDiscardIfGoneDropsAReservationTheServerEnded: a reservation that nothing is running on and
// whose connection is dead is released and its connection closed, ending nothing on the
// server; one whose connection is open stays, and one that a request is running on is left to that
// request.
func TestDiscardIfGoneDropsAReservationTheServerEnded(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()

	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, pool.DiscardIfGone(conn.ConnID()), "its connection is open")
	require.False(t, conn.IsReleased())

	// A connection that cannot be used any more, as one the server ended is (the end of the
	// stream is what the client's own tests cover); a real server does it in the end-to-end tests.
	require.NoError(t, conn.Conn().RawConn().Close())
	require.True(t, conn.BackendGone())

	end := pool.Hold(conn.ConnID())
	require.False(t, pool.DiscardIfGone(conn.ConnID()), "a request is running on it")
	end()
	require.True(t, pool.DiscardIfGone(conn.ConnID()))
	require.True(t, conn.IsReleased())
	require.False(t, pool.DiscardIfGone(conn.ConnID()), "it is gone: nothing more to drop")
	_, found := pool.Get(conn.ConnID())
	require.False(t, found, "a client that uses it finds it gone")
}

// TestHoldWaitsForADiscardThatIsLookingAtTheConnection: DiscardIfGone checks that a reservation is not
// in use and looks at its socket under p.mu, and Hold marks a request under the same lock, so a
// request cannot slip in between the check and the look: Hold waits until the discard is done, and
// finds the reservation released.
func TestHoldWaitsForADiscardThatIsLookingAtTheConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()
	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, conn.Conn().RawConn().Close()) // dead

	holdReturned := make(chan struct{})
	pool.afterNotInUseCheck = func() {
		go func() {
			end := pool.Hold(conn.ConnID())
			end()
			close(holdReturned)
		}()
		select {
		case <-holdReturned:
			t.Error("a request marked the reservation between the check and the look")
		case <-time.After(100 * time.Millisecond):
		}
	}
	require.True(t, pool.DiscardIfGone(conn.ConnID()))
	select {
	case <-holdReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("the request was never let in once the discard was done")
	}
	require.EqualValues(t, 0, conn.inUse.Load(), "the request found the reservation released and held nothing")
}

// TestHoldAndDiscardIfGoneDoNotInterleave: the mark that a request is running is made under the lock
// DiscardIfGone holds while it checks and looks at the connection, so a request cannot slip in
// between the check and the look. For a connection that is dead, racing the two: either the request
// marked it first and the discard leaves it alone, or the discard released it first and the request
// finds nothing to hold; a request is never left holding a connection that was released under it.
// Run under -race.
func TestHoldAndDiscardIfGoneDoNotInterleave(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()

	for round := range 300 {
		conn, err := pool.NewConn(context.Background(), nil)
		require.NoError(t, err)
		require.NoError(t, conn.Conn().RawConn().Close()) // dead, so a discard is allowed unless it is in use

		start := make(chan struct{})
		var (
			wg       sync.WaitGroup
			heldInUp bool
			released bool
		)
		wg.Go(func() {
			<-start
			end := pool.Hold(conn.ConnID())
			// Whatever Hold returned, if it marked the connection (it is in use) the connection is
			// not released until the request ends.
			if conn.inUse.Load() > 0 {
				heldInUp = true
				released = conn.IsReleased()
			}
			end()
		})
		wg.Go(func() {
			<-start
			pool.DiscardIfGone(conn.ConnID())
		})
		close(start)
		wg.Wait()
		require.False(t, heldInUp && released, "round %d: a request held a connection that was released under it", round)
		pool.DiscardIfGone(conn.ConnID()) // whichever lost, the dead connection goes now
		require.True(t, conn.IsReleased(), "round %d", round)
	}
}

// TestDiscardIfGoneIsNotAKill: nothing is sent to the server for a reservation whose connection
// the server ended, so it is released for its own reason and does not count as a kill.
func TestDiscardIfGoneIsNotAKill(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newTestPool(t, server)
	defer pool.Close()
	conn, err := pool.NewConn(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, conn.Conn().RawConn().Close())

	before := pool.Stats().KillCount
	require.True(t, pool.DiscardIfGone(conn.ConnID()))
	require.Equal(t, before, pool.Stats().KillCount, "not counted as a kill")
	require.Equal(t, "peer_gone", ReleasePeerGone.String())
}
