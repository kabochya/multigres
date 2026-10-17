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

package connpoolmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

func held(user string, id int64, heldFor time.Duration) HeldReservation {
	return HeldReservation{User: user, Held: reserved.Held{ConnID: id, HeldFor: heldFor}}
}

// TestHeldReservationsAreOrderedLongestHeldFirstAndCapped: what a busy fence reports is the
// longest held reservations, at most the limit, with a count of the ones left out; ties are
// broken by reservation id so the answer is stable.
func TestHeldReservationsAreOrderedLongestHeldFirstAndCapped(t *testing.T) {
	all := []HeldReservation{
		held("a", 3, 5*time.Second),
		held("b", 1, 90*time.Second),
		held("a", 2, 5*time.Second),
		held("c", 4, 40*time.Second),
	}
	kept, more := capHeld(append([]HeldReservation(nil), all...), 3)
	require.Equal(t, 1, more)
	require.Equal(t, []int64{1, 4, 2}, []int64{kept[0].ConnID, kept[1].ConnID, kept[2].ConnID})

	kept, more = capHeld(append([]HeldReservation(nil), all...), 20)
	require.Len(t, kept, 4)
	require.Zero(t, more)

	kept, more = capHeld(nil, 20)
	require.Empty(t, kept)
	require.Zero(t, more)
}

// TestManagerHeldReservationsMergesTheUserPools: the reservations of every user's pool are
// merged into one list, the longest held first and cut at the limit with a count of the
// rest, each with the user whose pool holds it; a request running on one shows as in use.
func TestManagerHeldReservationsMergesTheUserPools(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	manager := newTestManager(t, server)
	defer manager.Close()
	ctx := context.Background()

	var ids []int64
	var users []string
	for _, user := range []string{"alice", "bob", "carol", "alice"} {
		conn, err := manager.NewReservedConn(ctx, nil, user, nil, nil)
		require.NoError(t, err)
		conn.AddReservationReason(protoutil.ReasonTransaction)
		ids, users = append(ids, conn.ConnID()), append(users, user)
		time.Sleep(15 * time.Millisecond) // held for different times, the first the longest
	}
	require.EqualValues(t, 4, manager.ApplicationReservations())

	all, more := manager.HeldReservations(20)
	require.Zero(t, more)
	require.Len(t, all, 4)
	for i, h := range all {
		require.Equal(t, users[i], h.User, "longest held first, across users")
		require.Equal(t, ids[i], h.ConnID)
		require.False(t, h.InUse)
	}

	capped, more := manager.HeldReservations(3)
	require.Equal(t, 1, more)
	require.Len(t, capped, 3)
	for i, h := range capped {
		require.Equal(t, ids[i], h.ConnID, "the cut keeps the longest held")
	}

	end := manager.HoldReservedConn(ids[2], "carol")
	all, _ = manager.HeldReservations(20)
	require.True(t, all[2].InUse)
	require.Zero(t, all[2].IdleFor)
	end()
	all, _ = manager.HeldReservations(20)
	require.False(t, all[2].InUse)
	manager.HoldReservedConn(1<<40, "nobody")() // nothing to hold, nothing to do
}

// TestReleaseGoneDropsTheReservationsTheServerEnded: the reservations whose connection the server
// ended hold nothing, and are released; the others are returned, and a reservation that a request
// is running on is kept.
func TestReleaseGoneDropsTheReservationsTheServerEnded(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	manager := newTestManager(t, server)
	defer manager.Close()
	ctx := context.Background()

	var conns []*reserved.Conn
	for range 2 {
		conn, err := manager.NewReservedConn(ctx, nil, "app", nil, nil)
		require.NoError(t, err)
		conn.AddReservationReason(protoutil.ReasonTransaction)
		conns = append(conns, conn)
	}
	held, _ := manager.HeldReservations(20)
	require.Len(t, held, 2)
	require.Len(t, manager.ReleaseGone(held), 2, "every connection is open")

	for _, conn := range conns { // connections that cannot be used any more, as ended ones are
		require.NoError(t, conn.Conn().RawConn().Close())
	}

	end := manager.HoldReservedConn(conns[1].ConnID(), "app")
	held, _ = manager.HeldReservations(20)
	live := manager.ReleaseGone(held)
	require.Len(t, live, 1, "the one that a request is running on is kept")
	require.Equal(t, conns[1].ConnID(), live[0].ConnID)
	require.True(t, conns[0].IsReleased())
	require.EqualValues(t, 1, manager.ApplicationReservations())
	end()
	held, _ = manager.HeldReservations(20)
	require.Empty(t, manager.ReleaseGone(held))
	require.Zero(t, manager.ApplicationReservations())
}
