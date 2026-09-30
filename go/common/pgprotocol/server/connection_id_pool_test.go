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

package server

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
)

func TestListenersShareConnectionIDsAcrossWrapAndCleanup(t *testing.T) {
	pool := &ConnectionIDPool{}
	a := &Listener{gatewayID: 17, connectionIDs: pool}
	b := &Listener{gatewayID: 17, connectionIDs: pool}
	first, ok := a.assignConnectionID()
	require.True(t, ok)
	second, ok := b.assignConnectionID()
	require.True(t, ok)
	require.NotEqual(t, first, second)
	pool.next = pid.MaxLocalConnID
	third, ok := b.assignConnectionID()
	require.True(t, ok)
	require.NotEqual(t, first, third)
	require.NotEqual(t, second, third)
	pool.release(first)
	pool.next = pid.MaxLocalConnID
	reused, ok := a.assignConnectionID()
	require.True(t, ok)
	require.Equal(t, first, reused)
}

func TestSharedConnectionIDsConcurrentListeners(t *testing.T) {
	pool := &ConnectionIDPool{}
	ids := make(chan uint32, 100)
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			id, ok := pool.allocate()
			if ok {
				ids <- id
			}
		})
	}
	wg.Wait()
	close(ids)
	seen := map[uint32]bool{}
	for id := range ids {
		require.False(t, seen[id])
		seen[id] = true
	}
	require.Len(t, seen, 100)
}
