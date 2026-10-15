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

package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func present(pids ...uint32) func(context.Context, string) ([]uint32, error) {
	return func(context.Context, string) ([]uint32, error) { return pids, nil }
}

func TestAbandoned_PresentReportsAndPrunes(t *testing.T) {
	a := NewAbandoned()
	a.Record(10)
	a.Record(11)
	a.Record(11) // the same backend twice is one entry
	a.Record(0)  // no backend
	require.Equal(t, 2, a.Len())

	n, err := a.Present(t.Context(), present(11))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the backend still on the server")
	assert.Equal(t, 1, a.Len(), "the one that is gone is forgotten, the other is kept")

	n, err = a.Present(t.Context(), present())
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Zero(t, a.Len())
}

func TestAbandoned_AFailedLookKeepsEverything(t *testing.T) {
	a := NewAbandoned()
	a.Record(10)
	_, err := a.Present(t.Context(), func(context.Context, string) ([]uint32, error) { return nil, errors.New("down") })
	require.Error(t, err)
	assert.Equal(t, 1, a.Len(), "nothing is forgotten on a look that could not be made")
}

func TestAbandoned_QueryNamesTheListedBackends(t *testing.T) {
	a := NewAbandoned()
	a.Record(30)
	a.Record(20)
	var sql string
	_, err := a.Present(t.Context(), func(_ context.Context, q string) ([]uint32, error) { sql = q; return nil, nil })
	require.NoError(t, err)
	assert.Equal(t, "SELECT pid FROM pg_stat_activity WHERE pid IN (20,30)", sql)
}

func TestAbandoned_NothingListedAsksNothing(t *testing.T) {
	a := NewAbandoned()
	n, err := a.Present(t.Context(), func(context.Context, string) ([]uint32, error) {
		t.Fatal("a look at the server with nothing to look for")
		return nil, nil
	})
	require.NoError(t, err)
	assert.Zero(t, n)
}

// TestAbandoned_OverflowFailsClosed: an entry that does not fit is never dropped silently.
// The next check says so, once.
func TestAbandoned_OverflowFailsClosed(t *testing.T) {
	a := NewAbandoned()
	for i := range maxAbandoned + 5 {
		a.Record(uint32(i + 1))
	}
	require.Equal(t, maxAbandoned, a.Len())
	require.True(t, a.Overflowed())

	_, err := a.Present(t.Context(), present())
	require.ErrorIs(t, err, ErrAbandonedOverflow, "what was dropped cannot be looked for")
	require.False(t, a.Overflowed())
	n, err := a.Present(t.Context(), present())
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestAbandoned_NilListIsSafe(t *testing.T) {
	var a *Abandoned
	a.Record(5)
}
