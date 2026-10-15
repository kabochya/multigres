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
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
)

// maxAbandoned bounds the list of abandoned backends. Overflowing it is recorded,
// never silently forgotten: see Abandoned.
const maxAbandoned = 1024

// Abandoned is the list of backend process ids that the pooler gave up on while they
// might still have been running something: a statement whose cancel did not take
// effect before the connection was closed, a connection that failed after a request
// had been written to it, a connection closed with an operation in flight, a kill that
// failed. A fence looks for them on the server before it is acknowledged: one that is
// still there, or that cannot be looked for, makes the fence abort.
//
// It is deliberately conservative. A process id can be reused by an unrelated backend,
// which only makes a fence abort once more than it had to, so there is no matching on
// start time, no expiry and no precise pruning. The pid column of pg_stat_activity is
// visible to every role, so no privilege is needed to look. Recording is one append on
// rare paths; the check runs on a fence only.
//
// The list belongs to the pooler, not to a pool of connections: a reopen replaces the
// pools, and the backends they abandoned are still on the server.
type Abandoned struct {
	mu       sync.Mutex
	pids     map[uint32]struct{}
	overflow bool
}

// NewAbandoned returns an empty list.
func NewAbandoned() *Abandoned {
	return &Abandoned{pids: map[uint32]struct{}{}}
}

// Record adds a backend process id. It is safe to call on a nil list.
func (a *Abandoned) Record(pid uint32) {
	if a == nil || pid == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.pids[pid]; !ok && len(a.pids) >= maxAbandoned {
		// Dropping the entry would fail open. Remember that it was dropped: every
		// fence aborts until a check has run.
		a.overflow = true
		return
	}
	a.pids[pid] = struct{}{}
}

// Len returns how many process ids are on the list.
func (a *Abandoned) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pids)
}

// Overflowed reports whether entries have been dropped since the last check.
func (a *Abandoned) Overflowed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.overflow
}

// ErrAbandonedOverflow means entries were dropped, so the check cannot say that nothing
// abandoned is left.
var ErrAbandonedOverflow = errors.New("the list of abandoned backends overflowed")

// Present looks for the listed backends on the server, through run (a query on the
// admin pool), forgets those that are gone, and returns how many are still there. An
// error means nothing is known. If the list had overflowed, the check that completes
// clears that and still returns ErrAbandonedOverflow once: what was dropped cannot be
// looked for.
func (a *Abandoned) Present(ctx context.Context, run func(ctx context.Context, sql string) ([]uint32, error)) (int, error) {
	a.mu.Lock()
	listed := make([]uint32, 0, len(a.pids))
	for pid := range a.pids {
		listed = append(listed, pid)
	}
	overflowed := a.overflow
	a.mu.Unlock()

	present := map[uint32]bool{}
	if len(listed) > 0 {
		slices.Sort(listed)
		parts := make([]string, len(listed))
		for i, pid := range listed {
			parts[i] = strconv.FormatUint(uint64(pid), 10)
		}
		pids, err := run(ctx, "SELECT pid FROM pg_stat_activity WHERE pid IN ("+strings.Join(parts, ",")+")")
		if err != nil {
			return 0, fmt.Errorf("look for the backends the pooler gave up on: %w", err)
		}
		for _, pid := range pids {
			present[pid] = true
		}
	}

	a.mu.Lock()
	for _, pid := range listed {
		if !present[pid] {
			delete(a.pids, pid)
		}
	}
	if overflowed {
		a.overflow = false
	}
	a.mu.Unlock()
	if overflowed {
		return len(present), ErrAbandonedOverflow
	}
	return len(present), nil
}

// PresentAbandoned looks for the abandoned backends through a pooled admin connection.
func (p *Pool) PresentAbandoned(ctx context.Context) (int, error) {
	return p.abandoned.Present(ctx, func(ctx context.Context, sql string) ([]uint32, error) {
		pooled, err := p.pool.Get(ctx)
		if err != nil {
			return nil, err
		}
		defer pooled.Recycle()
		results, err := pooled.Conn.QueryWithRetry(ctx, sql)
		if err != nil {
			return nil, err
		}
		var pids []uint32
		for _, r := range results {
			for _, row := range r.StructuredRows() {
				if len(row.Values) == 0 {
					continue
				}
				n, err := strconv.ParseUint(string(row.Values[0]), 10, 32)
				if err != nil {
					return nil, mterrors.Wrap(err, "parse backend pid")
				}
				pids = append(pids, uint32(n))
			}
		}
		return pids, nil
	})
}

// RecordAbandoned records a backend the pooler gave up on; see Abandoned.
func (p *Pool) RecordAbandoned(pid uint32) { p.abandoned.Record(pid) }
