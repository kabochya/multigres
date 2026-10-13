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

import "sync"

// maxUnstopped bounds the tracker. Backends are only recorded when stopping them
// failed, so it stays tiny unless the backing role cannot signal backends at all;
// then the oldest entries are dropped rather than growing without limit.
const maxUnstopped = 4096

// UnstoppedBackends remembers backends that the pooler closed its connection to
// but could not be shown to have stopped: a pg_terminate_backend that failed, or
// a cancel that failed or did not take effect. Closing the client side does not
// stop a statement the backend is executing, so such a backend may still commit
// what it was running.
//
// An entry is matched by pid and backend start time, so a pid that the server
// reuses for another backend is recognized as the original having exited. One
// tracker is shared by every pool of a manager, so it survives a user pool being
// evicted and the pools being reopened.
type UnstoppedBackends struct {
	mu      sync.Mutex
	entries map[uint32]*unstoppedEntry
	order   []uint32
}

type unstoppedEntry struct {
	// start is the backend start time first seen for the pid; empty until the
	// first check.
	start string
}

// Record notes that the backend with this pid may still be running.
func (u *UnstoppedBackends) Record(pid uint32) {
	if u == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.entries == nil {
		u.entries = map[uint32]*unstoppedEntry{}
	}
	if _, ok := u.entries[pid]; ok {
		return
	}
	if len(u.entries) >= maxUnstopped {
		oldest := u.order[0]
		u.order = u.order[1:]
		delete(u.entries, oldest)
	}
	u.entries[pid] = &unstoppedEntry{}
	u.order = append(u.order, pid)
}

// Pids returns the recorded pids.
func (u *UnstoppedBackends) Pids() []uint32 {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	pids := make([]uint32, 0, len(u.entries))
	for pid := range u.entries {
		pids = append(pids, pid)
	}
	return pids
}

// Resolve reconciles the recorded pids with the backends the server lists: alive
// maps pid to the backend's start time. A pid that is no longer listed, or whose
// start time differs from the one first seen (the pid was reused), is forgotten.
// It returns how many recorded backends are still running.
func (u *UnstoppedBackends) Resolve(alive map[uint32]string) int {
	if u == nil {
		return 0
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	running := 0
	kept := u.order[:0]
	for _, pid := range u.order {
		e := u.entries[pid]
		if e == nil {
			continue
		}
		start, listed := alive[pid]
		switch {
		case !listed:
			delete(u.entries, pid)
			continue
		case e.start == "":
			e.start = start
		case e.start != start:
			delete(u.entries, pid)
			continue
		}
		running++
		kept = append(kept, pid)
	}
	u.order = kept
	return running
}
