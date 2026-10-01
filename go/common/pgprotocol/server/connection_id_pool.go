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

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
)

// ConnectionIDPool shares the local PID namespace among a gateway's listeners.
// IDs stay reserved through session cleanup, preventing one listener's prepared
// statement/cancellation identity from aliasing another listener's connection.
type ConnectionIDPool struct {
	mu   sync.Mutex
	next uint32
	used map[uint32]bool
}

func (p *ConnectionIDPool) allocate() (uint32, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.used == nil {
		p.used = make(map[uint32]bool)
	}
	for range pid.MaxLocalConnID {
		p.next++
		if p.next > pid.MaxLocalConnID {
			p.next = 1
		}
		if !p.used[p.next] {
			p.used[p.next] = true
			return p.next, true
		}
	}
	return 0, false
}

func (p *ConnectionIDPool) release(encoded uint32) {
	_, local := pid.DecodePID(encoded)
	p.mu.Lock()
	delete(p.used, local)
	p.mu.Unlock()
}
