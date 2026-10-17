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

package poolergateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWhatChangesInStepWithTheRoutingIsNeverBehindIt: the planner's tablegroup is updated in
// the same step as the routing view, so a reader that sees the new tablegroup through the
// routing never finds the planner still on the old one, which would let a statement planned at
// the old tablegroup meet reservations recorded at the new one. With the update made after the
// lock is released, as an ordinary OnChange callback is, readers do see it behind.
func TestWhatChangesInStepWithTheRoutingIsNeverBehindIt(t *testing.T) {
	for name, register := range map[string]func(r *AppRouting, f func(old, new string)){
		"in step":        func(r *AppRouting, f func(old, new string)) { r.OnChangeInStep(f) },
		"after the lock": func(r *AppRouting, f func(old, new string)) { r.OnChange(f) },
	} {
		t.Run(name, func(t *testing.T) {
			r := NewAppRouting("db")
			var planner atomic.Int64 // the version the planner plans for
			var version atomic.Int64
			register(r, func(_, _ string) {
				time.Sleep(50 * time.Microsecond) // the planner takes a moment to update
				planner.Store(version.Load())
			})
			stop := make(chan struct{})
			var behind atomic.Int64
			var wg sync.WaitGroup
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					// What a reader of the routing sees, then what the planner says.
					_, seen, known := r.snapshot()
					if known && planner.Load() < seen {
						behind.Add(1)
					}
				}
			})
			for v := int64(1); v <= 300; v++ {
				tg := "a"
				if v%2 == 0 {
					tg = "b"
				}
				version.Store(v)
				r.Update(tg, v)
			}
			close(stop)
			wg.Wait()
			if name == "in step" {
				require.Zero(t, behind.Load(), "the planner never lags the routing view")
			} else {
				t.Logf("the control: an after-the-lock callback lagged the routing view %d times", behind.Load())
			}
		})
	}
}
