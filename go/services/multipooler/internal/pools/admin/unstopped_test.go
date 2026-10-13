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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnstoppedBackendsAreRememberedUntilTheyExit(t *testing.T) {
	var u UnstoppedBackends
	u.Record(11)
	u.Record(12)
	u.Record(11) // idempotent
	require.ElementsMatch(t, []uint32{11, 12}, u.Pids())

	// Both still run: the first sighting records their start times.
	require.Equal(t, 2, u.Resolve(map[uint32]string{11: "t1", 12: "t2"}))
	require.Equal(t, 2, u.Resolve(map[uint32]string{11: "t1", 12: "t2"}), "no expiry while they run")

	// 11 exited; 12 did too, and its pid was reused by a different backend.
	require.Equal(t, 0, u.Resolve(map[uint32]string{12: "t9"}))
	require.Empty(t, u.Pids())
}

func TestUnstoppedBackendsTrackerIsBounded(t *testing.T) {
	var u UnstoppedBackends
	for pid := uint32(1); pid <= maxUnstopped+10; pid++ {
		u.Record(pid)
	}
	require.Len(t, u.Pids(), maxUnstopped)
	require.NotContains(t, u.Pids(), uint32(1), "the oldest entries are dropped")
}

func TestUnstoppedBackendsNilIsANoop(t *testing.T) {
	var u *UnstoppedBackends
	u.Record(1)
	require.Empty(t, u.Pids())
	require.Zero(t, u.Resolve(map[uint32]string{1: "t"}))
}
