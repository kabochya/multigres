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
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// BenchmarkPoolGet is the cost the reserved pool adds to every request that names a
// reserved connection: finding it and extending its life.
func BenchmarkPoolGet(b *testing.B) {
	server := fakepgserver.New(b)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newBenchPool(server)
	defer pool.Close()
	conn, err := pool.NewConn(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	conn.AddReservationReason(protoutil.ReasonTransaction)
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := pool.Get(conn.ConnID()); !ok {
			b.Fatal("connection not found")
		}
	}
	conn.Release(ReleaseCommit, nil)
}

func newBenchPool(server *fakepgserver.Server) *Pool {
	return NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: time.Minute,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig:   server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{Capacity: 2, MaxIdleCount: 2},
		},
	})
}

// BenchmarkPoolHold is what the service layer adds to such a request: marking the
// reservation in use for the length of the request, and ending it.
func BenchmarkPoolHold(b *testing.B) {
	server := fakepgserver.New(b)
	defer server.Close()
	server.SetNeverFail(true)
	pool := newBenchPool(server)
	defer pool.Close()
	conn, err := pool.NewConn(context.Background(), nil)
	if err != nil {
		b.Fatal(err)
	}
	conn.AddReservationReason(protoutil.ReasonTransaction)
	b.ReportAllocs()
	for b.Loop() {
		pool.Hold(conn.ConnID())()
	}
	conn.Release(ReleaseCommit, nil)
}
