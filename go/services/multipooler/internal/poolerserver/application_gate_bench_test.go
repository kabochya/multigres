// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolerserver

import (
	"testing"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// The admission gate is on every pooler, so what a request pays for it is on the query
// path. BenchmarkStartRequest is the path without per-request accounting (a pooler that
// is not admission controlled: one lock, nothing to release); BenchmarkBeginRequest is
// the path with it: the same admission decision, plus a count of the request in flight
// and a release that takes the lock a second time.

func benchServer() (*QueryPoolerServer, *query.Target) {
	s := newStartRequestTestServer()
	s.servingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	s.routingRole = servingstate.RoutingRolePrimary
	return s, &query.Target{}
}

func BenchmarkStartRequest(b *testing.B) {
	s, target := benchServer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.StartRequest(target, RequestSingleQuery); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBeginRequest(b *testing.B) {
	s, target := benchServer()
	s.EnableAdmissionControl(true)
	if !s.OpenApplication(s.AdmissionGeneration()) {
		b.Fatal("could not open the gate")
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			release, err := s.BeginRequest(target, RequestSingleQuery)
			if err != nil {
				b.Fatal(err)
			}
			release()
		}
	})
}

// BenchmarkBeginRequestWithoutAccounting is BeginRequest on a pooler that does not
// account: the cost of the extra function around StartRequest alone.
func BenchmarkBeginRequestWithoutAccounting(b *testing.B) {
	s, target := benchServer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			release, err := s.BeginRequest(target, RequestSingleQuery)
			if err != nil {
				b.Fatal(err)
			}
			release()
		}
	})
}
