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

package manager

import (
	"context"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// The coordinator's membership and refresh transport: who the members of a tablegroup are
// (topology), and how a member is told to re-read its admission (gRPC).

// topologyMembership enumerates a tablegroup's poolers from topology.
type topologyMembership struct{ ts topoclient.Store }

func (m topologyMembership) Snapshot(ctx context.Context, database, tablegroup string) ([]poolerRef, error) {
	list, err := metadataclient.Poolers(ctx, m.ts, database)
	if err != nil {
		return nil, err
	}
	var refs []poolerRef
	for _, p := range list {
		if p.GetShardKey().GetTableGroup() != tablegroup {
			continue
		}
		// A pooler that announced shutdown has stopped serving; its successor
		// registers again under the same id and is then part of the snapshot.
		if p.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
			continue
		}
		refs = append(refs, poolerRef{id: p.GetId(), record: p})
	}
	return refs, nil
}

// grpcRefresher calls RefreshAdmission on a pooler over gRPC.
type grpcRefresher struct{ transport grpc.DialOption }

func (r grpcRefresher) Refresh(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	conn, err := metadataclient.Dial(p.record, r.transport)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return multipoolerservicepb.NewMultipoolerServiceClient(conn).RefreshAdmission(ctx, req)
}
