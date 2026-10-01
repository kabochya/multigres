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

// Package migrationcontrol provides managed-authority discovery for serving control.
package migrationcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

func AuthorizedContext(ctx context.Context, key []byte) context.Context {
	hash := sha256.Sum256(key)
	return metadata.AppendToOutgoingContext(ctx, "x-multigres-serving-key", hex.EncodeToString(hash[:]))
}

// Poolers uses authoritative reads of every known cell. Partial listings are errors.
func Poolers(ctx context.Context, ts topoclient.Store, database string) ([]*pb.Multipooler, error) {
	cells, err := ts.GetCellNames(ctx)
	if err != nil {
		return nil, err
	}
	var result []*pb.Multipooler
	for _, cell := range cells {
		list, err := ts.GetMultipoolersByCell(ctx, cell, &topoclient.GetMultipoolersByCellOptions{DatabaseShard: &topoclient.DatabaseShard{Database: database}})
		if err != nil {
			return nil, err
		}
		for _, item := range list {
			result = append(result, item.Multipooler)
		}
	}
	return result, nil
}

// Authority never uses application serving status as a leadership condition.
// Existing consensus rule numbers resolve overlap. Equal-number different
// primaries fail closed rather than picking an arbitrary process.
func Authority(poolers []*pb.Multipooler) (*pb.Multipooler, error) {
	var leader *pb.Multipooler
	ambiguous := false
	for _, p := range poolers {
		if p.ManagementMode == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED || p.GetRoutingState().GetRole() != pb.RoutingRole_ROUTING_ROLE_PRIMARY {
			continue
		}
		if leader == nil {
			leader = p
			continue
		}
		r := p.GetRoutingState().GetRule()
		old := leader.GetRoutingState().GetRule()
		if r.GetCoordinatorTerm() == old.GetCoordinatorTerm() && r.GetLeaderSubterm() == old.GetLeaderSubterm() {
			ambiguous = true
		}
		if r.GetCoordinatorTerm() > old.GetCoordinatorTerm() || r.GetCoordinatorTerm() == old.GetCoordinatorTerm() && r.GetLeaderSubterm() > old.GetLeaderSubterm() {
			leader = p
			ambiguous = false
		}
	}
	if leader == nil {
		return nil, errors.New("managed authority unavailable")
	}
	if ambiguous {
		return nil, errors.New("ambiguous managed authority")
	}
	return leader, nil
}

func Dial(p *pb.Multipooler, transport grpc.DialOption) (*grpc.ClientConn, error) {
	return grpccommon.NewClient((&topoclient.MultipoolerInfo{Multipooler: p}).Addr(), grpccommon.WithDialOptions(transport))
}

func WithAuthority(ctx context.Context, ts topoclient.Store, database string, transport grpc.DialOption, fn func(rpc.MultipoolerServiceClient) error) error {
	list, err := Poolers(ctx, ts, database)
	if err != nil {
		return err
	}
	leader, err := Authority(list)
	if err != nil {
		return err
	}
	conn, err := Dial(leader, transport)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(rpc.NewMultipoolerServiceClient(conn))
}
