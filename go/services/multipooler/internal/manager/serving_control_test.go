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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"
	"testing"

	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestServingControlAuthorization(t *testing.T) {
	key := bytes.Repeat([]byte{3}, 32)
	pm := &MultipoolerManager{config: &Config{MigrationKey: key}}
	hash := sha256.Sum256(key)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-multigres-serving-key", hex.EncodeToString(hash[:])))
	require.Error(t, pm.authorizeControl(ctx))
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 123}})
	require.NoError(t, pm.authorizeControl(ctx))
	require.Error(t, pm.authorizeControl(metadata.NewIncomingContext(ctx, metadata.Pairs("x-multigres-serving-key", "wrong"))))
	require.Error(t, pm.authorizeControl(peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 123}})))
	pm.config.MigrationKey = nil
	require.Error(t, pm.authorizeControl(ctx))
}

func TestServingAuthorityIndependentOfApplicationStatus(t *testing.T) {
	pm := &MultipoolerManager{config: &Config{}, record: newRecordFromProto(&pb.Multipooler{ShardKey: &pb.ShardKey{Database: "postgres"}}), healthStreamer: newHealthStreamer(slog.Default(), nil, "default", "0")}
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}
	require.NoError(t, pm.servingAuthority("postgres")) // DISABLED application status still owns authority.
	require.Error(t, pm.servingAuthority("other"))
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	require.Error(t, pm.servingAuthority("postgres"))
}

func TestServingAdminRejectsSourceCredentialNameCollision(t *testing.T) {
	targetClient := bytes.Repeat([]byte{8}, 32)
	targetServer := bytes.Repeat([]byte{9}, 32)
	stored := sha256.Sum256(targetClient)
	target := &scram.ScramHash{StoredKey: stored[:], ServerKey: targetServer}
	require.True(t, matchesTargetVerifier(target, &query.UserAuth{ClientKey: targetClient, ServerKey: targetServer}))
	require.False(t, matchesTargetVerifier(target, &query.UserAuth{ClientKey: bytes.Repeat([]byte{3}, 32), ServerKey: targetServer}), "same role name on source cannot prove target identity")
	require.False(t, matchesTargetVerifier(target, &query.UserAuth{ClientKey: targetClient, ServerKey: bytes.Repeat([]byte{4}, 32)}))
	require.False(t, matchesTargetVerifier(target, nil), "trust sessions do not prove target password possession")
}
