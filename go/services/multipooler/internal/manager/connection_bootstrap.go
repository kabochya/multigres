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
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	code "github.com/multigres/multigres/go/pb/mtrpc"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Control calls additionally require the cluster key proof. Deployments must use
// the existing authenticated TLS transport; the local demo binds loopback only.
func (pm *MultipoolerManager) authorizeControl(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return mterrors.New(code.Code_PERMISSION_DENIED, "protected control transport required")
	}
	if _, tls := p.AuthInfo.(credentials.TLSInfo); !tls {
		if p.Addr == nil {
			return mterrors.New(code.Code_PERMISSION_DENIED, "protected control transport required")
		}
		host, _, err := net.SplitHostPort(p.Addr.String())
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			return mterrors.New(code.Code_PERMISSION_DENIED, "TLS control transport required")
		}
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-multigres-serving-key")
	hash := sha256.Sum256(pm.config.MigrationKey)
	expected := hex.EncodeToString(hash[:])
	if len(pm.config.MigrationKey) != 32 || len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
		return mterrors.New(code.Code_PERMISSION_DENIED, "serving control authorization required")
	}
	return nil
}

func (pm *MultipoolerManager) servingAuthority(database string) error {
	if database != pm.record.ShardKey().GetDatabase() {
		return mterrors.New(code.Code_INVALID_ARGUMENT, "database does not match pooler")
	}
	role := pb.RoutingRole_ROUTING_ROLE_UNKNOWN
	if pm.stateManager != nil {
		role = pm.stateManager.RoutingRole()
	} else if pm.healthStreamer != nil {
		role = pm.healthStreamer.getState().RoutingState.GetRole()
	}
	if pm.IsUnmanaged() || role != pb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return mterrors.New(code.Code_FAILED_PRECONDITION, "not the managed writable authority")
	}
	return nil
}

func (pm *MultipoolerManager) targetAdminVerifier(ctx context.Context, username string) (string, error) {
	result, err := pm.qsc.InternalQueryService().QueryAdminArgs(ctx, `SELECT rolpassword FROM pg_authid WHERE rolname=$1 AND rolsuper AND rolcanlogin AND (rolvaliduntil IS NULL OR rolvaliduntil > now())`, username)
	if err != nil {
		return "", errors.New("target administrator lookup unavailable")
	}
	var verifier string
	if executor.ScanSingleRow(result, &verifier) != nil || verifier == "" {
		return "", mterrors.New(code.Code_PERMISSION_DENIED, "target administrator required")
	}
	return verifier, nil
}

func (pm *MultipoolerManager) authorizeServingAdmin(ctx context.Context, username string, auth *query.UserAuth) error {
	verifier, err := pm.targetAdminVerifier(ctx, username)
	if err != nil {
		return err
	}
	hash, err := scram.ParseScramSHA256Hash(verifier)
	if err != nil || !matchesTargetVerifier(hash, auth) {
		return mterrors.New(code.Code_PERMISSION_DENIED, "target administrator credentials required")
	}
	return nil
}

func matchesTargetVerifier(hash *scram.ScramHash, auth *query.UserAuth) bool {
	if hash == nil || len(auth.GetClientKey()) != sha256.Size || len(auth.GetServerKey()) != sha256.Size {
		return false
	}
	stored := sha256.Sum256(auth.GetClientKey())
	return subtle.ConstantTimeCompare(stored[:], hash.StoredKey) == 1 && subtle.ConstantTimeCompare(auth.GetServerKey(), hash.ServerKey) == 1
}

// The existing action lock pins leadership through each transaction. No remote
// bootstrap, admission or drain wait occurs under this lock.
func (pm *MultipoolerManager) connectionCatalogLocked(ctx context.Context) (*connectioncatalog.Catalog, error) {
	if pm.connectionCatalog != nil {
		return pm.connectionCatalog, nil
	}
	c, err := connectioncatalog.New(pm.qsc.InternalQueryService(), pm.config.MigrationKey)
	if err != nil {
		return nil, err
	}
	if err = c.Initialize(ctx); err != nil {
		return nil, err
	}
	pm.connectionCatalog = c
	return c, nil
}

func (pm *MultipoolerManager) GetSourceConnection(ctx context.Context, r *rpc.GetSourceConnectionRequest) (*rpc.GetSourceConnectionResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	lockCtx, err := pm.actionLock.Acquire(ctx, "GetSourceConnection")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(r.GetDatabase()); err != nil {
		return nil, err
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return nil, err
	}
	record, err := c.Confirmed(lockCtx, r.GetConnectionName())
	if err != nil {
		return nil, err
	}
	return &rpc.GetSourceConnectionResponse{Connection: record.Configuration, ConfigurationBinding: record.Binding, AuthorityShardKey: proto.Clone(pm.record.ShardKey()).(*pb.ShardKey)}, nil
}

func (pm *MultipoolerManager) CreateSourceConnection(ctx context.Context, r *rpc.CreateSourceConnectionRequest) (*rpc.CreateSourceConnectionResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	if r.GetConnection().GetExpectedSystemIdentifier() == "" {
		return nil, mterrors.New(code.Code_INVALID_ARGUMENT, "expected source physical identity is required")
	}
	lockCtx, err := pm.actionLock.Acquire(ctx, "CreateSourceConnection")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(r.GetDatabase()); err != nil {
		return nil, err
	}
	if err = pm.authorizeServingAdmin(lockCtx, r.GetUsername(), r.GetUserAuth()); err != nil {
		return nil, err
	}
	c, err := pm.connectionCatalogLocked(lockCtx)
	if err != nil {
		return nil, err
	}
	var record *connectioncatalog.Record
	err = c.Transaction(lockCtx, func(ctx context.Context, tx executor.InternalTx) error {
		current, err := c.ReadTx(ctx, tx, r.GetConnection().GetName())
		if err == nil {
			if !proto.Equal(current.Configuration, r.Connection) {
				return errors.New("source connection is immutable")
			}
			record = current
			_, err = tx.QueryArgs(ctx, `UPDATE multigres.connections SET binding=binding WHERE name=$1`, r.Connection.Name)
			return err
		}
		if !errors.Is(err, connectioncatalog.ErrNotFound) {
			return err
		}
		record, err = c.Create(ctx, tx, r.Connection)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &rpc.CreateSourceConnectionResponse{ConfigurationBinding: record.Binding}, nil
}
