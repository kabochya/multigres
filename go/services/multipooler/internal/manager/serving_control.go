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
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	"github.com/multigres/multigres/go/pb/query"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/multigres/multigres/go/common/mterrors"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	code "github.com/multigres/multigres/go/pb/mtrpc"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
)

// Control calls additionally require the cluster key proof. Deployments must use
// the existing authenticated TLS transport; the local demo binds loopback only.
func (pm *MultipoolerManager) authorizeControl(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return mterrors.New(code.Code_PERMISSION_DENIED, "protected control transport required")
	}
	if _, tls := p.AuthInfo.(credentials.TLSInfo); !tls {
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

func (pm *MultipoolerManager) catalogLocked(ctx context.Context) (*servingcontrol.Catalog, error) {
	if pm.servingCatalog != nil {
		return pm.servingCatalog, nil
	}
	c, err := servingcontrol.New(pm.qsc.InternalQueryService(), pm.config.MigrationKey)
	if err != nil {
		return nil, err
	}
	if err = c.Initialize(ctx); err != nil {
		return nil, err
	}
	pm.servingCatalog = c
	return c, nil
}

// Caller owns servingMu. The action lock pins local leadership while confirming
// durability; remote drains never run under either this lock or the transaction.
func (pm *MultipoolerManager) confirmedRoutingLocked(ctx context.Context) (*pb.MigrationRouting, error) {
	lockCtx, err := pm.actionLock.Acquire(ctx, "ConfirmServingCatalog")
	if err != nil {
		return nil, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(pm.record.ShardKey().Database); err != nil {
		return nil, err
	}
	c, err := pm.catalogLocked(ctx)
	if err != nil {
		return nil, err
	}
	state, err := c.ConfirmedRouting(ctx)
	if err != nil {
		pm.invalidateServingPublication()
		return nil, err
	}
	pm.publishServingSnapshot(lockCtx, state)
	return state, nil
}

func (pm *MultipoolerManager) GetMigrationMode(ctx context.Context, r *rpc.GetMigrationModeRequest) (*rpc.GetMigrationModeResponse, error) {
	if err := pm.servingAuthority(r.Database); err != nil {
		return nil, err
	}
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	if len(pm.config.MigrationKey) == 0 {
		return &rpc.GetMigrationModeResponse{Routing: &pb.MigrationRouting{}}, nil
	}
	state, err := pm.confirmedRoutingLocked(ctx)
	return &rpc.GetMigrationModeResponse{Routing: state}, err
}

func (pm *MultipoolerManager) GetSourceConnection(ctx context.Context, r *rpc.GetSourceConnectionRequest) (*rpc.GetSourceConnectionResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	if err := pm.servingAuthority(r.Database); err != nil {
		return nil, err
	}
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	c, err := pm.catalogLocked(ctx)
	if err != nil {
		return nil, err
	}
	v, err := c.Connection(ctx, r.ConnectionName)
	if err != nil {
		return nil, err
	}
	state, err := pm.confirmedRoutingLocked(ctx)
	return &rpc.GetSourceConnectionResponse{Connection: v, Routing: state}, err
}

// AuthorizeServingAdminLookup permits credential lookup while the application
// gate is closed, only over protected control transport for a target superuser.
func (pm *MultipoolerManager) AuthorizeServingAdminLookup(ctx context.Context, database, username string) error {
	if err := pm.authorizeControl(ctx); err != nil {
		return err
	}
	if err := pm.servingAuthority(database); err != nil {
		return err
	}
	_, err := pm.targetAdminVerifier(ctx, username)
	return err
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

func (pm *MultipoolerManager) ServingControl(ctx context.Context, r *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	if err := pm.servingAuthority(r.Database); err != nil {
		return nil, err
	}
	if err := pm.authorizeServingAdmin(ctx, r.Username, r.UserAuth); err != nil {
		return nil, err
	}
	if r.Operation == "status" || r.Operation == "wait" {
		var status *servingcontrol.OperationStatus
		var err error
		if r.Operation == "wait" {
			status, err = pm.WaitServingOperation(ctx, r.RequestId)
		} else {
			status, err = pm.GetServingOperation(ctx, r.RequestId)
		}
		if err != nil {
			return nil, err
		}
		return &rpc.ServingControlResponse{OperationStatus: &rpc.ServingOperationStatus{RequestId: status.RequestID, Operation: status.Operation, Completed: status.Completed, Active: status.Active}}, nil
	}
	if response, handled, err := pm.demoServingControl(ctx, r); handled {
		return response, err
	}
	// Only operation/request identifiers enter the audit record.
	started := time.Now()
	defer func() {
		pm.logger.InfoContext(ctx, "serving control request", "operation", r.Operation, "request_id", r.RequestId, "duration", time.Since(started))
	}()
	if r.Operation != "show" {
		if !pm.transitionMu.TryLock() {
			return nil, mterrors.New(code.Code_FAILED_PRECONDITION, "serving transition already running; recover by request ID")
		}
		defer pm.transitionMu.Unlock()
	}
	switch r.Operation {
	case "attach", "pause", "resume", "detach":
		return pm.adminServingTransition(ctx, r)
	}
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	var err error
	switch r.Operation {
	case "show":
		state, err := pm.confirmedRoutingLocked(ctx)
		return &rpc.ServingControlResponse{Routing: state}, err
	case "create", "alter":
		_, err = pm.updateRoutingLocked(ctx, true, func(c *servingcontrol.Catalog, tx executor.InternalTx, state *pb.MigrationRouting) error {
			if state.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED && !state.ResumeSourceAllowed {
				return errors.New("controller owns the fenced transition")
			}
			done, err := c.ReserveRequest(ctx, tx, r.RequestId, pm.requestHash(r), r.Operation, state)
			if err != nil || done {
				return err
			}
			if state.SourceConnection == r.Connection.GetName() && state.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED {
				return errors.New("attached connection changes require FENCED mode")
			}
			if err := pm.servingAuthority(r.Database); err != nil {
				return err
			}
			if err := c.Put(ctx, tx, r.Connection, r.Operation == "alter"); err != nil {
				return err
			}
			return c.CompleteRequest(ctx, tx, r.RequestId)
		})
	default:
		return nil, mterrors.New(code.Code_UNIMPLEMENTED, "serving transition not implemented")
	}
	if err != nil {
		return nil, err
	}
	state, err := pm.confirmedRoutingLocked(ctx)
	return &rpc.ServingControlResponse{Routing: state}, err
}

func (pm *MultipoolerManager) RefreshRouting(ctx context.Context, r *rpc.RefreshRoutingRequest) (*rpc.RefreshRoutingResponse, error) {
	if err := pm.authorizeControl(ctx); err != nil {
		return nil, err
	}
	if r.Database != pm.record.ShardKey().Database || r.OperationId == "" || r.ExpectedMode < 0 || r.ExpectedMode > 3 {
		return nil, mterrors.New(code.Code_INVALID_ARGUMENT, "database, operation ID and valid expected mode required")
	}
	return pm.enforceRouting(ctx, &pb.MigrationRouting{ActiveRequestId: r.OperationId, Mode: r.ExpectedMode})
}
