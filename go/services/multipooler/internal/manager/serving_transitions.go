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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
)

// JournalWrite joins a controller decision to the mode transaction. It must not
// call remote services or commit independently. Replication barriers are owned
// by the controller; this package implements only the serving protocol.
type JournalWrite func(context.Context, executor.InternalTx) error

// servingPeerOperations keeps remote enforcement separate from catalog transactions.
type servingPeerOperations interface {
	Prepare(context.Context, string) (*pb.ExternalBackendIdentity, error)
	Enforce(context.Context, *pb.MigrationRouting) error
}

func (pm *MultipoolerManager) requestCompleted(ctx context.Context, r *rpc.ServingControlRequest) (bool, error) {
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	lockCtx, err := pm.actionLock.Acquire(ctx, "ConfirmServingRequest")
	if err != nil {
		return false, err
	}
	defer pm.actionLock.Release(lockCtx)
	if err = pm.servingAuthority(pm.record.ShardKey().Database); err != nil {
		return false, err
	}
	c, err := pm.catalogLocked(ctx)
	if err != nil {
		return false, err
	}
	return c.RequestCompleted(ctx, r.RequestId, pm.requestHash(r))
}

func (pm *MultipoolerManager) requestHash(r *rpc.ServingControlRequest) string {
	request := proto.Clone(r).(*rpc.ServingControlRequest)
	request.UserAuth = nil // Authorization is rechecked independently on every retry.
	data, _ := proto.Marshal(request)
	mac := hmac.New(sha256.New, pm.config.MigrationKey)
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func (pm *MultipoolerManager) updateRouting(ctx context.Context, fn func(*servingcontrol.Catalog, executor.InternalTx, *pb.MigrationRouting) error) error {
	_, err := pm.commitRouting(ctx, fn)
	return err
}

func (pm *MultipoolerManager) commitRouting(ctx context.Context, fn func(*servingcontrol.Catalog, executor.InternalTx, *pb.MigrationRouting) error) (*pb.MigrationRouting, error) {
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	return pm.updateRoutingLocked(ctx, true, fn)
}

// Caller holds servingMu. Capture the row protected by this transaction, then
// publish only after its synchronous commit succeeds, while leadership is pinned.
func (pm *MultipoolerManager) updateRoutingLocked(ctx context.Context, changed bool, fn func(*servingcontrol.Catalog, executor.InternalTx, *pb.MigrationRouting) error) (*pb.MigrationRouting, error) {
	lockCtx, err := pm.actionLock.Acquire(ctx, "ServingControlCatalog")
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
	var snapshot *pb.MigrationRouting
	callbackFailed := false
	err = c.Update(ctx, func(tx executor.InternalTx, state *pb.MigrationRouting) error {
		if err := fn(c, tx, state); err != nil {
			callbackFailed = true
			return err
		}
		snapshot = proto.Clone(state).(*pb.MigrationRouting)
		return nil
	})
	if err != nil {
		if !callbackFailed {
			pm.invalidateServingPublication()
			if changed {
				pm.notifyServingOperation()
				pm.signalServingRecovery()
			}
		}
		return nil, err
	}
	pm.publishServingSnapshot(lockCtx, snapshot)
	if changed {
		pm.notifyServingOperation()
	}
	return snapshot, nil
}

func (pm *MultipoolerManager) routingSnapshot(ctx context.Context) (*pb.MigrationRouting, error) {
	r, err := pm.GetMigrationMode(ctx, &rpc.GetMigrationModeRequest{Database: pm.record.ShardKey().Database})
	if err != nil {
		return nil, err
	}
	return r.Routing, nil
}

func (pm *MultipoolerManager) sourcePoolers(ctx context.Context, name string) ([]*pb.Multipooler, error) {
	all, err := migrationcontrol.Poolers(ctx, pm.config.TopoClient, pm.record.ShardKey().Database)
	if err != nil {
		return nil, err
	}
	var sources []*pb.Multipooler
	for _, p := range all {
		if p.ManagementMode != pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
			continue
		}
		if p.GetLifecycleStatus().GetStatus() == pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
			continue
		}
		if p.SourceConnection != name {
			return nil, errors.New("source endpoint belongs to a different or standalone connection")
		}
		sources = append(sources, p)
	}
	if len(sources) == 0 {
		return nil, errors.New("no registered source poolers")
	}
	return sources, nil
}

func (pm *MultipoolerManager) preparedSource(ctx context.Context, name string) (*pb.ExternalBackendIdentity, error) {
	if pm.servingPeers != nil {
		return pm.servingPeers.Prepare(ctx, name)
	}
	pm.servingMu.Lock()
	c, err := pm.catalogLocked(ctx)
	var connection *rpc.SourceConnection
	if err == nil {
		connection, err = c.Connection(ctx, name)
	}
	pm.servingMu.Unlock()
	if err != nil {
		return nil, err
	}
	sources, err := pm.sourcePoolers(ctx, name)
	if err != nil {
		return nil, err
	}
	var identity *pb.ExternalBackendIdentity
	for _, p := range sources {
		conn, err := migrationcontrol.Dial(p, pm.config.ControlTransport)
		if err != nil {
			return nil, err
		}
		readCtx, cancel := context.WithCancel(ctx)
		stream, err := rpc.NewMultipoolerServiceClient(conn).StreamPoolerHealth(readCtx, &rpc.StreamPoolerHealthRequest{})
		var health *rpc.StreamPoolerHealthResponse
		if err == nil {
			health, err = stream.Recv()
		}
		cancel()
		_ = conn.Close()
		if err != nil {
			return nil, fmt.Errorf("source %s readiness unavailable: %w", topoclient.ComponentIDString(p.Id), err)
		}
		actual := health.GetBackendIdentity()
		if !health.GetBackendReady() || actual.GetSystemIdentifier() == "" || actual.GetDatabase() != connection.Database {
			return nil, errors.New("source backend is not prepared or has incompatible identity")
		}
		if identity == nil {
			identity = actual
		} else if !proto.Equal(identity, actual) {
			return nil, errors.New("source poolers point to different physical databases")
		}
	}
	return identity, nil
}

// The protected snapshot is confirmed before dispatch. Every acknowledgment
// binds both the durable operation and the addressed process incarnation.
func (pm *MultipoolerManager) refreshDestinations(ctx context.Context, state *pb.MigrationRouting) error {
	if state.ActiveRequestId == "" {
		return errors.New("durable operation required")
	}
	var peers []*pb.Multipooler
	var err error
	if pm.servingPeers == nil {
		peers, err = pm.confirmRoutingMembership(ctx, state)
		if err != nil {
			return err
		}
	}
	if _, err = pm.enforceRouting(ctx, state); err != nil {
		return err
	}
	if pm.servingPeers != nil {
		return pm.servingPeers.Enforce(ctx, state)
	}
	sources := 0
	for _, p := range peers {
		if p.GetLifecycleStatus().GetStatus() == pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
			continue
		}
		if p.ManagementMode == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
			if state.SourceConnection != "" && p.SourceConnection != state.SourceConnection {
				return errors.New("source endpoint belongs to a different or standalone connection")
			}
			sources++
		}
		if proto.Equal(p.Id, pm.record.desired.Load().Id) {
			continue
		}
		conn, err := migrationcontrol.Dial(p, pm.config.ControlTransport)
		if err != nil {
			return err
		}
		reply, err := rpc.NewMultipoolerServiceClient(conn).RefreshRouting(migrationcontrol.AuthorizedContext(ctx, pm.config.MigrationKey), &rpc.RefreshRoutingRequest{Database: pm.record.ShardKey().Database, OperationId: state.ActiveRequestId, ExpectedMode: state.Mode})
		_ = conn.Close()
		if err != nil {
			return fmt.Errorf("process %s enforcement incomplete: %w", topoclient.ComponentIDString(p.Id), err)
		}
		if reply == nil || reply.OperationId != state.ActiveRequestId || reply.Mode != state.Mode || !proto.Equal(reply.ProcessId, p.Id) || reply.AdmissionOpen != routingAdmission(state, p.ManagementMode, p.SourceConnection) {
			return fmt.Errorf("process %s did not acknowledge the expected operation and incarnation", topoclient.ComponentIDString(p.Id))
		}
	}
	if state.SourceConnection != "" && sources == 0 {
		return errors.New("no registered source poolers")
	}
	return nil
}

func (pm *MultipoolerManager) finishServingRequest(ctx context.Context, id string, mode pb.MigrationMode, migrationID string) error {
	return pm.updateRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, state *pb.MigrationRouting) error {
		if err := c.RequireMigration(ctx, tx, migrationID); err != nil {
			return err
		}
		if state.ActiveRequestId != id || state.Mode != mode {
			return errors.New("serving operation superseded")
		}
		if state.Mode == pb.MigrationMode_MIGRATION_MODE_UNSET && state.SourceConnection == "" {
			if err := c.SetMigrationIdentity(ctx, tx, ""); err != nil {
				return err
			}
		}
		return c.CompleteRequest(ctx, tx, id)
	})
}

func (pm *MultipoolerManager) adminServingTransition(ctx context.Context, r *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error) {
	doneBefore, readErr := pm.requestCompleted(ctx, r)
	if readErr != nil {
		return nil, readErr
	}
	if doneBefore {
		state, err := pm.routingSnapshot(ctx)
		return &rpc.ServingControlResponse{Routing: state}, err
	}
	if r.Operation == "attach" {
		return pm.attachServing(ctx, r)
	}
	var identity *pb.ExternalBackendIdentity
	var err error
	if r.Operation == "resume" {
		state, readErr := pm.routingSnapshot(ctx)
		if readErr != nil {
			return nil, readErr
		}
		identity, err = pm.preparedSource(ctx, state.SourceConnection)
		if err != nil {
			return nil, err
		}
		if !proto.Equal(identity, state.SourceIdentity) {
			return nil, errors.New("source identity changed while fenced")
		}
	}
	migrationID, err := pm.MigrationIdentity(ctx)
	if err != nil {
		return nil, err
	}
	var done bool
	state, err := pm.commitRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, state *pb.MigrationRouting) error {
		var reserveErr error
		done, reserveErr = c.ReserveRequest(ctx, tx, r.RequestId, pm.requestHash(r), r.Operation, state)
		if reserveErr != nil || done {
			return reserveErr
		}
		switch r.Operation {
		case "pause":
			if state.Mode != pb.MigrationMode_MIGRATION_MODE_UNMANAGED && state.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED {
				return errors.New("pause requires source serving")
			}
			if state.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED && !state.ResumeSourceAllowed {
				return errors.New("controller owns the fenced transition")
			}
			state.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
			state.ResumeSourceAllowed = true
		case "resume":
			if !state.ResumeSourceAllowed || state.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED && state.Mode != pb.MigrationMode_MIGRATION_MODE_UNMANAGED {
				return errors.New("admin resume requires an admin-owned source fence")
			}
			state.Mode = pb.MigrationMode_MIGRATION_MODE_UNMANAGED
		case "detach":
			if r.ConnectionName != "" && r.ConnectionName != state.SourceConnection {
				return errors.New("detach connection does not match current attachment")
			}
			if !state.MigrationCompleted || state.Mode != pb.MigrationMode_MIGRATION_MODE_MANAGED {
				return errors.New("detach requires completed migration")
			}
			state.Mode = pb.MigrationMode_MIGRATION_MODE_UNSET
			state.SourceConnection = ""
			state.SourceIdentity = nil
			state.ResumeSourceAllowed = false
		default:
			return errors.New("unsupported serving operation")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !done {
		if err = pm.refreshDestinations(ctx, state); err != nil {
			return nil, err
		}
		if err = pm.finishServingRequest(ctx, r.RequestId, state.Mode, migrationID); err != nil {
			return nil, err
		}
	}
	return &rpc.ServingControlResponse{Routing: state}, nil
}

// ControllerTransition is deliberately an in-process interface, never an admin
// RPC. The controller supplies its journal write after its replication barrier.
// An interrupted transition can be repeated with the same request ID.
func (pm *MultipoolerManager) ControllerTransition(ctx context.Context, migrationID, id, fenceID string, next pb.MigrationMode, journal JournalWrite) error {
	if journal == nil {
		return errors.New("controller journal callback required")
	}
	if !pm.transitionMu.TryLock() {
		return errors.New("serving transition already running; recover by request ID")
	}
	defer pm.transitionMu.Unlock()
	r := pm.controllerRequest(migrationID, id, fenceID, "controller:"+next.String())
	alreadyDone, err := pm.requestCompleted(ctx, r)
	if err != nil || alreadyDone {
		return err
	}
	if err = pm.requireMigration(ctx, migrationID); err != nil {
		return err
	}
	state, err := pm.routingSnapshot(ctx)
	if err != nil {
		return err
	}
	if state.SourceConnection == "" {
		return errors.New("source attachment required")
	}
	if next != pb.MigrationMode_MIGRATION_MODE_FENCED && state.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED && !(state.Mode == next && state.ActiveRequestId == id) {
		return errors.New("destination activation requires FENCED")
	}
	if next != pb.MigrationMode_MIGRATION_MODE_FENCED && state.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED {
		if fenceID == "" || state.ActiveRequestId != fenceID || state.ResumeSourceAllowed {
			return errors.New("activation requires the owning controller fence request ID")
		}
		fence := pm.controllerRequest(migrationID, fenceID, "", "controller:"+pb.MigrationMode_MIGRATION_MODE_FENCED.String())
		complete, err := pm.requestCompleted(ctx, fence)
		if err != nil {
			return err
		}
		if !complete {
			return errors.New("owning fence is incomplete; recover by request ID")
		}

		if err = pm.refreshDestinations(ctx, state); err != nil {
			return err
		}
	}
	var done bool
	state, err = pm.commitRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, s *pb.MigrationRouting) error {
		if err := c.RequireMigration(ctx, tx, migrationID); err != nil {
			return err
		}
		replaying := s.ActiveRequestId == id
		var err error
		done, err = c.ReserveRequest(ctx, tx, id, pm.requestHash(r), r.Operation, s)
		if err != nil || done {
			return err
		}
		if s.Mode == next && replaying {
			return nil
		}
		if s.MigrationCompleted {
			return errors.New("migration is already completed")
		}
		switch next {
		case pb.MigrationMode_MIGRATION_MODE_FENCED:
			if s.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED && !s.ResumeSourceAllowed && !replaying {
				return errors.New("controller owns the fenced transition")
			}
			s.ResumeSourceAllowed = false
		case pb.MigrationMode_MIGRATION_MODE_MANAGED, pb.MigrationMode_MIGRATION_MODE_UNMANAGED:
			if s.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED {
				return errors.New("destination activation requires FENCED")
			}
		default:
			return errors.New("unsupported controller destination")
		}
		if err = journal(ctx, tx); err != nil {
			return err
		}
		s.Mode = next
		return nil
	})
	if err != nil || done {
		return err
	}
	if err = pm.refreshDestinations(ctx, state); err != nil {
		return err
	}
	return pm.finishServingRequest(ctx, id, next, migrationID)
}

func (pm *MultipoolerManager) CompleteMigration(ctx context.Context, migrationID, id string, journal JournalWrite) error {
	if journal == nil {
		return errors.New("controller journal callback required")
	}
	if !pm.transitionMu.TryLock() {
		return errors.New("serving transition already running; recover by request ID")
	}
	defer pm.transitionMu.Unlock()
	r := pm.controllerRequest(migrationID, id, "", "controller:complete")
	if done, err := pm.requestCompleted(ctx, r); err != nil || done {
		return err
	}
	return pm.updateRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, s *pb.MigrationRouting) error {
		if err := c.RequireMigration(ctx, tx, migrationID); err != nil {
			return err
		}
		done, err := c.ReserveRequest(ctx, tx, id, pm.requestHash(r), r.Operation, s)
		if err != nil || done {
			return err
		}
		if s.Mode != pb.MigrationMode_MIGRATION_MODE_MANAGED {
			return errors.New("completion requires MANAGED")
		}
		if err = journal(ctx, tx); err != nil {
			return err
		}
		s.MigrationCompleted = true
		return c.CompleteRequest(ctx, tx, id)
	})
}

// GetServingOperation is the controller's thin status API over serving_requests.
// It confirms durability even for completed requests and after pooler restarts.
func (pm *MultipoolerManager) GetServingOperation(ctx context.Context, id string) (*servingcontrol.OperationStatus, error) {
	var status *servingcontrol.OperationStatus
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	_, err := pm.updateRoutingLocked(ctx, false, func(c *servingcontrol.Catalog, tx executor.InternalTx, state *pb.MigrationRouting) error {
		var err error
		status, err = c.Operation(ctx, tx, state, id)
		if errors.Is(err, servingcontrol.ErrOperationNotFound) {
			fence, readErr := c.Operation(ctx, tx, state, id+"/fence")
			if readErr == nil && fence.Operation == "attach:fence" {
				status = &servingcontrol.OperationStatus{RequestID: id, Operation: "attach", Completed: false, Active: fence.Active}
				return nil
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return status, nil
}

// WaitServingOperation does not execute, cancel, or undo the operation. Recover
// an interrupted operation separately with the same request ID and arguments.
func (pm *MultipoolerManager) WaitServingOperation(ctx context.Context, id string) (*servingcontrol.OperationStatus, error) {
	runDone := pm.servingRunDone()
	for {
		// Subscribe before inspecting: a commit between inspection and select
		// closes this channel, including when several waiters share it.
		changed := pm.servingOperationChange()
		status, err := pm.GetServingOperation(ctx, id)
		if err != nil {
			return nil, err
		}
		if status.Completed {
			return status, nil
		}
		if !status.Active {
			return status, errors.New("serving operation superseded")
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-pm.servingShutdown():
			return status, errors.New("serving control stopped")
		case <-runDone:
			return status, errors.New("serving control stopped")
		case <-changed:
		}
	}
}

func (pm *MultipoolerManager) controllerRequest(migrationID, id, fenceID, operation string) *rpc.ServingControlRequest {
	// IDs are arguments, not credentials or ordering tokens.
	arguments, _ := json.Marshal([]string{migrationID, fenceID})
	return &rpc.ServingControlRequest{Database: pm.record.ShardKey().Database, RequestId: id, Operation: operation, ConnectionName: string(arguments)}
}

func (pm *MultipoolerManager) requireMigration(ctx context.Context, id string) error {
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	_, err := pm.updateRoutingLocked(ctx, false, func(c *servingcontrol.Catalog, tx executor.InternalTx, _ *pb.MigrationRouting) error {
		return c.RequireMigration(ctx, tx, id)
	})
	return err
}

// The migrator obtains/recover this identity from its successful attach request.
// Reading it here is for bootstrap/status, and still confirms the durable row.
func (pm *MultipoolerManager) MigrationIdentity(ctx context.Context) (string, error) {
	var id string
	pm.servingMu.Lock()
	defer pm.servingMu.Unlock()
	_, err := pm.updateRoutingLocked(ctx, false, func(c *servingcontrol.Catalog, tx executor.InternalTx, _ *pb.MigrationRouting) error {
		var err error
		id, err = c.MigrationIdentity(ctx, tx)
		return err
	})
	return id, err
}

// Discovery is not shutdown proof. Retain process IDs under the catalog lock
// before remote enforcement, and reject missing required records on recovery.
func (pm *MultipoolerManager) confirmRoutingMembership(ctx context.Context, expected *pb.MigrationRouting) ([]*pb.Multipooler, error) {
	peers, err := migrationcontrol.Poolers(ctx, pm.config.TopoClient, pm.record.ShardKey().Database)
	if err != nil {
		return nil, err
	}
	current := map[string]*pb.Multipooler{}
	ids := make([]*pb.ID, 0, len(peers))
	for _, p := range peers {
		current[string(topoclient.ComponentIDString(p.Id))] = p
		if p.GetLifecycleStatus().GetStatus() != pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
			ids = append(ids, p.Id)
		}
	}
	var required []*pb.ID
	err = pm.updateRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, s *pb.MigrationRouting) error {
		if s.Mode != expected.Mode || s.ActiveRequestId != expected.ActiveRequestId {
			return errors.New("routing membership requires the expected durable intent")
		}
		var err error
		required, err = c.RequiredPoolers(ctx, tx, ids)
		return err
	})
	if err != nil {
		return nil, err
	}
	resolved := make([]*pb.Multipooler, 0, len(required))
	for _, id := range required {
		process := current[string(topoclient.ComponentIDString(id))]
		if process == nil {
			return nil, fmt.Errorf("required process %s disappeared without shutdown proof", topoclient.ComponentIDString(id))
		}
		resolved = append(resolved, process)
	}
	return resolved, nil
}

// Attach completes a close/drain operation before activating its source under
// another stable ID. IDs support retry correlation, not ordered versions.
func (pm *MultipoolerManager) attachServing(ctx context.Context, r *rpc.ServingControlRequest) (*rpc.ServingControlResponse, error) {
	identity, err := pm.preparedSource(ctx, r.ConnectionName)
	if err != nil {
		return nil, err
	}
	fenceID := r.RequestId + "/fence"
	fenceRequest := proto.Clone(r).(*rpc.ServingControlRequest)
	fenceRequest.RequestId = fenceID
	state, err := pm.commitRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, state *pb.MigrationRouting) error {
		if state.Mode != pb.MigrationMode_MIGRATION_MODE_UNSET {
			if err := c.RequireMigration(ctx, tx, r.RequestId); err != nil {
				return err
			}
			if state.SourceConnection != r.ConnectionName || !proto.Equal(state.SourceIdentity, identity) || !state.ResumeSourceAllowed ||
				!(state.ActiveRequestId == fenceID && state.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED || state.ActiveRequestId == r.RequestId && state.Mode == pb.MigrationMode_MIGRATION_MODE_UNMANAGED) {
				return errors.New("database already has a source attachment")
			}
			return nil
		}
		if _, err := c.ReserveRequest(ctx, tx, fenceID, pm.requestHash(fenceRequest), "attach:fence", state); err != nil {
			return err
		}
		if err := c.SetMigrationIdentity(ctx, tx, r.RequestId); err != nil {
			return err
		}
		state.SourceConnection = r.ConnectionName
		state.SourceIdentity = identity
		state.ResumeSourceAllowed = true
		state.MigrationCompleted = false
		state.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
		return nil
	})
	if err != nil {
		return nil, err
	}
	if state.Mode == pb.MigrationMode_MIGRATION_MODE_FENCED {
		if err = pm.refreshDestinations(ctx, state); err != nil {
			return nil, err
		}
		if err = pm.finishServingRequest(ctx, fenceID, pb.MigrationMode_MIGRATION_MODE_FENCED, r.RequestId); err != nil {
			return nil, err
		}
		state, err = pm.commitRouting(ctx, func(c *servingcontrol.Catalog, tx executor.InternalTx, s *pb.MigrationRouting) error {
			if err := c.RequireMigration(ctx, tx, r.RequestId); err != nil {
				return err
			}
			if s.ActiveRequestId != fenceID || s.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED {
				return errors.New("attachment fence superseded")
			}
			if _, err := c.ReserveRequest(ctx, tx, r.RequestId, pm.requestHash(r), r.Operation, s); err != nil {
				return err
			}
			s.Mode = pb.MigrationMode_MIGRATION_MODE_UNMANAGED
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if err = pm.refreshDestinations(ctx, state); err != nil {
		return nil, err
	}
	if err = pm.finishServingRequest(ctx, r.RequestId, state.Mode, r.RequestId); err != nil {
		return nil, err
	}
	return &rpc.ServingControlResponse{Routing: state}, nil
}
