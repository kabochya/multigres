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
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	qmock "github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingcontrol"
)

type (
	requestRecord struct {
		hash      string
		operation string
		done      bool
	}
	transitionQueries struct {
		executor.InternalQueryService
		mu             sync.Mutex
		state          *pb.MigrationRouting
		requests       map[string]requestRecord
		journal        []string
		migrationID    string
		required       []*pb.ID
		failNextCommit error
		beginErr       error
		begins         int
		afterCommit    func()
	}
)

func routingResult(s *pb.MigrationRouting) *sqltypes.Result {
	return qmock.MakeQueryResult([]string{"mode", "connection", "sysid", "database", "completed", "resume", "request"}, [][]any{{int32(s.Mode), s.SourceConnection, s.GetSourceIdentity().GetSystemIdentifier(), s.GetSourceIdentity().GetDatabase(), s.MigrationCompleted, s.ResumeSourceAllowed, s.ActiveRequestId}})
}

func requestResult(requests map[string]requestRecord, id string) *sqltypes.Result {
	r, ok := requests[id]
	if !ok {
		return &sqltypes.Result{}
	}
	return qmock.MakeQueryResult([]string{"hash", "completed"}, [][]any{{r.hash, r.done}})
}

func (q *transitionQueries) QueryAdmin(context.Context, string) (*sqltypes.Result, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return routingResult(q.state), nil
}

func (q *transitionQueries) QueryAdminArgs(_ context.Context, _ string, args ...any) (*sqltypes.Result, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return requestResult(q.requests, args[0].(string)), nil
}

func (q *transitionQueries) BeginAdmin(context.Context) (executor.InternalTx, error) {
	q.mu.Lock()
	q.begins++
	if q.beginErr != nil {
		q.mu.Unlock()
		return nil, q.beginErr
	}
	return &transitionTx{q: q, state: proto.Clone(q.state).(*pb.MigrationRouting), requests: maps.Clone(q.requests), migrationID: q.migrationID, required: append([]*pb.ID(nil), q.required...), journal: append([]string(nil), q.journal...)}, nil
}

type transitionTx struct {
	q           *transitionQueries
	state       *pb.MigrationRouting
	requests    map[string]requestRecord
	journal     []string
	migrationID string
	required    []*pb.ID
	finished    bool
}

func (tx *transitionTx) Query(_ context.Context, query string) (*sqltypes.Result, error) {
	if strings.HasPrefix(query, "SELECT required_poolers") {
		data, _ := json.Marshal(tx.required)
		if len(tx.required) == 0 {
			data = []byte("[]")
		}
		return qmock.MakeQueryResult([]string{"required_poolers"}, [][]any{{string(data)}}), nil
	}
	if strings.HasPrefix(query, "SELECT migration_id") {
		return qmock.MakeQueryResult([]string{"migration_id"}, [][]any{{tx.migrationID}}), nil
	}
	if strings.Contains(query, "migration_routing") {
		return routingResult(tx.state), nil
	}
	if strings.HasPrefix(query, "journal:") {
		tx.journal = append(tx.journal, query)
	}
	return &sqltypes.Result{}, nil
}

func (tx *transitionTx) QueryArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	switch {
	case strings.HasPrefix(query, "SELECT operation"):
		r, ok := tx.requests[args[0].(string)]
		if !ok {
			return &sqltypes.Result{}, nil
		}
		return qmock.MakeQueryResult([]string{"operation", "completed"}, [][]any{{r.operation, r.done}}), nil
	case strings.HasPrefix(query, "SELECT completed"):
		r, ok := tx.requests[args[0].(string)]
		if !ok {
			return &sqltypes.Result{}, nil
		}
		return qmock.MakeQueryResult([]string{"completed"}, [][]any{{r.done}}), nil
	case strings.HasPrefix(query, "SELECT request_hash"):
		return requestResult(tx.requests, args[0].(string)), nil
	case strings.HasPrefix(query, "INSERT INTO multigres.serving_requests"):
		tx.requests[args[0].(string)] = requestRecord{hash: args[1].(string), operation: args[2].(string)}
	case strings.HasPrefix(query, "UPDATE multigres.serving_requests"):
		id := args[0].(string)
		r := tx.requests[id]
		r.done = true
		tx.requests[id] = r
	case strings.HasPrefix(query, "UPDATE multigres.migration_routing SET required_poolers"):
		if err := json.Unmarshal([]byte(args[0].(string)), &tx.required); err != nil {
			return nil, err
		}
	case strings.HasPrefix(query, "UPDATE multigres.migration_routing SET migration_id"):
		tx.migrationID = args[0].(string)
		if tx.migrationID == "" {
			tx.required = nil
		}
	case strings.HasPrefix(query, "UPDATE multigres.migration_routing"):
		tx.state = &pb.MigrationRouting{Mode: pb.MigrationMode(args[0].(int32)), SourceConnection: args[1].(string), SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: args[2].(string), Database: args[3].(string)}, MigrationCompleted: args[4].(bool), ResumeSourceAllowed: args[5].(bool), ActiveRequestId: args[6].(string)}
	default:
		return nil, errors.New("unexpected test query")
	}
	return &sqltypes.Result{}, nil
}

func (tx *transitionTx) Commit(context.Context) error {
	tx.q.state = tx.state
	tx.q.requests = tx.requests
	tx.q.journal = tx.journal
	tx.q.migrationID = tx.migrationID
	tx.q.required = tx.required
	tx.finished = true
	err := tx.q.failNextCommit
	tx.q.failNextCommit = nil
	callback := tx.q.afterCommit
	tx.q.afterCommit = nil
	tx.q.mu.Unlock()
	if callback != nil {
		callback()
	}
	return err
}

func (tx *transitionTx) Rollback(context.Context) error {
	if !tx.finished {
		tx.finished = true
		tx.q.mu.Unlock()
	}
	return nil
}

type testServingPeers struct {
	prepare func() (*pb.ExternalBackendIdentity, error)
	enforce func(bool) error
}

func (p testServingPeers) Prepare(context.Context, string) (*pb.ExternalBackendIdentity, error) {
	return p.prepare()
}

func (p testServingPeers) Enforce(_ context.Context, state *pb.MigrationRouting) error {
	return p.enforce(state.Mode == pb.MigrationMode_MIGRATION_MODE_UNMANAGED)
}

func newTransitionManager(t *testing.T) (*MultipoolerManager, *transitionQueries, *fakeApplicationGate) {
	q := &transitionQueries{state: &pb.MigrationRouting{}, requests: map[string]requestRecord{}}
	key := bytes.Repeat([]byte{1}, 32)
	c, err := servingcontrol.New(q, key)
	require.NoError(t, err)
	gate := &fakeApplicationGate{}
	gate.open.Store(true)
	pm := &MultipoolerManager{config: &Config{MigrationKey: key}, actionLock: actionlock.NewActionLock(), record: newRecordFromProto(&pb.Multipooler{ShardKey: &pb.ShardKey{Database: "postgres"}}), healthStreamer: newHealthStreamer(slog.Default(), nil, "default", "0"), servingCatalog: c, qsc: gate}
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}
	pm.servingPeers = testServingPeers{prepare: func() (*pb.ExternalBackendIdentity, error) {
		return &pb.ExternalBackendIdentity{SystemIdentifier: "123", Database: "postgres"}, nil
	}, enforce: func(open bool) error {
		state, err := pm.routingSnapshot(t.Context())
		require.NoError(t, err)
		if open {
			require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, state.Mode)
		} else {
			require.Contains(t, []pb.MigrationMode{pb.MigrationMode_MIGRATION_MODE_FENCED, pb.MigrationMode_MIGRATION_MODE_MANAGED, pb.MigrationMode_MIGRATION_MODE_UNSET}, state.Mode)
		}
		return nil
	}}
	return pm, q, gate
}

func TestAttachPauseResumeAndRetry(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	attach := &rpc.ServingControlRequest{Database: "postgres", Operation: "attach", ConnectionName: "source", RequestId: "attach-1"}
	_, err := pm.adminServingTransition(t.Context(), attach)
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, q.state.Mode)
	require.False(t, gate.open.Load())
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "pause", RequestId: "pause-1"})
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, q.state.Mode)
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "resume", RequestId: "resume-1"})
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, q.state.Mode)
	pm.servingPeers = testServingPeers{prepare: func() (*pb.ExternalBackendIdentity, error) {
		t.Fatal("completed retries must not repeat preparation")
		return nil, nil
	}}
	_, err = pm.adminServingTransition(t.Context(), attach)
	require.NoError(t, err)
	changed := proto.Clone(attach).(*rpc.ServingControlRequest)
	changed.ConnectionName = "other"
	_, err = pm.adminServingTransition(t.Context(), changed)
	require.ErrorContains(t, err, "different arguments")
}

func TestIncompleteAttachRemainsFencedAndCanRecover(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	peers := pm.servingPeers.(testServingPeers)
	original := peers.enforce
	peers.enforce = func(bool) error { return errors.New("source unreachable") }
	pm.servingPeers = peers
	request := &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"}
	_, err := pm.adminServingTransition(t.Context(), request)
	require.Error(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, q.state.Mode)
	require.False(t, gate.open.Load())
	require.False(t, q.requests["attach/fence"].done)
	status, statusErr := pm.GetServingOperation(t.Context(), "attach")
	require.NoError(t, statusErr)
	require.True(t, status.Active)
	require.False(t, status.Completed)

	peers.enforce = original
	pm.servingPeers = peers
	_, err = pm.adminServingTransition(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, q.state.Mode)
}

func TestControllerJournalAtomicityAndCompletion(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	_, err := pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	reject := func(ctx context.Context, tx executor.InternalTx) error {
		_, err := tx.Query(ctx, "journal:rejected")
		require.NoError(t, err)
		return errors.New("barrier unavailable")
	}
	require.Error(t, pm.ControllerTransition(t.Context(), "attach", "fence-fail", "", pb.MigrationMode_MIGRATION_MODE_FENCED, reject))
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, q.state.Mode)
	require.Empty(t, q.journal)
	journal := func(ctx context.Context, tx executor.InternalTx) error {
		_, err := tx.Query(ctx, "journal:accepted")
		return err
	}
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	require.False(t, q.state.ResumeSourceAllowed)
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "resume", RequestId: "unauthorized-resume"})
	require.Error(t, err)
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_MANAGED, q.state.Mode)
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	require.Len(t, q.journal, 2)
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "detach", RequestId: "early-detach"})
	require.Error(t, err)
	require.NoError(t, pm.CompleteMigration(t.Context(), "attach", "complete", journal))
	require.True(t, q.state.MigrationCompleted)
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "detach", RequestId: "detach"})
	require.NoError(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNSET, q.state.Mode)
	require.Empty(t, q.state.SourceConnection)
}

func TestAttachCannotStealControllerFence(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	attach := &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"}
	_, err := pm.adminServingTransition(t.Context(), attach)
	require.NoError(t, err)
	journal := func(context.Context, executor.InternalTx) error { return nil }
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "controller-fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "conflicting-attach"})
	require.Error(t, err, "a fresh attach must not reopen a migration-owned fence")
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, q.state.Mode)
	require.Equal(t, "controller-fence", q.state.ActiveRequestId)
}

func TestLostFenceAcknowledgmentRequiresSameRequestRecovery(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	_, err := pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	peers := pm.servingPeers.(testServingPeers)
	original := peers.enforce
	peers.enforce = func(open bool) error {
		require.False(t, open)
		return errors.New("fence applied but acknowledgment lost")
	}
	pm.servingPeers = peers
	journal := func(context.Context, executor.InternalTx) error { return nil }
	require.Error(t, pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	status, err := pm.GetServingOperation(t.Context(), "fence")
	require.NoError(t, err)
	require.True(t, status.Active)
	require.False(t, status.Completed)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = pm.WaitServingOperation(ctx, "fence")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, gate.open.Load())
	require.False(t, q.requests["fence"].done)
	require.Error(t, pm.ControllerTransition(t.Context(), "attach", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	require.Error(t, pm.ControllerTransition(t.Context(), "attach", "steal", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	_, err = pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "resume", RequestId: "resume"})
	require.Error(t, err)
	peers.enforce = original
	pm.servingPeers = peers
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	status, err = pm.WaitServingOperation(t.Context(), "fence")
	require.NoError(t, err)
	require.True(t, status.Completed)
	require.Error(t, pm.ControllerTransition(t.Context(), "attach", "activate", "wrong-owner", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
}

func TestCompletedRetryRequiresDurableConfirmationAfterPoolerRestart(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	attach := &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"}
	_, err := pm.adminServingTransition(t.Context(), attach)
	require.NoError(t, err)
	// A new pooler has no remembered acknowledgments. Its locally visible completed
	// journal row alone must not let a retry shortcut bypass synchronous durability.
	restarted, _, _ := newTransitionManager(t)
	restarted.servingCatalog, err = servingcontrol.New(q, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	q.failNextCommit = errors.New("confirmation acknowledgment uncertain")
	_, err = restarted.adminServingTransition(t.Context(), attach)
	require.ErrorContains(t, err, "confirmation acknowledgment uncertain")
	require.True(t, q.requests["attach"].done, "COMMIT error does not imply rollback")
	_, err = restarted.adminServingTransition(t.Context(), attach)
	require.NoError(t, err)
}

func TestUncertainCommitCannotPublishViaLocalReread(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	q.failNextCommit = errors.New("commit acknowledgment lost")
	err := pm.updateRouting(t.Context(), func(_ *servingcontrol.Catalog, _ executor.InternalTx, state *pb.MigrationRouting) error {
		state.Mode = pb.MigrationMode_MIGRATION_MODE_UNMANAGED
		state.SourceConnection = "source"
		return nil
	})
	require.Error(t, err)
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, q.state.Mode, "local visibility survives lost acknowledgment")
	q.failNextCommit = errors.New("quorum not available")
	_ = pm.recoverServingControl(t.Context())
	require.False(t, gate.open.Load())
	require.Nil(t, pm.healthStreamer.getState().MigrationRouting)
	_ = pm.recoverServingControl(t.Context())
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_UNMANAGED, pm.healthStreamer.getState().MigrationRouting.Mode)
}

func TestStatusAndWaitDoNotHoldDrainCallbackLocks(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	_, err := pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	peers := pm.servingPeers.(testServingPeers)
	original := peers.enforce
	peers.enforce = func(open bool) error {
		require.False(t, open)
		// Fence's callback into its authority must work while the operation waits.
		state, err := pm.routingSnapshot(t.Context())
		require.NoError(t, err)
		require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, state.Mode)
		close(entered)
		<-release
		return original(open)
	}
	pm.servingPeers = peers
	result := make(chan error, 1)
	go func() {
		result <- pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, func(context.Context, executor.InternalTx) error { return nil })
	}()
	<-entered
	status, err := pm.GetServingOperation(t.Context(), "fence")
	require.NoError(t, err)
	require.False(t, status.Completed)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = pm.WaitServingOperation(ctx, "fence")
	require.ErrorIs(t, err, context.Canceled)
	q.mu.Lock()
	require.Equal(t, pb.MigrationMode_MIGRATION_MODE_FENCED, q.state.Mode)
	q.mu.Unlock()
	close(release)
	require.NoError(t, <-result)
	status, err = pm.WaitServingOperation(t.Context(), "fence")
	require.NoError(t, err)
	require.True(t, status.Completed)
}

func TestDrainTimeoutRetainsJournalAndControllerFence(t *testing.T) {
	pm, q, gate := newTransitionManager(t)
	_, err := pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	gate.drainErr = context.DeadlineExceeded
	journal := func(context.Context, executor.InternalTx) error { return nil }
	require.ErrorIs(t, pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal), context.DeadlineExceeded)
	require.False(t, gate.open.Load())
	require.False(t, q.requests["fence"].done)
	require.False(t, q.state.ResumeSourceAllowed)
	require.Error(t, pm.ControllerTransition(t.Context(), "attach", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	gate.drainErr = nil
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	require.True(t, q.requests["fence"].done)
}

func TestLeadershipLossRejectsPublicationAndCompletedRetry(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	attach := &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"}
	_, err := pm.adminServingTransition(t.Context(), attach)
	require.NoError(t, err)
	pm.healthStreamer.routingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_REPLICA}
	q.failNextCommit = errors.New("must not attempt confirmation as a former leader")
	_, err = pm.routingSnapshot(t.Context())
	require.Error(t, err)
	_, err = pm.adminServingTransition(t.Context(), attach)
	require.Error(t, err)
	_, err = pm.GetServingOperation(t.Context(), "attach")
	require.Error(t, err)
	require.NotNil(t, q.failNextCommit)
}

func TestDurableMigrationOwnerSurvivesControllerRestart(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	_, err := pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	require.Equal(t, "attach", q.migrationID)
	journal := func(context.Context, executor.InternalTx) error { return nil }
	require.ErrorContains(t, pm.ControllerTransition(t.Context(), "other-migration", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal), "active owner")
	require.NoError(t, pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal))
	restarted, _, _ := newTransitionManager(t)
	restarted.servingCatalog = pm.servingCatalog
	identity, err := restarted.MigrationIdentity(t.Context())
	require.NoError(t, err)
	require.Equal(t, "attach", identity)
	require.Error(t, restarted.ControllerTransition(t.Context(), "other-migration", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	require.NoError(t, restarted.ControllerTransition(t.Context(), "attach", "activate", "fence", pb.MigrationMode_MIGRATION_MODE_MANAGED, journal))
	require.Equal(t, "attach", q.migrationID, "activation does not release migration ownership")
	require.Error(t, restarted.CompleteMigration(t.Context(), "other-migration", "complete", journal))
	require.NoError(t, restarted.CompleteMigration(t.Context(), "attach", "complete", journal))
	require.Equal(t, "attach", q.migrationID, "completion alone does not release ownership")
	_, err = restarted.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "detach", RequestId: "detach"})
	require.NoError(t, err)
	require.Empty(t, q.migrationID)
	require.NoError(t, restarted.CompleteMigration(t.Context(), "attach", "complete", journal), "historical completion retry must remain idempotent")
}

func TestConcurrentTransitionRejectsRatherThanQueues(t *testing.T) {
	pm, q, _ := newTransitionManager(t)
	_, err := pm.adminServingTransition(t.Context(), &rpc.ServingControlRequest{Operation: "attach", ConnectionName: "source", RequestId: "attach"})
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	peers := pm.servingPeers.(testServingPeers)
	peers.enforce = func(bool) error { close(entered); <-release; return nil }
	pm.servingPeers = peers
	journal := func(context.Context, executor.InternalTx) error { return nil }
	completed := make(chan error, 1)
	go func() {
		completed <- pm.ControllerTransition(t.Context(), "attach", "fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal)
	}()
	<-entered
	require.ErrorContains(t, pm.ControllerTransition(t.Context(), "attach", "competing-fence", "", pb.MigrationMode_MIGRATION_MODE_FENCED, journal), "already running")
	require.ErrorContains(t, pm.CompleteMigration(t.Context(), "attach", "competing-complete", journal), "already running")
	require.Equal(t, "fence", q.state.ActiveRequestId)
	close(release)
	require.NoError(t, <-completed)
}
