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
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protometadata"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// The persisted admission fences and routing rules, and the operations on them. This side
// knows nothing of connections: a tablegroup is a name here.
//
// Every write is one statement, because the admin pool does not hold a transaction across
// calls. A statement that reads and locks rows and writes in data-modifying CTEs commits all of
// it or nothing, and sees one snapshot of what it reads, which is the transaction the contract
// asks for. Reading a fence that a write depends on takes a row lock (FOR SHARE, and FOR UPDATE
// for what the statement changes), so that neither can change between the check and the write.

// fenceRow is the persisted admission fence of one scope.
type fenceRow struct {
	// id is the identity of the row, which the compare-and-set of a write names.
	id          int64
	operationID string
	tablegroup  string
	state       multipoolerservicepb.AdmissionState
	version     int64
	updatedAt   time.Time
}

// closed reports whether the scope refuses application work. A scope with no fence is open.
func (r *fenceRow) closed() bool { return r != nil && r.state != stateUnfenced }

func (r *fenceRow) proto() *multipoolerservicepb.AdmissionFence {
	return &multipoolerservicepb.AdmissionFence{
		OperationId: r.operationID,
		Scope:       &multipoolerservicepb.AdmissionScope{Tablegroup: r.tablegroup},
		State:       r.state,
		Version:     r.version,
		UpdatedAt:   timestampOrNil(r.updatedAt),
	}
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// ruleRow is a persisted routing rule.
type ruleRow struct {
	operationID    string
	from, to       string
	toShard        string
	createdVersion int64
}

func (r ruleRow) proto() *multipoolerservicepb.RoutingRuleState {
	return &multipoolerservicepb.RoutingRuleState{
		OperationId:    r.operationID,
		Rule:           &multipoolerservicepb.RoutingRule{FromTablegroup: r.from, ToTablegroup: r.to, ToShard: r.toShard},
		CreatedVersion: r.createdVersion,
	}
}

// The states of an operation, as they are persisted.
const (
	operationPrepare  = "PREPARE"
	operationSwitched = "SWITCHED"
	operationReverted = "REVERTED"
	operationFolded   = "FOLDED"
	operationCleaned  = "CLEANED"
)

// operationRow is a persisted operation: where it is, and the routing_version its last switch,
// revert or fold produced.
type operationRow struct {
	operationID string
	state       string
	version     int64
	// effective is the tablegroup that received the traffic after the last change.
	effective string
}

// routingSnapshot is the persisted routing and admission state, read in one statement.
type routingSnapshot struct {
	routingVersion int64
	// home is empty when the database has no catalog state.
	home       string
	rules      []ruleRow
	fences     []fenceRow
	operations []operationRow
}

// operationOf returns the operation, nil when nothing has been recorded of it.
func (s *routingSnapshot) operationOf(operationID string) *operationRow {
	for i := range s.operations {
		if s.operations[i].operationID == operationID {
			return &s.operations[i]
		}
	}
	return nil
}

// effective resolves where the traffic goes: the home tablegroup, or, when a rule applies to
// it, the rule's destination, applied once.
func (s *routingSnapshot) effective() string {
	for _, r := range s.rules {
		if r.from == s.home {
			return r.to
		}
	}
	return s.home
}

// fenceOf returns the fence of the tablegroup scope, nil when it has none.
func (s *routingSnapshot) fenceOf(tablegroup string) *fenceRow {
	for i := range s.fences {
		if s.fences[i].tablegroup == tablegroup {
			return &s.fences[i]
		}
	}
	return nil
}

// ruleOf returns the rule of the operation, nil when it has none.
func (s *routingSnapshot) ruleOf(operationID string) *ruleRow {
	for i := range s.rules {
		if s.rules[i].operationID == operationID {
			return &s.rules[i]
		}
	}
	return nil
}

// fenceWrite is a compare-and-set of the fence of a tablegroup scope.
type fenceWrite struct {
	tablegroup  string
	operationID string
	// prev is the fence the caller read, nil when it read none. A write that names no fence
	// creates one; one that does updates it only if it is still exactly that, which is also how
	// an operation takes a scope over from another one that left it UNFENCED.
	prev *fenceRow
	// state and version are what the fence becomes.
	state   multipoolerservicepb.AdmissionState
	version int64
}

// routingReader is the read side of the persisted admission and routing state, which lives on the
// default primary.
type routingReader interface {
	// Snapshot returns the state. A database whose tables do not exist reads as empty: it never
	// fenced or routed anything, and reading creates nothing.
	Snapshot(ctx context.Context) (*routingSnapshot, error)
}

// routingStore is the durable store of admission fences and routing rules. Every mutation is a
// conditional update on what the caller read, so concurrent or stale writers cannot overwrite each
// other. An error means the outcome is uncertain; the caller must read again.
type routingStore interface {
	routingReader
	// TablegroupExists reports whether the catalog has the tablegroup, and ShardExists whether it
	// has the shard of the tablegroup.
	TablegroupExists(ctx context.Context, tablegroup string) (bool, error)
	ShardExists(ctx context.Context, tablegroup, shard string) (bool, error)
	// OperationCleaned reports whether the operation was cleaned up, which the snapshot does not say:
	// it lists the operations that are not.
	OperationCleaned(ctx context.Context, operationID string) (bool, error)
	// WriteFence writes a fence and reports whether it did: false when the fence is no longer the
	// one the write names, or another operation created one first.
	WriteFence(ctx context.Context, w fenceWrite) (bool, error)
	// SwitchRouting adds the rule of the operation and bumps routing_version, only if the routing
	// is at expectedVersion, the rule's from is the home tablegroup and not the default one, no rule
	// exists, and the fences of both tablegroups are FENCED for the operation. It returns the new
	// version, and false when it did not.
	SwitchRouting(ctx context.Context, operationID string, rule ruleRow, expectedVersion int64) (int64, bool, error)
	// RevertRouting deletes the rule of the operation and bumps routing_version, only if the
	// routing is at expectedVersion and the fences of the rule's tablegroups are FENCED for the
	// operation.
	RevertRouting(ctx context.Context, operationID string, expectedVersion int64) (int64, bool, error)
	// FoldRouting makes the rule's destination the home tablegroup, deletes the rule and bumps
	// routing_version, only if the routing is at expectedVersion and the rule applies to the home
	// tablegroup.
	FoldRouting(ctx context.Context, operationID string, expectedVersion int64) (int64, bool, error)
	// CleanupOperation deletes the fences of the operation and marks it cleaned up, for good, unless
	// the routing is no longer at routingVersion (the version the caller read, when the operation
	// had no rule), or the operation has a rule or a fence that is FENCING; blocked says it did not
	// for that reason.
	CleanupOperation(ctx context.Context, operationID string, routingVersion int64) (deleted int64, blocked bool, err error)
}

// newRoutingStore builds the store of admission fences and routing rules of this pooler, which
// must be the default primary. Where the state is kept is not decided here, so nothing is
// configured by default and the calls that need the store answer UNIMPLEMENTED until something is
// registered (a pooler that reads UNIMPLEMENTED from the default primary opens: it has no
// admission state to enforce).
var newRoutingStore func(pm *MultipoolerManager) routingStore

// routingStore returns the store of this pooler.
func (pm *MultipoolerManager) routingStore() (routingStore, error) {
	if newRoutingStore == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "no store of admission fences and routing rules is configured")
	}
	return newRoutingStore(pm), nil
}

// PROTOTYPE STUB: the store of the prototype, in throwaway tables on the default primary. Delete
// it, and the tables with it, when the real routing-rule catalog lands.

// prototypeRoutingStore keeps the fences and rules in the tables of protometadata, through the
// admin pool.
type prototypeRoutingStore struct {
	query func(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error)
	exec  func(ctx context.Context, sql string) error
	// schemaReady is set once the tables are known to exist, so the statement that creates them
	// runs once per process, not before every write.
	schemaReady atomic.Bool
}

func newPrototypeRoutingStore(pm *MultipoolerManager) *prototypeRoutingStore {
	return &prototypeRoutingStore{query: pm.adminQueryArgs, exec: pm.adminExec}
}

// prototypeRoutingStores holds the store of each manager, built once.
var prototypeRoutingStores sync.Map // *MultipoolerManager -> *prototypeRoutingStore

func prototypeRoutingStoreOf(pm *MultipoolerManager) *prototypeRoutingStore {
	if s, ok := prototypeRoutingStores.Load(pm); ok {
		return s.(*prototypeRoutingStore)
	}
	s, _ := prototypeRoutingStores.LoadOrStore(pm, newPrototypeRoutingStore(pm))
	return s.(*prototypeRoutingStore)
}

func init() {
	newRoutingStore = func(pm *MultipoolerManager) routingStore { return prototypeRoutingStoreOf(pm) }
}

// isUndefinedTable reports whether err is PostgreSQL's undefined_table: the metadata tables have
// not been created, because nothing has ever been fenced.
func isUndefinedTable(err error) bool {
	var diag *mterrors.PgDiagnostic
	return errors.As(err, &diag) && diag.Code == mterrors.PgSSUndefinedTable
}

// ensureSchema creates the tables if they do not exist; it is idempotent and one statement, and
// runs once per process once it succeeded. Writes run it first, reads never do: reading creates
// nothing.
func (s *prototypeRoutingStore) ensureSchema(ctx context.Context) error {
	if s.schemaReady.Load() {
		return nil
	}
	if err := protometadata.EnsureRoutingSchema(ctx, s.exec); err != nil {
		return err
	}
	s.schemaReady.Store(true)
	return nil
}

type snapshotJSON struct {
	RoutingVersion *int64 `json:"routing_version"`
	Home           string `json:"home"`
	Rules          []struct {
		OperationID    string `json:"operation_id"`
		From           string `json:"from"`
		To             string `json:"to"`
		ToShard        string `json:"to_shard"`
		CreatedVersion int64  `json:"created_version"`
	} `json:"rules"`
	Operations []struct {
		OperationID string `json:"operation_id"`
		State       string `json:"state"`
		Version     int64  `json:"version"`
		Effective   string `json:"effective"`
	} `json:"operations"`
	Fences []struct {
		ID          int64   `json:"id"`
		OperationID string  `json:"operation_id"`
		Tablegroup  string  `json:"tablegroup"`
		State       string  `json:"state"`
		Version     int64   `json:"version"`
		UpdatedAt   float64 `json:"updated_at"`
	} `json:"fences"`
}

func (s *prototypeRoutingStore) Snapshot(ctx context.Context) (*routingSnapshot, error) {
	result, err := s.query(ctx, protometadata.SnapshotSQL)
	if isUndefinedTable(err) {
		return &routingSnapshot{}, nil
	}
	if err != nil {
		return nil, mterrors.Wrap(err, "read routing state")
	}
	var text string
	if err := executor.ScanSingleRow(result, &text); err != nil {
		return nil, errors.Join(errors.New("decode routing state"), err)
	}
	var raw snapshotJSON
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, errors.Join(errors.New("decode routing state"), err)
	}
	snap := &routingSnapshot{home: raw.Home}
	if raw.RoutingVersion != nil {
		snap.routingVersion = *raw.RoutingVersion
	}
	for _, r := range raw.Rules {
		snap.rules = append(snap.rules, ruleRow{operationID: r.OperationID, from: r.From, to: r.To, toShard: r.ToShard, createdVersion: r.CreatedVersion})
	}
	for _, o := range raw.Operations {
		snap.operations = append(snap.operations, operationRow{operationID: o.OperationID, state: o.State, version: o.Version, effective: o.Effective})
	}
	for _, f := range raw.Fences {
		row := fenceRow{id: f.ID, operationID: f.OperationID, tablegroup: f.Tablegroup, state: parseAdmissionState(f.State), version: f.Version}
		row.updatedAt = time.Unix(0, int64(f.UpdatedAt*float64(time.Second)))
		snap.fences = append(snap.fences, row)
	}
	return snap, nil
}

// admissionStateText is the text a state is stored as: the name of the enum value without its
// prefix.
func admissionStateText(state multipoolerservicepb.AdmissionState) string {
	return strings.TrimPrefix(state.String(), "ADMISSION_STATE_")
}

// parseAdmissionState reads a stored state; an unknown text is UNSPECIFIED, which no
// compare-and-set matches.
func parseAdmissionState(text string) multipoolerservicepb.AdmissionState {
	return multipoolerservicepb.AdmissionState(multipoolerservicepb.AdmissionState_value["ADMISSION_STATE_"+text])
}

func (s *prototypeRoutingStore) OperationCleaned(ctx context.Context, operationID string) (bool, error) {
	result, err := s.query(ctx, protometadata.OperationCleanedSQL, operationID)
	if isUndefinedTable(err) {
		return false, nil
	}
	if err != nil {
		return false, mterrors.Wrap(err, "look up the operation")
	}
	return len(result.Rows) > 0, nil
}

func (s *prototypeRoutingStore) TablegroupExists(ctx context.Context, tablegroup string) (bool, error) {
	result, err := s.query(ctx, "SELECT 1 FROM multigres.tablegroup WHERE name = $1", tablegroup)
	if err != nil {
		return false, mterrors.Wrap(err, "look up the tablegroup")
	}
	return len(result.Rows) > 0, nil
}

func (s *prototypeRoutingStore) ShardExists(ctx context.Context, tablegroup, shard string) (bool, error) {
	result, err := s.query(ctx, `SELECT 1 FROM multigres.shard s JOIN multigres.tablegroup tg ON tg.oid = s.tablegroup_oid
WHERE tg.name = $1 AND s.shard_name = $2`, tablegroup, shard)
	if err != nil {
		return false, mterrors.Wrap(err, "look up the shard")
	}
	return len(result.Rows) > 0, nil
}

// WriteFence is one statement. A scope that has no fence is unfenced at version zero, and a fence
// from there inserts one; the unique index on the scope makes a concurrent insert lose. Neither
// writes for an operation that was cleaned up.
func (s *prototypeRoutingStore) WriteFence(ctx context.Context, w fenceWrite) (bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return false, err
	}
	var (
		result *sqltypes.Result
		err    error
	)
	if w.prev == nil {
		result, err = s.query(ctx, protometadata.InsertFenceSQL, w.operationID, w.tablegroup, admissionStateText(w.state), w.version)
	} else {
		result, err = s.query(ctx, protometadata.UpdateFenceSQL, w.operationID, admissionStateText(w.state), w.version,
			w.prev.id, w.prev.operationID, admissionStateText(w.prev.state), w.prev.version)
	}
	if err != nil {
		return false, err
	}
	return len(result.Rows) == 1, nil
}

func (s *prototypeRoutingStore) SwitchRouting(ctx context.Context, operationID string, rule ruleRow, expectedVersion int64) (int64, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return 0, false, err
	}
	return s.routingWrite(ctx, protometadata.SwitchRoutingSQL, operationID, rule.from, rule.to, rule.toShard, expectedVersion, constants.DefaultTableGroup)
}

func (s *prototypeRoutingStore) RevertRouting(ctx context.Context, operationID string, expectedVersion int64) (int64, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return 0, false, err
	}
	return s.routingWrite(ctx, protometadata.RevertRoutingSQL, operationID, expectedVersion)
}

func (s *prototypeRoutingStore) FoldRouting(ctx context.Context, operationID string, expectedVersion int64) (int64, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return 0, false, err
	}
	return s.routingWrite(ctx, protometadata.FoldRoutingSQL, operationID, expectedVersion)
}

// routingWrite runs a statement that returns the new routing_version when it wrote and no row
// when a condition did not hold.
func (s *prototypeRoutingStore) routingWrite(ctx context.Context, sql string, args ...any) (int64, bool, error) {
	result, err := s.query(ctx, sql, args...)
	if err != nil {
		return 0, false, err
	}
	if len(result.Rows) == 0 {
		return 0, false, nil
	}
	var version int64
	if err := executor.ScanSingleRow(result, &version); err != nil {
		return 0, false, errors.Join(errors.New("decode the routing version"), err)
	}
	return version, true, nil
}

// CleanupOperation is two statements. The first marks the operation CLEANED, in the same statement
// that checks that nothing blocks it, and once it has committed no fence of the operation can be
// written; the second, with a snapshot of its own, deletes the fences there are. A cleanup that
// stops between them is finished by calling it again.
func (s *prototypeRoutingStore) CleanupOperation(ctx context.Context, operationID string, routingVersion int64) (int64, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return 0, false, err
	}
	result, err := s.query(ctx, protometadata.CleanupMarkSQL, operationID, routingVersion)
	if err != nil {
		return 0, false, err
	}
	var blocked string
	if err := executor.ScanSingleRow(result, &blocked); err != nil {
		return 0, false, errors.Join(errors.New("decode the cleanup"), err)
	}
	if blocked == "true" {
		return 0, true, nil
	}
	result, err = s.query(ctx, protometadata.CleanupDeleteSQL, operationID)
	if err != nil {
		return 0, false, mterrors.Wrap(err, "the operation is marked cleaned up, and its fences are not deleted yet")
	}
	return int64(len(result.Rows)), false, nil
}
