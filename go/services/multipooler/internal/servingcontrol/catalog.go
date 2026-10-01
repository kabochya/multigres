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

// Package servingcontrol owns migration routing and source connections on the target.
package servingcontrol

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/timeouts"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Schema is idempotent so an existing managed database can enable the module.
// No schema or credentials are written on an external source.
const Schema = `CREATE TABLE IF NOT EXISTS multigres.migration_routing (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
 mode INT NOT NULL DEFAULT 0 CHECK (mode BETWEEN 0 AND 3),
 source_connection TEXT NOT NULL DEFAULT ''
);
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS source_system_identifier TEXT NOT NULL DEFAULT '';
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS source_database TEXT NOT NULL DEFAULT '';
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS migration_completed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS resume_source_allowed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS required_poolers TEXT NOT NULL DEFAULT '[]';
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS migration_id TEXT NOT NULL DEFAULT '';
ALTER TABLE multigres.migration_routing ADD COLUMN IF NOT EXISTS active_request_id TEXT NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS multigres.serving_requests (
 request_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, operation TEXT NOT NULL,
 completed BOOLEAN NOT NULL DEFAULT FALSE
);
REVOKE ALL ON multigres.serving_requests FROM PUBLIC;
INSERT INTO multigres.migration_routing(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS multigres.connections (
 name TEXT PRIMARY KEY, configuration TEXT NOT NULL
);
REVOKE ALL ON multigres.migration_routing, multigres.connections FROM PUBLIC;`

// Catalog operates on admin transactions. The owning manager checks leadership.
type Catalog struct {
	Queries executor.InternalQueryService
	key     []byte
	keyID   string
}

func New(q executor.InternalQueryService, key []byte) (*Catalog, error) {
	if len(key) != 32 {
		return nil, errors.New("migration key must contain exactly 32 bytes")
	}
	fingerprint := sha256.Sum256(key)
	return &Catalog{Queries: q, key: append([]byte(nil), key...), keyID: hex.EncodeToString(fingerprint[:8])}, nil
}

func (c *Catalog) Initialize(ctx context.Context) error {
	return c.Queries.QueryAdminMultiStatement(ctx, Schema)
}

func Read(ctx context.Context, tx executor.InternalTx) (*pb.MigrationRouting, error) {
	r, err := tx.Query(ctx, `SELECT mode, source_connection, source_system_identifier, source_database, migration_completed, resume_source_allowed, active_request_id FROM multigres.migration_routing WHERE singleton FOR UPDATE`)
	if err != nil {
		return nil, err
	}
	var mode int32
	var name, sysid, database, requestID string
	var completed, resumeAllowed bool
	if err = executor.ScanSingleRow(r, &mode, &name, &sysid, &database, &completed, &resumeAllowed, &requestID); err != nil {
		return nil, err
	}
	if mode < 0 || mode > 3 {
		return nil, errors.New("invalid migration mode in catalog")
	}
	return &pb.MigrationRouting{Mode: pb.MigrationMode(mode), SourceConnection: name, SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: sysid, Database: database}, MigrationCompleted: completed, ResumeSourceAllowed: resumeAllowed, ActiveRequestId: requestID}, nil
}

// Update runs the journal callback and routing update in one transaction.
// remote_apply prevents publishing ahead of the configured synchronous cohort.
func (c *Catalog) Update(ctx context.Context, update func(executor.InternalTx, *pb.MigrationRouting) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeouts.RuleWriteTimeout)
	defer cancel()
	tx, err := c.Queries.BeginAdmin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Query(ctx, `SET LOCAL synchronous_commit = 'remote_apply'`); err != nil {
		return err
	}
	state, err := Read(ctx, tx)
	if err != nil {
		return err
	}
	if err = update(tx, state); err != nil {
		return err
	}
	if state.Mode < 0 || state.Mode > 3 {
		return errors.New("invalid migration mode")
	}
	if _, err = tx.QueryArgs(ctx, `UPDATE multigres.migration_routing SET mode=$1, source_connection=$2,source_system_identifier=$3,source_database=$4,migration_completed=$5,resume_source_allowed=$6,active_request_id=$7 WHERE singleton`, int32(state.Mode), state.SourceConnection, state.GetSourceIdentity().GetSystemIdentifier(), state.GetSourceIdentity().GetDatabase(), state.MigrationCompleted, state.ResumeSourceAllowed, state.ActiveRequestId); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ConfirmedRouting follows confirmProposalQuorum: a synchronous, WAL-generating
// update proves the observed row and all preceding WAL have reached the current
// synchronous cohort. A local reread alone cannot resolve an uncertain COMMIT.
// No process cache is needed, including after a pooler-only restart.
func (c *Catalog) ConfirmedRouting(ctx context.Context) (*pb.MigrationRouting, error) {
	var state *pb.MigrationRouting
	err := c.Update(ctx, func(_ executor.InternalTx, s *pb.MigrationRouting) error {
		state = proto.Clone(s).(*pb.MigrationRouting)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (c *Catalog) Routing(ctx context.Context) (*pb.MigrationRouting, error) {
	r, err := c.Queries.QueryAdmin(ctx, `SELECT mode, source_connection, source_system_identifier, source_database, migration_completed, resume_source_allowed, active_request_id FROM multigres.migration_routing WHERE singleton`)
	if err != nil {
		return nil, err
	}
	var mode int32
	var name, sysid, database, requestID string
	var completed, resumeAllowed bool
	if err = executor.ScanSingleRow(r, &mode, &name, &sysid, &database, &completed, &resumeAllowed, &requestID); err != nil {
		return nil, err
	}
	return &pb.MigrationRouting{Mode: pb.MigrationMode(mode), SourceConnection: name, SourceIdentity: &pb.ExternalBackendIdentity{SystemIdentifier: sysid, Database: database}, MigrationCompleted: completed, ResumeSourceAllowed: resumeAllowed, ActiveRequestId: requestID}, nil
}

func ValidateConnection(v *rpc.SourceConnection) error {
	if v == nil || v.Name == "" || v.Host == "" || v.Database == "" || v.Username == "" || v.Port == 0 || v.Port > 65535 {
		return errors.New("connection name, host, database, username and valid port are required")
	}
	switch v.SslMode {
	case "disable", "require", "verify-ca", "verify-full":
	default:
		return errors.New("explicit source ssl_mode is required")
	}
	return nil
}

func (c *Catalog) seal(v *rpc.SourceConnection) (string, error) {
	if err := ValidateConnection(v); err != nil {
		return "", err
	}
	b, err := aes.NewCipher(c.key)
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	plaintext, err := proto.Marshal(v)
	if err != nil {
		return "", err
	}
	ciphertext := g.Seal(nonce, nonce, plaintext, []byte(v.Name))
	return "v1." + c.keyID + "." + base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (c *Catalog) open(name, encoded string) (*rpc.SourceConnection, error) {
	b, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(encoded, ".")
	if len(parts) != 3 || parts[0] != "v1" || parts[1] != c.keyID {
		return nil, errors.New("source connection key unavailable")
	}
	data, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil || len(data) < g.NonceSize() {
		return nil, errors.New("invalid encrypted connection")
	}
	plain, err := g.Open(nil, data[:g.NonceSize()], data[g.NonceSize():], []byte(name))
	if err != nil {
		return nil, errors.New("cannot decrypt source connection")
	}
	result := &rpc.SourceConnection{}
	if err = proto.Unmarshal(plain, result); err != nil {
		return nil, errors.New("invalid source connection payload")
	}
	return result, ValidateConnection(result)
}

func (c *Catalog) Put(ctx context.Context, tx executor.InternalTx, v *rpc.SourceConnection, alter bool) error {
	encrypted, err := c.seal(v)
	if err != nil {
		return err
	}
	sql := `INSERT INTO multigres.connections(name,configuration) VALUES($1,$2)`
	if alter {
		sql = `UPDATE multigres.connections SET configuration=$2 WHERE name=$1 RETURNING name`
	}
	r, err := tx.QueryArgs(ctx, sql, v.Name, encrypted)
	if err != nil {
		return errors.New("cannot persist source connection")
	}
	if alter && len(r.Rows) == 0 {
		return errors.New("connection does not exist")
	}
	return nil
}

func (c *Catalog) Connection(ctx context.Context, name string) (*rpc.SourceConnection, error) {
	r, err := c.Queries.QueryAdminArgs(ctx, `SELECT configuration FROM multigres.connections WHERE name=$1`, name)
	if err != nil {
		return nil, errors.New("cannot read source connection")
	}
	var encoded string
	if err = executor.ScanSingleRow(r, &encoded); err != nil {
		return nil, errors.New("connection does not exist")
	}
	return c.open(name, encoded)
}

// ReserveRequest records an operation in the same transaction as its first mode
// change. Requests are durable retry identities; they do not order migration modes.
func (c *Catalog) ReserveRequest(ctx context.Context, tx executor.InternalTx, id, hash, operation string, state *pb.MigrationRouting) (bool, error) {
	if id == "" {
		return false, errors.New("request ID is required")
	}
	r, err := tx.QueryArgs(ctx, `SELECT request_hash,completed FROM multigres.serving_requests WHERE request_id=$1 FOR UPDATE`, id)
	if err != nil {
		return false, err
	}
	if len(r.Rows) > 0 {
		var previous string
		var done bool
		if err = executor.ScanSingleRow(r, &previous, &done); err != nil {
			return false, err
		}
		if previous != hash {
			return false, errors.New("request ID reused with different arguments")
		}
		if done {
			return true, nil
		}
		if state.ActiveRequestId != id {
			return false, errors.New("operation superseded; inspect current serving mode")
		}
	} else {
		if _, err = tx.QueryArgs(ctx, `INSERT INTO multigres.serving_requests(request_id,request_hash,operation) VALUES($1,$2,$3)`, id, hash, operation); err != nil {
			return false, err
		}
	}
	if state.ActiveRequestId != "" && state.ActiveRequestId != id {
		previous, err := tx.QueryArgs(ctx, `SELECT completed FROM multigres.serving_requests WHERE request_id=$1`, state.ActiveRequestId)
		if err != nil {
			return false, err
		}
		var complete bool
		if err = executor.ScanSingleRow(previous, &complete); err != nil {
			return false, err
		}
		if !complete {
			return false, errors.New("previous serving operation is incomplete; recover by request ID")
		}
	}
	state.ActiveRequestId = id
	return false, nil
}

func (c *Catalog) CompleteRequest(ctx context.Context, tx executor.InternalTx, id string) error {
	_, err := tx.QueryArgs(ctx, `UPDATE multigres.serving_requests SET completed=TRUE WHERE request_id=$1`, id)
	return err
}

func (c *Catalog) RequestCompleted(ctx context.Context, id, hash string) (bool, error) {
	var done bool
	err := c.Update(ctx, func(tx executor.InternalTx, _ *pb.MigrationRouting) error {
		if id == "" {
			return errors.New("request ID is required")
		}
		r, err := tx.QueryArgs(ctx, `SELECT request_hash,completed FROM multigres.serving_requests WHERE request_id=$1`, id)
		if err != nil {
			return err
		}
		if len(r.Rows) == 0 {
			return nil
		}
		var previous string
		if err = executor.ScanSingleRow(r, &previous, &done); err != nil {
			return err
		}
		if previous != hash {
			return errors.New("request ID reused with different arguments")
		}
		return nil
	})
	return done && err == nil, err
}

// OperationStatus reports durable progress, not merely routing intent. Completed
// means the owning operation recorded all required enforcement acknowledgments.
var ErrOperationNotFound = errors.New("serving operation not found")

type OperationStatus struct {
	RequestID string
	Operation string
	Completed bool
	Active    bool
}

func (c *Catalog) Operation(ctx context.Context, tx executor.InternalTx, state *pb.MigrationRouting, id string) (*OperationStatus, error) {
	r, err := tx.QueryArgs(ctx, `SELECT operation,completed FROM multigres.serving_requests WHERE request_id=$1`, id)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, ErrOperationNotFound
	}
	status := &OperationStatus{RequestID: id, Active: state.ActiveRequestId == id}
	if err = executor.ScanSingleRow(r, &status.Operation, &status.Completed); err != nil {
		return nil, err
	}
	return status, nil
}

// Migration identity is the initial attach request, independent of subsequent
// operation IDs. The caller already owns the singleton row lock in Update.
func (c *Catalog) MigrationIdentity(ctx context.Context, tx executor.InternalTx) (string, error) {
	r, err := tx.Query(ctx, `SELECT migration_id FROM multigres.migration_routing WHERE singleton`)
	if err != nil {
		return "", err
	}
	var id string
	err = executor.ScanSingleRow(r, &id)
	return id, err
}

func (c *Catalog) RequireMigration(ctx context.Context, tx executor.InternalTx, id string) error {
	owner, err := c.MigrationIdentity(ctx, tx)
	if err != nil {
		return err
	}
	if id == "" || owner != id {
		return errors.New("migration identity does not match active owner")
	}
	return nil
}

func (c *Catalog) SetMigrationIdentity(ctx context.Context, tx executor.InternalTx, id string) error {
	_, err := tx.QueryArgs(ctx, `UPDATE multigres.migration_routing SET migration_id=$1,required_poolers=CASE WHEN $1='' THEN '[]' ELSE required_poolers END WHERE singleton`, id)
	return err
}

// RequiredPoolers retains every process seen by this migration's fences, so
// retries and later fences cannot forget a vanished but potentially live process.
// Caller owns the routing row lock; terminal detach alone clears membership.
func (c *Catalog) RequiredPoolers(ctx context.Context, tx executor.InternalTx, discovered []*pb.ID) ([]*pb.ID, error) {
	result, err := tx.Query(ctx, `SELECT required_poolers FROM multigres.migration_routing WHERE singleton`)
	if err != nil {
		return nil, err
	}
	var encoded string
	if err = executor.ScanSingleRow(result, &encoded); err != nil {
		return nil, err
	}
	var required []*pb.ID
	if err = json.Unmarshal([]byte(encoded), &required); err != nil {
		return nil, errors.New("invalid required process membership")
	}
	known := map[string]*pb.ID{}
	for _, id := range append(required, discovered...) {
		if id.GetComponent() != pb.ID_MULTIPOOLER || id.GetCell() == "" || id.GetName() == "" {
			return nil, errors.New("required process identity is incomplete")
		}
		known[id.Cell+"/"+id.Name] = id
	}
	keys := make([]string, 0, len(known))
	for key := range known {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	required = make([]*pb.ID, 0, len(keys))
	for _, key := range keys {
		required = append(required, known[key])
	}
	updated, err := json.Marshal(required)
	if err != nil {
		return nil, err
	}
	if _, err = tx.QueryArgs(ctx, `UPDATE multigres.migration_routing SET required_poolers=$1 WHERE singleton`, string(updated)); err != nil {
		return nil, err
	}
	return required, nil
}

// AwaitRouting waits only during an explicit enforcement request. A local row
// is replay evidence, not quorum confirmation; the sender confirms first.
// Opaque IDs are compared for equality, never ordered or used to override state.
func (c *Catalog) AwaitRouting(ctx context.Context, id string, mode pb.MigrationMode) (*pb.MigrationRouting, error) {
	for {
		state, err := c.Routing(ctx)
		if err != nil {
			return nil, err
		}
		if state.ActiveRequestId == id && state.Mode == mode {
			return state, nil
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(errors.New("routing operation/mode has not replayed; recover the same operation"), ctx.Err())
		case <-timer.C:
		}
	}
}
