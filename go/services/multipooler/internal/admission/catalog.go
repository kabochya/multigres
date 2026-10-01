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

// Package admission defines the normalized controller-owned projection. It has
// no workflow, peer discovery, fanout or operation completion responsibilities.
package admission

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/multigres/multigres/go/common/sqltypes"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

const scopeSchema = `CREATE TABLE IF NOT EXISTS multigres.admission_scopes (
 database TEXT PRIMARY KEY, table_group TEXT NOT NULL, shard TEXT NOT NULL,
 controlled BOOLEAN NOT NULL DEFAULT FALSE, owner TEXT NOT NULL DEFAULT '',
 CHECK (NOT controlled OR owner <> '')
)`

const intentSchema = `CREATE TABLE IF NOT EXISTS multigres.admission_intents (
 database TEXT NOT NULL REFERENCES multigres.admission_scopes(database),
 subject INT NOT NULL CHECK (subject IN (1,2)), intent TEXT NOT NULL,
 PRIMARY KEY (database,subject)
)`

// InitializeOrdinary creates an explicit ordinary marker under confirmed managed
// authority. ON CONFLICT never clears a controlled scope or terminal owner.
func InitializeOrdinary(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey) error {
	for _, sql := range []string{scopeSchema, intentSchema, `REVOKE ALL ON multigres.admission_scopes, multigres.admission_intents FROM PUBLIC`} {
		if _, err := tx.Query(ctx, sql); err != nil {
			return err
		}
	}
	_, err := tx.QueryArgs(ctx, `INSERT INTO multigres.admission_scopes(database,table_group,shard) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, sk.Database, sk.TableGroup, sk.Shard)
	return err
}

type Reader interface {
	QueryArgs(context.Context, string, ...any) (*sqltypes.Result, error)
}

const readSQL = `SELECT s.table_group,s.shard,s.controlled,s.owner,COALESCE(i.intent,'') FROM multigres.admission_scopes s LEFT JOIN multigres.admission_intents i ON i.database=s.database AND i.subject=$2 WHERE s.database=$1`

func Read(ctx context.Context, q Reader, sk *pb.ShardKey, subject pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
	if subject != pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET && subject != pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE {
		return nil, errors.New("admission subject required")
	}
	r, err := q.QueryArgs(ctx, readSQL, sk.Database, int32(subject))
	if err != nil {
		return nil, err
	}
	var tg, shard, owner, encoded string
	var controlled bool
	if err = executor.ScanSingleRow(r, &tg, &shard, &controlled, &owner, &encoded); err != nil {
		return nil, errors.New("admission applicability unknown")
	}
	s := &pb.AdmissionSnapshot{AuthorityShardKey: &pb.ShardKey{Database: sk.Database, TableGroup: tg, Shard: shard}, Controlled: controlled, Owner: owner}
	if !proto.Equal(sk, s.AuthorityShardKey) {
		return nil, errors.New("admission authority cohort mismatch")
	}
	if encoded != "" {
		data, err := hex.DecodeString(encoded)
		if err != nil {
			return nil, errors.New("invalid admission projection")
		}
		s.Intent = &pb.AdmissionIntent{}
		if err = proto.Unmarshal(data, s.Intent); err != nil {
			return nil, err
		}
	}
	if controlled && (owner == "" || s.Intent == nil || s.Intent.Owner != owner || s.Intent.Subject != subject) {
		return nil, errors.New("controlled admission intent missing or ambiguous")
	}
	if s.Intent != nil {
		if err = Validate(s.Intent); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func Validate(i *pb.AdmissionIntent) error {
	if i == nil || i.Owner == "" || i.IntentId == "" {
		return errors.New("admission owner and exact intent ID required")
	}
	if i.Subject != pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE && i.Subject != pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET {
		return errors.New("invalid admission subject")
	}
	if i.Permission != pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED && i.Permission != pb.AdmissionPermission_ADMISSION_PERMISSION_OPEN {
		return errors.New("invalid admission permission")
	}
	if i.Subject == pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE && (i.SourceConnection == "" || i.SourceConfigurationBinding == "" || i.GetSourceIdentity().GetSystemIdentifier() == "" || i.GetSourceIdentity().GetDatabase() == "") {
		return errors.New("source intent requires configuration and physical identity binding")
	}
	if i.Subject == pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET && (i.SourceConnection != "" || i.SourceConfigurationBinding != "" || i.SourceIdentity != nil) {
		return errors.New("target intent must not retain source association")
	}
	return nil
}

// LockScope is shared by controller writes and confirmed authority callbacks.
// Row locks cover BOTH subjects; the controller additionally validates its
// active journal operation/predecessor in this same short transaction.
func LockScope(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey) error {
	r, err := tx.QueryArgs(ctx, `SELECT table_group,shard FROM multigres.admission_scopes WHERE database=$1 FOR UPDATE`, sk.Database)
	if err != nil {
		return err
	}
	var tg, shard string
	if err = executor.ScanSingleRow(r, &tg, &shard); err != nil {
		return err
	}
	if tg != sk.TableGroup || shard != sk.Shard {
		return errors.New("admission authority cohort mismatch")
	}
	return nil
}

// Enable is called by the controller in its intent+journal transaction. Existing
// processes must complete the CLOSED barrier before any destination is opened.
func Enable(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, source, target *pb.AdmissionIntent) error {
	if err := Validate(source); err != nil {
		return err
	}
	if err := Validate(target); err != nil {
		return err
	}
	if source.Subject != pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE || target.Subject != pb.AdmissionSubject_ADMISSION_SUBJECT_TARGET || source.Owner != target.Owner || source.Permission != pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED || target.Permission != pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED {
		return errors.New("enable requires same-owner source and target CLOSED")
	}
	if err := LockScope(ctx, tx, sk); err != nil {
		return err
	}
	r, err := tx.QueryArgs(ctx, `UPDATE multigres.admission_scopes SET controlled=TRUE,owner=$2 WHERE database=$1 AND (NOT controlled OR owner=$2) RETURNING database`, sk.Database, source.Owner)
	if err != nil {
		return err
	}
	if len(r.Rows) != 1 {
		return errors.New("conflicting admission owner")
	}
	if err = Write(ctx, tx, sk, source); err != nil {
		return err
	}
	return Write(ctx, tx, sk, target)
}

// Write is a transaction primitive, never a workflow permission check. Its
// caller must lock scope/journal and validate cross-subject transition evidence.
func Write(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, i *pb.AdmissionIntent) error {
	if err := Validate(i); err != nil {
		return err
	}
	if err := LockScope(ctx, tx, sk); err != nil {
		return err
	}
	r, err := tx.QueryArgs(ctx, `SELECT controlled,owner FROM multigres.admission_scopes WHERE database=$1`, sk.Database)
	if err != nil {
		return err
	}
	var controlled bool
	var owner string
	if err = executor.ScanSingleRow(r, &controlled, &owner); err != nil {
		return err
	}
	if !controlled || owner != i.Owner {
		return errors.New("admission owner mismatch")
	}
	data, err := proto.Marshal(i)
	if err != nil {
		return err
	}
	_, err = tx.QueryArgs(ctx, `INSERT INTO multigres.admission_intents(database,subject,intent) VALUES($1,$2,$3) ON CONFLICT(database,subject) DO UPDATE SET intent=EXCLUDED.intent`, sk.Database, int32(i.Subject), hex.EncodeToString(data))
	return err
}

// Confirm must commit synchronously after existing quorum activation. No local
// reread resolves an uncertain commit; a no-op WAL write confirms the whole row.
func Confirm(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, subject pb.AdmissionSubject) (*pb.AdmissionSnapshot, error) {
	if err := LockScope(ctx, tx, sk); err != nil {
		return nil, err
	}
	s, err := Read(ctx, tx, sk, subject)
	if err != nil {
		return nil, err
	}
	_, err = tx.QueryArgs(ctx, `UPDATE multigres.admission_scopes SET owner=owner WHERE database=$1`, sk.Database)
	return s, err
}
