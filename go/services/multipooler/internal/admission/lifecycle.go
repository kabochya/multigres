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

package admission

import (
	"context"
	"encoding/hex"
	"errors"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

const lifecycleSchema = `CREATE TABLE IF NOT EXISTS multigres.source_lifecycle (database TEXT PRIMARY KEY REFERENCES multigres.admission_scopes(database), authorization TEXT NOT NULL)`

func InitializeLifecycle(ctx context.Context, tx executor.InternalTx) error {
	for _, sql := range []string{lifecycleSchema, `REVOKE ALL ON multigres.source_lifecycle FROM PUBLIC`} {
		if _, err := tx.Query(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

func ValidateLifecycle(a *pb.SourceLifecycleAuthorization, s *pb.AdmissionSnapshot) error {
	if a == nil || s == nil || !s.Controlled || a.Owner == "" || a.Owner != s.Owner || a.ClosedIntentId == "" || a.ClosedIntentId != s.Intent.GetIntentId() || s.Intent.GetPermission() != pb.AdmissionPermission_ADMISSION_PERMISSION_CLOSED || a.SourceConnection != s.Intent.GetSourceConnection() || a.SourceConfigurationBinding != s.Intent.GetSourceConfigurationBinding() {
		return errors.New("source lifecycle authorization requires current owned CLOSED intent and binding")
	}
	if len(a.ProofReleaseProcesses) > 0 && !a.RetireSource {
		return errors.New("proof release requires retirement authorization")
	}
	seen := map[string]bool{}
	for _, id := range a.ProofReleaseProcesses {
		if id.GetComponent() != pb.ID_MULTIPOOLER || id.GetName() == "" || id.GetCell() == "" {
			return errors.New("proof release requires complete process incarnation")
		}
		key := id.String()
		if seen[key] {
			return errors.New("duplicate proof release process")
		}
		seen[key] = true
	}
	return nil
}

// WriteLifecycle composes with the controller's terminal/journal/proof checks.
// It cannot infer migration completion or process termination from routing.
func WriteLifecycle(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey, a *pb.SourceLifecycleAuthorization) error {
	if err := LockScope(ctx, tx, sk); err != nil {
		return err
	}
	s, err := Read(ctx, tx, sk, pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE)
	if err != nil {
		return err
	}
	if err = ValidateLifecycle(a, s); err != nil {
		return err
	}
	data, err := proto.Marshal(a)
	if err != nil {
		return err
	}
	_, err = tx.QueryArgs(ctx, `INSERT INTO multigres.source_lifecycle(database,authorization) VALUES($1,$2) ON CONFLICT(database) DO UPDATE SET authorization=EXCLUDED.authorization`, sk.Database, hex.EncodeToString(data))
	return err
}

func ConfirmLifecycle(ctx context.Context, tx executor.InternalTx, sk *pb.ShardKey) (*pb.SourceLifecycleAuthorization, error) {
	s, err := Confirm(ctx, tx, sk, pb.AdmissionSubject_ADMISSION_SUBJECT_SOURCE)
	if err != nil {
		return nil, err
	}
	r, err := tx.QueryArgs(ctx, `SELECT authorization FROM multigres.source_lifecycle WHERE database=$1`, sk.Database)
	if err != nil {
		return nil, err
	}
	if len(r.Rows) == 0 {
		return nil, nil
	}
	var encoded string
	if err = executor.ScanSingleRow(r, &encoded); err != nil {
		return nil, err
	}
	data, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	a := &pb.SourceLifecycleAuthorization{}
	if err = proto.Unmarshal(data, a); err != nil {
		return nil, err
	}
	if err = ValidateLifecycle(a, s); err != nil {
		return nil, err
	}
	return a, nil
}
