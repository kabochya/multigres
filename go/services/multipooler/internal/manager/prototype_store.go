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
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protometadata"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// PROTOTYPE STUB: the store of the prototype metadata, registered as the pooler's store of
// serving state and directory of backing connections.

// isUndefinedTable reports whether err is PostgreSQL's undefined_table: the metadata
// tables have not been created, because nothing has ever been fenced.
func isUndefinedTable(err error) bool {
	var diag *mterrors.PgDiagnostic
	return errors.As(err, &diag) && diag.Code == mterrors.PgSSUndefinedTable
}

// prototypeStore keeps the cluster metadata of the prototype in throwaway tables on the default
// primary, through the admin pool: the backing connections, the serving state of each
// tablegroup, and the routing pointer. It is one store because it is one piece of
// scaffolding: delete this file, and the tables with it, when the real tablegroup and
// connection catalog lands.
type prototypeStore struct {
	query func(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error)
	exec  func(ctx context.Context, sql string) error
	// schemaReady is set once the tables are known to exist, so the statement that creates
	// them runs once per process, not before every write.
	schemaReady atomic.Bool
}

func newPrototypeStore(pm *MultipoolerManager) *prototypeStore {
	return &prototypeStore{query: pm.adminQueryArgs, exec: pm.adminExec}
}

// prototypeStores holds the store of each manager, built once.
var prototypeStores sync.Map // *MultipoolerManager -> *prototypeStore

func prototypeStoreOf(pm *MultipoolerManager) *prototypeStore {
	if s, ok := prototypeStores.Load(pm); ok {
		return s.(*prototypeStore)
	}
	s, _ := prototypeStores.LoadOrStore(pm, newPrototypeStore(pm))
	return s.(*prototypeStore)
}

func init() {
	newServingStore = func(pm *MultipoolerManager) servingStore { return prototypeStoreOf(pm) }
	newConnectionDirectory = func(pm *MultipoolerManager) connectionDirectory { return prototypeStoreOf(pm) }
}

// ensureSchema creates the tables if they do not exist; it is idempotent and one statement,
// and runs once per process once it succeeded. Writes and the connection lookup run it
// first, reads never do: reading creates nothing.
func (s *prototypeStore) ensureSchema(ctx context.Context) error {
	if s.schemaReady.Load() {
		return nil
	}
	if err := protometadata.EnsureSchema(ctx, s.exec); err != nil {
		return err
	}
	s.schemaReady.Store(true)
	return nil
}

// BackingConnection reads the named connection and decodes its stored form, a plaintext
// postgres:// URL, into the structured connection the contract carries.
func (s *prototypeStore) BackingConnection(ctx context.Context, name string) (*multipoolerservicepb.BackingConnection, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return nil, false, err
	}
	result, err := s.query(ctx, "SELECT dsn, expected_system_identifier FROM multigres.proto_connections WHERE name = $1", name)
	if err != nil {
		return nil, false, err
	}
	if len(result.Rows) == 0 {
		return nil, false, nil
	}
	var dsn, sysID string
	if err := executor.ScanSingleRow(result, &dsn, &sysID); err != nil {
		return nil, false, errors.Join(errors.New("decode backing connection"), err)
	}
	conn, err := parseBackingDSN(dsn)
	if err != nil {
		return nil, false, err
	}
	conn.Name = name
	conn.ExpectedSystemIdentifier = sysID
	return conn, true, nil
}

// parseBackingDSN decodes a stored connection URL:
//
//	postgres://user:password@host:port/database?sslmode=...&sslrootcert=...&sslnegotiation=...
//
// A URL that could name more than one server is refused here, before the pooler's own
// validation, because the parameters that would do it (host, hostaddr, port,
// target_session_attrs, load_balance_hosts) exist in this stored form and not in the
// structured one.
func parseBackingDSN(raw string) (*multipoolerservicepb.BackingConnection, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("backing connection URL is not valid")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, errors.New("backing connection URL must use the postgres scheme")
	}
	if strings.Contains(u.Host, ",") {
		return nil, errors.New("backing connection URL names more than one host; a single server is required")
	}
	q := u.Query()
	for _, param := range []string{"host", "hostaddr", "port", "target_session_attrs", "load_balance_hosts"} {
		if q.Has(param) {
			return nil, fmt.Errorf("backing connection URL sets %q as a parameter; a single server given in the URL authority is required", param)
		}
	}
	port := int32(5432)
	if p := u.Port(); p != "" {
		n, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			return nil, errors.New("backing connection URL has an invalid port")
		}
		port = int32(n)
	}
	password, _ := u.User.Password()
	return &multipoolerservicepb.BackingConnection{
		Host:           u.Hostname(),
		Port:           port,
		Database:       u.Path[min(1, len(u.Path)):],
		User:           u.User.Username(),
		Password:       password,
		SslMode:        q.Get("sslmode"),
		SslRootCert:    q.Get("sslrootcert"),
		SslNegotiation: q.Get("sslnegotiation"),
	}, nil
}

func (s *prototypeStore) GetRow(ctx context.Context, database, tablegroup string) (*servingRow, error) {
	result, err := s.query(ctx,
		"SELECT COALESCE(connection, ''), state, version, extract(epoch FROM updated_at)::text FROM multigres.proto_tablegroup_serving WHERE database = $1 AND tablegroup = $2",
		database, tablegroup)
	if isUndefinedTable(err) {
		return nil, nil
	}
	if err != nil {
		return nil, mterrors.Wrap(err, "read tablegroup serving state")
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	var connection, state, epoch string
	var version int64
	if err := executor.ScanSingleRow(result, &connection, &state, &version, &epoch); err != nil {
		return nil, errors.Join(errors.New("decode tablegroup serving state"), err)
	}
	row := &servingRow{connection: connection, state: parseAdmissionState(state), version: version}
	if secs, err := strconv.ParseFloat(epoch, 64); err == nil {
		row.updatedAt = time.Unix(0, int64(secs*float64(time.Second)))
	}
	return row, nil
}

// admissionStateText is the text a state is stored as: the name of the enum value
// without its prefix.
func admissionStateText(state multipoolerservicepb.AdmissionState) string {
	return strings.TrimPrefix(state.String(), "ADMISSION_STATE_")
}

// parseAdmissionState reads a stored state; an unknown text is UNSPECIFIED, which no
// compare-and-set matches.
func parseAdmissionState(text string) multipoolerservicepb.AdmissionState {
	return multipoolerservicepb.AdmissionState(multipoolerservicepb.AdmissionState_value["ADMISSION_STATE_"+text])
}

// CompareAndSet is one statement. A tablegroup that has never been fenced has no row, and
// is UNFENCED at version zero: a fence from there inserts it, in its next state.
func (s *prototypeStore) CompareAndSet(ctx context.Context, database, tablegroup string, prevState multipoolerservicepb.AdmissionState, prevVersion int64, nextState multipoolerservicepb.AdmissionState, nextVersion int64) (bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return false, err
	}
	result, err := s.query(ctx, `WITH updated AS (
  UPDATE multigres.proto_tablegroup_serving
  SET state = $5, version = $6, updated_at = now()
  WHERE database = $1 AND tablegroup = $2 AND state = $3 AND version = $4
  RETURNING 1
), inserted AS (
  INSERT INTO multigres.proto_tablegroup_serving (database, tablegroup, state, version, updated_at)
  SELECT $1::text, $2::text, $5::text, $6::bigint, now()
  WHERE $3 = 'UNFENCED' AND $4 = 0
    AND NOT EXISTS (SELECT 1 FROM multigres.proto_tablegroup_serving WHERE database = $1 AND tablegroup = $2)
  ON CONFLICT DO NOTHING
  RETURNING 1
)
SELECT (SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted)`,
		database, tablegroup, admissionStateText(prevState), prevVersion, admissionStateText(nextState), nextVersion)
	if err != nil {
		return false, err
	}
	var n int64
	if err := executor.ScanSingleRow(result, &n); err != nil {
		return false, errors.Join(errors.New("decode the number of rows written"), err)
	}
	return n == 1, nil
}

func (s *prototypeStore) GetRouting(ctx context.Context, database string) (string, int64, bool, error) {
	result, err := s.query(ctx, "SELECT app_tablegroup, version FROM multigres.proto_routing WHERE database = $1", database)
	if isUndefinedTable(err) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, mterrors.Wrap(err, "read routing")
	}
	if len(result.Rows) == 0 {
		return "", 0, false, nil
	}
	var app string
	var version int64
	if err := executor.ScanSingleRow(result, &app, &version); err != nil {
		return "", 0, false, errors.Join(errors.New("decode routing"), err)
	}
	return app, version, true, nil
}

func (s *prototypeStore) MoveRouting(ctx context.Context, database, from, to string, expectedVersion int64) (string, int64, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return "", 0, false, err
	}
	// One statement, so the pointer and both admission checks commit atomically. Both
	// serving rows must be FENCED, a completed fence; FENCING is not. The rows are locked
	// FOR SHARE: neither can change under it.
	result, err := s.query(ctx, `WITH serving AS (
  SELECT state FROM multigres.proto_tablegroup_serving
  WHERE database = $1 AND tablegroup IN ($2, $3) FOR SHARE
), checked AS (
  SELECT count(*) FILTER (WHERE state = 'FENCED') = 2 AS both_fenced FROM serving
)
UPDATE multigres.proto_routing
SET app_tablegroup = $3, version = version + 1
WHERE database = $1 AND app_tablegroup = $2 AND version = $4 AND (SELECT both_fenced FROM checked)
RETURNING app_tablegroup, version`, database, from, to, expectedVersion)
	if err != nil {
		return "", 0, false, err
	}
	if len(result.Rows) == 0 {
		return "", 0, false, nil
	}
	var app string
	var version int64
	if err := executor.ScanSingleRow(result, &app, &version); err != nil {
		return "", 0, false, errors.Join(errors.New("decode routing"), err)
	}
	return app, version, true, nil
}
