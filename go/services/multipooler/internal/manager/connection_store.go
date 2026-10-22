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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protometadata"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// PROTOTYPE STUB: the directory of backing connections, and the binding of a tablegroup to
// one, registered as the pooler's connection directory. The connections are in the
// multigres.connection table and the binding is the backing_connection column of the existing
// multigres.tablegroup table. This side knows nothing of admission and routing.

// connectionStore reads the backing connections of the prototype from the default primary,
// through the admin pool.
type connectionStore struct {
	query func(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error)
	exec  func(ctx context.Context, sql string) error
	// schemaReady is set once the connection table and the binding column are known to
	// exist, so the statement that creates them runs once per process.
	schemaReady atomic.Bool
}

func newConnectionStore(pm *MultipoolerManager) *connectionStore {
	return &connectionStore{query: pm.adminQueryArgs, exec: pm.adminExec}
}

// connectionStores holds the store of each manager, built once.
var connectionStores sync.Map // *MultipoolerManager -> *connectionStore

func connectionStoreOf(pm *MultipoolerManager) *connectionStore {
	if s, ok := connectionStores.Load(pm); ok {
		return s.(*connectionStore)
	}
	s, _ := connectionStores.LoadOrStore(pm, newConnectionStore(pm))
	return s.(*connectionStore)
}

func init() {
	newConnectionDirectory = func(pm *MultipoolerManager) connectionDirectory { return connectionStoreOf(pm) }
}

// ensureSchema creates the connection table and the binding column if they do not exist; it is
// idempotent and one statement, and runs once per process once it succeeded.
func (s *connectionStore) ensureSchema(ctx context.Context) error {
	if s.schemaReady.Load() {
		return nil
	}
	if err := protometadata.EnsureConnectionSchema(ctx, s.exec); err != nil {
		return err
	}
	s.schemaReady.Store(true)
	return nil
}

// BackingConnection reads the named connection and decodes its stored form, a plaintext
// postgres:// URL, into the structured connection the contract carries.
func (s *connectionStore) BackingConnection(ctx context.Context, name string) (*multipoolerservicepb.BackingConnection, bool, error) {
	if err := s.ensureSchema(ctx); err != nil {
		return nil, false, err
	}
	result, err := s.query(ctx, "SELECT dsn, expected_system_identifier FROM multigres.connection WHERE name = $1", name)
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

// TablegroupBinding reads the connection the named tablegroup is bound to. found is false when
// there is no such tablegroup; the name is empty when the tablegroup is served by its managed
// cohort. Like every read it creates nothing: a database whose tablegroup table has no binding
// column has never bound a tablegroup to a connection, so every tablegroup is managed.
func (s *connectionStore) TablegroupBinding(ctx context.Context, tablegroup string) (string, bool, error) {
	result, err := s.query(ctx, "SELECT COALESCE(backing_connection, '') FROM multigres.tablegroup WHERE name = $1", tablegroup)
	if isUndefinedColumn(err) {
		result, err = s.query(ctx, "SELECT '' FROM multigres.tablegroup WHERE name = $1", tablegroup)
	}
	if err != nil {
		return "", false, mterrors.Wrap(err, "read tablegroup binding")
	}
	if len(result.Rows) == 0 {
		return "", false, nil
	}
	var name string
	if err := executor.ScanSingleRow(result, &name); err != nil {
		return "", false, errors.Join(errors.New("decode tablegroup binding"), err)
	}
	return name, true, nil
}

// isUndefinedColumn reports whether err is PostgreSQL's undefined_column.
func isUndefinedColumn(err error) bool {
	var diag *mterrors.PgDiagnostic
	return errors.As(err, &diag) && diag.Code == mterrors.PgSSUndefinedColumn
}
