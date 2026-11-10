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
	"fmt"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// connectionStore reads how a tablegroup is served from the sharding catalog on the default
// primary, through the admin pool: the tablegroup row, and the named connection of
// multigres.connections it is bound to, the table the migrator shares.
type connectionStore struct {
	queryService func() executor.InternalQueryService
}

func newConnectionStore(pm *MultipoolerManager) *connectionStore {
	return &connectionStore{queryService: pm.internalQueryService}
}

// TableGroupBinding reads the tablegroup's row and the connection it is bound to, and decodes
// the stored form of that connection, a libpq connection string in the dsn column (a
// postgres:// URI or keyword=value pairs, the form the migrator stores), into the structured
// connection the contract carries. A dsn that does not name exactly one server is refused. A
// managed tablegroup has no connection, and one being dropped is served to no one.
//
// A failure to read is UNAVAILABLE when the query service is missing or the connection to the
// database is lost, which the caller retries; anything else is passed on.
func (s *connectionStore) TableGroupBinding(ctx context.Context, tablegroupID int64) (*multipoolerservicepb.BackingConnection, bool, error) {
	qs := s.queryService()
	if qs == nil {
		return nil, false, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "internal query service not available")
	}
	res, err := qs.QueryAdminArgs(ctx,
		"SELECT t.state, c.connection_id IS NOT NULL, coalesce(c.name, ''), coalesce(c.dsn, '') FROM multigres.tablegroup t "+
			"LEFT JOIN multigres.connections c ON c.connection_id = t.backing_connection_id WHERE t.tablegroup_id = $1", tablegroupID)
	switch {
	case mterrors.IsConnectionError(err):
		return nil, false, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "read tablegroup %d: %v", tablegroupID, err)
	case err != nil:
		return nil, false, err
	}
	if len(res.Rows) == 0 {
		return nil, false, nil
	}
	var state, name, dsn string
	var bound bool
	if err := executor.ScanRow(res.Rows[0], &state, &bound, &name, &dsn); err != nil {
		return nil, false, mterrors.Wrap(err, "read tablegroup row")
	}
	if state == tableGroupStateDropping {
		return nil, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %d is being dropped", tablegroupID)
	}
	if !bound {
		return nil, true, nil
	}
	conn, err := parseBackingDSN(dsn)
	if err != nil {
		return nil, false, mterrors.Wrap(err, fmt.Sprintf("connection %q", name))
	}
	conn.Name = name
	return conn, true, nil
}

// errInvalidBackingDSN is a stored dsn that cannot name the one server a backing connection
// must. It is a fact about the persisted connection, not a transient failure, so it is
// FAILED_PRECONDITION. The message never carries a value of the dsn, which holds the password:
// the errors of backingconn.ParseDSN name at most a libpq parameter, and the rest are fixed text.
func errInvalidBackingDSN(format string, args ...any) error {
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "backing connection dsn is not usable: "+format, args...)
}

// parseBackingDSN decodes a stored libpq connection string, in either form, with the strict
// parser of backingconn (see backingconn.ParseDSN for what it refuses and why), and checks that
// it names one server by the rules the pooler applies to the connection it receives
// (backingconn.Endpoint, backingconn.Validate), so that the default primary never serves a
// connection that the pooler refuses.
func parseBackingDSN(raw string) (*multipoolerservicepb.BackingConnection, error) {
	conn, err := backingconn.ParseDSN(raw)
	if err != nil {
		return nil, errInvalidBackingDSN("%s", err)
	}
	host, port, err := backingconn.Endpoint(conn.Hosts, conn.Ports)
	if err == nil {
		err = backingconn.Validate(host, port, conn.Database, conn.User)
	}
	if err != nil {
		return nil, errInvalidBackingDSN("%s", err)
	}
	ports := make([]int32, len(conn.Ports))
	for i, p := range conn.Ports {
		ports[i] = int32(p)
	}
	return &multipoolerservicepb.BackingConnection{
		Host:           conn.Hosts,
		Port:           ports,
		Database:       conn.Database,
		User:           conn.User,
		Password:       conn.Password,
		SslMode:        conn.SSLMode,
		SslRootCert:    conn.SSLRootCert,
		SslNegotiation: conn.SSLNegotiation,
	}, nil
}
