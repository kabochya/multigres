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

package multipooler

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/common/backingconn"
)

// backingConnection is the resolved configuration of the external PostgreSQL an
// unmanaged pooler fronts. It is read from the sharding catalog, by the id of the pooler's
// tablegroup; no endpoint or credential is ever supplied directly on the command line.
type backingConnection struct {
	Name           string
	Host           string
	Port           int
	Database       string
	User           string
	Password       string
	SSLMode        string
	SSLRootCert    string
	SSLNegotiation string
}

// Validate checks that the connection is complete enough to dial, by the rules the default
// primary applies to the connections it serves (backingconn.Validate).
func (c *backingConnection) Validate() error {
	return backingconn.Validate(c.Host, c.Port, c.Database, c.User)
}

// checkBackingDatabase refuses a backing connection that serves another database than the
// pooler's own: the pooler advertises --database to the cluster and dials the connection's.
func checkBackingDatabase(conn *backingConnection, database string) error {
	if conn.Database != database {
		return fmt.Errorf("backing connection %q serves database %q, which does not match --database %q", conn.Name, conn.Database, database)
	}
	return nil
}

// connectionSource tells a pooler how its tablegroup is served, by the tablegroup's catalog id:
// by the external PostgreSQL whose connection it returns, or by managed shards. The pooler's own
// source asks the default primary of its database.
type connectionSource interface {
	// TableGroupBinding returns the connection of the tablegroup, or managed when the tablegroup
	// has none. A source that says neither has said nothing: it is never taken for managed.
	TableGroupBinding(ctx context.Context, tablegroupID int64) (conn *backingConnection, managed bool, err error)
}

// errInvalidBackingConnection marks an answer of a source that can never be used as it is: it
// says nothing, or its connection is incomplete or names more than one server. Asking again
// gets the same answer until the catalog is corrected.
var errInvalidBackingConnection = errors.New("invalid backing connection")

// fetchBinding asks a source how a tablegroup is served and returns the answer only if it is
// usable: a source that answers with nothing, or with a connection that is not complete or names
// more than one server, is refused, whoever the source is.
func fetchBinding(ctx context.Context, source connectionSource, tablegroupID int64) (conn *backingConnection, managed bool, err error) {
	conn, managed, err = source.TableGroupBinding(ctx, tablegroupID)
	switch {
	case err != nil:
		return nil, false, err
	case managed && conn != nil:
		return nil, false, fmt.Errorf("%w: tablegroup %d: the source returned a connection for a managed tablegroup", errInvalidBackingConnection, tablegroupID)
	case managed:
		return nil, true, nil
	case conn == nil:
		return nil, false, fmt.Errorf("%w: tablegroup %d: the source said neither that it is managed nor which connection serves it", errInvalidBackingConnection, tablegroupID)
	}
	if err := conn.Validate(); err != nil {
		return nil, false, fmt.Errorf("%w: backing connection %q: %w", errInvalidBackingConnection, conn.Name, err)
	}
	return conn, false, nil
}

// The flags that describe a postgres managed through pgctld. A managed pooler adopts the first
// two groups from pgctld's Status unless they are set (adoptPgctldValues); an unmanaged pooler
// fronts an external postgres that has no pgctld, no pgBackRest and no data directory of its own,
// so it refuses every one of them (validateBackendFlags).
var (
	// pgctldRequiredFlags are the identity of the postgres pgctld runs: Init fails when pgctld
	// is unreachable and these are not set.
	pgctldRequiredFlags = []string{"pg-port", "pooler-dir"}
	// pgctldOptionalFlags have workable defaults: Init starts degraded when pgctld is
	// unreachable and these are not set.
	pgctldOptionalFlags = []string{"pgbackrest-port", "pgbackrest-cert-file", "pgbackrest-key-file", "pgbackrest-ca-file"}
	// managedOnlyFlags are never adopted from pgctld and still mean nothing without it.
	managedOnlyFlags = []string{"socket-file", "pgctld-addr", "pgbackrest-cipher-key-file"}
)

// managedPostgresFlags returns every flag that belongs to pgctld or to a postgres it manages.
func managedPostgresFlags() []string {
	return slices.Concat(pgctldRequiredFlags, pgctldOptionalFlags, managedOnlyFlags)
}

// validateBackendFlags rejects flags that describe a pgctld-managed local postgres when the
// pooler is unmanaged, rather than silently ignoring them: the endpoint of an unmanaged pooler
// comes only from the backing connection its tablegroup is bound to in the catalog, and it has no
// pgctld or backups to configure.
func validateBackendFlags(unmanaged bool, explicit func(name string) bool) error {
	if !unmanaged {
		return nil
	}
	for _, name := range managedPostgresFlags() {
		if explicit(name) {
			return fmt.Errorf("--%s configures a pgctld-managed postgres, but this pooler is unmanaged: its tablegroup is bound to a backing connection in the catalog, so it takes its endpoint from there and has no pgctld or backups", name)
		}
	}
	return nil
}
