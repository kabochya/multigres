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
	"strings"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// backingConnection is the resolved configuration of the external PostgreSQL an
// unmanaged pooler fronts. It is loaded by name (--backing-connection); no
// endpoint or credential is ever supplied directly on the command line.
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

	// ExpectedSystemIdentifier is the pg_control_system() identifier the
	// endpoint must report.
	ExpectedSystemIdentifier string
}

// Validate checks that the connection is complete enough to dial.
func (c *backingConnection) Validate() error {
	// One server: the pooler's admin connection must reach the same server as its
	// application connections, because what a fence concludes from looking at the backends
	// of the server it reaches has to concern the server the application uses. A list of
	// hosts, a path or credentials in the host could send the two apart.
	if strings.TrimSpace(c.Host) == "" || strings.ContainsAny(c.Host, "/ @,\t\n") {
		return errors.New("backing connection requires a single hostname or IP address host")
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("backing connection port must be between 1 and 65535, got %d", c.Port)
	}
	if c.Database == "" {
		return errors.New("backing connection requires a database")
	}
	if c.User == "" {
		return errors.New("backing connection requires a user")
	}
	return nil
}

// connectionSource gives a pooler the configuration of the external PostgreSQL it
// fronts, by the name given in --backing-connection. The pooler's own source asks the
// default primary of its database.
type connectionSource interface {
	BackingConnection(ctx context.Context, name string) (*backingConnection, error)
}

// errInvalidBackingConnection marks a connection that a source returned and that can never be
// used as it is: none at all, incomplete, or naming more than one server. Asking again gets
// the same answer.
var errInvalidBackingConnection = errors.New("invalid backing connection")

// fetchBackingConnection asks a source for a connection and returns it only if it is usable:
// a source that answers with nothing, or with a connection that is not complete or names more
// than one server, is refused, whoever the source is.
func fetchBackingConnection(ctx context.Context, source connectionSource, name string) (*backingConnection, error) {
	conn, err := source.BackingConnection(ctx, name)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, fmt.Errorf("%w: backing connection %q: the source returned none", errInvalidBackingConnection, name)
	}
	if err := conn.Validate(); err != nil {
		return nil, fmt.Errorf("%w: backing connection %q: %w", errInvalidBackingConnection, name, err)
	}
	return conn, nil
}

// managementModeFor derives the pooler's management mode. A pooler that names a
// backing connection fronts an external PostgreSQL and is unmanaged; every
// other pooler owns its postgres through pgctld and is managed.
func managementModeFor(backingConnection string) clustermetadatapb.PoolerManagementMode {
	if backingConnection != "" {
		return clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	}
	return clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED
}

// validateBackendFlags rejects flags that describe a pgctld-managed local
// postgres when --backing-connection is set, rather than silently ignoring
// them: the endpoint of an unmanaged pooler comes only from its backing
// connection.
func validateBackendFlags(backingConnection string, explicit func(name string) bool) error {
	if backingConnection == "" {
		return nil
	}
	for _, name := range []string{"socket-file", "pooler-dir", "pg-port"} {
		if explicit(name) {
			return fmt.Errorf("--%s describes a managed postgres and cannot be combined with --backing-connection; the endpoint comes from the backing connection", name)
		}
	}
	return nil
}
