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
}

// Validate checks that the connection is complete enough to dial.
func (c *backingConnection) Validate() error {
	if strings.TrimSpace(c.Host) == "" || strings.ContainsAny(c.Host, "/ @\t\n") {
		return errors.New("backing connection requires a hostname or IP address host")
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

// parseManagementMode converts the --management-mode flag value.
func parseManagementMode(value string) (clustermetadatapb.PoolerManagementMode, error) {
	switch value {
	case "managed":
		return clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED, nil
	case "unmanaged":
		return clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, nil
	default:
		return 0, fmt.Errorf("invalid management-mode %q: expected managed or unmanaged", value)
	}
}

// validateBackendFlags enforces the flag contract between management modes. An
// unmanaged pooler is configured only through --backing-connection; the flags
// that describe a pgctld-managed local postgres are rejected rather than
// silently ignored. A managed pooler must not name a backing connection.
func validateBackendFlags(mode clustermetadatapb.PoolerManagementMode, backingConnection string, explicit func(name string) bool) error {
	if mode != clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
		if backingConnection != "" {
			return errors.New("--backing-connection requires --management-mode=unmanaged")
		}
		return nil
	}
	if backingConnection == "" {
		return errors.New("--management-mode=unmanaged requires --backing-connection")
	}
	for _, name := range []string{"socket-file", "pooler-dir", "pg-port"} {
		if explicit(name) {
			return fmt.Errorf("--%s describes a managed postgres and cannot be combined with --management-mode=unmanaged; the endpoint comes from --backing-connection", name)
		}
	}
	return nil
}
