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
	"strings"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// resolveExternalBackend validates unmanaged-only inputs without touching pgctld.
func resolveExternalBackend(mode pb.PoolerManagementMode, host, database, logicalDatabase string, port int, socket, poolerDir string) (string, error) {
	if mode != pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED {
		if host != "" || database != "" {
			return "", errors.New("backend-host and backend-database require unmanaged mode")
		}
		return "", nil
	}
	if strings.TrimSpace(host) == "" || strings.ContainsAny(host, "/ @\t\n") {
		return "", errors.New("unmanaged mode requires a backend-host hostname or IP address")
	}
	if port < 1 || port > 65535 {
		return "", errors.New("pg-port must be between 1 and 65535")
	}
	if socket != "" || poolerDir != "" {
		return "", errors.New("unmanaged mode does not support socket-file or pooler-dir")
	}
	if database == "" {
		database = logicalDatabase
	}
	if database == "" {
		return "", errors.New("backend database is required")
	}
	return database, nil
}
