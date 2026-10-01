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
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestExternalBackend(t *testing.T) {
	unmanaged := pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	for _, tc := range []struct {
		name, host, db, logical, socket, dir string
		port                                 int
		want                                 string
	}{
		{name: "mapped", host: "db.example.com", db: "physical", logical: "logical", port: 5432, want: "physical"},
		{name: "default database", host: "::1", logical: "logical", port: 5432, want: "logical"},
		{name: "missing host", logical: "db", port: 5432},
		{name: "dsn rejected", host: "postgres://db", logical: "db", port: 5432},
		{name: "socket conflict", host: "db", logical: "db", socket: "/tmp/socket", port: 5432},
		{name: "directory conflict", host: "db", logical: "db", dir: "/tmp/data", port: 5432},
		{name: "invalid port", host: "db", logical: "db", port: 65536},
		{name: "missing database", host: "db", port: 5432},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveExternalBackend(unmanaged, tc.host, tc.db, tc.logical, tc.port, tc.socket, tc.dir)
			if tc.want == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
	_, err := resolveExternalBackend(0, "db", "", "db", 5432, "", "")
	require.Error(t, err)
	_, err = resolveExternalBackend(0, "", "", "db", 5432, "/tmp/socket", "/tmp/data")
	require.NoError(t, err)
}
