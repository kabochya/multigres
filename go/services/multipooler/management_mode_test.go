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

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestManagementModeFor(t *testing.T) {
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED, managementModeFor(""))
	require.Equal(t, clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED, managementModeFor("src"))
}

func TestValidateBackendFlags(t *testing.T) {
	none := func(string) bool { return false }
	only := func(name string) func(string) bool { return func(n string) bool { return n == name } }

	// Managed poolers may set any of the local-postgres flags.
	require.NoError(t, validateBackendFlags("", none))
	require.NoError(t, validateBackendFlags("", only("pg-port")))

	require.NoError(t, validateBackendFlags("src", none))
	for _, name := range []string{"socket-file", "pooler-dir", "pg-port"} {
		require.ErrorContains(t, validateBackendFlags("src", only(name)), "--"+name, name)
	}
}

func TestBackingConnectionValidate(t *testing.T) {
	good := backingConnection{Host: "db.example.com", Port: 5432, Database: "app", User: "u"}
	require.NoError(t, good.Validate())
	for name, mutate := range map[string]func(*backingConnection){
		"empty host":      func(c *backingConnection) { c.Host = "" },
		"host with path":  func(c *backingConnection) { c.Host = "/tmp/socket" },
		"host with creds": func(c *backingConnection) { c.Host = "u@db" },
		"port zero":       func(c *backingConnection) { c.Port = 0 },
		"port too large":  func(c *backingConnection) { c.Port = 65536 },
		"no database":     func(c *backingConnection) { c.Database = "" },
		"no user":         func(c *backingConnection) { c.User = "" },
	} {
		c := good
		mutate(&c)
		require.Error(t, c.Validate(), name)
	}
}

func TestResolveBackingConnectionStub(t *testing.T) {
	t.Setenv(stubBackingConnectionEnv, "postgres://alice:s3cret@db.example.com:6543/app?sslmode=verify-full&sslrootcert=/ca.pem&sslnegotiation=direct")
	got, err := resolveBackingConnectionStub("src")
	require.NoError(t, err)
	require.Equal(t, &backingConnection{
		Name: "src", Host: "db.example.com", Port: 6543, Database: "app", User: "alice", Password: "s3cret",
		SSLMode: "verify-full", SSLRootCert: "/ca.pem", SSLNegotiation: "direct",
	}, got)

	t.Setenv(stubBackingConnectionEnv, "postgres://alice@db.example.com/app")
	got, err = resolveBackingConnectionStub("src")
	require.NoError(t, err)
	require.Equal(t, 5432, got.Port)

	for _, bad := range []string{"", "mysql://u@h/db", "postgres://u@h:notaport/db", "postgres://h/db", "postgres://u@h"} {
		t.Setenv(stubBackingConnectionEnv, bad)
		_, err := resolveBackingConnectionStub("src")
		require.Error(t, err, bad)
	}
}

func TestResolveManagementMode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    clustermetadatapb.PoolerManagementMode
		wantErr string
	}{
		{name: "managed by default", want: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED},
		{name: "managed keeps local flags", args: []string{"--pg-port=5433", "--pooler-dir=/tmp/p"}, want: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED},
		{name: "backing connection", args: []string{"--backing-connection=src"}, want: clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED},
		{name: "socket file", args: []string{"--backing-connection=src", "--socket-file=/tmp/s"}, wantErr: "--socket-file"},
		{name: "pooler dir", args: []string{"--backing-connection=src", "--pooler-dir=/tmp/p"}, wantErr: "--pooler-dir"},
		{name: "pg port", args: []string{"--backing-connection=src", "--pg-port=5432"}, wantErr: "--pg-port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := NewMultipooler(nil)
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			mp.RegisterFlags(flags)
			require.NoError(t, flags.Parse(tc.args))
			got, err := mp.resolveManagementMode()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
