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

func TestParseManagementMode(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  clustermetadatapb.PoolerManagementMode
	}{
		{"managed", clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED},
		{"unmanaged", clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseManagementMode(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, value := range []string{"", "MANAGED", "typo"} {
		_, err := parseManagementMode(value)
		require.Error(t, err, value)
	}
}

func TestValidateBackendFlags(t *testing.T) {
	managed := clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED
	unmanaged := clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	none := func(string) bool { return false }
	only := func(name string) func(string) bool { return func(n string) bool { return n == name } }

	require.NoError(t, validateBackendFlags(managed, "", none))
	require.NoError(t, validateBackendFlags(managed, "", only("pg-port")))
	require.ErrorContains(t, validateBackendFlags(managed, "src", none), "requires --management-mode=unmanaged")

	require.NoError(t, validateBackendFlags(unmanaged, "src", none))
	require.ErrorContains(t, validateBackendFlags(unmanaged, "", none), "requires --backing-connection")
	for _, name := range []string{"socket-file", "pooler-dir", "pg-port"} {
		require.ErrorContains(t, validateBackendFlags(unmanaged, "src", only(name)), "--"+name, name)
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

func TestResolveManagementModeRejectsDirectBackendFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"missing connection", []string{"--management-mode=unmanaged"}, "requires --backing-connection"},
		{"socket file", []string{"--management-mode=unmanaged", "--backing-connection=src", "--socket-file=/tmp/s"}, "--socket-file"},
		{"pooler dir", []string{"--management-mode=unmanaged", "--backing-connection=src", "--pooler-dir=/tmp/p"}, "--pooler-dir"},
		{"pg port", []string{"--management-mode=unmanaged", "--backing-connection=src", "--pg-port=5432"}, "--pg-port"},
		{"connection on managed", []string{"--backing-connection=src"}, "requires --management-mode=unmanaged"},
		{"bad mode", []string{"--management-mode=typo"}, "invalid management-mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := NewMultipooler(nil)
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			mp.RegisterFlags(flags)
			require.Equal(t, "managed", mp.managementMode.Get())
			require.NoError(t, flags.Parse(append([]string{"--database=app", "--table-group=migrateTG", "--shard=0-inf", "--cell=zone1"}, tc.args...)))
			_, err := mp.resolveManagementMode()
			require.ErrorContains(t, err, tc.want)
		})
	}
}
