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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestManagementMode(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  pb.PoolerManagementMode
	}{
		{"managed", pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_MANAGED},
		{"unmanaged", pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseManagementMode(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	for _, value := range []string{"", "MANAGED", "typo"} {
		_, err := parseManagementMode(value)
		require.Error(t, err)
	}
}

func TestUnmanagedStartupIsGated(t *testing.T) {
	mp := NewMultipooler(nil)
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(flags)
	require.Equal(t, "managed", mp.managementMode.Get())
	require.NoError(t, flags.Parse([]string{"--management-mode=unmanaged", "--backend-host=db.example.com"}))
	require.ErrorContains(t, mp.Init(context.Background()), "unmanaged serving is not implemented yet")
}
