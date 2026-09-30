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

package parser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

func TestServingControlGrammarAndRedaction(t *testing.T) {
	valid := []string{
		"CREATE CONNECTION source WITH (host='127.0.0.1', port='5432', database='postgres', username='postgres', password='secret') REQUEST ID 'create-1'",
		"ALTER CONNECTION source WITH (password=$$new-secret$$) REQUEST ID 'alter-1';",
		"ATTACH CONNECTION source REQUEST ID 'attach-1'", "DETACH CONNECTION source REQUEST ID 'detach-1'",
		"PAUSE SERVING REQUEST ID 'pause-1'", "RESUME SERVING REQUEST ID 'resume-1'", "SHOW SERVING MODE",
		`/* comment */ CREATE CONNECTION "Mixed" WITH (password='a''b') REQUEST ID 'c'`,
	}
	for _, sql := range valid {
		t.Run(sql, func(t *testing.T) {
			stmts, err := ParseSQL(sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			s := stmts[0].(*ast.ServingControlStmt)
			require.NotContains(t, s.SqlString(), "secret")
			require.NotContains(t, RedactServingSQL(sql), "secret")
			require.Equal(t, s, ast.CloneNode(s))
		})
	}
	invalid := []string{
		"ATTACH CONNECTION source", "PAUSE SERVING REQUEST ID ''", "RESUME SERVING MANAGED REQUEST ID 'a'",
		"CREATE CONNECTION source WITH (password='secret',password='other') REQUEST ID 'a'",
		"ALTER CONNECTION source WITH (invalid='secret') REQUEST ID 'a'",
		"PAUSE SERVING REQUEST ID 'a'; SELECT 1", "SHOW SERVING MODE WHERE true",
		"CREATE CONNECTION source WITH (password='unterminated-secret",
	}
	for _, sql := range invalid {
		_, err := ParseSQL(sql)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
		require.NotContains(t, RedactServingSQL(sql), "secret")
	}
	require.Equal(t, "SERVING CONTROL [redacted]", RedactServingSQL("SELECT 1; CREATE CONNECTION source WITH (password='secret')"))
	regular := "SELECT 'CREATE CONNECTION source'"
	require.Equal(t, regular, RedactServingSQL(regular))
}
