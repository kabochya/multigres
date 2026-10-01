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

package ast

import "strings"

// ServingControlStmt is a standalone gateway administrative command. It is
// never sent as SQL to either PostgreSQL instance. Credentials stay in the
// structured payload; printable forms omit all connection values.
type ServingControlStmt struct {
	BaseNode
	Operation      string
	ConnectionName string
	OptionNames    []string
	OptionValues   []string
	RequestID      string
}

func (s *ServingControlStmt) StatementType() string { return "SERVING CONTROL" }
func (s *ServingControlStmt) SqlString() string {
	return strings.ToUpper(s.Operation) + " SERVING CONTROL [redacted]"
}
func (s *ServingControlStmt) String() string { return s.SqlString() }

func (s *ServingControlStmt) Option(name string) string {
	for i, key := range s.OptionNames {
		if key == name {
			return s.OptionValues[i]
		}
	}
	return ""
}
