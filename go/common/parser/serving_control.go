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
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// Serving SQL extends the entry point with a small utility grammar, using the
// existing PostgreSQL lexer for quoting, comments and string-literal semantics.
// Commands must stand alone; they do not participate in client transactions.
func servingPrefix(l *Lexer) bool {
	a, b := l.NextToken(), l.NextToken()
	first, second := strings.ToLower(a.Value.Str), strings.ToLower(b.Value.Str)
	return ((first == "create" || first == "alter" || first == "attach" || first == "detach") && second == "connection") ||
		((first == "pause" || first == "resume" || first == "show") && second == "serving")
}

// RedactServingSQL also covers malformed commands, before any logger sees them.
func RedactServingSQL(input string) string {
	lower := strings.ToLower(input)
	if !strings.Contains(lower, "connection") && !strings.Contains(lower, "serving") {
		return input
	}
	l := NewLexer(input)
	var previous string
	for token := l.NextToken(); token.Type != EOF && token.Type != INVALID; token = l.NextToken() {
		current := strings.ToLower(token.Value.Str)
		if token.Type == SCONST {
			previous = ""
			continue
		}
		if ((previous == "create" || previous == "alter" || previous == "attach" || previous == "detach") && current == "connection") || ((previous == "pause" || previous == "resume" || previous == "show") && current == "serving") {
			return "SERVING CONTROL [redacted]"
		}
		previous = current
	}
	return input
}

func parseServingSQL(input string, opts *ParseOptions) ([]ast.Stmt, bool, error) {
	if !servingPrefix(NewLexerWithOptions(input, opts)) {
		return nil, false, nil
	}
	l := NewLexerWithOptions(input, opts)
	var tokens []*Token
	for t := l.NextToken(); t.Type != EOF; t = l.NextToken() {
		if t.Type == INVALID {
			return nil, true, &ParseSyntaxError{Message: "invalid serving-control statement"}
		}
		tokens = append(tokens, t)
	}
	invalid := func() ([]ast.Stmt, bool, error) {
		return nil, true, &ParseSyntaxError{Message: "invalid serving-control statement"}
	}
	if l.HasErrors() {
		return invalid()
	}
	if len(tokens) > 0 && tokens[len(tokens)-1].Type == ';' {
		tokens = tokens[:len(tokens)-1]
	}
	i := 0
	word := func(w string) bool {
		if i < len(tokens) && strings.EqualFold(tokens[i].Value.Str, w) && tokens[i].Type != SCONST {
			i++
			return true
		}
		return false
	}
	punct := func(t int) bool {
		if i < len(tokens) && tokens[i].Type == t {
			i++
			return true
		}
		return false
	}
	ident := func() (string, bool) {
		if i < len(tokens) && (tokens[i].Type == IDENT || tokens[i].Value.Keyword != "" && LookupKeyword(tokens[i].Value.Keyword).Category == UnreservedKeyword) {
			v := tokens[i].Value.Str
			i++
			return v, v != ""
		}
		return "", false
	}
	literal := func() (string, bool) {
		if i < len(tokens) && tokens[i].Type == SCONST {
			v := tokens[i].Value.Str
			i++
			return v, true
		}
		return "", false
	}
	if len(tokens) < 2 {
		return invalid()
	}
	op := strings.ToLower(tokens[0].Value.Str)
	i = 2
	stmt := &ast.ServingControlStmt{BaseNode: ast.BaseNode{Tag: ast.T_ServingControlStmt}, Operation: op}
	if op == "show" {
		if !word("mode") || i != len(tokens) {
			return invalid()
		}
		return []ast.Stmt{stmt}, true, nil
	}
	if op == "create" || op == "alter" || op == "attach" || op == "detach" {
		var ok bool
		stmt.ConnectionName, ok = ident()
		if !ok {
			return invalid()
		}
	}
	if op == "create" || op == "alter" {
		if !word("with") || !punct('(') {
			return invalid()
		}
		seen := make(map[string]bool)
		allowed := map[string]bool{"host": true, "port": true, "database": true, "username": true, "password": true, "sslmode": true, "sslrootcert": true, "sslnegotiation": true}
		for {
			if i >= len(tokens) {
				return invalid()
			}
			// Option names may be PostgreSQL keywords (database, user, ...).
			key := strings.ToLower(tokens[i].Value.Str)
			i++
			if !allowed[key] || tokens[i-1].Type == SCONST || !punct('=') {
				return invalid()
			}
			value, ok := literal()
			if !ok {
				return invalid()
			}
			if seen[key] {
				return invalid()
			}
			seen[key] = true
			stmt.OptionNames = append(stmt.OptionNames, key)
			stmt.OptionValues = append(stmt.OptionValues, value)
			if punct(')') {
				break
			}
			if !punct(',') {
				return invalid()
			}
		}
	}
	if !word("request") || !word("id") {
		return invalid()
	}
	var ok bool
	stmt.RequestID, ok = literal()
	if !ok || stmt.RequestID == "" || len(stmt.RequestID) > 128 || i != len(tokens) {
		return invalid()
	}
	return []ast.Stmt{stmt}, true, nil
}
