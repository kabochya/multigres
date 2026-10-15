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
	"net/url"
	"strconv"
	"strings"
)

// PROTOTYPE STUB: backing connections are stored as postgres:// URLs.
//
// The prototype connection table keeps one plaintext URL per connection. Delete
// this file when connections move to the encrypted catalog, which stores their
// fields separately.

// parseBackingURL parses a stored connection URL:
//
//	postgres://user:password@host:port/database?sslmode=...&sslrootcert=...&sslnegotiation=...
func parseBackingURL(name, raw string) (*backingConnection, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("backing connection URL is not valid")
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, errors.New("backing connection URL must use the postgres scheme")
	}
	// The pooler's admin connection must reach the same server as its application
	// connections: fence safety looks at the backends of that one server. A list of
	// hosts, a host given as a parameter, or a target_session_attrs choice could
	// send the two to different servers.
	if strings.Contains(u.Host, ",") {
		return nil, errors.New("backing connection URL names more than one host; a single server is required")
	}
	for _, param := range []string{"host", "hostaddr", "port", "target_session_attrs", "load_balance_hosts"} {
		if u.Query().Has(param) {
			return nil, fmt.Errorf("backing connection URL sets %q as a parameter; a single server given in the URL authority is required", param)
		}
	}
	port := 5432
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return nil, errors.New("backing connection URL has an invalid port")
		}
	}
	password, _ := u.User.Password()
	q := u.Query()
	conn := &backingConnection{
		Name:           name,
		Host:           u.Hostname(),
		Port:           port,
		Database:       u.Path[min(1, len(u.Path)):],
		User:           u.User.Username(),
		Password:       password,
		SSLMode:        q.Get("sslmode"),
		SSLRootCert:    q.Get("sslrootcert"),
		SSLNegotiation: q.Get("sslnegotiation"),
	}
	if err := conn.Validate(); err != nil {
		return nil, err
	}
	return conn, nil
}
