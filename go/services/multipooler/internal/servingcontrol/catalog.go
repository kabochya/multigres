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

// Package servingcontrol owns migration routing and source connections on the target.
package servingcontrol

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Schema is idempotent so an existing managed database can enable the module.
// No schema or credentials are written on an external source.
const Schema = `CREATE TABLE IF NOT EXISTS multigres.migration_routing (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
 mode INT NOT NULL DEFAULT 0 CHECK (mode BETWEEN 0 AND 3),
 source_connection TEXT NOT NULL DEFAULT ''
);
INSERT INTO multigres.migration_routing(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS multigres.connections (
 name TEXT PRIMARY KEY, configuration TEXT NOT NULL
);
REVOKE ALL ON multigres.migration_routing, multigres.connections FROM PUBLIC;`

// Catalog operates on admin transactions. The owning manager checks leadership.
type Catalog struct {
	Queries executor.InternalQueryService
	key     []byte
	keyID   string
}

func New(q executor.InternalQueryService, key []byte) (*Catalog, error) {
	if len(key) != 32 {
		return nil, errors.New("migration key must contain exactly 32 bytes")
	}
	fingerprint := sha256.Sum256(key)
	return &Catalog{Queries: q, key: append([]byte(nil), key...), keyID: hex.EncodeToString(fingerprint[:8])}, nil
}

func (c *Catalog) Initialize(ctx context.Context) error {
	return c.Queries.QueryAdminMultiStatement(ctx, Schema)
}

func Read(ctx context.Context, tx executor.InternalTx) (*pb.MigrationRouting, error) {
	r, err := tx.Query(ctx, `SELECT mode, source_connection FROM multigres.migration_routing WHERE singleton FOR UPDATE`)
	if err != nil {
		return nil, err
	}
	var mode int32
	var name string
	if err = executor.ScanSingleRow(r, &mode, &name); err != nil {
		return nil, err
	}
	if mode < 0 || mode > 3 {
		return nil, errors.New("invalid migration mode in catalog")
	}
	return &pb.MigrationRouting{Mode: pb.MigrationMode(mode), SourceConnection: name}, nil
}

// Update runs the journal callback and routing update in one transaction.
// remote_apply prevents publishing ahead of the configured synchronous cohort.
func (c *Catalog) Update(ctx context.Context, update func(executor.InternalTx, *pb.MigrationRouting) error) error {
	tx, err := c.Queries.BeginAdmin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Query(ctx, `SET LOCAL synchronous_commit = 'remote_apply'`); err != nil {
		return err
	}
	state, err := Read(ctx, tx)
	if err != nil {
		return err
	}
	if err = update(tx, state); err != nil {
		return err
	}
	if state.Mode < 0 || state.Mode > 3 {
		return errors.New("invalid migration mode")
	}
	if _, err = tx.QueryArgs(ctx, `UPDATE multigres.migration_routing SET mode=$1, source_connection=$2 WHERE singleton`, int32(state.Mode), state.SourceConnection); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Catalog) Routing(ctx context.Context) (*pb.MigrationRouting, error) {
	r, err := c.Queries.QueryAdmin(ctx, `SELECT mode, source_connection FROM multigres.migration_routing WHERE singleton`)
	if err != nil {
		return nil, err
	}
	var mode int32
	var name string
	if err = executor.ScanSingleRow(r, &mode, &name); err != nil {
		return nil, err
	}
	return &pb.MigrationRouting{Mode: pb.MigrationMode(mode), SourceConnection: name}, nil
}

func ValidateConnection(v *rpc.SourceConnection) error {
	if v == nil || v.Name == "" || v.Host == "" || v.Database == "" || v.Username == "" || v.Port == 0 || v.Port > 65535 {
		return errors.New("connection name, host, database, username and valid port are required")
	}
	switch v.SslMode {
	case "disable", "require", "verify-ca", "verify-full":
	default:
		return errors.New("explicit source ssl_mode is required")
	}
	return nil
}

func (c *Catalog) seal(v *rpc.SourceConnection) (string, error) {
	if err := ValidateConnection(v); err != nil {
		return "", err
	}
	b, err := aes.NewCipher(c.key)
	if err != nil {
		return "", err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, g.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	plaintext, err := proto.Marshal(v)
	if err != nil {
		return "", err
	}
	ciphertext := g.Seal(nonce, nonce, plaintext, []byte(v.Name))
	return "v1." + c.keyID + "." + base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (c *Catalog) open(name, encoded string) (*rpc.SourceConnection, error) {
	b, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(encoded, ".")
	if len(parts) != 3 || parts[0] != "v1" || parts[1] != c.keyID {
		return nil, errors.New("source connection key unavailable")
	}
	data, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil || len(data) < g.NonceSize() {
		return nil, errors.New("invalid encrypted connection")
	}
	plain, err := g.Open(nil, data[:g.NonceSize()], data[g.NonceSize():], []byte(name))
	if err != nil {
		return nil, errors.New("cannot decrypt source connection")
	}
	result := &rpc.SourceConnection{}
	if err = proto.Unmarshal(plain, result); err != nil {
		return nil, errors.New("invalid source connection payload")
	}
	return result, ValidateConnection(result)
}

func (c *Catalog) Put(ctx context.Context, tx executor.InternalTx, v *rpc.SourceConnection, alter bool) error {
	encrypted, err := c.seal(v)
	if err != nil {
		return err
	}
	sql := `INSERT INTO multigres.connections(name,configuration) VALUES($1,$2)`
	if alter {
		sql = `UPDATE multigres.connections SET configuration=$2 WHERE name=$1 RETURNING name`
	}
	r, err := tx.QueryArgs(ctx, sql, v.Name, encrypted)
	if err != nil {
		return errors.New("cannot persist source connection")
	}
	if alter && len(r.Rows) == 0 {
		return errors.New("connection does not exist")
	}
	return nil
}

func (c *Catalog) Connection(ctx context.Context, name string) (*rpc.SourceConnection, error) {
	r, err := c.Queries.QueryAdminArgs(ctx, `SELECT configuration FROM multigres.connections WHERE name=$1`, name)
	if err != nil {
		return nil, errors.New("cannot read source connection")
	}
	var encoded string
	if err = executor.ScanSingleRow(r, &encoded); err != nil {
		return nil, errors.New("connection does not exist")
	}
	return c.open(name, encoded)
}
