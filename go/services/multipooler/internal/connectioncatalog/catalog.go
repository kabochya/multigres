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

// Package connectioncatalog stores immutable external-backend configuration on
// the managed authority. It does not choose routing or authorize admission.
package connectioncatalog

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

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/timeouts"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// Schema is installed only in managed PostgreSQL. Configuration contains an
// authenticated encrypted payload. Binding is an opaque, non-secret identity;
// it is not a hash of a password and does not order configurations.
const Schema = `CREATE TABLE IF NOT EXISTS multigres.connections (
 name TEXT PRIMARY KEY,
 configuration TEXT NOT NULL,
 binding TEXT NOT NULL UNIQUE
);
REVOKE ALL ON multigres.connections FROM PUBLIC;`

type Catalog struct {
	Queries executor.InternalQueryService
	key     []byte
	keyID   string
}

// Record must only be returned over the protected bootstrap channel.
var ErrNotFound = errors.New("connection does not exist")

type Record struct {
	Configuration *rpc.SourceConnection
	Binding       string
}

func New(q executor.InternalQueryService, key []byte) (*Catalog, error) {
	if len(key) != 32 {
		return nil, errors.New("connection key must contain exactly 32 bytes")
	}
	fingerprint := sha256.Sum256(key)
	return &Catalog{Queries: q, key: append([]byte(nil), key...), keyID: hex.EncodeToString(fingerprint[:8])}, nil
}

func ValidateConnection(v *rpc.SourceConnection) error {
	if v == nil || strings.TrimSpace(v.Name) == "" || strings.TrimSpace(v.Host) == "" || v.Database == "" || v.Username == "" || v.Port == 0 || v.Port > 65535 {
		return errors.New("connection name, host, database, username and valid port are required")
	}
	if strings.ContainsAny(v.Host, "/ @\t\n") {
		return errors.New("source host must be a hostname or IP address")
	}
	switch v.SslMode {
	case "disable", "require", "verify-ca", "verify-full":
	default:
		return errors.New("explicit source ssl_mode is required")
	}
	mode, err := client.ParseSSLMode(v.SslMode)
	if err != nil {
		return errors.New("invalid source TLS mode")
	}
	negotiation, err := client.ParseSSLNegotiation(v.SslNegotiation)
	if err != nil {
		return errors.New("invalid source TLS negotiation")
	}
	if err = client.ValidateSSLNegotiation(negotiation, mode); err != nil {
		return errors.New("source TLS negotiation requires TLS")
	}
	if (v.SslMode == "verify-ca" || v.SslMode == "verify-full") && v.SslRootCert == "" {
		return errors.New("source TLS verification requires a CA certificate")
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
	if result.Name != name {
		return nil, errors.New("source connection name does not match payload")
	}
	return result, ValidateConnection(result)
}

// Create participates in the caller's authority-checked transaction. There is
// deliberately no ALTER/upsert API: rotation and hot reconfiguration are out of
// scope. Existing names cannot be silently retargeted.
func (c *Catalog) Create(ctx context.Context, tx executor.InternalTx, v *rpc.SourceConnection) (*Record, error) {
	encoded, err := c.seal(v)
	if err != nil {
		return nil, err
	}
	binding := make([]byte, 32)
	if _, err = rand.Read(binding); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(binding)
	if _, err = tx.QueryArgs(ctx, `INSERT INTO multigres.connections(name,configuration,binding) VALUES($1,$2,$3)`, v.Name, encoded, id); err != nil {
		return nil, errors.New("cannot create source connection; names are immutable")
	}
	return &Record{Configuration: proto.Clone(v).(*rpc.SourceConnection), Binding: id}, nil
}

// ReadTx is for controller/authority transactions. A successful local read is
// not durability confirmation. Publish or bootstrap only after confirmed commit.
func (c *Catalog) ReadTx(ctx context.Context, tx executor.InternalTx, name string) (*Record, error) {
	r, err := tx.QueryArgs(ctx, `SELECT configuration,binding FROM multigres.connections WHERE name=$1 FOR UPDATE`, name)
	if err != nil {
		return nil, errors.New("cannot read source connection")
	}
	if len(r.Rows) == 0 {
		return nil, ErrNotFound
	}
	var encoded, binding string
	if err = executor.ScanSingleRow(r, &encoded, &binding); err != nil {
		return nil, errors.New("connection does not exist")
	}
	config, err := c.open(name, encoded)
	if err != nil {
		return nil, err
	}
	if binding == "" {
		return nil, errors.New("source configuration binding is missing")
	}
	return &Record{Configuration: config, Binding: binding}, nil
}

// Confirmed loads a protected snapshot and generates WAL under remote_apply,
// following confirmProposalQuorum. A failed or uncertain COMMIT returns no
// snapshot. A local reread cannot resolve that uncertainty, including after a
// pooler-only restart. Callers must establish current managed authority.
func (c *Catalog) Confirmed(ctx context.Context, name string) (*Record, error) {
	var record *Record
	err := c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
		var err error
		record, err = c.ReadTx(ctx, tx, name)
		if err != nil {
			return err
		}
		_, err = tx.QueryArgs(ctx, `UPDATE multigres.connections SET binding=binding WHERE name=$1`, name)
		return err
	})
	if err != nil {
		return nil, err
	}
	return record, nil
}

// Transaction is a durability primitive, not an authority check or coordinator.
// The owning service holds its existing leadership/action lock. Nothing inside
// the callback may wait on remote RPCs. An uncertain commit is propagated.
func (c *Catalog) Transaction(ctx context.Context, update func(context.Context, executor.InternalTx) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeouts.RuleWriteTimeout)
	defer cancel()
	tx, err := c.Queries.BeginAdmin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Query(ctx, `SET LOCAL synchronous_commit = 'remote_apply'`); err != nil {
		return err
	}
	if err = update(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Catalog) Initialize(ctx context.Context) error {
	return c.Transaction(ctx, func(ctx context.Context, tx executor.InternalTx) error {
		for statement := range strings.SplitSeq(Schema, ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, err := tx.Query(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
}
