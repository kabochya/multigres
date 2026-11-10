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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/topoclient"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Bootstrap retry timing. A pooler that starts before the default primary is reachable waits for
// it, trying again with a growing pause; it never serves, and never guesses, in the meantime.
var (
	bootstrapAttemptTimeout = 5 * time.Second
	bootstrapBackoffBase    = 100 * time.Millisecond
	bootstrapBackoffMax     = 5 * time.Second
)

// warnInsecureCredentialTransport warns when a backing connection with a password was fetched
// from the default primary over a gRPC transport without TLS: the credential of the external
// database then crossed the network in the clear. The credential is not logged.
func warnInsecureCredentialTransport(ctx context.Context, logger *slog.Logger, conn *backingConnection, tls bool) {
	if conn.Password == "" || tls {
		return
	}
	logger.WarnContext(ctx, "the password of the backing connection was fetched from the default primary over a gRPC transport without TLS; configure TLS between the poolers",
		"connection", conn.Name)
}

// defaultPrimarySource fetches a backing connection from the default primary of the
// database, found through topology.
type defaultPrimarySource struct {
	ts        topoclient.Store
	database  string
	transport grpc.DialOption
}

// TableGroupBinding asks the default primary how the tablegroup is served. What it replies is
// converted as it is: fetchBinding decides whether it is usable.
func (d *defaultPrimarySource) TableGroupBinding(ctx context.Context, tablegroupID int64) (*backingConnection, bool, error) {
	var (
		conn    *backingConnection
		managed bool
	)
	err := metadataclient.WithDefaultPrimary(ctx, d.ts, d.database, d.transport, func(client multipoolerservicepb.MultipoolerServiceClient) error {
		reply, err := client.GetBackingConnection(ctx, &multipoolerservicepb.GetBackingConnectionRequest{
			Database:     d.database,
			TablegroupId: tablegroupID,
		})
		if err != nil {
			return err
		}
		managed = reply.GetManaged()
		if c := reply.GetConnection(); c != nil {
			conn, err = backingConnectionFromProto(c)
		}
		return err
	})
	return conn, managed, err
}

// backingConnectionFromProto converts a connection returned by the default primary. A reply that
// names more than one server is refused, since the connection cannot hold it
// (backingconn.Endpoint); the rest is not validated here: fetchBinding validates whatever a
// source returns, once.
func backingConnectionFromProto(c *multipoolerservicepb.BackingConnection) (*backingConnection, error) {
	host, port, err := backingconn.Endpoint(c.GetHost(), c.GetPort())
	if err != nil {
		return nil, fmt.Errorf("%w: backing connection %q: %w", errInvalidBackingConnection, c.GetName(), err)
	}
	return &backingConnection{
		Name:           c.GetName(),
		Host:           host,
		Port:           port,
		Database:       c.GetDatabase(),
		User:           c.GetUser(),
		Password:       c.GetPassword(),
		SSLMode:        c.GetSslMode(),
		SSLRootCert:    c.GetSslRootCert(),
		SSLNegotiation: c.GetSslNegotiation(),
	}, nil
}

// retryBootstrap runs attempt until it succeeds or ctx ends, each try bounded in time and
// followed, when it fails, by a growing pause. It returns an error only when ctx ends, or when
// attempt's last failure is all there is to report because ctx ended during it.
func retryBootstrap(ctx context.Context, attempt func(context.Context) error, onFailure func(attempt int, err error)) error {
	for n := 0; ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, bootstrapAttemptTimeout)
		err := attempt(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if onFailure != nil {
			onFailure(n+1, err)
		}
		// The shift is capped so a long wait cannot overflow it.
		backoff := min(bootstrapBackoffBase<<min(n, 20), bootstrapBackoffMax)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
}
