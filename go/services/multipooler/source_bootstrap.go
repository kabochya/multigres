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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

// Bootstrap retry bounds. A restarted pooler may come up before the default
// primary is reachable; it retries for a bounded time and then exits so its
// supervisor restarts it. It never serves in the meantime.
var (
	bootstrapAttempts       = 8
	bootstrapAttemptTimeout = 5 * time.Second
	bootstrapBackoffBase    = 100 * time.Millisecond
	bootstrapBackoffMax     = 2 * time.Second
)

// bootstrapBackingConnection runs the first steps of an unmanaged pooler's
// bootstrap, in the order the design requires:
//
//  1. register in topology as DISABLED/UNMANAGED, before any metadata read, so
//     a coordinator that snapshots topology can see this pooler;
//  2. discover the default primary through topology;
//  3. fetch the named backing connection from it.
//
// A failure returns an error and the pooler never opens. initial is the
// pooler's topology record; it must already carry identity, ports and the
// UNMANAGED mode.
func bootstrapBackingConnection(ctx context.Context, ts topoclient.Store, initial *clustermetadatapb.Multipooler, name string, source connectionSource, logger *slog.Logger) (*backingConnection, error) {
	if err := requireSingleUnmanagedPooler(ctx, ts, initial); err != nil {
		return nil, err
	}
	if err := manager.RegisterPreparing(ctx, ts, initial); err != nil {
		return nil, fmt.Errorf("register preparing pooler: %w", err)
	}

	var conn *backingConnection
	err := retryBootstrap(ctx, func(ctx context.Context) error {
		var err error
		conn, err = fetchBackingConnection(ctx, source, name)
		return err
	}, func(attempt int, err error) {
		logger.WarnContext(ctx, "backing connection bootstrap attempt failed", "attempt", attempt, "error", err)
	})
	if err != nil {
		// Do not leave a DISABLED entry behind for a pooler that is about to exit:
		// it would keep gateways and coordinators waiting on it.
		markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if markErr := manager.MarkPreparingFailed(markCtx, ts, initial); markErr != nil {
			logger.WarnContext(ctx, "could not mark the failed pooler as stopped", "error", markErr)
		}
		return nil, errors.Join(errors.New("backing connection bootstrap unavailable; pooler stays closed"), err)
	}
	return conn, nil
}

// defaultPrimarySource fetches a backing connection from the default primary of the
// database, found through topology.
type defaultPrimarySource struct {
	ts        topoclient.Store
	database  string
	transport grpc.DialOption
}

func (d *defaultPrimarySource) BackingConnection(ctx context.Context, name string) (*backingConnection, error) {
	var conn *backingConnection
	err := metadataclient.WithDefaultPrimary(ctx, d.ts, d.database, d.transport, func(client multipoolerservicepb.MultipoolerServiceClient) error {
		reply, err := client.GetBackingConnection(ctx, &multipoolerservicepb.GetBackingConnectionRequest{
			Database:       d.database,
			ConnectionName: name,
		})
		if err != nil {
			return err
		}
		conn, err = backingConnectionFromProto(name, reply.GetConnection())
		return err
	})
	return conn, err
}

// requireSingleUnmanagedPooler refuses to start a second unmanaged pooler for a tablegroup:
// exactly one is supported per tablegroup (the coordinator refuses to fence a
// tablegroup with more). A restart of the same pooler, under the same id, is not a second one,
// and neither is a pooler that announced shutdown. The record of one that crashed stays until
// it is removed, and blocks a pooler with another id.
//
// This is a check, not a lock: two poolers that start at the same instant can both pass it.
// The coordinator's refusal is what holds then.
func requireSingleUnmanagedPooler(ctx context.Context, ts topoclient.Store, initial *clustermetadatapb.Multipooler) error {
	key := initial.GetShardKey()
	list, err := metadataclient.Poolers(ctx, ts, key.GetDatabase())
	if err != nil {
		return fmt.Errorf("list the poolers of database %q: %w", key.GetDatabase(), err)
	}
	self := topoclient.ComponentIDString(initial.GetId())
	for _, p := range list {
		if p.GetShardKey().GetTableGroup() != key.GetTableGroup() ||
			p.GetManagementMode() != clustermetadatapb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED ||
			topoclient.ComponentIDString(p.GetId()) == self ||
			p.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
			continue
		}
		return fmt.Errorf("tablegroup %q already has the unmanaged pooler %q; exactly one is supported per tablegroup (remove its record if it is gone)",
			key.GetTableGroup(), p.GetId().GetName())
	}
	return nil
}

// backingConnectionFromProto validates a connection returned by the default
// primary. A reply for a different name, or one that lacks the identity the
// endpoint must prove, is rejected: reaching an endpoint is not proof that it is
// the intended source.
func backingConnectionFromProto(name string, c *multipoolerservicepb.BackingConnection) (*backingConnection, error) {
	if c.GetName() != name {
		return nil, fmt.Errorf("%w: reply is for %q, want %q", errInvalidBackingConnection, c.GetName(), name)
	}
	if c.GetExpectedSystemIdentifier() == "" {
		return nil, fmt.Errorf("%w: no expected system identifier", errInvalidBackingConnection)
	}
	conn := &backingConnection{
		Name:                     name,
		Host:                     c.GetHost(),
		Port:                     int(c.GetPort()),
		Database:                 c.GetDatabase(),
		User:                     c.GetUser(),
		Password:                 c.GetPassword(),
		SSLMode:                  c.GetSslMode(),
		SSLRootCert:              c.GetSslRootCert(),
		SSLNegotiation:           c.GetSslNegotiation(),
		ExpectedSystemIdentifier: c.GetExpectedSystemIdentifier(),
	}
	if err := conn.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidBackingConnection, err)
	}
	return conn, nil
}

// isPermanentBootstrapError reports whether asking again can only get the same answer: the
// default primary has no directory of connections (UNIMPLEMENTED), there is no connection of
// that name (NOT_FOUND), the request or the stored state is refused as it is
// (INVALID_ARGUMENT, FAILED_PRECONDITION, PERMISSION_DENIED), or what it returned is not a
// usable connection. Everything else (the default primary unreachable, failing over, not the
// default primary any more) is worth another attempt.
func isPermanentBootstrapError(err error) bool {
	if errors.Is(err, errInvalidBackingConnection) {
		return true
	}
	switch status.Code(err) {
	case codes.Unimplemented, codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied:
		return true
	}
	return false
}

// retryBootstrap runs attempt with bounded, cancellation-aware retries.
func retryBootstrap(ctx context.Context, attempt func(context.Context) error, onFailure func(attempt int, err error)) error {
	var err error
	for n := range bootstrapAttempts {
		if err = ctx.Err(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, bootstrapAttemptTimeout)
		err = attempt(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if onFailure != nil {
			onFailure(n+1, err)
		}
		if n == bootstrapAttempts-1 || isPermanentBootstrapError(err) {
			break
		}
		backoff := min(bootstrapBackoffBase<<n, bootstrapBackoffMax)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
