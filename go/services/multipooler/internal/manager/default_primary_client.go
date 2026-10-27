// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/metadataclient"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// defaultPrimaryClient is the connection a pooler keeps to the default primary, to read the
// cluster metadata, such as the binding of its tablegroup, and its admission row. It resolves
// the default primary from topology and dials once, and reuses the connection for every read; a
// read that fails because the connection is gone, or because that pooler is not the default
// primary any more, drops it and resolves again once. Without it every read listed the poolers
// of the database and dialed a new connection.
type defaultPrimaryClient struct {
	pm *MultipoolerManager

	mu   sync.Mutex
	conn *grpc.ClientConn
}

func (pm *MultipoolerManager) defaultPrimaryClient() *defaultPrimaryClient {
	pm.defaultPrimaryOnce.Do(func() { pm.defaultPrimary = &defaultPrimaryClient{pm: pm} })
	return pm.defaultPrimary
}

// close closes the cached connection, at shutdown.
func (c *defaultPrimaryClient) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropLocked()
}

func (c *defaultPrimaryClient) dropLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
}

func (c *defaultPrimaryClient) get(ctx context.Context, fresh bool) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fresh {
		c.dropLocked()
	}
	if c.conn != nil {
		return c.conn, nil
	}
	pm := c.pm
	database := pm.record.ShardKey().GetDatabase()
	list, err := metadataclient.Poolers(ctx, pm.config.TopoClient, database)
	if err != nil {
		return nil, err
	}
	primary, err := metadataclient.DefaultPrimary(list)
	if err != nil {
		return nil, err
	}
	transport, err := pm.config.DefaultPrimaryDialOption()
	if err != nil {
		return nil, err
	}
	conn, err := metadataclient.Dial(primary, transport)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

// with runs fn against the default primary. If fn fails in a way that says the cached
// connection or the primary is stale, it runs once more against a fresh resolution.
func (c *defaultPrimaryClient) with(ctx context.Context, fn func(multipoolerservicepb.MultipoolerServiceClient) error) error {
	var err error
	for attempt := range 2 {
		var conn *grpc.ClientConn
		conn, err = c.get(ctx, attempt > 0)
		if err != nil {
			return err
		}
		err = fn(multipoolerservicepb.NewMultipoolerServiceClient(conn))
		if err == nil || !staleDefaultPrimary(err) {
			return err
		}
	}
	return err
}

// staleDefaultPrimary reports whether err is what a connection to a pooler that is gone, or
// that is not the default primary any more, answers.
func staleDefaultPrimary(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.FailedPrecondition, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
