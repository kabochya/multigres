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
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

func bootstrapSource(ctx context.Context, ts topoclient.Store, initial *pb.Multipooler, logger *slog.Logger, key []byte, transport grpc.DialOption) (*rpc.GetSourceConnectionResponse, error) {
	if err := manager.RegisterPreparingSource(ctx, logger, ts, initial); err != nil {
		return nil, errors.New("cannot register preparing source")
	}
	var reply *rpc.GetSourceConnectionResponse
	err := retrySourceBootstrap(ctx, func(ctx context.Context) error {
		return migrationcontrol.WithAuthority(ctx, ts, initial.ShardKey.Database, transport, func(client rpc.MultipoolerServiceClient) error {
			v, err := client.GetSourceConnection(migrationcontrol.AuthorizedContext(ctx, key), &rpc.GetSourceConnectionRequest{Database: initial.ShardKey.Database, ConnectionName: initial.SourceConnection})
			if err != nil {
				return err
			}
			if !proto.Equal(initial.ShardKey, v.GetAuthorityShardKey()) {
				return errors.New("source scope differs from managed authority cohort")
			}
			if err = validateSourceBootstrap(initial.SourceConnection, v); err != nil {
				return err
			}
			reply = v
			return nil
		})
	})
	if err != nil {
		return nil, errors.Join(errors.New("source bootstrap unavailable; process remains closed"), err)
	}
	return reply, nil
}

func validateSourceBootstrap(name string, v *rpc.GetSourceConnectionResponse) error {
	if v.GetConfigurationBinding() == "" || v.GetConnection().GetName() != name || v.GetConnection().GetExpectedSystemIdentifier() == "" {
		return errors.New("source configuration identity is incomplete")
	}
	return connectioncatalog.ValidateConnection(v.Connection)
}

func retrySourceBootstrap(ctx context.Context, attempt func(context.Context) error) error {
	var err error
	for n := range 5 {
		if err = ctx.Err(); err != nil {
			return err
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = attempt(readCtx)
		cancel()
		if err == nil {
			return nil
		}
		if n == 4 {
			break
		}
		timer := time.NewTimer((100 * time.Millisecond) << n)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
