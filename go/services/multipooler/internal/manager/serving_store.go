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

package manager

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// PROTOTYPE STUB: the serving store, membership and refresh transport used by
// the admission coordinator. The store is backed by the throwaway
// multigres.proto_* tables; delete it with them.

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// stateText is the persisted spelling of an admission state.
func stateText(s multipoolerservicepb.AdmissionState) string {
	return strings.TrimPrefix(s.String(), "ADMISSION_STATE_")
}

// sqlServingStore keeps serving state in the prototype tables on the default
// primary, through the admin pool.
type sqlServingStore struct {
	query func(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error)
}

func (s sqlServingStore) GetRow(ctx context.Context, database, tablegroup string) (*servingRow, error) {
	result, err := s.query(ctx,
		"SELECT COALESCE(connection, ''), admission_state, request_id, extract(epoch FROM updated_at)::text FROM multigres.proto_tablegroup_serving WHERE database = $1 AND tablegroup = $2",
		database, tablegroup)
	if err != nil {
		return nil, mterrors.Wrap(err, "read tablegroup serving state")
	}
	if len(result.Rows) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "tablegroup %q has no serving row", tablegroup)
	}
	var connection, state, requestID, epoch string
	if err := executor.ScanSingleRow(result, &connection, &state, &requestID, &epoch); err != nil {
		return nil, errors.Join(errors.New("decode tablegroup serving state"), err)
	}
	admissionState, err := parseAdmissionState(state)
	if err != nil {
		return nil, err
	}
	row := &servingRow{connection: connection, state: admissionState, requestID: requestID}
	if secs, err := strconv.ParseFloat(epoch, 64); err == nil {
		row.updatedAt = time.Unix(0, int64(secs*float64(time.Second)))
	}
	return row, nil
}

func (s sqlServingStore) CompareAndSet(ctx context.Context, database, tablegroup string, prevState multipoolerservicepb.AdmissionState, prevRequestID string, nextState multipoolerservicepb.AdmissionState, nextRequestID string) (bool, error) {
	result, err := s.query(ctx, `UPDATE multigres.proto_tablegroup_serving
SET admission_state = $5, request_id = $6, updated_at = now()
WHERE database = $1 AND tablegroup = $2 AND admission_state = $3 AND request_id = $4
RETURNING 1`, database, tablegroup, stateText(prevState), prevRequestID, stateText(nextState), nextRequestID)
	if err != nil {
		return false, err
	}
	return len(result.Rows) == 1, nil
}

func (s sqlServingStore) GetRouting(ctx context.Context, database string) (string, int64, bool, error) {
	result, err := s.query(ctx, "SELECT app_tablegroup, version FROM multigres.proto_routing WHERE database = $1", database)
	if err != nil {
		return "", 0, false, mterrors.Wrap(err, "read routing")
	}
	if len(result.Rows) == 0 {
		return "", 0, false, nil
	}
	var app string
	var version int64
	if err := executor.ScanSingleRow(result, &app, &version); err != nil {
		return "", 0, false, errors.Join(errors.New("decode routing"), err)
	}
	return app, version, true, nil
}

func (s sqlServingStore) MoveRouting(ctx context.Context, database, from, to string) (string, int64, bool, error) {
	// One statement, so the pointer and both admission checks commit atomically.
	// The serving rows are locked FOR SHARE: neither can change state under it.
	result, err := s.query(ctx, `WITH serving AS (
  SELECT admission_state FROM multigres.proto_tablegroup_serving
  WHERE database = $1 AND tablegroup IN ($2, $3) FOR SHARE
), checked AS (
  SELECT count(*) FILTER (WHERE admission_state = 'FENCED') = 2 AS both_fenced FROM serving
)
UPDATE multigres.proto_routing
SET app_tablegroup = $3, version = version + 1
WHERE database = $1 AND app_tablegroup = $2 AND (SELECT both_fenced FROM checked)
RETURNING app_tablegroup, version`, database, from, to)
	if err != nil {
		return "", 0, false, err
	}
	if len(result.Rows) == 0 {
		return "", 0, false, nil
	}
	var app string
	var version int64
	if err := executor.ScanSingleRow(result, &app, &version); err != nil {
		return "", 0, false, errors.Join(errors.New("decode routing"), err)
	}
	return app, version, true, nil
}

// topologyMembership enumerates a tablegroup's poolers from topology.
type topologyMembership struct{ ts topoclient.Store }

func (m topologyMembership) Snapshot(ctx context.Context, database, tablegroup string) ([]poolerRef, error) {
	list, err := metadataclient.Poolers(ctx, m.ts, database)
	if err != nil {
		return nil, err
	}
	var refs []poolerRef
	for _, p := range list {
		if p.GetShardKey().GetTableGroup() != tablegroup {
			continue
		}
		// A pooler that announced shutdown has stopped serving; its successor
		// registers again with a new incarnation.
		if p.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN {
			continue
		}
		refs = append(refs, poolerRef{id: p.GetId(), incarnation: p.GetProcessIncarnation(), record: p})
	}
	return refs, nil
}

// grpcRefresher calls RefreshAdmission on a pooler over gRPC.
type grpcRefresher struct{ transport grpc.DialOption }

func (r grpcRefresher) Refresh(ctx context.Context, p poolerRef, req *multipoolerservicepb.RefreshAdmissionRequest) (*multipoolerservicepb.RefreshAdmissionResponse, error) {
	conn, err := metadataclient.Dial(p.record, r.transport)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return multipoolerservicepb.NewMultipoolerServiceClient(conn).RefreshAdmission(ctx, req)
}
