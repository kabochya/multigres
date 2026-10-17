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
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// servingRow is one tablegroup's persisted serving row.
type servingRow struct {
	connection string
	state      multipoolerservicepb.AdmissionState
	version    int64
	updatedAt  time.Time
}

// closed reports whether the tablegroup refuses application work.
func (r *servingRow) closed() bool { return r.state != stateUnfenced }

func (r *servingRow) proto(tablegroup string) *multipoolerservicepb.TablegroupServingState {
	return &multipoolerservicepb.TablegroupServingState{
		Tablegroup:        tablegroup,
		BackingConnection: r.connection,
		State:             r.state,
		Version:           r.version,
		UpdatedAt:         timestampOrNil(r.updatedAt),
	}
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// servingReader is the read side of the durable store of serving state, which lives on the
// default primary.
type servingReader interface {
	// GetRow returns the row, or nil when the tablegroup has none (or there are no
	// tables yet): such a tablegroup was never fenced, and is unfenced at version zero.
	GetRow(ctx context.Context, database, tablegroup string) (*servingRow, error)
	// GetRouting returns the routing pointer; found is false when none exists.
	GetRouting(ctx context.Context, database string) (appTablegroup string, version int64, found bool, err error)
}

// servingStore is the durable store of serving state. Every mutation is a conditional update
// on the expected prior value, so concurrent or stale writers cannot overwrite each other.
type servingStore interface {
	servingReader
	// CompareAndSet moves a row from (prevState, prevVersion) to (nextState,
	// nextVersion), and reports whether it did. A tablegroup with no row is UNFENCED at
	// version zero, and a fence from there creates the row. An error means the outcome
	// is uncertain; the caller must re-read.
	CompareAndSet(ctx context.Context, database, tablegroup string, prevState multipoolerservicepb.AdmissionState, prevVersion int64, nextState multipoolerservicepb.AdmissionState, nextVersion int64) (bool, error)
	// MoveRouting atomically moves the pointer from one tablegroup to another and
	// bumps its version, only if the pointer is at from at expectedVersion and both
	// tablegroups are FENCED (a completed fence). It reports whether it did.
	MoveRouting(ctx context.Context, database, from, to string, expectedVersion int64) (appTablegroup string, version int64, moved bool, err error)
}

// newServingStore builds the store of serving state of this pooler, which must be the
// default primary. Where the state is kept is not decided here, so nothing is configured by
// default and the calls that need the store answer UNIMPLEMENTED until something is
// registered (a pooler that reads UNIMPLEMENTED from the default primary opens: it has no
// admission state to enforce).
var newServingStore func(pm *MultipoolerManager) servingStore

// servingStore returns the store of serving state on this pooler.
func (pm *MultipoolerManager) servingStore() (servingStore, error) {
	if newServingStore == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNIMPLEMENTED, "no store of serving state is configured")
	}
	return newServingStore(pm), nil
}
