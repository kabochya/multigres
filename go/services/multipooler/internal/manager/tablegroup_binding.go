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
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// The binding of a tablegroup to a connection is read from the default primary. A pooler that
// fronts one connection must never serve a tablegroup that is bound to another, or to none. A
// tablegroup that is not in the catalog is bound to nothing, so the pooler of an external source,
// which needs a binding that names its connection, stays not ready until the tablegroup is bound.
//
// The external monitor applies this policy (see confirmBinding):
//   - a pooler that is not ready becomes ready only on a definitive answer that the tablegroup is
//     bound to its connection: a binding that is another one or none, a default primary that is
//     down and one that has no directory of connections all keep it not ready;
//   - a pooler that is ready asks again every bindingRecheckInterval. A definitive answer that
//     the tablegroup is bound to another connection or to none withdraws readiness; no answer,
//     for a default primary that is unreachable (a failover of the default cohort, say) or one
//     that has no directory, keeps it as it is, so that a ready pooler does not flap with the
//     default cohort.

// errBindingContradicted marks a definitive answer that the tablegroup is not bound to this
// pooler's connection.
var errBindingContradicted = errors.New("tablegroup is not bound to this pooler's connection")

// errBindingUnconfirmed marks that nothing could say what the tablegroup is bound to.
var errBindingUnconfirmed = errors.New("the binding of the tablegroup could not be confirmed")

// backingConnectionName is the connection this pooler fronts: empty for a managed pooler.
func (pm *MultipoolerManager) backingConnectionName() string {
	if pm.config == nil {
		return ""
	}
	return pm.config.BackingConnectionName
}

// checkTablegroupBinding fails unless this pooler's connection is the one its tablegroup is
// bound to. The error wraps errBindingContradicted for a definitive answer that it is not, and
// errBindingUnconfirmed when nothing could answer; any other error is a failed read. A pooler
// that fronts a connection fails closed: no directory to ask is no confirmation.
func (pm *MultipoolerManager) checkTablegroupBinding(ctx context.Context) error {
	bound, known, err := pm.readTablegroupBinding(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errBindingUnconfirmed, err)
	}
	switch {
	case !known && pm.backingConnectionName() != "":
		return fmt.Errorf("%w: the default primary has no directory of connections", errBindingUnconfirmed)
	case known && bound != pm.backingConnectionName():
		return fmt.Errorf("%w: it is backed by connection %q, this pooler by %q", errBindingContradicted, bound, pm.backingConnectionName())
	}
	return nil
}

// readTablegroupBinding reads the connection this pooler's tablegroup is bound to, from the
// default primary, or from this pooler itself when it is the default primary. known is false
// when there is no directory of connections to ask, on this pooler or on a default primary that
// answers Unimplemented: nothing can have bound the tablegroup then.
func (pm *MultipoolerManager) readTablegroupBinding(ctx context.Context) (name string, known bool, err error) {
	if pm.bindingSource != nil {
		return pm.bindingSource(ctx)
	}
	key := pm.record.ShardKey()
	if newConnectionDirectory == nil {
		return "", false, nil
	}
	req := &multipoolerservicepb.GetTablegroupBindingRequest{Database: key.GetDatabase(), Tablegroup: key.GetTableGroup()}
	if pm.requireDefaultPrimary(key.GetDatabase()) == nil {
		resp, err := pm.GetTablegroupBinding(ctx, req)
		return bindingOf(resp, err)
	}
	if pm.config == nil || pm.config.TopoClient == nil {
		return "", false, errors.New("default primary discovery unavailable")
	}
	err = pm.defaultPrimaryClient().with(ctx, func(c multipoolerservicepb.MultipoolerServiceClient) error {
		resp, err := c.GetTablegroupBinding(ctx, req)
		name, known, err = bindingOf(resp, err)
		return err
	})
	return name, known, err
}

// bindingOf reads the answer to a binding request: a tablegroup that is not found is bound to
// nothing, and a default primary that does not know the call has no bindings.
func bindingOf(resp *multipoolerservicepb.GetTablegroupBindingResponse, err error) (string, bool, error) {
	switch {
	case err == nil:
		return resp.GetBackingConnection(), true, nil
	case status.Code(err) == codes.Unimplemented, mterrors.Code(err) == mtrpcpb.Code_UNIMPLEMENTED:
		return "", false, nil
	case status.Code(err) == codes.NotFound, mterrors.Code(err) == mtrpcpb.Code_NOT_FOUND:
		return "", true, nil
	}
	return "", false, err
}
