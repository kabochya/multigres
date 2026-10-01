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

// Package routingpolicy persists gateway destination independently of admission.
package routingpolicy

import (
	"context"
	"errors"

	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/connectioncatalog"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

const Schema = `CREATE TABLE IF NOT EXISTS multigres.gateway_routing (
 database TEXT PRIMARY KEY,
 destination INT NOT NULL CHECK(destination BETWEEN 1 AND 3),
 source_connection TEXT NOT NULL DEFAULT '',
 source_system_identifier TEXT NOT NULL DEFAULT '',
 source_database TEXT NOT NULL DEFAULT '',
 source_configuration_binding TEXT NOT NULL DEFAULT ''
)`

func Initialize(ctx context.Context, tx executor.InternalTx, database string) error {
	if _, err := tx.Query(ctx, Schema); err != nil {
		return err
	}
	if _, err := tx.Query(ctx, `REVOKE ALL ON multigres.gateway_routing FROM PUBLIC`); err != nil {
		return err
	}
	_, err := tx.QueryArgs(ctx, `INSERT INTO multigres.gateway_routing(database,destination) VALUES($1,1) ON CONFLICT DO NOTHING`, database)
	return err
}

func Validate(p *pb.GatewayRoutingPolicy) error {
	if p == nil {
		return errors.New("routing policy is required")
	}
	switch p.Destination {
	case pb.RoutingDestination_ROUTING_DESTINATION_MANAGED, pb.RoutingDestination_ROUTING_DESTINATION_BLOCKED:
		if p.SourceConnection != "" || p.SourceConfigurationBinding != "" || p.SourceIdentity != nil {
			return errors.New("managed/blocked policy must not retain a source association")
		}
	case pb.RoutingDestination_ROUTING_DESTINATION_SOURCE:
		if p.SourceConnection == "" || p.SourceConfigurationBinding == "" || p.GetSourceIdentity().GetSystemIdentifier() == "" || p.GetSourceIdentity().GetDatabase() == "" {
			return errors.New("source policy requires complete configuration and physical identity")
		}
	default:
		return errors.New("invalid routing destination")
	}
	return nil
}

func Read(ctx context.Context, tx executor.InternalTx, database string) (*pb.GatewayRoutingPolicy, error) {
	r, err := tx.QueryArgs(ctx, `SELECT destination,source_connection,source_system_identifier,source_database,source_configuration_binding FROM multigres.gateway_routing WHERE database=$1 FOR UPDATE`, database)
	if err != nil {
		return nil, err
	}
	var destination int32
	var name, sysid, db, binding string
	if err = executor.ScanSingleRow(r, &destination, &name, &sysid, &db, &binding); err != nil {
		return nil, err
	}
	p := &pb.GatewayRoutingPolicy{Destination: pb.RoutingDestination(destination), SourceConnection: name, SourceConfigurationBinding: binding}
	if sysid != "" || db != "" {
		p.SourceIdentity = &pb.ExternalBackendIdentity{SystemIdentifier: sysid, Database: db}
	}
	return p, Validate(p)
}

// Write composes with an existing authority-checked, synchronous transaction.
// The caller publishes this exact snapshot only after successful COMMIT.
func Write(ctx context.Context, tx executor.InternalTx, database string, p *pb.GatewayRoutingPolicy, c *connectioncatalog.Catalog) error {
	if err := Validate(p); err != nil {
		return err
	}
	if p.Destination == pb.RoutingDestination_ROUTING_DESTINATION_SOURCE {
		record, err := c.ReadTx(ctx, tx, p.SourceConnection)
		if err != nil {
			return err
		}
		v := record.Configuration
		if record.Binding != p.SourceConfigurationBinding || v.Database != p.SourceIdentity.Database || v.ExpectedSystemIdentifier != p.SourceIdentity.SystemIdentifier {
			return errors.New("source policy differs from provisioned configuration")
		}
	}
	_, err := tx.QueryArgs(ctx, `UPDATE multigres.gateway_routing SET destination=$2,source_connection=$3,source_system_identifier=$4,source_database=$5,source_configuration_binding=$6 WHERE database=$1`, database, int32(p.Destination), p.SourceConnection, p.GetSourceIdentity().GetSystemIdentifier(), p.GetSourceIdentity().GetDatabase(), p.SourceConfigurationBinding)
	return err
}

// Confirm generates synchronous WAL. Local visibility after an uncertain commit
// is insufficient for publication, even after a pooler-only restart.
func Confirm(ctx context.Context, tx executor.InternalTx, database string) (*pb.GatewayRoutingPolicy, error) {
	p, err := Read(ctx, tx, database)
	if err != nil {
		return nil, err
	}
	_, err = tx.QueryArgs(ctx, `UPDATE multigres.gateway_routing SET destination=destination WHERE database=$1`, database)
	return p, err
}
