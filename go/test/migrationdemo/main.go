//go:build migration_demo

// Copyright 2026 Supabase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// migrationdemo invokes the loopback-only controller stub in tagged test builds.
// It does not replicate data or implement a migration controller.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

func run() error {
	path := flag.String("endpoints", "/tmp/multigres-migration-demo/endpoints.json", "local fixture endpoints")
	operation := flag.String("operation", "", "demo-fence, demo-managed, demo-unmanaged, or demo-complete")
	fenceID := flag.String("fence-request-id", "", "owning completed fence ID, required for activation")
	request := flag.String("request-id", "", "unique idempotency ID")
	flag.Parse()
	if *request == "" {
		return errors.New("request-id required")
	}
	if (*operation == "demo-managed" || *operation == "demo-unmanaged") && *fenceID == "" {
		return errors.New("fence-request-id required for activation")
	}
	switch *operation {
	case "demo-fence", "demo-managed", "demo-unmanaged", "demo-complete":
	default:
		return errors.New("unsupported demo operation")
	}
	var cfg struct {
		TargetRPCPort int    `json:"target_rpc_port"`
		TargetPGPort  int    `json:"target_pg_port"`
		KeyFile       string `json:"key_file"`
		PGPassFile    string `json:"pgpass_file"`
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, &cfg); err != nil {
		return err
	}
	key, err := os.ReadFile(cfg.KeyFile)
	if err != nil {
		return err
	}
	pass, err := os.ReadFile(cfg.PGPassFile)
	if err != nil {
		return err
	}
	var password string
	for line := range strings.SplitSeq(string(pass), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) == 5 && fields[0] == "127.0.0.1" && fields[1] == strconv.Itoa(cfg.TargetPGPort) && fields[3] == "postgres" {
			password = fields[4]
		}
	}
	if password == "" {
		return errors.New("fixture target credential unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", cfg.TargetRPCPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := rpc.NewMultipoolerServiceClient(conn)
	ctx = migrationcontrol.AuthorizedContext(ctx, key)
	creds, err := client.GetAuthCredentials(ctx, &rpc.GetAuthCredentialsRequest{Database: "postgres", Username: "postgres", ServingAdmin: true})
	if err != nil {
		return err
	}
	hash, err := scram.ParseScramSHA256Hash(creds.ScramHash)
	if err != nil {
		return err
	}
	salted := scram.ComputeSaltedPassword(password, hash.Salt, hash.Iterations)
	result, err := client.ServingControl(ctx, &rpc.ServingControlRequest{Database: "postgres", Username: "postgres", Operation: *operation, RequestId: *request, ConnectionName: *fenceID, UserAuth: &query.UserAuth{ClientKey: scram.ComputeClientKey(salted), ServerKey: scram.ComputeServerKey(salted)}})
	if err != nil {
		return err
	}
	fmt.Printf("%s source=%q completed=%t\n", result.GetRouting().GetMode(), result.GetRouting().GetSourceConnection(), result.GetRouting().GetMigrationCompleted())
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1) //nolint:forbidigo // run has returned and completed its deferred cleanup.
	}
}
