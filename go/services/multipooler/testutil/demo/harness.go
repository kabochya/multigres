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

package demo

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

// Command/Reply are test-process IPC, not public RPC or catalog interfaces.
type (
	Command struct {
		CallID        string
		Method        string
		Request       Request
		TimeoutMillis int
	}
	Reply struct {
		CallID    string
		Operation *Operation
		Error     string
	}
)

// RunHarness serves blocking pipe commands in an isolated test process. No
// polling loop, additional listener, or production workflow service is created.
func RunHarness(ctx context.Context, pm *manager.MultipoolerManager, ts topoclient.Store, key []byte, in io.Reader, out io.Writer) {
	var mu, outMu sync.Mutex
	var c *Controller
	encoder := json.NewEncoder(out)
	decoder := json.NewDecoder(in)
	for {
		var command Command
		if decoder.Decode(&command) != nil {
			return
		}
		go func() {
			lifetime := time.Duration(command.TimeoutMillis) * time.Millisecond
			if lifetime <= 0 {
				lifetime = 30 * time.Second
			}
			callCtx, cancel := context.WithTimeout(ctx, lifetime)
			defer cancel()
			reply := Reply{CallID: command.CallID}
			mu.Lock()
			var err error
			if c == nil || c.writer.Context().Err() != nil {
				var w *manager.MetadataWriter
				w, err = pm.ActivateMetadataWriter(callCtx, "postgres")
				if err == nil {
					c, err = New(callCtx, w, ts, key, grpc.WithTransportCredentials(insecure.NewCredentials()))
				}
			}
			active := c
			mu.Unlock()
			if err == nil {
				switch command.Method {
				case "status":
					reply.Operation, err = active.Status(callCtx, command.Request.ID)
				case "wait":
					reply.Operation, err = active.Wait(callCtx, command.Request.ID)
				default:
					reply.Operation, err = active.Advance(callCtx, command.Request)
				}
			}
			if err != nil {
				reply.Error = err.Error()
			}
			outMu.Lock()
			_ = encoder.Encode(reply)
			outMu.Unlock()
		}()
	}
}
