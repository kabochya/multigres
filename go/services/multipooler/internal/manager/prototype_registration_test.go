// Copyright 2025 Supabase, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestThePrototypeStoreIsRegistered: the store of serving state and the directory of backing
// connections are what the prototype registers; without it (this file's package alone, or a
// build that drops prototype_store.go) both stay nil and the calls answer UNIMPLEMENTED.
func TestThePrototypeStoreIsRegistered(t *testing.T) {
	require.NotNil(t, newServingStore)
	require.NotNil(t, newConnectionDirectory)
}
