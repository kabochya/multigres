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

// Package protometadata holds the throwaway metadata tables the unmanaged
// pooler prototype keeps on the default primary.
//
// PROTOTYPE STUB: delete this package when the real catalog lands. There are two
// independent pieces, each created lazily by one idempotent statement:
//
//   - the connection side (connection_schema.go): the connections table, and the column of
//     the existing multigres.tablegroup table that binds a tablegroup to one;
//   - the fence and routing side (routing_schema.go): the admission fences, the routing rules
//     and the catalog state.
//
// Neither refers to the other.
package protometadata
