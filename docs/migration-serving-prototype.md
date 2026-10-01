# Migration serving prototype

The target PostgreSQL catalog is the migration-mode authority. Serving control in the current managed primary owns writes; management mode and consensus rule numbers retain their existing meanings. There are no migration-mode versions.

## Catalog foundation

The migration key is a raw 32-byte Secret mounted via `--migration-key-file`. Connection payloads use standard-library AES-256-GCM with a random nonce and the connection name as authenticated associated data. The stored envelope records the format and a SHA-256 key fingerprint. Missing or incorrect keys fail closed. Preserve the key alongside database backups; automated rotation is not implemented in this prototype.

Catalog initialization is idempotent and runs lazily on the managed leader for existing databases. `multigres.migration_routing` contains one row per physical database, initially UNSET. Row locking serializes mutations. The controller journal callback uses the same admin transaction as the mode update. Both rollback together.

Mutations set transaction-local `synchronous_commit=remote_apply`. This waits for the configured synchronous cohort, not an invented quorum. A single-node database has local durability only. Availability and failover durability depend on the existing synchronous replication configuration. Real replication validation belongs to the E2E layer; unit tests alone do not establish it.

Secret-bearing reads and administrative writes require a cluster key proof over TLS. Loopback transport is allowed for the disposable local demo. Target superuser authorization is checked independently of source login. No connection credentials belong in topology, health or normal SQL results.

## Stack

1. Catalog, encrypted connections and serving-control foundation.
2. Register-before-read startup and independent application gates; unversioned Fence/Unfence.
3. Attach, pause, resume and controller transitions; complete fence-set coordination.
4. Authoritative health propagation, coexistence selection and buffering.
5. Administrative SQL surface and safe control routing.
6. Disposable local E2E tests and runbook.

The operator companion uses `Shard.spec.unmanagedPoolers` with cells and resources. Source credentials remain in the target catalog. MANAGED mode retains source poolers; completion is required for decommissioning.

This is a draft stack. Subsequent steps implement the declared transition RPCs; until then they return UNIMPLEMENTED. Production migrations and replication controllers are outside this prototype.

## Startup and fencing

Standalone unmanaged configuration continues using M1 endpoint flags. Migration-enabled sources instead use `--source-connection=<catalog-name>` and `--migration-key-file`. They register a unique disabled topology identity before retrieving credentials from the managed authority. Use the existing `multipooler-grpc-*` client TLS flags for protected control connections. A source starts closed until an authoritative mode read succeeds. Readiness reports the physical PostgreSQL system identifier and database separately from admission.

Fence and Unfence have empty requests. The source discovers the managed authority and reads its current mode on each call. Unfence requires UNMANAGED and the matching connection name. Fence requires FENCED or MANAGED. A local mutex covers remote reads and gate application, including initial authorization. An earlier delayed Unfence therefore cannot apply after a later Fence has acknowledged. Health probes never write the admission flag.

Fence immediately blocks new requests and waits for handler admission permits plus regular and reserved backend connections. Existing reservations can finish on their original backend. COPY holds a permit for its full handler lifetime. Timeout/cancellation returns an error and leaves the gate closed; it does not force rollback and claim completion. Serving control and replication internals use their existing narrow admin paths.

A running source retains its accepted state through target outages. Restarting sources need target access to retrieve their catalog configuration and authorization. Cold gateways are addressed by the gateway PR. Managed poolers with the key start their application gates closed and read durable mode before opening.

## Serving transitions

Attach verifies that every registered source endpoint is prepared and identifies the same physical database. It commits FENCED, drains the target, fences the full registered source set, then commits UNMANAGED and calls Unfence. Pause commits FENCED before source enumeration and enforcement. Resume validates the prepared identity before committing UNMANAGED and opening source endpoints. No database lock is held across source RPCs, which read the committed mode themselves.

Administrative request IDs and keyed request hashes are persisted with the first decision. Completed retries return current state without replaying network work. Pending operations can be repeated with the same ID; different arguments or superseded requests fail. A failed fence leaves FENCED. Partial activation leaves its committed destination selected and reports incomplete enforcement; it never falls back to the other database.

The in-process `ControllerTransition` and `CompleteMigration` hooks require a journal callback in the same transaction. There is no administrative MANAGED switch. Controller-owned fences cannot be resumed by admin SQL. Completion is persisted independently of MANAGED; detach requires completion and restores ordinary managed routing. The operator must retain source workloads until completion.

Connection changes require FENCED. This prototype requires restarting prepared source poolers after a connection change; Unfence compares their bootstrapped settings to the current catalog. Changing the physical source identity is rejected. Source process IDs must be unique, and topology records must be retained until shutdown is proven. The fence set cannot establish that an untracked or prematurely pruned process stopped; the operator companion must preserve this contract.
