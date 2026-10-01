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
