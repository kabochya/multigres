# Migration serving prototype

The target PostgreSQL catalog is the migration-mode authority. Serving control in the current managed primary owns writes; management mode and consensus rule numbers retain their existing meanings. There are no migration-mode versions.

## Catalog foundation

The migration key is a raw 32-byte Secret mounted via `--migration-key-file`. Connection payloads use standard-library AES-256-GCM with a random nonce and the connection name as authenticated associated data. The stored envelope records the format and a SHA-256 key fingerprint. Missing or incorrect keys fail closed. Preserve the key alongside database backups; automated rotation is not implemented in this prototype.

Catalog initialization is idempotent and runs lazily on the managed leader for existing databases. `multigres.migration_routing` contains one row per physical database, initially UNSET. Row locking serializes mutations. The controller journal callback uses the same admin transaction as the mode update. Both rollback together.

Mutations set transaction-local `synchronous_commit=remote_apply`. This waits for the configured synchronous cohort, not an invented quorum. A single-node database has local durability only. Availability and failover durability depend on the existing synchronous replication configuration. Real replication validation belongs to the E2E layer; unit tests alone do not establish it.

Secret-bearing reads and administrative writes require a cluster key proof over TLS. Loopback transport is allowed for the disposable local demo. Target superuser authorization is checked independently of source login. No connection credentials belong in topology, health or normal SQL results.

## Stack

1. Catalog, encrypted connections and serving-control foundation.
2. Register-before-read startup and independent application gates; operation-correlated RefreshRouting.
3. Attach, pause, resume and controller transitions; complete fence-set coordination.
4. Authoritative health propagation, coexistence selection and buffering.
5. Administrative SQL surface and safe control routing.
6. Disposable local E2E tests and runbook.

The operator companion uses `Shard.spec.unmanagedPoolers` with cells and resources. Source credentials remain in the target catalog. MANAGED mode retains source poolers; completion is required for decommissioning.

This is a draft stack. Subsequent steps implement the declared transition RPCs; until then they return UNIMPLEMENTED. Production migrations and replication controllers are outside this prototype.

## Startup and fencing

Standalone unmanaged configuration continues using M1 endpoint flags. Migration-enabled sources instead use `--source-connection=<catalog-name>` and `--migration-key-file`. They register a unique disabled topology identity before retrieving credentials from the managed authority. Use the existing `multipooler-grpc-*` client TLS flags for protected control connections. A source starts closed until an authoritative mode read succeeds. Readiness reports the physical PostgreSQL system identifier and database separately from admission.

RefreshRouting carries database, expected durable operation ID and expected mode. These are expectations, never an override. Both management modes use one serialized read/apply/drain routine. Sources obtain one protected confirmed current-authority response; managed followers wait for the exact operation and mode in their local replicated catalog, bounded by the request deadline and a three-second limit. Matching mode alone is insufficient, and opaque IDs are never ordered.

Closing blocks new requests and waits for handler permits plus regular and reserved backend connections. Existing reservations can finish on their original backend. COPY holds a permit for its full lifetime. Timeout/cancellation reports incomplete enforcement and leaves admission closed. An acknowledgment identifies the exact operation, mode and process incarnation. Opening validates source configuration/identity where applicable, backend readiness and admission-generation guards. A cold managed follower must initialize against current authority before a delayed activation can open its gate. Health probes cannot clear admission permission.

A running source retains its accepted state through target outages. Restarting sources need target access to retrieve their catalog configuration and authorization. Cold gateways are addressed by the gateway PR. Managed poolers with the key start their application gates closed and read durable mode before opening.

## Serving transitions

Attach verifies source readiness and physical identity, commits a FENCED operation under a stable derived fence ID, calls every required pooler and durably completes that barrier. It then commits a separate source-activation operation and refreshes every required pooler. Pause, resume and controller transitions use the same confirmed intent → local enforcement → cross-cell refresh → exact-process acknowledgments → durable completion path. Activation and close/drain remain separate transitions. Catalog transactions and callback-required action locks are released before remote waits.

Administrative request IDs and keyed request hashes are persisted with the first decision. Completed retries return current state without replaying network work. Pending operations can be repeated with the same ID; different arguments or superseded requests fail. A failed fence leaves FENCED. Partial activation leaves its committed destination selected and reports incomplete enforcement; it never falls back to the other database.

The in-process `ControllerTransition` and `CompleteMigration` hooks require a journal callback in the same transaction. There is no administrative MANAGED switch. Controller-owned fences cannot be resumed by admin SQL. Completion is persisted independently of MANAGED; detach requires completion and restores ordinary managed routing. The operator must retain source workloads until completion.

Connection changes require FENCED. This prototype requires restarting prepared source poolers after a connection change; RefreshRouting compares their bootstrapped settings to the current catalog. Changing the physical source identity is rejected. Source process IDs must be unique, and topology records must be retained until shutdown is proven. The fence set cannot establish that an untracked or prematurely pruned process stopped; the operator companion must preserve this contract.

## Gateway propagation

The managed writable leader publishes catalog mode in health independently of its application serving status. Gateways use existing consensus rules to reject an older managed authority; unmanaged endpoints never join the managed primary set. Topology remains discovery, not a second routing authority. A role change closes managed application admission and invalidates reads started in the prior role before a fresh policy can open it.

Gateway policy survives managed endpoint removal for the lifetime of the gateway. Cold coexistence discovery without authoritative mode buffers. UNMANAGED routes every application mode to ready, identity-matching source endpoints, preferring the gateway cell. FENCED uses the existing bounded failover buffer. MANAGED and authoritative UNSET use managed routing. Source readiness cannot release a buffer awaiting the target or vice versa. By-ID reservations and cleanup keep their original owner; no session state transfers at a mode switch.

Only the dedicated ServingControl RPC uses the managed-control selector while target application admission is closed. It does not retry an ambiguous transition automatically. Administrative retries supply the same durable request ID.

## Administrative SQL

Commands must stand alone and run from an idle session without a reserved backend. Both simple and extended protocol are supported; bind parameters and multi-statement batches are not supported for control commands. Mutations require an explicit request ID, up to 128 characters. Retry an incomplete operation with the same ID and arguments; SHOW inspects the current authority. ALTER replaces the full connection configuration and requires FENCED for an attached connection.

```sql
CREATE CONNECTION source WITH (
 host='127.0.0.1', port='5432', database='postgres', username='postgres',
 password='<secret>', sslmode='disable'
) REQUEST ID 'create-source-1';
ATTACH CONNECTION source REQUEST ID 'attach-source-1';
SHOW SERVING MODE;
PAUSE SERVING REQUEST ID 'pause-source-1';
RESUME SERVING REQUEST ID 'resume-source-1';
DETACH CONNECTION source REQUEST ID 'detach-source-1';
```

TLS defaults to verify-full when sslmode is omitted. Never put real credentials in shell arguments or versioned SQL files. Commands/results and malformed control input are redacted in protocol, handler, planner and executor logs. Audit records contain operation/request identifiers only. Results contain mode, connection name and completion, not endpoint credentials.

The gateway's `--serving-control-token-file` contains the 64-character hexadecimal SHA-256 digest of the poolers' raw migration key. It is a bearer secret and needs protected storage/TLS, but cannot decrypt catalog credentials. The optional `--pg-admin-port` authenticates target superusers directly through protected control lookup even while applications are fenced. It uses normal PostgreSQL listener TLS settings. It grants no ordinary SQL bypass. Target-only administrators use this port; existing source-authenticated sessions may administer only if their SCRAM client/server keys match the current target verifier and the role is an unexpired LOGIN superuser there. Role-name equality alone is insufficient. Trust sessions cannot administer.

Transparent routing switches for application sessions require identical SCRAM verifiers on source and target; the same plaintext password with independently generated salts does not preserve passthrough keys. Importing those verifiers and role changes remains the migration controller's responsibility. The disposable demo uses matching verifiers. Admin SQL cannot select MANAGED or override a controller-owned fence.

## Correctness and recovery contracts

Catalog updates reuse the existing admin transaction, routing-row lock, action
lock, and configured synchronous replication policy with `remote_apply`.
Authoritative reads, operation status and completed-request retries perform a
WAL-generating synchronous confirmation write to that row, following
`confirmProposalQuorum`. An uncertain COMMIT is not classified as rollback.
A local reread is insufficient, including after a pooler-only restart. Cold followers confirm initialization with managed authority. Operation-time
refreshes read local WAL metadata; local admission generations reject reads
crossing promotion or backend withdrawal. Publication is pinned by the action lock.
Publication uses the private routing snapshot captured by the successful commit,
while the existing action lock pins leadership. Health heartbeats reuse this
confirmed snapshot. Role/rule changes and backend loss invalidate it. No mode
version, lease, or additional consensus is used.

FENCED is routing intent. A `serving_requests.completed` marker is recorded only
after the required managed and source gates close, existing application work
including reservations drains, and all required acknowledgments return. Partial
fanout, lost acknowledgments or drain timeout retain FENCED and an incomplete
request. Recover using the same request ID and arguments. Conflicting mutations
cannot supersede an incomplete operation. Administrative attach/resume and
connection changes cannot take a controller-owned fence. A completed migration
cannot be reversed before decommissioning.

`ControllerTransition(ctx, migrationID, requestID, fenceRequestID, destination, journal)`
requires the active migration identity and completed owning fence ID for activation.
The initial attach request ID is persisted as `migration_id` in the routing
row. The `required_poolers` field retains process IDs observed by fences until
terminal detach. A retry cannot forget a missing process, and a replacement must
acknowledge independently. Explicit SHUTDOWN evidence lets recovery omit a dead
process from remote enforcement. Controller operations include migration identity in their argument hash and validate it
under the singleton lock. Completion retains ownership; only completed detach
clears it. IDs identify work, rather than authenticating controllers. Conflicting
local submissions are rejected immediately, rather than queued. The fence ID is an
operation precondition, not a migration-mode version. The journal callback joins
the routing transaction and must not perform remote operations. RefreshRouting validates the exact durable operation under local serialization.
Healthy source readiness monitoring preserves lifecycle status during an explicit
fence drain; it cannot trigger the lifecycle force-close timeout underneath
reservation draining. Admission closes immediately, and completed closing enforcement publishes DISABLED
only after draining finishes.
Managed peer drains do not hold the authority's catalog callback lock.

`GetServingOperation` and `WaitServingOperation` expose durable journal progress.
The existing authenticated ServingControl RPC accepts `status` and `wait` with
`request_id` and returns `operation_status` (request ID, operation, completed,
active). Waiting does not drive recovery or hold the transition mutex; timeout
never undoes fencing. Clients rediscover the authority and recover by request ID
after leader changes. No retry blindly replays an ambiguous mutation.

Source startup still registers before reading configuration/mode, so preparing
processes remain in complete fencing membership. New source processes verify
backend identity and fail closed; warm sources retain accepted service during
target outages. The operator retains a SHUTDOWN record after process-death proof until confirmed
terminal cleanup. Existing graceful pooler shutdown already publishes that
record.
Its existing GetMigrationMode completion read now receives synchronous
confirmation; Kubernetes lifecycle semantics are unchanged.

These guarantees depend on the existing consensus recovery and configured
synchronous cohort. They do not add a replication barrier, fence direct source
connections, authenticate a new controller identity, or prove Kubernetes
node-loss shutdown. Explicit confirmation reads still generate WAL. Disposable
PostgreSQL tests cover
uncertain commits, completed retries, catalog recreation and physical promotion;
deterministic concurrency and coexistence tests cover drain/acknowledgment
recovery, ownership conflicts, delayed commands, joining sources, fresh replacement
identities and restart. The three-target coexistence test elects a new consensus
leader while fencing waits for a reserved source transaction, then recovers
the durable incomplete operation by the same ID.

## Event-driven propagation and waiting

There is no migration state-refresh or operation-status ticker. Successful
catalog mutations publish their protected transaction snapshot after synchronous
commit, covering admin/controller transitions, request completion, connection
changes, migration completion and detach. Ambiguous outcomes withdraw authority
and schedule confirmation; they never publish a local reread or imply rollback.

Startup, StateManager role/rule transitions, and changes observed by the existing
PostgreSQL monitor schedule recovery. Role changes cancel obsolete recovery;
backend loss withdraws routing advertisement even when consensus role has not
changed. Each recovery burst has at most five attempts, three seconds per attempt,
with cancellation-aware exponential jittered backoff from 100 ms to one second.
Exhaustion leaves admission closed until another lifecycle/reconnect event or an
explicit control call. No confirmation transaction recurs while unchanged and
idle. This intentionally does not promise automatic recovery without any new
event after a retry budget is exhausted.

Managed poolers use the shared RefreshRouting routine. Followers read local metadata at operation time, waiting within a bounded deadline for the exact operation ID and mode. WAL replay is neither quorum confirmation nor proof of application drain; confirmed sender dispatch and explicit per-process acknowledgments provide those separate guarantees.

Sources use one protected GetSourceConnection callback per RefreshRouting command,
returning confirmed routing and configuration together. Configuration changes
reject opening but cannot prevent closing/drain. Catalog-backed credential
bootstrap remains protected. Unmanaged sources keep register-before-read and
backend/identity checks; readiness changes can retry initialization, but warm
sources do not reinterpret policy during target outages. RefreshRouting remains
their enforcement interface. Routing is published through gateway health only;
the unused topology routing projection is no longer written.

Operation waiters subscribe to a shared local change channel before inspecting
the journal, so completion before subscription and between inspection/select
cannot be lost. Mutations, confirmed recovery and authority loss notify all
waiters; notifications are hints and each wake rechecks confirmed journal state.
Status confirmation does not notify itself. Cancellation, manager shutdown or
leadership loss releases waiters without changing the operation. After failover,
clients rediscover authority and recover or wait using the durable request ID.

General health heartbeats, PostgreSQL/external readiness monitors, topology watch
reconnects and topology cache sweeps remain. They supply existing lifecycle
signals or reuse snapshots; they do not periodically confirm migration state.

## Retained communication paths

| Path | Purpose |
| --- | --- |
| Target PostgreSQL to managed replicas | WAL replicates routing, owner and journal metadata; consensus recovery establishes leadership and durability. |
| Managed/source poolers to gateways | Existing health streams supply confirmed routing and endpoint readiness; queries, authentication and cancellation keep their existing RPCs. |
| Migrator to target serving control | In-process transition, journal callback, migration identity and operation status/wait APIs. |
| Target leader to managed peers | Operation-time RefreshRouting observes the exact local WAL operation, enforces admission and acknowledges completed drain/activation. |
| Target leader to source poolers | Operation-time RefreshRouting validates confirmed authority and closes/drains or activates the selected source. Attach preparation reads one initial readiness snapshot. |
| Source to target authority | One protected configuration/routing validation callback per enforcement command, plus credential bootstrap. |
| Cold managed follower to target authority | Bounded fresh validation establishes admission; local WAL reads cannot establish quorum proof. |
| Admin gateway to target authority | Narrow authenticated administration and durable status/wait requests. |
| Operator to target authority | Infrequent confirmed migration-completion check before source removal. |
| All components to topology | Existing discovery, process identity and lifecycle publication. |

A legacy attached prototype catalog with an empty `migration_id` fails controller
ownership checks. Upgrading such a catalog requires explicitly identifying its
owning attach request; recovery does not guess or silently adopt ownership.
Topology records must remain until the existing shutdown lifecycle proves the
process stopped. Privileged out-of-band deletion is not a substitute for drain
acknowledgment or process-death evidence.


## Draft protocol compatibility

RefreshRouting replaces the two draft enforcement RPCs; empty Fence/Unfence adapters are not retained. Deploy the updated M2 binaries together. Before upgrading older prototype catalogs, complete pending operations with the previous binaries or explicitly migrate their ownership and operation identities; the new split attach phases cannot safely infer those identities. Pending operations created by this protocol recover using their original IDs. Existing catalog fields and SQL/controller interfaces remain; attach now exposes an internal `<request-id>/fence` operation before its activation operation. Standalone M1 endpoint operation is unchanged. The operator does not call enforcement RPCs and needs no lifecycle redesign.

A disposable PostgreSQL test pauses an asynchronous standby while a synchronous standby confirms a new operation with the same mode. The asynchronous reader cannot acknowledge until the exact new operation replays. The local coexistence test additionally elects a new managed-cohort leader during an incomplete source drain, recovers by the original operation ID, and checks gateway fencing. These tests do not implement migration replication or fence direct external clients.
