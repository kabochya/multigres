# Unmanaged poolers

Unmanaged poolers front an external PostgreSQL backend without owning pgctld,
backups, replication management or consensus. The managed cohort remains the
metadata authority. Managed ownership remains the default for existing topology
records and deployments.

The replacement implementation is being built in three milestones: routable
coexistence, independent pooler admission control, and an integrated test
controller. Gateway routing alone does not provide safe cutover under stale
clients. Replication and a production migration controller are outside scope.

## Connection provisioning foundation

`multigres.connections` lives in managed PostgreSQL, never on the source. Each
named configuration contains endpoint/database, credentials, and explicit TLS
mode/negotiation. The payload is encrypted with AES-256-GCM; authenticated data
binds it to its connection name. A separate random configuration binding is
non-secret and does not reveal a password hash or impose an ordered version.
PUBLIC has no privileges on the table.

Provisioning is create-only. Existing names cannot be overwritten or retargeted;
credential rotation, automatic refresh and hot reconfiguration are not supported
in the initial version. Reading configuration or discovering a healthy backend
does not authorize application admission.

The owning managed service must establish current consensus authority and hold
its existing action lock before using catalog write/confirmation helpers. They
accept a bounded transaction callback for composing metadata work. Transactions
must never wait on remote RPCs. Confirmation returns the protected snapshot from
a synchronous WAL-generating transaction using `remote_apply`; an uncertain
commit returns no snapshot. A local reread, including after a process restart,
is insufficient to authorize protected bootstrap or publication.

The initial migration scope has one managed target authority cohort. This is not
a cluster-wide database limit; it permits multiple managed replicas and source
poolers across cells. Migrations spanning unrelated cohorts must be rejected
before control enablement or source activation.

## Protected bootstrap

Deploy the routing-aware gateway implementation before introducing source
poolers. Older gateways that cannot distinguish backend ownership are not
supported in a coexistence deployment.

Sources require `--management-mode=unmanaged`, `--source-connection` and
`--migration-key-file`. Endpoint, password and TLS settings are loaded from the
managed catalog, not a standalone DSN. Register the fresh disabled process in
topology before the first authority read. Bootstrap uses bounded retries and the
existing protected RPC transport/key proof. Authority selection rejects unrelated
managed cohorts; the returned authority shard key must match the source scope.

Provisioning additionally proves target-superuser SCRAM credentials. Matching
retries return the same immutable binding; attempts to replace configuration are
rejected. Bootstrap confirms the protected snapshot synchronously while pinned
to managed leadership. Failure or an uncertain commit returns no configuration.

The external observation monitor validates writable state, expected PostgreSQL
system identifier and database before advertising readiness. It never repairs,
configures or stops external PostgreSQL. The initial routing milestone allows
prepared sources to serve; this is not exclusive migration admission. The
admission milestone adds independent controller-owned permission.

`/ready` remains control-plane reachability. Backend availability travels through
the existing health stream. Pooler shutdown withdraws service and leaves external
PostgreSQL running. Rotation and configuration hot reload remain unsupported.

## Gateway destination policy

`multigres.gateway_routing` stores a database-level MANAGED, SOURCE or BLOCKED
destination. SOURCE requires the named configuration binding and expected
physical identity. It does not contain admission intent, migration ownership or
workflow completion. `SetRoutingPolicy` validates protected target-admin
credentials and commits metadata once under `remote_apply`; it publishes the
snapshot from that transaction after confirmed commit, never a subsequent local
reread. It does not drain work, call peers or advance a migration.

Managed authority health streams deliver the confirmed policy. Startup,
leadership/backend transitions and a reconnect while initialization is incomplete
trigger bounded recovery bursts. Stable health broadcasts reuse the confirmed
snapshot. There is no periodic migration state-refresh transaction.

Sources remain outside the managed primary set. Gateways select ready source
processes with matching association/binding/physical identity, prefer their local
cell, and refuse automatic source/target fallback. A warm gateway retains its
accepted source destination during target outages. A cold gateway observing the
routing capability requires confirmed policy, even before source discovery.
Obsolete authority rules, canceled streams and replaced riders cannot replace
accepted policy. Existing reservations and cancel requests retain process
ownership; routing changes govern new destination selection.

Routing BLOCKED is traffic intent, not proof of completed fencing. M1 source
poolers can still admit requests from a stale gateway. Controller-owned admission
and close/drain barriers are implemented in M2; they remain separate from gateway
destination activation.

## Admission control

Gateway routing and backend readiness do not grant application admission.
Migration-capable processes register before applicability reads and start with
closed gates. The managed catalog retains a sticky `admission_scopes` marker
and per-subject normalized `admission_intents`. A controlled scope with a
missing intent fails closed; terminal cleanup must retain explicit source CLOSED
and target OPEN intents and the durable owner identity.

The controller composes intent and journal writes in a short authority-checked
synchronous transaction. Catalog helpers do not implement workflow sequencing,
fanout, global completion, or controller recovery. Both subjects share the same
scope row lock. Controller journal preconditions must prevent obsolete workers
from creating a conflicting next operation.

Authenticated `RefreshAdmission` carries an expected owner, exact opaque intent
ID, permission and binding. These are expectations, not overrides. Source
processes call the current managed authority; initialized managed followers
read replicated local metadata. Cold followers first confirm the authority and
wait for its exact replay boundary. Replay waits and lifecycle recovery retries
are bounded and cancellation-aware; unchanged idle state has no refresh ticker.

The shared actuator serializes reads, application and drain. OPEN validates
source preparation/binding and local lifecycle generation. CLOSED blocks new
work and waits for handler permits and backend reservations to drain. Existing
reserved work can finish on its original backend. No remote/drain waits hold
catalog transactions or the manager action lock. Acknowledgments identify the
exact intent and process incarnation, after enforcement completes and its local
lifecycle generation is rechecked. Timeout leaves admission closed and is not a
completion acknowledgment. Routing publication has separate initialization.

The local integration fixture writes normalized intents to test the primitives;
it is not a production migration controller and supplies no replication barrier.

## Lifecycle and retirement contract

Stable topology/consensus membership IDs are distinct from random process-start
incarnations. Preparing registration and health advertise the incarnation;
RefreshAdmission requires the expected incarnation and returns it with the
acknowledgment. A replacement cannot inherit an old process acknowledgment.
Gateways reject health from an obsolete incarnation when topology identifies the
replacement. Coordinated deployment is required for controlled migrations.

ReadSourceLifecycle provides a normalized controller-written authorization,
independent of routing and readiness. Retirement refers to the exact retained
source CLOSED intent, owner and configuration binding. Proof release is a
separate exact-process authorization, granted only after the controller checks
termination evidence and all recoverable journal references. The metadata
helper validates binding/closure; it does not decide workflow completion or
infer termination. Source Pods use never-restart semantics and unique Pod UID
component identities; operator termination proof covers that exact Pod process.

Uninterrupted initialized sources retain admission during target outages.
Backend withdrawal/reconnect invalidates initialization and closes the gate;
reopening requires current authority and physical/configuration identity.
Invalid authoritative controlled metadata closes admission. First installation
can establish an ordinary marker; a missing scope row in an already installed
catalog is not recreated as ordinary during restart/recovery. Controlled marker
and terminal intents must survive routine migration cleanup.

Lifecycle and health reconnect events trigger bounded initialization retries.
Read-only health publication of an initialized process triggers no recurring
confirmation transaction. No migration-specific periodic state poller or peer
subscription is introduced.
