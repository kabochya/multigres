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
