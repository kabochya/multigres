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

The first foundation PR intentionally keeps unmanaged runtime startup disabled
until catalog bootstrap, register-before-read, physical identity and readiness
validation are connected. Plaintext standalone endpoint flags do not provide a
supported bypass.
