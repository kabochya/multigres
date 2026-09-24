# Standalone unmanaged multipooler prototype (M1)

An unmanaged multipooler routes client SQL to an externally operated PostgreSQL
server. Deploy exactly one pooler for a logical shard, with no managed poolers in
that shard. The gateway rejects discovered mixed or duplicate membership,
including reserved-session lookups. This is a deployment guard using each
gateway's discovered membership, not an atomic cluster-wide routing policy or a
cutover fence. Coexistence and migration cutover are outside this prototype.

## Start

Create the logical database and cell in topology as for a normal deployment.
A backup location and durability policy are not required by the unmanaged pooler.
Run multigateway against the same topology and cell. Start the pooler with:

```sh
multipooler \
  --management-mode=unmanaged \
  --backend-host=source.example.com --pg-port=5432 \
  --backend-database=postgres \
  --database=postgres --table-group=default --shard=0-inf \
  --cell=zone1 --service-id=external-source --hostname=pooler.example.com \
  --grpc-port=15270 --http-port=15271 \
  --topo-global-server-addresses=http://etcd.example.com:2379 \
  --topo-global-root=/multigres/global \
  --connpool-admin-user=credential_reader \
  --connpool-admin-password-file=/run/secrets/source-reader-password \
  --pg-client-sslmode=verify-full \
  --pg-client-sslrootcert=/run/secrets/source-ca.pem \
  --service-map=grpc-pooler
```

`--backend-database` defaults to `--database`. `--hostname` advertises the pooler's
address; `--backend-host` identifies PostgreSQL. Supply a hostname or IP, not a
DSN. Backend TLS uses the existing PostgreSQL TLS options. The source reader's
password comes from the existing secret-file/environment configuration, never
from topology. No new gateway password store or client DSN passthrough is added.

Do not set `--pooler-dir` or `--socket-file` in unmanaged mode. PGDATA and pgctld
are unnecessary. Management mode is immutable for the lifetime of a process;
changing ownership is not a supported operation. Legacy unspecified topology
records retain managed behavior.

## Authentication and client SQL

Clients connect to multigateway using their existing PostgreSQL username,
password, and logical database. The existing SCRAM path reads the user's SCRAM
verifier from the external source and authenticates backend connections as that
user. Passwords must use SCRAM-SHA-256; other auth formats are outside M1.

The configured catalog-reader role needs LOGIN, CONNECT to the source database,
and SELECT access to `pg_catalog.pg_authid`. A source administrator can provision
this role and grant SELECT; the reader itself need not be a superuser. Treat
catalog read access and verifier material as sensitive credentials. The smoke
test exercises a non-superuser reader and a separate unprivileged client role.
Hosted-source policy may prevent granting this access; validate it before rollout.

Explicit client SQL, including role DDL, uses PostgreSQL's privileges. The pooler
does not import users or synchronize password changes into future targets.
Preparing target roles and credentials for transparent cutover belongs to the
migration project. Existing credential-cache behavior is unchanged.

## Readiness and ownership invariants

The pooler starts DISABLED. A bounded read-only probe checks that PostgreSQL is
reachable, out of recovery, and transaction-writable. It then advertises
PRIMARY/SERVING without a managed consensus rule. Failed, malformed, or read-only
probe results disable serving; subsequent successful probes restore it.

This is periodic health observation, not instantaneous fencing. It must not be
used as the migration routing switch: the monitor automatically re-enables
serving when the source becomes writable. There is no replication or automatic
failover to a target, and no unmanaged replica-read service in M1.

Unmanaged poolers are excluded from multiorch's recovery cache. They construct no
pgctld client, consensus manager, or backup engine. They do not create sidecar
schemas, publish database heartbeats, manage replication, load backup settings,
or enable backend VPID tracking. Manager and consensus gRPC services are not
registered even when requested in the service map. Query/health service and the
HTTP status endpoint remain available; management Status RPC is unavailable.

## Decommission

Remove the pooler from its deployment's restart/reconciliation policy and send
SIGTERM. Allow the configured graceful-shutdown timeout: the pooler withdraws
serving, drains client work using the existing shutdown mechanism, and closes its
connections. It never stops, promotes, demotes, backs up, or deletes source
PostgreSQL. Verify the source remains accessible directly and the pooler is no
longer serving. Remove the retired topology record through the deployment's
normal topology cleanup after the process is stopped. Do not remove a live
source record as a routing-switch operation.

## Verification

The stack includes unit tests for mode/config validation, orchestration
exclusion, management-free construction, readiness failure/recovery, and gateway
membership guards (including reserved sessions).

```sh
make build
scripts/portpool.sh start
MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  go test -run '^TestUnmanagedStandalone$' -count=1 \
  ./go/test/endtoend/shardsetup
```

The test needs stock PostgreSQL binaries and etcd on PATH. It starts PostgreSQL,
etcd, a pooler, and a gateway, with no pgctld or multiorch. It verifies SCRAM login,
wrong-password rejection, SQL writes, no sidecar creation, unavailable management
RPCs, read-only/recovery transitions, and shutdown leaving the source running.
TLS option handling uses the existing implementation and unit coverage; this
standalone smoke test uses loopback plaintext and does not claim V2/TLS rollout
validation.
