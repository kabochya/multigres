# Local migration serving prototype

This demo runs a two-node managed target, stock PostgreSQL as the external
source, two unmanaged poolers in different topology cells, and a gateway with
application and target-admin listeners. It uses disposable local databases.
The fixture does not replicate source data. The `migration_demo` build tag adds
an authenticated, loopback-only controller stub so the actual journal and mode
transaction hooks can be exercised without implementing a migrator.
Never deploy a tagged demo binary.

## Run the assertions

From the tip of the draft stack, with PostgreSQL 17 tools, etcd, pgBackRest and
protobuf generators installed according to the repository development setup:

```sh
rtk proxy env GOTOOLCHAIN=go1.26.8 GOFLAGS=-tags=migration_demo make build
rtk proxy scripts/portpool.sh start
rtk proxy env GOTOOLCHAIN=go1.26.8 MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock go test -tags=migration_demo -run '^TestMigrationServingCoexistence$' -count=1 -timeout=15m -v ./go/test/endtoend/shardsetup
```

Put `bin` and the PostgreSQL tools on PATH first. A passing run proves source
preparation in two cells, encrypted connection storage, administrative retries,
source-only writes, preserved source traffic during target loss, cold gateway
failure when authority is unavailable, managed leader recovery, reserved
transaction drain, admin authentication during fencing, journal rollback,
forward/reverse routing, source endpoint failover, completion and detach.
It also verifies that decommissioning leaves the source running and does not
install a Multigres schema there.

The target and source share Alice's complete SCRAM verifier. Identical plaintext
passwords with independently generated salts do not support the existing SCRAM
client-key backend authentication path.

## Keep the cluster running

```sh
rtk proxy env GOTOOLCHAIN=go1.26.8 MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock MULTIGRES_MIGRATION_DEMO_DIR=/tmp/multigres-migration-demo go test -tags=migration_demo -run '^TestMigrationServingCoexistence$' -count=1 -timeout=0 -v ./go/test/endtoend/shardsetup
```

After the assertions, this restores source serving and writes:

- `/tmp/multigres-migration-demo/endpoints.json`: allocated ports and fixture paths.
- `/tmp/multigres-migration-demo/pgpass`: local fixture credentials, mode 0600.

Use the ports from `endpoints.json`. For application queries:

```sh
rtk proxy env PGPASSFILE=/tmp/multigres-migration-demo/pgpass psql -h 127.0.0.1 -p GATEWAY_PORT -U alice -d postgres -c 'SELECT value FROM routing_marker'
```

It returns `unmanaged`. On `ADMIN_PORT`, authenticate as `postgres` and run:

```sql
SHOW SERVING MODE;
PAUSE SERVING REQUEST ID 'manual-pause-1';
RESUME SERVING REQUEST ID 'manual-resume-1';
```

The target-admin listener only bypasses the application gate for serving-control
operations and administrator credential lookup. Ordinary SQL remains subject
to routing. New application traffic cannot authenticate/query while FENCED;
whether it waits or immediately returns an unavailable error depends on gateway
buffer configuration. Existing reserved work finishes at its owning source.

To exercise the controller hooks explicitly:

```sh
rtk proxy env GOTOOLCHAIN=go1.26.8 go run -tags=migration_demo ./go/test/migrationdemo -operation demo-fence -request-id manual-controller-fence
rtk proxy env GOTOOLCHAIN=go1.26.8 go run -tags=migration_demo ./go/test/migrationdemo -operation demo-managed -request-id manual-controller-managed -fence-request-id manual-controller-fence
```

The same application login now returns `managed`. The admin cannot resume a
controller-owned fence. For a reverse switch, call `demo-fence` with a new ID,
then `demo-unmanaged` with another new ID and `-fence-request-id` naming that fence. To decommission, switch to MANAGED,
call `demo-complete`, and run `DETACH CONNECTION source REQUEST ID 'manual-detach'`
on the admin listener. These hooks simulate the controller's replication barrier;
they provide no replication correctness guarantee.

The helper uses the recorded target RPC endpoint; rerun the fixture if that
endpoint loses leadership. SQL control through the gateway discovers the current
managed authority independently of application serving status.

Stop and clean up the fixture with:

```sh
rtk proxy touch /tmp/multigres-migration-demo/stop
```

## Remaining validation

This prototype does not demonstrate a real replication barrier, source role
revocation, or data reconciliation. Full fault testing of COPY/cancellation,
LISTEN/NOTIFY and reserved sessions across switches, target failover during
an in-flight catalog transaction, and Kubernetes node-loss lifecycle behavior
remain acceptance work. Authoritative publication and completed retries now
confirm durability using synchronous WAL-generating writes; durability still
depends on the managed cohort's configured replication policy. The disposable
PostgreSQL regression is `TestPostgresUncertainCommitAndRestartConfirmation` in
the servingcontrol package (run without `-short` and with PostgreSQL tools on PATH).
