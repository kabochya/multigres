# Test migration controller

This package is an executable test controller, excluded from the production
multipooler binary. It demonstrates one managed target cohort and its unmanaged
source poolers across all cells. It does not implement replication or promise a
data-consistent migration. `SIMULATED-replication-barrier` is explicitly a test
hook, not evidence of source/target synchronization.

## Run the integrated demonstration

From the repository root, using the repository development-test workflow:

```sh
rtk proxy env GOTOOLCHAIN=go1.26.8 make build
rtk proxy scripts/portpool.sh start
rtk proxy env GOTOOLCHAIN=go1.26.8 MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock go test ./go/services/multipooler -run '^TestDemoControllerElectionDuringDrain$' -count=1 -timeout=5m
```

The test launches actual managed multipoolers/PostgreSQL, an external PostgreSQL
server and unmanaged poolers, multiorch, topology and multigateway. An isolated
child-test process hosts the demo controller alongside the managed manager.
Blocking private pipes drive it; no production workflow RPC or listener exists.

The demo enables control with both destinations CLOSED, drains and acknowledges
all required process incarnations, opens source admission, then publishes SOURCE
routing. It holds a source transaction while closing source admission, kills the
managed primary PostgreSQL, elects a new authority, and recovers the same durable
close request. The reservation can finish while stale source-routed requests are
rejected. After the simulated barrier, the restored targets acknowledge OPEN;
only then does the controller publish MANAGED routing and authorize retirement.
Terminal source CLOSED/target OPEN projections persist. A subsequently joining
source initializes CLOSED and rejects the old OPEN request.

## Ownership and cross-service calls

An authorized provisioner stores immutable encrypted connection configuration.
The operator references its name, mounts the migration key, and owns workload
lifecycle. Readiness and loading configuration do not authorize admission.

`demo_operations` and `demo_current` belong exclusively to this test controller.
Poolers never read their phases. A real migration controller would supply its
own journal and compose the stable catalog primitives in the same transaction.
Its required sequence is:

1. Activate against the current managed authority. The existing action lock and
   consensus-derived writable role pin each short transaction. Replaced local
   workers and leadership changes invalidate the activation handle.
2. Lock controlled scope, current journal operation and both exact admission
   projections. Validate owner, request payload and completed predecessor.
3. Atomically journal intent and update normalized admission; commit using the
   current synchronous durability policy. Uncertain outcome returns no authority
   to advance. Recovery performs a WAL-generating confirmation write.
4. Release catalog/action locks. Discover every cell and persist the required
   process set. Invoke authenticated `RefreshAdmission` with exact expected
   intent and process incarnation. Sources validate via `ReadAdmissionIntent`
   against the current managed authority; initialized followers read local replay.
5. Record exact acknowledgments, re-enumerate joins, revalidate journal ownership
   and both projections, and synchronously journal completion. Missing processes
   are retained obligations. Actual process termination may supply separate
   evidence; topology disappearance and a shutdown advertisement cannot.
6. Keep source close/drain and target activation as different operations. Publish
   routing only from the snapshot of a confirmed catalog transaction. Existing
   managed-authority health streams deliver it to gateways, with no fallback.
7. Retain controlled applicability and terminal intents. Authorize retirement
   separately. The operator supplies exact container-termination evidence. Proof
   release requires a further controller decision that no recoverable operation
   references that evidence; the demo deliberately retains it.

`Advance` makes a bounded attempt; interrupted work is recovered by the same
request ID and identical arguments on a newly activated authority. It never
rolls back intent or reopens gates. `Status` confirms durable journal state.
`Wait` subscribes before inspecting status, uses notifications as hints and
supports multiple/canceled waiters. Status confirmation does not notify. A waiter
cannot cancel the migration operation. Supersession and authority loss stop it.

No migration-specific idle poller, lease, receiver version, persistent peer
subscription or new consensus protocol is introduced. General health, topology
and PostgreSQL monitoring infrastructure is retained.

## Validation boundaries

Deterministic tests cover transition ownership/predecessors, process identity,
writer invalidation, lost wakeups, multiple/canceled waiters, read-only confirmation
and supersession. Disposable PostgreSQL tests cover uncertain intent/completion,
lost acknowledgments, confirmation after restart, cross-cell membership, joins,
missing-process obligations and WAL replay. Existing connection/routing/admission
PostgreSQL tests retain their durability and replay coverage. Existing admission
unit tests retain delayed requests, drain timeout, backend acquisition races,
warm-source outages and idle-state behavior.

The integrated test uses actual managed election during incomplete enforcement.
The operator companion has unit/race tests for exact container termination,
retirement and proof-release authorization. Kubernetes node-loss acceptance is a
separate environment test, not demonstrated by fake-client unit tests. Direct
clients of the external PostgreSQL server remain outside pooler fencing.
