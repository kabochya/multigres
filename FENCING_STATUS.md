# Status of the fencing (f) and integration (i) stacks

Built on `feat/unmanaged-s-07` (`732c4da7`, base main `d063d0b4`) from
`wip/unmanaged-prototype-v10` (`b637bcf4`, base main `3d7ea959`, 12 commits). Nothing of
`feat/unmanaged-s-*`, `t-*` or the wip branch was touched; no pull request was opened or
changed. The branches are new: `feat/unmanaged-f-01` .. `f-06`, `i-01` .. `i-03`, each on
the one before it, `f-01` on `s-07`. The head of `i-03` carries this file as its last
commit (a docs-only commit on top of the code commit below).

## Heads (code commits)

| branch | head | subject | prod +/- | test +/- |
|---|---|---|---|---|
| `feat/unmanaged-f-01` | `3d2aa8b6` | feat(multipooler): define the admission fence and routing RPCs | +744 / -4 | +36 / -0 |
| `feat/unmanaged-f-02` | `9861ae39` | feat(multipooler): add the application gate and report what holds a fence | +961 / -43 | +1357 / -6 |
| `feat/unmanaged-f-03` | `d81510e0` | feat(multipooler): admit application work from the persisted admission state | +1573 / -44 | +1306 / -3 |
| `feat/unmanaged-f-04` | `1923a078` | feat(multipooler): coordinate fences and routing on the default primary | +1354 / -50 | +2360 / -0 |
| `feat/unmanaged-f-05` | `9b03a76a` | feat(multigateway): hold requests while a tablegroup is fenced and follow the routing | +681 / -80 | +1760 / -84 |
| `feat/unmanaged-f-06` | `98a7ab4e` | test(multipooler): add end-to-end tests of the routing statements | +0 / -0 | +623 / -0 |
| `feat/unmanaged-i-01` | `d423187e` | feat(multipooler): admit an unmanaged pooler from the same admission state | +58 / -2 | +127 / -3 |
| `feat/unmanaged-i-02` | `5aa97979` | test(multipooler): add a reference migrator and a fake control server | +0 / -0 | +3556 / -0 |
| `feat/unmanaged-i-03` | `c411b7ea` | test(multipooler): add end-to-end scenarios for unmanaged poolers and migration | +0 / -0 | +2926 / -0 |

Lines are `git show --numstat` of the commit, generated files (`go/pb`, `web`) excluded;
"test" is `_test.go` and `go/test/**`, "prod" is the rest (the proto, and
`FENCING_OPEN_QUESTIONS.md` in f-01).

## Moves from the planned boundaries

- **Routing tables read path in f-03, not f-04.** Every pooler reads its admission row
  with `GetRoutingState`, so `routing_store.go` (the whole file, with its schema and SQL
  in `go/common/protometadata`) is in f-03. Its write half, the coordinator and the
  routing ops are in f-04. In f-03 the helpers only the write half uses
  (`fenceRow.closed`, the operation state constants and four snapshot methods) are
  absent and f-04 adds them, for the unused-code lint. `admission.go` ends before
  `newAdmissionCoordinator`; f-04 restores the rest. f-03 carries a stand-in
  `admission_scope.go` (`requestScope`, `canonicalOperationID`) that f-04 deletes, since
  those live in `coordinator.go`. The RPC handlers and the `RoutingProvider` interface
  have only `GetRoutingState` and `RefreshAdmission` in f-03.
- **No unmanaged backend in f-03.** `newAdmissionBackend` returns the managed one
  whatever the mode; i-01 restores the choice. Five admission unit tests that need it
  (`TestAdmissionBackendUnavailableNeverOpens`,
  `TestAdmissionLoopRedecidesAfterTheGateWasClosedBehindIt`,
  `TestReplicaWithADownBackendAcknowledgesAnUnfence`,
  `TestAPoolerThatCouldNotReadKeepsTryingUntilItCan`,
  `TestAnUnmanagedPoolersDecisionStandsAcrossAReconnect`) and the unmanaged half of
  `admission_backend_test.go` come in i-01.
- **f-06 has only `routing_sql_test.go`.** `coordinator_test.go` (an e2e scenario),
  `gateway_routing_test.go` (helpers of the migration scenarios) and
  `unmanaged_admission_test.go` need the migration harness, so they are in i-03.
- **The large COPY test (`large_result_test.go`) and the docs of MTF02 are in f-05.**
- `MTF02` and `PgSSUndefinedTable` are not in `s-07`; f-01 adds them to `go/common/mterrors/code.go`.

## Checks

Per commit: `go build ./...`, `go vet ./go/...`, `gofmt -l`, and
`golangci-lint run --new-from-rev=<previous commit>` (v2.15.0, built with go 1.26; the
installed 2.5.0 cannot load this module's config) report nothing; `go test -race -short`
on the packages the commit touches passes except for the failures below, which fail the
same on `s-07`.

At the top (`i-03`): `go test -race -short` over `go/services/multipooler/...`,
`go/services/multigateway/...`, `go/common/...`, `go/services/multiorch/...`,
`go/test/migrator`, `go/test/fakecontrol`.

Failing on `s-07` too (environment: the tests run as root, so a read-only file can be
written): `servenv` `TestRun_ServesEveryGrpcServer`, `TestServiceIdentityInLogs`,
`TestServEnvTelemetryIntegration`; `manager/consensus`
`TestConsensusPromises_SetRecruitBlockedUntilFilesystemReadOnly`; `manager`
`TestRecruit/PersistFailure_FilesystemReadOnly`,
`TestRestartAsStandby_FileWriteFailureFallsBackToSQL`. `poolergateway` has a data race in
`execute_stream_pool_test.go` (`TestMalformedFrames`, `TestTransportLossNotReplayed`,
`TestTransportEOFCannotReportSuccess`) that appears at random under `-race`, also on `s-07`
(20 runs of those tests on `s-07`: 1 failure).

End to end (run as an unprivileged user with the built binaries and the port pool):
the routing statements (`TestRoutingStatementsRunAnOperationThroughItsLifecycle`,
`TestCleanupWaitsForASwitchAndDeletesNothingUnderItsRule`,
`TestRoutingChangesAreSerializedAndNeverDeadlock`,
`TestFenceWritesRacingACleanupLeaveNothingBehind`,
`TestReplyOfARetryIsTheReplyOfTheFirstCall`) pass.
**Not run:** everything that needs a managed cohort: the migration scenarios, the
unmanaged-admission and gateway tests, `queryserving` and `multipooler` e2e. In this
container the managed cluster never becomes ready (`pooler-1 should become ready within
40s`, its first backup fails), and `TestMultigatewaySetup` fails the same on `s-07`. So the
migration scenarios (i-03), which were adapted (see below), have been compiled and vetted
but not run in this session; neither has the adapted
`TestASessionWhoseReservationWasReapedIsEndedAfterACutover`.

Generated files: `go/pb` was regenerated with `make pb` (protoc 25.1, the plugins
`make tools` installs); `web/multiadmin/.../clustermetadata_pb.ts` is not changed by any
of this. The `clustermetadata.pb.go` that go 1.26 gofmt reformats was left as it was.
`validate-generated-files` should be checked in CI.

## Open questions

See `FENCING_OPEN_QUESTIONS.md` at the root of f-01: TLS to the default primary,
session state left on another tablegroup, the second routing source, a binding that
cannot be confirmed.

## Skipped

- `discard_temp_primitive.go` deferred BEGIN (#1591) and the test
  `TestScatterConn_DiscardTempInsideATransactionKeepsItsDeferredBegin`: not re-added.
- Parser identifier truncation (#1587) and the IPv6 dial (#1588): in the base of `s-07`
  already, the wip copies are dropped. The code of #1589, #1590, #1592 and #1594 could not
  be checked: the session has no upstream remote, and `origin/main` stops at #1595 with none
  of them in its history. If any of them touches what f-02 (`liveness` peek), f-05
  (copy target pinning, the large COPY test) or `pubsub` adds, it will conflict or
  duplicate.
- The wip's `TransportCredentialsOr`, `TLSPaths` and the static-to-polling replacement of
  the routing source (see open questions 1 and 3).
- Out of scope, as asked: F1/F4/F7 gap fixes, shard and table scopes, health-stream
  propagation, the table alignment.

## Upstream candidates

- Copy target pinning (`MultigatewayConnectionState.copyTarget`, `copyDataTarget`,
  `copyService` in `scatter_conn.go`) is independent of fencing: it pins the data phase of
  a COPY to the pooler and target that started it.
- The liveness peek on a pooled backend connection (`go/common/pgprotocol/client/liveness*`).

## Proof: `git diff wip/unmanaged-prototype-v10 feat/unmanaged-i-03`

The diff has 209 files. Every hunk is one of: (M) an upstream main change between the two
bases (22 commits, `3d7ea959`..`d063d0b4`); (U) a change of the `s-*` stack since stack12;
(D) an upstream or deliberately dropped piece; (A) an adaptation. Besides, the two
`FENCING_*.md` files are added.

### (M) main drift, 150 files, no hunk of mine

Files the wip did not touch and that differ only because `s-07` sits on a newer main:

- `.github`: 8 files
- `.gitignore`: 1 files
- `Dockerfile.integration-test`: 1 files
- `config`: 2 files
- `docs`: 3 files
- `go/cmd/minigres`: 4 files
- `go/common/consensus`: 10 files
- `go/common/constants`: 3 files
- `go/common/fakepgserver`: 2 files
- `go/common/keyrange`: 2 files
- `go/common/mterrors`: 2 files
- `go/common/pgprotocol`: 2 files
- `go/common/queryrpc`: 1 files
- `go/common/rpcclient`: 3 files
- `go/common/topoclient`: 6 files
- `go/observability/metriccatalog`: 1 files
- `go/services/multigateway`: 22 files
- `go/services/multiorch`: 42 files
- `go/services/multipooler`: 19 files
- `go/test/endtoend`: 4 files
- `go/test/utils`: 2 files
- `go/tools/pathutil`: 2 files
- `go/tools/viperutil`: 6 files
- `web`: 2 files

### (D) dropped

- `go/common/parser/delimited.go`, `delimited_test.go`, `lexer.go`: #1587, in main.
- `go/common/pgprotocol/client/conn.go`, `conn_test.go`: #1588, in main.
- `go/common/servenv/grpc_server.go` (`TLSPaths`), `go/common/rpcclient/conn_cache.go`
  (`TransportCredentialsOr`): the TLS fallback, not wanted (open question 1). `s-07`'s
  `TLSConfigured` stays.
- `go/services/multigateway/engine/discard_temp_primitive.go`: #1591.
- `go/services/multipooler/grpcmanagerservice/service.go` (a comment about unmanaged
  reload): the stack's own text is kept.

### (U) the `s-*` stack since stack12: the file is `s-07`'s, 12 files

`go/common/rpcclient/conn_cache_test.go`, `conn_cache.go`;
`go/services/multiorch/consensus/coordinator.go`;
`go/services/multipooler/internal/connpoolmanager/config.go` (sslmode `require` as the
default for the external connection), `config_test.go`;
`go/services/multipooler/internal/manager/external_monitor.go`, `external_monitor_test.go`
(the binding recheck every 30 s, fail closed), `tablegroup_binding.go`;
`go/services/multipooler/source_bootstrap_test.go` (the insecure transport warning);
`go/test/endtoend/shardsetup/unmanaged_backend_test.go` (tablegroup argument of
`startUnmanagedPooler`, the `Recruit` check), `unmanaged_gateway_test.go`.
Their hunks are (U) wholly.

### Files with F/I content: what differs from the wip

In each file below the (M) and (U) hunks are everything not listed as (A).

- **Generated** `go/pb/**`, `proto/*.proto`: (U) the connection and binding messages of
  `s-07`, comment wording of `GetTablegroupBinding` (A: the `s-07` text, because binding is
  confirmed while ready too), and the `Target.control` comment (A: the `s-07` text and the
  gate sentence joined). The pb files are regenerated from the protos.
- **`go/common/mterrors/code.go`**: (M) the session-state-lost error of #1568.
- **`go/services/multigateway/init.go`, `routing_source.go`, `prototype_routing.go`,
  `prototype_routing_test.go`**: (U) per-gateway routing sources and the static source;
  (A) `Start` takes a `ControlPlane` (the wip's signature, kept); the static source keeps its
  type and flags and ignores it; the wip's polling source moves to the new
  `polling_routing.go`/`polling_routing_test.go` as `pollingRouting`
  (wip `prototypeRouting`), registered by a factory like the static one. The wip's file
  `prototype_routing.go` therefore appears as (U) static source + (A) one hunk.
- **`go/services/multigateway/scatterconn/scatter_conn.go`**: (M) `applyReservedStateOnError`,
  `droppedSessionState` (#1568); (A) 8 hunks: `rejectSessionStateOnAnotherTablegroup` takes a
  context and hands an entry that held session state to `sessionStateLostError`
  (open question 2); `CopyOutStream` applies the reserved state with
  `applyReservedStateOnError` and the pinned service.
- **`scatter_conn_test.go`**: (M) mock changes of #1568 (`streamExecuteTarget`,
  `discardTempTablesErr`); (A) the tests that expected 40001 and a session that goes on for
  session state now expect the FATAL: `TestScatterConn_AReservationOnAnotherTablegroupEndsTheSession`,
  `TestScatterConn_ASuspendedPortalOutsideATransactionEndsTheSession` (renamed from
  `...IsLostWith40001`, `...IsRefusedWith40001`), the temporary-table subtests of
  `...AReapedReservationAndAMove...`, the WITH HOLD iteration of
  `...ASuspendedPortalInATransaction...`; unused helpers removed;
  `TestScatterConn_DiscardTempInsideATransactionKeepsItsDeferredBegin` dropped (D).
- **`poolergateway/app_routing.go`, `app_routing_test.go`, `unmanaged_routing_test.go`**:
  (U) the helper functions and `AppRouting` tests moved into `app_routing_test.go`; (A) the
  wip's helper `reportHealth` is `reportGateHealth` (it takes the gate flag), duplicates of
  `unmanagedPooler`, `controlTarget`, `writableTarget` and the three tests `s-07` already
  has are removed from `unmanaged_routing_test.go`, and `TestColdGatewayFailsClosedUntilRoutingIsKnown`
  is the wip's (with the buffer) in `unmanaged_routing_test.go` instead of `s-07`'s in
  `app_routing_test.go`; (A) `TestWhatChangesInStepWithTheRoutingIsNeverBehindIt` is the
  wip's table version.
- **`poolergateway/pooler_gateway.go`, `pooler_connection.go`, `grpc_query_service_test.go`**:
  (U) `GetBackingConnection`/`GetTablegroupBinding` of the client mock (the wip's duplicates
  removed); otherwise (M).
- **`go/services/multipooler/init.go`**: (U) validation with `keyrange`, `Record()`, the
  consensus service registered only when enabled; (A) the default primary transport is built
  for every pooler, with `TransportCredentials` only; (A) `StartAdmission` and the
  `StopTopoRegistration` result kept from the wip.
- **`manager/manager.go`**: (U) `bindingSource`, `bindingConfirmedAt`, keyrange validation,
  database cache removal; (A) one hunk: the `defaultPrimary` comment names admission reads.
- **`manager/config.go`, `default_primary_client.go`**: (A) comment wording that names
  admission next to the metadata reads.
- **`manager/admission.go`, `admission_test.go`, `admission_read_test.go`**: (A) the test hook
  for the binding is `pm.bindingSource` (`s-07`'s) and `admissionRuntime.binding` is gone; a
  definitive contradiction of the binding is FAILED_PRECONDITION; "a default primary that
  predates the RPC" no longer opens an unmanaged pooler (open question 4).
- **`executor_test.go`**: (A) gofumpt blank lines only.
- **`go/test/endtoend/shardsetup/*`**: (A) `startUnmanagedPooler` has the tablegroup
  argument; the helpers `appRole`, `appPassword`, `appDSN`, `createAppRole`, `backendPort`
  are `unmanaged_gateway_test.go`'s (removed from `gateway_routing_test.go`);
  `TestASessionWhoseReservationWasReapedIsEndedAfterACutover` (was `...Gets40001...AndGoesOn`)
  expects the FATAL and a fresh session; `errcheck` and `modernize` fixes in
  `migration_default_test.go`, `migration_scenarios_test.go`, `routing_sql_test.go`.
- **docs, comments**: (U) the NOTIFY and SCRAM identity notes of `s-07` kept in
  `notify.go` and `failover_buffering.md`; the wip's gate wording is added next to them.
