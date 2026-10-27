# Open questions of the fencing and integration stacks

Each question is a design decision that was not taken here. The code carries the
proposed default, which is the most conservative option, and each is kept to one
place so that changing it is small. Where a question changes a later layer, the
layer is named.

## 1. TLS to the default primary (f-03)

**Situation.** In the fencing stack every managed pooler dials the default primary
to read its admission state. The unmanaged stack builds that transport with
`TransportCredentials`, which uses only the `--multipooler-grpc-*` client flags, and
nothing else. A cluster whose poolers serve TLS and that upgrades therefore dials
the default primary without TLS from every managed pooler until those flags are set
on each of them.

**Default taken.** Keep the plain `TransportCredentials`; no fallback to the
pooler's own server certificate. A misconfiguration of the client flags fails the
process at startup (the transport is built in `initMultipooler`).

**Proposed fix, not made.** Default the pooler's client CA from its server CA at
multipooler init, CA only: when no `--multipooler-grpc-*` client flag is set and the
pooler serves TLS, trust the CA it serves with, and present no client certificate.
The change is local to `go/services/multipooler/init.go`.

## 2. Session state left on another tablegroup after the traffic moved (f-05)

**Situation.** `rejectSessionStateOnAnotherTablegroup` finds a reservation the
gateway still tracks on a tablegroup the traffic left. A fence completes only when
its pooler holds no application reservation, so the reservation is gone, and the
move hides that: the next statement goes to the new tablegroup. The stack12 rule
answered 40001 and let the session go on. Upstream has since decided that a lost
reservation that held session state a retry cannot restore (a temporary table, a
session advisory lock, a seeded PRNG, a WITH HOLD cursor, a portal outside a
transaction) ends the session with a FATAL (`lostSessionState`,
`sessionStateLostError`).

**Default taken.** Hand such an entry to that handling: the session ends. A
reservation that held only a transaction keeps the 40001 and rollback of stack12.

**Alternative.** Return to 40001 and let the session go on. Delete the
`if lostState != 0` branch in `rejectSessionStateOnAnotherTablegroup`
(`go/services/multigateway/scatterconn/scatter_conn.go`) and restore the
expectations of the tests that name `requireSessionEnded`.

## 3. A second routing source (f-05)

**Situation.** The unmanaged stack has a static routing source
(`--routing-database`, `--routing-tablegroup`); stack12 replaced it with one that
follows the default primary.

**Default taken.** Keep the static source, and add the polling source next to it as
`pollingRouting` (`--routing-poll-database`, `--routing-poll-interval`), so a
gateway can run either. `RoutingSource.Start` gained a `ControlPlane` argument for the
reads of the polling source; the static one ignores it.

**Alternative.** Replace the static source, as stack12 did.

## 4. A binding that cannot be confirmed (f-03, i-01)

**Situation.** The unmanaged stack fails closed: an unmanaged pooler is ready only on
a definitive answer that its tablegroup is bound to its connection, and a default
primary with no directory of connections is no answer. Stack12 opened in that case.

**Default taken.** Fail closed, as the unmanaged stack does. Admission applies the
same check (`checkTablegroupBinding`) before it reads the fence, and answers
FAILED_PRECONDITION to a definitive contradiction; any other error is returned as it is.

## 5. What an unmanaged pooler does before i-01 (layering, not a design question)

Admission is in f-03 with the managed backend only. An unmanaged pooler on the
fencing stack without i-01 therefore takes the managed rules (it closes when its
role changes, and registers after it starts). i-01 gives it its own.
