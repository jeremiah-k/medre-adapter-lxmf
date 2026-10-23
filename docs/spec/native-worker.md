# Native worker ownership and message delivery

## Scope and implementation status

Real Reticulum adapters use an owned native process for SDK initialization,
message submission, inbound normalization, terminal facts and teardown. The
parent uses WorkerSession; SDK-free fake adapters retain the local session.
Discovery, configuration and parent adapter construction MUST NOT import native
SDK modules. The native session runs on the worker's main thread.

Moving the native session behind this interface keeps the SDK's signal handlers,
Reticulum singleton, native threads and exit hooks inside its own process. The
MEDRE process retains canonical admission, routing, outbox and feedback authority.
The worker MUST NOT receive core runtime handles or executable object references.

## Framing and authority

Configuration crosses a private stdin pipe, never command arguments. Frames use
length-prefixed MessagePack with a one-MiB limit and a closed tagged vocabulary.
Unknown tags, unknown fields, oversized lengths and truncated frames MUST fail
without exposing their payload in error text. There is no pickle or generic RPC
method lookup. SDK stdout MUST be separated from protocol output before SDK
imports; stderr MUST be drained with bounded private retention.

Start carries validated configuration and explicitly selects lifecycle-only or
message-plane operation. Ready identifies the child PID and reports
native initialization and its diagnostics snapshot. A parent MUST verify that
identity and require connected/router readiness; process exit zero alone MUST
NOT establish readiness. Ready MUST NOT imply a message handoff or delivery.

## Ownership and cancellation

One NativeWorker instance owns one process lifetime. It MUST retain a process
spawn task until ownership of its result is settled, including cancellation
before process creation returns. Repeated cancellation MUST NOT detach cleanup.
Caller cancellation MUST remain the raised outcome if cleanup fails, retaining
that failure as its exception cause.
No worker may announce readiness after shutdown has taken ownership.

Native initialization has a parent-owned deadline. A constructor that is blocked
or ignores signals MUST be terminable in its worker without taking over the
host's SIGINT or SIGTERM handlers. The operating system's own process creation
and reaping primitives remain prerequisites; no timeout can preempt a blocked
kernel operation.

A ready worker receives a cooperative Stop request. Its native session-stop
budget MUST be smaller than the enclosing parent deadline, reserving time for
pipe delivery, the Stopped response, and process exit. Stopped acknowledges
session cleanup; the parent MUST additionally observe and reap process exit. On a
shutdown deadline the parent MUST force termination and retain that fact in
WorkerExit. A received Stopped frame records native cleanup acknowledgement
independently of whether the leader exits before the deadline. Forced cleanup
of remaining process-group descendants MUST also be reported even when the
leader exited normally. A pre-readiness worker is force-terminated rather than
waiting for an SDK constructor to read stdin. Concurrent stop callers share
one cleanup task.
SDK descendants in the owned POSIX process group are retired before pipe EOF is
awaited. A native descendant that leaves that process group is outside the
worker's ownership; the parent MUST bound its own pipe draining, discard unsent
stdin bytes after the cooperative exchange, and close its pipe read ends so an
escaped descendant cannot retain owned pipes or hang stop() indefinitely. This
does NOT terminate or reap such an escaped descendant. A POSIX process-group
signal denied by the operating system MUST NOT bypass remaining pipe and leader
cleanup or be reported as a successful group reap.
POSIX group identity is reserved by a separate control child created outside
asyncio's automatic child reaper. The SDK worker joins that private group.
Before the final group signal, the control child acknowledges moving into the
host's group while its PID remains reserved. This leaves only native members
in the SDK group, so an empty group is distinguishable from forced descendant
cleanup. The parent MUST retain the control child without polling or reaping it
until all group signalling is complete, including unexpected control-child
exit. It MUST reject a missing acknowledgement and preserve that failure.

The host MUST NOT install a competing child reaper that consumes these owned
children. Supported qualification uses Python's default asyncio child handling.
The control pipe is a lifetime lease: its EOF retires the private group, including
when the host exits. Closing that lease and reaping the control child complete
teardown. Control exchanges and the owner's exit wait MUST be bounded; an
unresponsive owner MUST be terminated and reported as a cleanup failure. Windows descendant ownership requires separate qualification before
production rollout; terminating its leader alone is not a process-tree guarantee.

## Message plane and authority

Submit carries plain content, integer-keyed LXMF fields and an opaque operation
identity. Submitted acknowledges only a returned SDK call and a committed local
handoff fence. It MUST NOT establish recipient delivery. Native byte values and
integer map keys MUST survive framing without JSON coercion. SDK objects,
non-finite numbers and values nested deeper than 32 levels MUST be rejected.

The parent MUST derive operation identity from the immutable core attempt
provenance and retain that provenance locally. The child MUST NOT receive core
objects. DeliveryUpdate carries an opaque operation identity, native message ID
and provider-emitted terminal state. The parent MUST echo its captured attempt
context into PostHandoffObservation, including when feedback precedes the
submission reply. Unknown operations MUST NOT be correlated by destination or
message-text guesses. Contradictory native IDs MUST fail the generation.

One reader owns reply dispatch. Separate bounded inbound and feedback consumers
MUST keep slow canonical admission from blocking terminal facts or submission
replies. The channel permits 32 pending requests, 64 inbound packets and 128
terminal facts; the child permits 128 output frames. The parent retains at most
1,000 pending attempt contexts, refusing sends before native tracking can evict
a pending operation. Overflow or invalid protocol
MUST fail the generation and trigger owned process cleanup. Cancelling a request
caller MUST NOT detach response consumption or discard its pending fact context.
A timed-out native status probe degrades local health without interrupting an
active submission. Worker loss fails local health; remote reachability remains
unknown. Worker restart is an explicit lifecycle operation, never a blind resend.

Inbound normalization uses the established codec, classification, deduplication
and canonical admission path. LXMF acknowledges native receipt before invoking
its callback. Neither that native acknowledgement nor worker forwarding proves
durable canonical admission. Uncommitted inbound packets and terminal facts can
be lost across a crash; the adapter MUST NOT claim lossless ingestion or replay
terminal facts it did not preserve. Core owns durable event and feedback evidence.

## Persistent identity and submission fences

A message worker MUST exclusively lease its native queue storage and identity
for its lifetime. These are cooperative MEDRE ownership checks, not protection
against arbitrary third-party SDK consumers. A missing explicit identity uses a
private, atomically published identity under storage_path; restarts MUST retain
that identity. A missing or invalid explicit identity MUST fail startup rather
than silently generating a replacement.

The private .medre-handoffs.sqlite journal stores operation IDs, payload digests,
native IDs and dispatch outcomes, without plaintext message bodies. The worker
MUST durably reserve dispatching before entering native submission and commit
accepted before replying. An interrupted reservation is unknown. The same
operation with a different payload MUST be refused. A committed accepted
operation MUST return its saved handoff without another SDK call. Journal fences
MUST NOT be evicted automatically; deleting a profile discards its duplicate
protection and requires operator reconciliation of its native queue.

Possible acceptance followed by an exception, worker loss or reply timeout MUST
produce an explicit unknown outcome. Core receives a non-retryable dispatch
error requiring reconciliation, not a native negative delivery observation.
The worker MUST NOT claim exactly-once delivery or replay uncertain operations.
An accepted journal row does not preserve native SDK callback objects across
restart; absent terminal evidence remains unknown. SDK queues remain SDK-owned,
with no independent adapter retransmission loop.

Worker environments use an explicit OS/interpreter/locale/certificate-path
allowlist. Other adapters' credential variables MUST NOT be inherited. The SDK
and process-group owner receive the same filtered environment. This restricts
accidental secret inheritance; trusted native code retains ordinary filesystem
and operating-system access and is not sandboxed.

## Qualification

Real-process tests exercise bounded message forwarding, integer/byte fields,
early terminal facts, exact-attempt parent correlation, caller cancellation,
uncertain worker loss, durable handoff reuse and independent callback dispatch.
Lifecycle tests cover blocked construction, repeated cancellation, late spawn
ownership, noisy stderr, forced shutdown and actual host signals. Native SDK
tests require persistent default identity, exclusive ownership and clean restart
without radio interfaces. The core campaign independently checks native peers,
rendered attribution, durable outbox evidence and feedback observations.

Native bidirectional RF and platform process-tree termination are separate
qualification profiles. Linux POSIX worker ownership is supported; Windows
leader termination does not establish descendant cleanup. No hardware guarantee
may be inferred from SDK-only or local loopback tests.

## Repeatable RNode lifecycle profile

Run the hardware tier only while the selected radio and native profile are not
owned by another runtime. Set `LXMF_WORKER_HARDWARE_PROFILE` to a private JSON
fixture containing `config` (the LxmfConfig fields, including explicit identity,
storage and Reticulum paths) and `expected_destination_hash` (an independently
recorded 32-character hex destination). Map those paths and the serial device
into the test container. Keep the fixture and native state outside version
control.

```console
python -m pytest tests/test_native_worker_hardware.py -m hardware -v
```

The test opens two consecutive workers against the same profile, checks the
expected identity, requires acknowledged clean exits and verifies that host
signal handlers retain ownership. It does not establish message delivery,
peer reachability or worker data-plane qualification. Run the core's native
adapter campaign separately for bidirectional RF traffic and rendering evidence.
