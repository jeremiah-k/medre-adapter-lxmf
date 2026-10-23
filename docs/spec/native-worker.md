# Native worker lifecycle

## Scope and implementation status

The internal NativeWorker interface owns a dedicated process for native SDK
initialization and teardown. The production adapter continues to use its
established session implementation. Message submission, inbound forwarding,
terminal delivery observations and recovery are not wired through this worker.
This lifecycle module MUST NOT be reported as production transport qualification.

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

Start carries validated configuration. Ready identifies the child PID and reports
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
Because asyncio may reap the worker group leader before the parent signals the
group, its numeric process-group ID can subsequently be reused. A stable
group-resident supervisor/sentinel or another verified process-group ownership
mechanism is required to eliminate this risk; the present draft does NOT make
that guarantee. Production rollout MUST NOT claim safe arbitrary-descendant
reaping until this boundary is qualified. Windows descendant ownership requires
separate qualification before production rollout; terminating its leader alone
is not a process-tree guarantee.

## Qualification and production wiring gate

Deterministic real-process tests cover blocked construction, startup deadline,
repeated cancellation, late spawn ownership, ignored shutdown, early zero exit,
verbose stderr and actual host SIGINT/SIGTERM during blocked initialization.
A native SDK tier initializes both pinned SDKs with no network or radio
interfaces, verifying native readiness, host handler preservation and clean exit.
These tests do not establish RF connectivity or native delivery behavior.

Before the production session uses the worker, qualification MUST cover bounded
inbound data and delivery-state forwarding, integer/byte native metadata fields,
exact-attempt correlation retained by the parent, worker loss after a possible
handoff, persistent identity/queue ownership and restart isolation. Unknown
handoff outcomes MUST NOT become invented success, negative confirmation or an
unqualified automatic resend. Native bidirectional RF and platform process-tree
termination are separate required profiles for their respective guarantees.

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
