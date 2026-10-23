# Runtime ownership during SDK initialization

Reticulum and LXMF constructors install process-global SIGINT and SIGTERM
handlers. The session MUST preserve the enclosing runtime's handlers around
both constructors, restoring them immediately on success or failure. Reusing an
existing Reticulum instance MUST NOT transfer process shutdown authority.

Native atexit registrations and the shared Reticulum lifecycle remain SDK-owned.
Session teardown MUST NOT invoke the shared Reticulum exit handler or shut down
other consumers. Session teardown MAY invoke and unregister its own router atexit
hook; construction does not change registrations. A failed handler snapshot
MUST abort SDK construction with an actionable error. Signal restoration
failures MUST abort the start and be logged.

The pinned constructors require the main thread. Production adapters execute the
session inside an adapter-owned worker process, so its synchronous constructors
and signal handlers cannot block the core event loop or replace MEDRE's host
handlers. The local session preserves its enclosing process's handlers around
construction and remains available for SDK-free fakes and independent peers.

The [native worker contract](native-worker.md) defines message forwarding,
exact-attempt feedback, persistent profile ownership and bounded process cleanup.
An SDK descendant that escapes the owned POSIX process group requires external
operating-system supervision; a process group is not a sandbox.

## Native submission uncertainty

Failures before the native router call MAY use bounded construction retries.
Once handle_outbound has been entered, any raised exception MUST be treated as
an unknown handoff outcome. The adapter MUST NOT automatically resubmit that
attempt or infer a negative recipient confirmation. Operator reconciliation
is required before retrying an uncertain dispatch.
