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

The pinned constructors require the main thread. Moving them to an executor
without explicit SDK embedding support is unsafe. Concurrent core startup does
not preempt their synchronous initialization; threading changes require separate
qualification against the SDK lifecycle and signal contracts.

Signal delivery while a synchronous native constructor is executing cannot be
preempted by the asyncio scheduler. The host MUST NOT assume instant cancellation
of the constructor; it SHOULD use a process supervision boundary when hard
shutdown guarantees are required.
