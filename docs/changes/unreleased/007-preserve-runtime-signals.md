# Preserve runtime signals across both SDK constructors

Restore the enclosing runtime's SIGINT and SIGTERM handlers around Reticulum
and LXMF construction, including failures. Do not change native atexit
registrations during construction (router teardown may unregister its hook);
preserve shared Reticulum ownership. Reject startup
if host handlers cannot be snapshotted, rather than risking loss of shutdown
authority. Synchronous constructor execution cannot preempt SDK-installed
handlers mid-construction.
