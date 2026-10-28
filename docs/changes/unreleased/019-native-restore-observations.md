# Require fresh native evidence after LXMF endpoint restore

Distinguish the restored endpoint's announcements with a unique display-name
marker while retaining the same native address. Queued pre-shutdown announces
cannot satisfy the restored readiness check. Keep inbound-first and
outbound-first delivery assertions and their existing twenty-second deadlines.

Report bounded peer message/link state and cached worker health on receipt
timeout. Diagnostic collection never resends messages or resets native routing.
