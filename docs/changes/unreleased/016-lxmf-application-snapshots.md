# Capture LXMF application state independently of Reticulum service state

Resolve a bounded LXMF endpoint inventory under retained queue/identity leases.
Preserve private identity and ratchets, native duplicate-suppression and router
metadata, and the SQLite handoff journal. An existing shared Reticulum service
can remain running; its network state and configuration stay outside the
application snapshot. Refuse unknown/temporary files and local propagation-node
message stores instead of silently dropping state. Restore retains accepted and
uncertain submission fences without authorizing a resend.
