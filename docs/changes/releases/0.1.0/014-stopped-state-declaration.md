# Refuse incomplete native LXMF/RNS stopped-state backups

Replace the invented `storage/database` SQLite selection with an explicit
unsupported complete-installation boundary. Keep worker identity and handoff
journal selected and correctly typed; exclude the worker lock inode, which
cannot transfer ownership. Distinguish generated/configured identity,
Reticulum binary state, shared-instance ownership, and propagated queues.
Real LXMF remains unsupported by generic complete planning until native
export/restore and shared-owner exclusion are qualified; fake mode is stateless.
