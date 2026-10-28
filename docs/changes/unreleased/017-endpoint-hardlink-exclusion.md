# Refuse hard-linked LXMF native state during endpoint capture

Reject hard-linked endpoint files during exclusion-retained selection so a
native profile cannot silently capture external inode aliases. Validate identity,
lock inodes, handoff ledger and SQLite sidecars, router files, and private ratchets.
Lock inodes remain excluded from copied state.
