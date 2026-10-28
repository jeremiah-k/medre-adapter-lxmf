"""Bounded selection of one MEDRE-owned LXMF endpoint's native application state.

The Reticulum network service and its configuration/storage are dependencies,
not this endpoint's profile. Selection never traverses those directories or
imports an SDK. Callers retain the endpoint's queue and identity exclusions
while resolving these files and publishing their snapshot.
"""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path

from medre.maintenance.stopped_declare import DeclaredFile, StoppedPlanError

_MAX_FILES = 4096
_MAX_FILE_BYTES = 1024 * 1024 * 1024
_ROUTER_FILES = frozenset(
    {
        "local_deliveries",
        "locally_processed",
        "node_stats",
        "outbound_stamp_costs",
        "available_tickets",
        "peers",
    }
)
_RATCHET_NAME = re.compile(r"[0-9a-f]{32}\.ratchets")
_JOURNAL = ".medre-handoffs.sqlite"
_LOCK = ".medre-worker.lock"


def _directory(path: Path) -> None:
    """Require an existing, canonical directory without any symlink component."""
    if not path.is_absolute() or ".." in path.parts:
        raise StoppedPlanError("endpoint paths must be canonical absolute paths")
    if any(component.is_symlink() for component in (path, *path.parents)):
        raise StoppedPlanError("endpoint state cannot traverse symlinks")
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise StoppedPlanError("endpoint state requires an existing directory")


def _regular(path: Path, *, identity: bool = False) -> None:
    """Check the actual opened inode without blocking on a special file."""
    if path.is_symlink():
        raise StoppedPlanError("endpoint state cannot traverse symlinks")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_FILE_BYTES:
            raise StoppedPlanError("endpoint state must be bounded regular files")
        if info.st_nlink != 1:
            raise StoppedPlanError(
                "hard-linked endpoint state requires a provider export"
            )
        if identity and info.st_size != 64:
            raise StoppedPlanError(
                "endpoint identity does not match the native key format"
            )
    finally:
        os.close(descriptor)


def _entries(directory: Path, allowed: frozenset[str] | set[str]) -> dict[str, Path]:
    """Stop at an unknown file instead of crawling unrelated profile contents."""
    entries = {}
    for child in directory.iterdir():
        if child.name not in allowed:
            raise StoppedPlanError(
                "unrecognized LXMF profile state requires a provider export"
            )
        entries[child.name] = child
    return entries


def application_files(storage: Path, identity: Path) -> tuple[DeclaredFile, ...]:
    """Resolve exact endpoint files under retained local profile exclusion.

    Identity and the durable handoff journal are required. Every existing
    native router file and private ratchet file is preserved; unknown state,
    interrupted temporary writes, and propagation-node message stores refuse
    this endpoint-only selection. Empty SDK directories are recreated at
    native reopen. Network-service state and remote propagation nodes remain
    independent dependencies.
    """
    try:
        return _application_files(storage, identity)
    except StoppedPlanError:
        raise
    except (OSError, ValueError):
        raise StoppedPlanError(
            "LXMF endpoint state cannot be selected safely"
        ) from None


def _application_files(storage: Path, identity: Path) -> tuple[DeclaredFile, ...]:
    """Validate source-owned paths without loading native application globals."""
    _directory(storage)
    _directory(identity.parent)
    _regular(identity, identity=True)
    files = [
        DeclaredFile(
            identity,
            "native_identity",
            "opaque",
            purpose="LXMF endpoint private identity",
        ),
        DeclaredFile(
            storage / _JOURNAL,
            "adapter_ledger",
            "sqlite",
            purpose="durable native handoff fences",
        ),
    ]
    _regular(storage / _JOURNAL)
    allowed = {_LOCK, _JOURNAL, _JOURNAL + "-wal", _JOURNAL + "-shm", "lxmf"}
    try:
        relative_identity = identity.relative_to(storage)
    except ValueError:
        relative_identity = None
    if relative_identity is not None:
        allowed.add(relative_identity.parts[0])
        cursor = storage
        for component in relative_identity.parts[:-1]:
            cursor = cursor / component
            _directory(cursor)
            expected = relative_identity.parts[len(cursor.relative_to(storage).parts)]
            _entries(cursor, {expected})
    root_entries = _entries(storage, allowed)
    for name, path in root_entries.items():
        if name not in {
            "lxmf",
            relative_identity.parts[0] if relative_identity is not None else None,
        }:
            _regular(path)
    router = storage / "lxmf"
    _directory(router)
    entries = _entries(router, _ROUTER_FILES | {"ratchets"})
    for name, path in sorted(entries.items()):
        if name == "ratchets":
            continue
        _regular(path)
        files.append(
            DeclaredFile(
                path,
                "native_identity",
                "opaque",
                purpose="native router application state",
            )
        )
    ratchets = router / "ratchets"
    _directory(ratchets)
    for path in ratchets.iterdir():
        if _RATCHET_NAME.fullmatch(path.name) is None:
            raise StoppedPlanError(
                "unrecognized private ratchet state requires a provider export"
            )
        _regular(path)
        files.append(
            DeclaredFile(
                path,
                "native_identity",
                "opaque",
                purpose="signed native private ratchet history",
            )
        )
        if len(files) > _MAX_FILES:
            raise StoppedPlanError("LXMF endpoint file selection exceeds its bound")
    return tuple(sorted(files, key=lambda item: item.path.as_posix()))
