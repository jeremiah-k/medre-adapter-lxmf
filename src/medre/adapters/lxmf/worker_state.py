"""Exclusive native profile ownership and durable exact-attempt handoff fences."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import sys
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, BinaryIO

import msgspec

from medre.adapters.lxmf.errors import LxmfConnectionError, LxmfSendError
from medre.config.adapters.lxmf import LxmfConfig


@dataclass(frozen=True)
class SubmissionRecord:
    """Durable dispatch facts, independent of recipient delivery confirmation."""

    status: str
    native_id: str | None
    state: str
    error: str | None


def payload_digest(payload: object) -> bytes:
    """Hash native values without changing byte values or integer map keys."""

    def canonical(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: canonical(value[key])
                for key in sorted(value, key=msgspec.msgpack.encode)
            }
        if isinstance(value, (list, tuple)):
            return [canonical(item) for item in value]
        return value

    return hashlib.sha256(msgspec.msgpack.encode(canonical(payload))).digest()


class WorkerProfile:
    """Hold the native queue and identity leases for exactly one worker lifetime."""

    def __init__(self, config: LxmfConfig) -> None:
        self.config = config
        self._leases: list[BinaryIO] = []
        self._db: sqlite3.Connection | None = None
        self._exit_lease_fds: list[int] = []
        try:
            if config.storage_path:
                root = Path(config.storage_path).expanduser().resolve()
                root.mkdir(parents=True, exist_ok=True, mode=0o700)
                self._lease(root / ".medre-worker.lock", create=True)
                path = root / ".medre-handoffs.sqlite"
                if path.is_symlink():
                    raise LxmfConnectionError(
                        "Native handoff journal cannot be a symlink"
                    )
                fd = os.open(
                    path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600
                )
                os.close(fd)
                self._db = sqlite3.connect(path, timeout=2)
                self.config = replace(config, storage_path=str(root))
            else:
                self._db = sqlite3.connect(":memory:")
            version = self._db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise LxmfConnectionError("Unsupported native handoff journal version")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("""CREATE TABLE IF NOT EXISTS handoffs (
                operation_id TEXT PRIMARY KEY, digest BLOB NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('dispatching','accepted','rejected','retryable','unknown')),
                native_id TEXT, state TEXT NOT NULL, error TEXT
            )""")
            self._db.execute("PRAGMA user_version=1")
            self._db.commit()
            if config.connection_type != "fake":
                self._identity()
        except BaseException:
            self.close()
            raise

    def _lease(self, path: Path, *, create: bool = False) -> None:
        """Acquire a nonblocking cooperative profile lease without rewriting keys."""
        flags = os.O_RDWR | os.O_CREAT if create else os.O_RDONLY
        fd = os.open(path, flags | getattr(os, "O_NOFOLLOW", 0), 0o600)
        stream = os.fdopen(fd, "r+b" if create else "rb")
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            stream.close()
            raise LxmfConnectionError(
                "LXMF native identity or queue is already owned"
            ) from None
        self._leases.append(stream)

    def _identity(self) -> None:
        """Persist a generated identity before native initialization can announce it."""
        from medre.adapters.lxmf.compat import _require_lxmf

        if self.config.identity_path:
            path = Path(self.config.identity_path).expanduser().resolve()
        else:
            assert self.config.storage_path is not None
            path = Path(self.config.storage_path) / ".medre-identity"
            if not path.exists():
                rns, _ = _require_lxmf()
                fd, temporary = tempfile.mkstemp(prefix=".identity-", dir=path.parent)
                os.close(fd)
                try:
                    if rns.Identity().to_file(temporary) is False:
                        raise LxmfConnectionError(
                            "Could not persist the native identity"
                        )
                    with open(temporary, "rb") as stream:
                        os.fsync(stream.fileno())
                    os.replace(temporary, path)
                    if os.name != "nt":
                        directory = os.open(path.parent, os.O_RDONLY)
                        try:
                            os.fsync(directory)
                        finally:
                            os.close(directory)
                finally:
                    Path(temporary).unlink(missing_ok=True)
        self._lease(path)
        self.config = replace(self.config, identity_path=str(path))

    def begin(self, operation_id: str, digest: bytes) -> SubmissionRecord | None:
        """Reserve before SDK dispatch; an interrupted reservation is uncertain."""
        assert self._db is not None
        row = self._db.execute(
            "SELECT digest,status,native_id,state,error FROM handoffs WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if row is not None:
            if bytes(row[0]) != digest:
                raise LxmfSendError(
                    "Attempt payload conflicts with its native handoff fence",
                    transient=False,
                )
            if row[1] == "retryable":
                # This state is committed only after a known pre-handoff
                # failure. The same exact attempt may safely try again.
                with self._db:
                    self._db.execute(
                        "UPDATE handoffs SET status='dispatching',native_id=NULL,"
                        "state='unmapped',error=NULL WHERE operation_id=?",
                        (operation_id,),
                    )
                return None
            status = "unknown" if row[1] == "dispatching" else row[1]
            return SubmissionRecord(status, row[2], row[3], row[4])
        with self._db:
            self._db.execute(
                "INSERT INTO handoffs VALUES (?,?,'dispatching',NULL,'unmapped',NULL)",
                (operation_id, digest),
            )
        return None

    def finish(self, operation_id: str, record: SubmissionRecord) -> None:
        """Commit observed native facts before acknowledging the handoff."""
        assert self._db is not None
        with self._db:
            self._db.execute(
                "UPDATE handoffs SET status=?,native_id=?,state=?,error=? WHERE operation_id=?",
                (
                    record.status,
                    record.native_id,
                    record.state,
                    record.error,
                    operation_id,
                ),
            )

    def retain_until_process_exit(self) -> None:
        """Keep POSIX flock ownership until this native worker actually exits.

        The raw duplicate descriptors intentionally have process lifetime and
        remain close-on-exec. Python cleanup can close the journal and original
        streams after SDK exit handlers while these references fence any late
        native writes. Only the native worker calls this method; the OS releases
        the descriptors on normal termination or kill.
        """
        if os.name != "posix" or self._exit_lease_fds:
            return
        retained = []
        try:
            for stream in self._leases:
                retained.append(os.dup(stream.fileno()))
        except BaseException:
            for descriptor in retained:
                os.close(descriptor)
            raise
        self._exit_lease_fds = retained

    def close(self) -> None:
        """Close the journal and session handles after native persistence.

        POSIX descriptors transferred to process ownership remain held until
        OS process exit, including after Python-level cleanup completes.
        """
        if self._db is not None:
            self._db.close()
            self._db = None
        for stream in reversed(self._leases):
            stream.close()
        self._leases.clear()
