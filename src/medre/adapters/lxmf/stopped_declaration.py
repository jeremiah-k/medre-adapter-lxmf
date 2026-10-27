"""LXMF worker selection and unsupported complete native-state capture.

The worker's SQLite handoff fence and identity are NOT a complete LXMF
installation. RNS stores named binary transport/identity/path records, not a
``storage/database`` SQLite database. LXMRouter owns additional state beneath
its ``storagepath``; a shared RNS instance or propagation node may own state
outside the worker's flock domain. The generic inventory must refuse real
LXMF until a qualified native export/ownership procedure exists.
"""

from __future__ import annotations

from pathlib import Path

from medre.maintenance.stopped_declare import (
    DeclaredFile,
    ExclusionRefused,
    LocalExclusion,
    ProviderCoverage,
    StoppedStateDeclaration,
    declared,
)

_WORKER_LOCK = ".medre-worker.lock"
_HANDOFFS = ".medre-handoffs.sqlite"
_IDENTITY = ".medre-identity"


def _resolve(value: object) -> Path | None:
    """Normalize an explicitly configured path without reading native state."""
    if not isinstance(value, str) or not value:
        return None
    return Path(value).expanduser().absolute()


def _check_no_symlinks(path: Path) -> None:
    """Reject owner paths that could lock a different inode through an alias."""
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ExclusionRefused("LXMF native owner path traverses a symlink")


class LxmfStoppedState:
    """Worker-selected bytes with an explicit complete-capture refusal."""

    provider = "lxmf"

    def coverage(self, config: object, *, adapter_id: str) -> ProviderCoverage:
        """Identify worker files while refusing incomplete native installation state."""
        if getattr(config, "connection_type", "") == "fake":
            return declared(
                provider=self.provider,
                adapter_id=adapter_id,
                files=(),
                state_class="stateless_local",
                notes=("fake mode does not open native RNS",),
            )
        storage = _resolve(getattr(config, "storage_path", ""))
        if storage is None:
            return declared(
                provider=self.provider,
                adapter_id=adapter_id,
                files=(),
                state_class="unsupported",
                notes=("storage_path required for real LXMF",),
            )
        identity = _resolve(getattr(config, "identity_path", "")) or storage / _IDENTITY
        config_dir = _resolve(getattr(config, "reticulum_config_dir", ""))
        files = [
            DeclaredFile(
                storage / _HANDOFFS,
                "adapter_ledger",
                "sqlite",
                purpose="worker SQLite accepted/unknown native handoff fence",
            ),
            DeclaredFile(
                identity,
                "native_identity",
                "opaque",
                purpose="actual configured or generated RNS worker identity",
            ),
        ]
        notes = [
            "complete native LXMF/RNS state selection and shared ownership are not implemented",
            "worker lock is an excluded inode, never a restored exclusion grant",
            "worker flock cannot stop a shared Reticulum instance or another host",
            "LXMRouter queues/storage and RNS binary identity, path and cache records need coherent export",
        ]
        if config_dir is None:
            notes.append(
                "RNS default config directory must not be guessed without the native runtime"
            )
        else:
            files.append(
                DeclaredFile(
                    config_dir / "config",
                    "native_identity",
                    "opaque",
                    presence="export_only",
                    purpose="RNS interface configuration, not complete native state",
                )
            )
            notes.append(
                "explicit config directory does not establish shared-instance ownership"
            )
        if getattr(config, "default_delivery_method", "") == "propagated":
            notes.append(
                "propagated LXMF queues and remote propagation state require native export"
            )
        return declared(
            provider=self.provider,
            adapter_id=adapter_id,
            files=files,
            state_class="unsupported",
            notes=notes,
        )

    def local_exclusion(self, config: object, *, adapter_id: str) -> LocalExclusion:
        """Retain worker flocks only; this never excludes shared RNS owners."""
        storage = _resolve(getattr(config, "storage_path", ""))
        if storage is None:
            raise ExclusionRefused("LXMF worker storage path is required")
        identity = _resolve(getattr(config, "identity_path", "")) or storage / _IDENTITY
        _check_no_symlinks(storage)
        _check_no_symlinks(identity)
        exclusion = LocalExclusion(self.provider, note="uninitialized")
        try:
            exclusion.retain_flock(storage / _WORKER_LOCK, purpose="worker lease")
            exclusion.retain_flock(identity, purpose="identity lease")
        except BaseException:
            exclusion.close()
            raise
        exclusion.note = "local worker/identity flock only; shared RNS and other hosts excluded from claim"
        return exclusion


def declaration() -> StoppedStateDeclaration:
    """Return the SDK-free worker selection and complete-capture refusal."""
    return LxmfStoppedState()
