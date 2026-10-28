"""LXMF application-state capture with an independent Reticulum dependency.

Native endpoint identity, ratchets, duplicate-suppression state, and durable
handoff fences belong to this MEDRE profile. Shared or independently configured
Reticulum service state is deliberately outside that application selection.
Only exclusion-retained resolution may inspect native profile files.
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
    """Endpoint-owned selection without acquiring the shared network service."""

    provider = "lxmf"

    def coverage(self, config: object, *, adapter_id: str) -> ProviderCoverage:
        """Preview configured endpoint paths without filesystem discovery or SDK imports."""
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
        return declared(
            provider=self.provider,
            adapter_id=adapter_id,
            files=(
                DeclaredFile(
                    storage / _HANDOFFS,
                    "adapter_ledger",
                    "sqlite",
                    purpose="native handoff fence",
                ),
                DeclaredFile(
                    identity,
                    "native_identity",
                    "opaque",
                    purpose="LXMF endpoint private identity",
                ),
            ),
            state_class="unsupported",
            notes=(
                "native application-state resolution requires retained endpoint exclusion and capture-config",
                "Reticulum service state is independent of the LXMF application profile",
            ),
        )

    def resolve_coverage(self, config: object, *, adapter_id: str) -> ProviderCoverage:
        """Select bounded endpoint state while core retains its local owner leases."""
        from medre.adapters.lxmf.stopped_layout import application_files

        if getattr(config, "connection_type", "") == "fake":
            return self.coverage(config, adapter_id=adapter_id)
        storage = _resolve(getattr(config, "storage_path", ""))
        if storage is None:
            return self.coverage(config, adapter_id=adapter_id)
        identity = _resolve(getattr(config, "identity_path", "")) or storage / _IDENTITY
        return declared(
            provider=self.provider,
            adapter_id=adapter_id,
            files=application_files(storage, identity),
            notes=(
                "LXMF application identity, private ratchets, router state and handoff journal selected",
                "Reticulum service configuration/state is a separate dependency and is not captured",
                "keep the existing Reticulum service available or restore its setup separately",
                "remote propagation-node state is outside this endpoint; local propagation-node stores require export",
                "local endpoint leases do not establish ownership on other hosts or in non-cooperating applications",
            ),
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
    """Return pure preview, endpoint exclusion, and bounded native-state resolution."""
    return LxmfStoppedState()
