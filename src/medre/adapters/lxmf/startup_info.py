"""Startup-info facts from state the LXMF adapter already holds.

Core's runtime duck-types an optional ``startup_info()`` callable on a
built adapter and renders the returned scalar facts at the adapter's
ready transition (see the adapter-startup-info specification in the
core distribution).  This module shapes the facts this adapter holds at
that transition — the adapter's own LXMF delivery-destination hash, once the session has registered the delivery identity, and the
configured transport mode — into the plain mapping the contract
expects.

Pure read: the provider performs no RNS or interface I/O, acquires no
resources, and mutates no session state, so a synchronous callable is
safe to run on any worker thread.  The only lookup beyond held state is
the installed RNS distribution version via ``importlib.metadata`` —
local package metadata, never a source literal (the repository's
pin-hygiene guard keeps pin values in ``pyproject.toml`` only) — and
that fact is omitted when RNS is not installed.  Facts that are not
held yet are omitted rather than probed: startup info must never gate,
block, or delay the interface.  This module imports nothing from
``medre.core``: the returned mapping is plain JSON-like data, validated
and redacted by core at the render boundary.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any

from medre.config.adapters.lxmf import LxmfConfig

__all__ = ["build_startup_info"]

# LXMF destinations use Reticulum's 128-bit destination hashes.
_DESTINATION_BYTES = 16


def _installed_rns_version() -> str | None:
    """Return the installed RNS distribution version, or ``None``.

    Reads local package metadata via ``importlib.metadata``; the value
    is never a source literal.  ``None`` means RNS is not installed and
    the fact is omitted.
    """
    try:
        return importlib.metadata.version("rns")
    except importlib.metadata.PackageNotFoundError:
        return None


def build_startup_info(
    config: LxmfConfig,
    *,
    destination_hash: bytes | None = None,
) -> dict[str, Any]:
    """Build the startup-info mapping from held state.

    ``destination_hash`` is the adapter's own LXMF delivery-destination
    hash once the session has registered the delivery identity; it is
    omitted when not held (fake mode never registers one).  The
    destination address forms the first row and the configured transport
    facts the second, per the contract's row guidance.
    """
    rows: list[list[dict[str, Any]]] = []
    if destination_hash is not None and len(destination_hash) == _DESTINATION_BYTES:
        rows.append(
            [
                {
                    "name": "destination_hash",
                    "value": destination_hash.hex(),
                    "label": "destination",
                }
            ]
        )
    endpoint: list[dict[str, Any]] = [
        {
            "name": "connection_type",
            "value": config.connection_type,
            "label": "link",
        }
    ]
    rns_version = _installed_rns_version()
    if rns_version:
        endpoint.append({"name": "rns_version", "value": rns_version, "label": "rns"})
    rows.append(endpoint)
    return {"rows": rows}
