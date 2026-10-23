"""Optional LXMF/Reticulum dependency guard.

When the ``lxmf`` and ``RNS`` packages are available,
:data:`HAS_LXMF` is ``True`` and the adapter can create real
LXMRouter instances.  When absent, the adapter raises
:class:`~medre.adapters.lxmf.errors.LxmfConnectionError`
on :meth:`start` for non-fake connection types.

This module is the **sole** import site for ``lxmf`` and ``RNS``.
All other LXMF adapter modules must not import ``lxmf`` or ``RNS``
directly — they access the modules through :data:`rns_module` and
:data:`lxmf_module` here.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any

HAS_LXMF: bool

rns_module: ModuleType | None
"""The ``RNS`` module, or ``None`` when not installed."""

lxmf_module: ModuleType | None
"""The ``lxmf`` module, or ``None`` when not installed."""


def _available(name: str) -> bool:
    """Inspect installation metadata without importing an SDK into the host."""
    import importlib.util
    import sys

    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return sys.modules.get(name) is not None


rns_module = None
lxmf_module = None
HAS_LXMF = _available("RNS") and _available("LXMF")


def _require_lxmf() -> tuple[Any, Any]:
    """Return ``(RNS, lxmf)`` modules or raise :class:`ImportError`.

    Convenience for session internals that need both modules.
    """
    import importlib

    global rns_module, lxmf_module
    if not HAS_LXMF:
        raise ImportError(
            "lxmf and/or RNS packages are not installed; "
            "pip install 'medre-adapter-lxmf' or use connection_type='fake'"
        )
    if rns_module is None:
        rns_module = importlib.import_module("RNS")
    if lxmf_module is None:
        lxmf_module = importlib.import_module("LXMF")
    return rns_module, lxmf_module
