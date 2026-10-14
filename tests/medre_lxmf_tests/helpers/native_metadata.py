"""Canonical native-metadata fixtures for built-in transport tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _native_data(
    namespace: str,
    schema_version: int,
    defaults: Mapping[str, object],
    field_overrides: Mapping[str, object] | None,
    keyword_overrides: Mapping[str, object],
) -> dict[str, object]:
    """Build one fixture with deterministic override precedence.

    ``field_overrides`` is the optional positional mapping supplied by callers;
    ``keyword_overrides`` is applied last so explicit keyword fields win.
    """
    native: dict[str, object] = {"schema_version": schema_version, **defaults}
    if field_overrides is not None:
        native.update(field_overrides)
    native.update(keyword_overrides)
    return {namespace: native}


def lxmf_native_data(
    field_overrides: Mapping[str, object] | None = None,
    /,
    **overrides: Any,
) -> dict[str, object]:
    """Return a schema-complete LXMF v1 native-metadata fixture."""
    from medre.adapters.lxmf.event_shape import LXMF_NATIVE_SCHEMA_VERSION

    return _native_data(
        "lxmf",
        LXMF_NATIVE_SCHEMA_VERSION,
        {
            "source_hash": "",
            "destination_hash": None,
            "message_id": None,
            "timestamp": None,
            "title": "",
            "delivery_method": None,
            "has_fields": False,
        },
        field_overrides,
        overrides,
    )
