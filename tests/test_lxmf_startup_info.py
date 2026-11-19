"""Startup-info provider: duck-typed emission of held LXMF facts.

Covers:
- ``startup_info`` is discovered duck-typed on the adapter object (real
  and fake).
- Facts come only from already-held state: the configured connection
  type, the adapter's own LXMF delivery-destination hash prefix once
  the session has registered the delivery identity, and the installed
  RNS distribution version read via ``importlib.metadata`` (never a
  source literal — the pin-hygiene guard keeps pins in pyproject only).
- Facts not held at ready (the delivery hash before the delivery
  identity registers, an RNS version when the package is absent) are
  omitted, never probed.
- Pure read: calling the hook mutates no adapter or session state and
  is safe on a worker thread, matching core's off-loop invocation.
- The 16-character destination prefix renders in full through core's
  redaction boundary (a published mesh identifier below the generic
  long-key threshold, matching the session's own log convention).

Core-side collection/rendering symbols are imported in tests only; the
adapter source must not import them.  Fake connection mode only — no
RNS traffic, no interface access.  See core
``docs/spec/adapter-startup-info.md``.
"""

from __future__ import annotations

import ast
import importlib.metadata
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from medre.core.startup_info import (
    collect_startup_info,
    render_startup_info_block,
    startup_info_callable,
)

from medre.adapters.fakes.lxmf import FakeLxmfAdapter
from medre.adapters.fakes.lxmf import build_startup_info as _fake_build_startup_info
from medre.adapters.lxmf import startup_info as startup_info_module
from medre.adapters.lxmf.adapter import LxmfAdapter
from medre.config.adapters.lxmf import LxmfConfig

_EMITTED_AT = "2026-11-19T00:00:00+00:00"

# Synthetic delivery destination hash: 16 bytes.  The displayed prefix
# is the first 16 hex characters, matching the session's announce-log
# convention.
_SYNTHETIC_HASH = bytes.fromhex("a1b2c3d4e5f60718" "9988776655443322")
_EXPECTED_PREFIX = "a1b2c3d4e5f60718"

# Synthetic RNS version for deterministically exercising the held
# version fact.  Not a pin value; the repo pins live in pyproject only.
_SYNTHETIC_RNS_VERSION = "1.2.3"


# -- Helpers ---------------------------------------------------------------


def _make_config(**overrides: Any) -> LxmfConfig:
    defaults: dict[str, Any] = {"adapter_id": "lxmf-startup-info"}
    defaults.update(overrides)
    return LxmfConfig(**defaults)


def _installed_rns_version() -> str | None:
    """The real environment's answer, computed dynamically (no literals)."""
    try:
        return importlib.metadata.version("rns")
    except importlib.metadata.PackageNotFoundError:
        return None


def _endpoint_row(rns_version: str | None) -> list[dict[str, Any]]:
    row: list[dict[str, Any]] = [
        {"name": "connection_type", "value": "fake", "label": "link"}
    ]
    if rns_version is not None:
        row.append({"name": "rns_version", "value": rns_version, "label": "rns"})
    return row


def _hold_destination_hash(adapter: LxmfAdapter, raw: bytes) -> None:
    """Publish a delivery-destination hash into the session's held state.

    Fake mode never registers a delivery identity, so tests publish the
    same attribute the real connect path sets once registration
    succeeds.
    """
    session = adapter._session
    assert session is not None
    session._delivery_destination_hash = raw


async def _started_adapter(config: LxmfConfig, make_adapter_context) -> LxmfAdapter:
    adapter = LxmfAdapter(config)
    await adapter.start(make_adapter_context(config.adapter_id))
    return adapter


# -- Duck-typed discovery ---------------------------------------------------


def test_startup_info_symbol_is_discoverable() -> None:
    """Core's getattr discovery finds a callable on the adapter object."""
    adapter = LxmfAdapter(_make_config())
    discovered = startup_info_callable(adapter)
    assert discovered is not None
    assert callable(discovered)
    assert discovered.__name__ == "startup_info"


def test_fake_adapter_exposes_discoverable_startup_info() -> None:
    """The fake mirrors the real adapter's duck-typed seam."""
    fake = FakeLxmfAdapter(_make_config())
    discovered = startup_info_callable(fake)
    assert discovered is not None
    assert callable(discovered)
    assert discovered.__name__ == "startup_info"


# -- Held-fact formatting ---------------------------------------------------


def test_fresh_adapter_omits_unheld_facts() -> None:
    """A fresh adapter reports only configured transport facts.

    No delivery hash has been registered and RNS may be absent, so both
    facts are omitted rather than probed.  The environment's RNS
    availability is computed dynamically — no pin literals here.
    """
    adapter = LxmfAdapter(_make_config())
    expected = {"rows": [_endpoint_row(_installed_rns_version())]}
    assert adapter.startup_info() == expected


async def test_held_destination_hash_forms_first_row(
    monkeypatch: pytest.MonkeyPatch, make_adapter_context
) -> None:
    """The session's own delivery-destination prefix leads the rows."""
    monkeypatch.setattr(startup_info_module, "_installed_rns_version", lambda: None)
    adapter = await _started_adapter(_make_config(), make_adapter_context)
    try:
        _hold_destination_hash(adapter, _SYNTHETIC_HASH)
        assert adapter.startup_info() == {
            "rows": [
                [
                    {
                        "name": "destination_hash_prefix",
                        "value": _EXPECTED_PREFIX,
                        "label": "dest",
                    }
                ],
                [{"name": "connection_type", "value": "fake", "label": "link"}],
            ]
        }
    finally:
        await adapter.stop()


async def test_rns_version_fact_is_reported_when_module_holds_one(
    monkeypatch: pytest.MonkeyPatch, make_adapter_context
) -> None:
    """A resolvable RNS version rides the endpoint row after the link."""
    monkeypatch.setattr(
        startup_info_module,
        "_installed_rns_version",
        lambda: _SYNTHETIC_RNS_VERSION,
    )
    adapter = await _started_adapter(_make_config(), make_adapter_context)
    try:
        _hold_destination_hash(adapter, _SYNTHETIC_HASH)
        assert adapter.startup_info() == {
            "rows": [
                [
                    {
                        "name": "destination_hash_prefix",
                        "value": _EXPECTED_PREFIX,
                        "label": "dest",
                    }
                ],
                _endpoint_row(_SYNTHETIC_RNS_VERSION),
            ]
        }
    finally:
        await adapter.stop()


async def test_rns_version_fact_follows_installed_metadata(
    make_adapter_context,
) -> None:
    """The version fact tracks ``importlib.metadata`` exactly.

    When RNS is installed the fact equals the queried metadata value;
    when it is absent the fact is omitted.  Either way the comparison
    is against a computed value, never a literal pin.
    """
    installed = _installed_rns_version()
    adapter = await _started_adapter(_make_config(), make_adapter_context)
    try:
        rows = adapter.startup_info()["rows"]
        names = [fact["name"] for row in rows for fact in row]
        if installed is None:
            assert "rns_version" not in names
        else:
            values = [
                fact["value"]
                for row in rows
                for fact in row
                if fact["name"] == "rns_version"
            ]
            assert values == [installed]
    finally:
        await adapter.stop()


def test_fake_adapter_reports_configured_facts_only() -> None:
    """The fake owns no session, so no delivery hash is ever reported."""
    fake = FakeLxmfAdapter(_make_config())
    assert fake.startup_info() == {"rows": [_endpoint_row(_installed_rns_version())]}


# -- Purity and thread safety -----------------------------------------------


async def test_startup_info_is_pure_read(make_adapter_context) -> None:
    """Reading facts neither mutates held state nor drifts between calls."""
    adapter = await _started_adapter(_make_config(), make_adapter_context)
    try:
        _hold_destination_hash(adapter, _SYNTHETIC_HASH)
        session = adapter._session
        assert session is not None
        before = session.delivery_destination_hash
        first = adapter.startup_info()
        second = adapter.startup_info()
        assert first == second
        assert session.delivery_destination_hash == before
        diag = adapter.diagnostics()
        assert diag["inbound_published"] == 0
        assert diag["classifier_messages_seen"] == 0
    finally:
        await adapter.stop()


def test_startup_info_snapshot_is_safe_from_worker_threads() -> None:
    """Core runs synchronous providers off-loop; the read must be stable."""
    adapter = LxmfAdapter(_make_config())
    with ThreadPoolExecutor(max_workers=4) as pool:
        payloads = list(pool.map(lambda _: adapter.startup_info(), range(32)))
    assert all(payload == payloads[0] for payload in payloads)
    assert payloads[0] == {"rows": [_endpoint_row(_installed_rns_version())]}


def test_provider_module_imports_nothing_from_core() -> None:
    """The provider stays duck-typed: plain data, no medre.core coupling."""
    source = Path(startup_info_module.__file__ or "").read_text(encoding="utf-8")
    offenders: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            offenders.extend(
                alias.name
                for alias in node.names
                if alias.name == "medre.core" or alias.name.startswith("medre.core.")
            )
        elif isinstance(node, ast.ImportFrom) and not node.level:
            module = node.module or ""
            if module == "medre.core" or module.startswith("medre.core."):
                offenders.append(module)
    assert offenders == []


def test_adapter_and_fake_import_the_shared_builder() -> None:
    """Both surfaces shape facts through the same pure module."""
    assert _fake_build_startup_info is startup_info_module.build_startup_info


# ---------------------------------------------------------------------------
# Core boundary: collection, rendering, cache
# ---------------------------------------------------------------------------


async def test_started_adapter_emits_held_facts_through_core(
    monkeypatch: pytest.MonkeyPatch, make_adapter_context
) -> None:
    """Core's supervised collection preserves rows and renders the prefix
    in full — a published mesh identifier, not key material."""
    monkeypatch.setattr(startup_info_module, "_installed_rns_version", lambda: None)
    adapter = await _started_adapter(_make_config(), make_adapter_context)
    try:
        _hold_destination_hash(adapter, _SYNTHETIC_HASH)
        info = await collect_startup_info(
            "lxmf-1", "lxmf", adapter, emitted_at=_EMITTED_AT
        )
        assert info is not None
        assert [(f.name, f.row, f.label) for f in info.fields] == [
            ("destination_hash_prefix", 0, "dest"),
            ("connection_type", 1, "link"),
        ]
        assert info.fields[0].value == _EXPECTED_PREFIX
        assert info.fields[1].value == "fake"
        assert render_startup_info_block("lxmf.lxmf-1  ready", info) == (
            "lxmf.lxmf-1  ready\n" f"  dest {_EXPECTED_PREFIX}\n" "  link fake"
        )
    finally:
        await adapter.stop()


async def test_core_collection_keeps_version_fact_when_held(
    monkeypatch: pytest.MonkeyPatch, make_adapter_context
) -> None:
    """A held RNS version renders on the endpoint row through core."""
    monkeypatch.setattr(
        startup_info_module,
        "_installed_rns_version",
        lambda: _SYNTHETIC_RNS_VERSION,
    )
    adapter = await _started_adapter(_make_config(), make_adapter_context)
    try:
        info = await collect_startup_info(
            "lxmf-1", "lxmf", adapter, emitted_at=_EMITTED_AT
        )
        assert info is not None
        assert [(f.name, f.row, f.label) for f in info.fields] == [
            ("connection_type", 0, "link"),
            ("rns_version", 0, "rns"),
        ]
        assert info.fields[1].value == _SYNTHETIC_RNS_VERSION
        assert render_startup_info_block("lxmf.lxmf-1  ready", info) == (
            "lxmf.lxmf-1  ready\n" "  link fake · rns 1.2.3"
        )
    finally:
        await adapter.stop()


async def test_worker_session_property_mirrors_held_state(
    make_adapter_context,
) -> None:
    """The native-mode session exposes the same held hash accessor.

    ``WorkerSession`` publishes the worker's ready-state hash; fake-mode
    sessions never hold one, so a fresh ``WorkerSession`` reports
    ``None`` without any worker traffic.
    """
    from medre.adapters.lxmf.worker_session import WorkerSession

    session = WorkerSession(_make_config())
    assert session.delivery_destination_hash is None
    session._delivery_destination_hash = _SYNTHETIC_HASH
    assert session.delivery_destination_hash == _SYNTHETIC_HASH
