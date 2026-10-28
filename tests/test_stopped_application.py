"""LXMF endpoint snapshots preserve application state without claiming a network node."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest

from medre.adapters.lxmf.stopped_declaration import declaration
from medre.config.adapters.lxmf import LxmfConfig
from medre.maintenance.stopped_declare import ExclusionRefused, StoppedPlanError


def _profile(root):
    """Create a bounded disposable endpoint using the native file naming contract."""
    storage = root / "endpoint"
    ratchets = storage / "lxmf/ratchets"
    ratchets.mkdir(parents=True)
    (storage / ".medre-worker.lock").write_bytes(b"")
    (storage / ".medre-identity").write_bytes(bytes(range(64)))
    with closing(sqlite3.connect(storage / ".medre-handoffs.sqlite")) as db, db:
        db.execute("CREATE TABLE handoffs(operation_id TEXT, status TEXT)")
        db.execute("INSERT INTO handoffs VALUES('held', 'unknown')")
    (storage / "lxmf/local_deliveries").write_bytes(b"native-state-fixture")
    (ratchets / ("ab" * 16 + ".ratchets")).write_bytes(b"signed-ratchet-fixture")
    return LxmfConfig(
        "endpoint", connection_type="reticulum", storage_path=str(storage)
    )


def _resolved(config):
    """Use the same endpoint locks that configured capture must retain."""
    owner = declaration().local_exclusion(config, adapter_id=config.adapter_id)
    try:
        return declaration().resolve_coverage(config, adapter_id=config.adapter_id)
    finally:
        owner.close()


def test_application_selection_preserves_ratchets_and_excludes_reticulum(tmp_path):
    """A shared network dependency is not silently claimed as endpoint-owned state."""
    config = replace(
        _profile(tmp_path), reticulum_config_dir=str(tmp_path / "shared-network")
    )
    coverage = _resolved(config)
    names = {item.path.name for item in coverage.files}
    assert coverage.state_class == "file_backed"
    assert names == {
        ".medre-identity",
        ".medre-handoffs.sqlite",
        "local_deliveries",
        "ab" * 16 + ".ratchets",
    }
    assert any("separate dependency" in note for note in coverage.notes)
    assert not (tmp_path / "shared-network").exists()
    assert all("reticulum" not in str(item.path) for item in coverage.files)


@pytest.mark.parametrize(
    "relative",
    [
        "unrecognized",
        "lxmf/messagestore",
        "lxmf/local_deliveries.tmp",
        "lxmf/ratchets/key.tmp",
    ],
)
def test_unknown_or_interrupted_native_state_refuses_selection(tmp_path, relative):
    config = _profile(tmp_path)
    (tmp_path / "endpoint" / relative).write_bytes(b"unselected state")
    with pytest.raises(StoppedPlanError):
        _resolved(config)
    # Resolution failure must not retain any endpoint owner.
    declaration().local_exclusion(config, adapter_id=config.adapter_id).close()


def test_symlinked_native_state_refuses_without_reading_target(tmp_path):
    config = _profile(tmp_path)
    path = tmp_path / "endpoint/lxmf/local_deliveries"
    path.unlink()
    path.symlink_to(tmp_path / "missing-external")
    with pytest.raises(StoppedPlanError):
        _resolved(config)


@pytest.mark.parametrize(
    "relative",
    [
        ".medre-identity",
        ".medre-worker.lock",
        ".medre-handoffs.sqlite",
        ".medre-handoffs.sqlite-wal",
        ".medre-handoffs.sqlite-shm",
        "lxmf/local_deliveries",
        "lxmf/ratchets/" + "ab" * 16 + ".ratchets",
    ],
)
def test_hardlinked_native_state_refuses_external_inode_alias(tmp_path, relative):
    """A hard link bypasses symlink checks and can alias state outside the profile."""
    config = _profile(tmp_path)
    source = tmp_path / "endpoint" / relative
    outside = tmp_path / "external-native-state"
    if not source.exists():
        source.write_bytes(b"")
    source.rename(outside)
    source.hardlink_to(outside)
    with pytest.raises(StoppedPlanError, match="hard-linked"):
        _resolved(config)
    declaration().local_exclusion(config, adapter_id=config.adapter_id).close()


def test_endpoint_file_count_is_bounded(tmp_path, monkeypatch):
    from medre.adapters.lxmf import stopped_layout

    config = _profile(tmp_path)
    monkeypatch.setattr(stopped_layout, "_MAX_FILES", 3)
    with pytest.raises(StoppedPlanError):
        _resolved(config)


def test_supplied_identity_is_captured_under_the_same_identity_lease(tmp_path):
    config = _profile(tmp_path)
    generated = tmp_path / "endpoint/.medre-identity"
    explicit = tmp_path / "operator-identity"
    generated.rename(explicit)
    config = replace(config, identity_path=str(explicit))
    assert explicit in {item.path for item in _resolved(config).files}
    held = declaration().local_exclusion(config, adapter_id="first")
    try:
        with pytest.raises(ExclusionRefused):
            declaration().local_exclusion(config, adapter_id="second")
    finally:
        held.close()


def test_two_profiles_can_share_a_reticulum_setup(tmp_path):
    """Endpoint exclusion does not serialize unrelated applications on a service."""
    first = replace(
        _profile(tmp_path / "first"), reticulum_config_dir=str(tmp_path / "network")
    )
    second = replace(
        _profile(tmp_path / "second"), reticulum_config_dir=first.reticulum_config_dir
    )
    held = declaration().local_exclusion(first, adapter_id="first")
    try:
        assert _resolved(second).state_class == "file_backed"
    finally:
        held.close()
