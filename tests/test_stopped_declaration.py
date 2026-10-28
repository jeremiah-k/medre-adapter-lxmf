"""Native LXMF worker bytes are not proof of complete RNS/LXMF installation."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

from medre.adapters.lxmf.plugin import adapter_spec
from medre.adapters.lxmf.stopped_declaration import declaration
from medre.maintenance.stopped_declare import (
    DeclaredFile,
    ExclusionRefused,
    LocalExclusion,
    StoppedPlanError,
    declaration_from_spec,
    declared,
    plan_from_coverages,
)
from medre.maintenance.stopped_snapshot import (
    create_stopped_snapshot,
    restore_stopped_snapshot,
    verify_stopped_snapshot,
)


def _settings(root: Path, **kw: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "connection_type": "reticulum",
        "storage_path": str(root / "worker"),
        "identity_path": "",
        "reticulum_config_dir": "",
        "default_delivery_method": "direct",
    }
    values.update(kw)
    return SimpleNamespace(**values)


def _fixture(root: Path) -> tuple[Path, Path]:
    worker = root / "worker"
    worker.mkdir(parents=True)
    (worker / ".medre-worker.lock").write_bytes(b"")
    identity = worker / ".medre-identity"
    identity.write_bytes(b"test identity, never a real key")
    journal = worker / ".medre-handoffs.sqlite"
    with closing(sqlite3.connect(journal)) as db, db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE handoffs (operation_id TEXT PRIMARY KEY, status TEXT)")
        db.executemany(
            "INSERT INTO handoffs VALUES (?,?)",
            [("accepted", "accepted"), ("unknown", "unknown")],
        )
    return identity, journal


def test_declared_spec_loads_without_sdk():
    assert adapter_spec().stopped_state_declaration is not None
    assert declaration_from_spec(adapter_spec()).provider == "lxmf"


def test_real_worker_refuses_generic_completeness_and_no_fake_database(tmp_path):
    config = _settings(tmp_path)
    coverage = declaration().coverage(config, adapter_id="node")
    assert coverage.state_class == "unsupported"
    assert {p.path.name for p in coverage.files} == {
        ".medre-identity",
        ".medre-handoffs.sqlite",
    }
    assert "database" not in {p.path.name for p in coverage.files}
    assert all(p.path.name != ".medre-worker.lock" for p in coverage.files)
    assert all(p.presence == "required" for p in coverage.files)
    with pytest.raises(StoppedPlanError):
        plan_from_coverages(tmp_path, [coverage])


def test_configured_identity_is_required_not_assumed_external(tmp_path):
    identity = tmp_path / "worker" / "operator.identity"
    coverage = declaration().coverage(
        _settings(tmp_path, identity_path=str(identity)), adapter_id="node"
    )
    selected = next(p for p in coverage.files if p.role == "native_identity")
    assert selected.path == identity
    assert selected.presence == "required"


@pytest.mark.parametrize(
    "config_dir,method",
    [
        ("", "direct"),
        ("", "propagated"),
        ("explicit", "direct"),
        ("explicit", "propagated"),
    ],
)
def test_configuration_only_preview_never_reads_reticulum_state(
    tmp_path, config_dir, method
):
    config = _settings(
        tmp_path,
        reticulum_config_dir=str(tmp_path / "reticulum") if config_dir else "",
        default_delivery_method=method,
    )
    coverage = declaration().coverage(config, adapter_id="node")
    assert coverage.state_class == "unsupported"
    assert any("capture-config" in note for note in coverage.notes)
    assert all(p.path.name != "config" for p in coverage.files)
    assert not (tmp_path / "reticulum").exists()


def test_fake_mode_only_is_stateless(tmp_path):
    coverage = declaration().coverage(
        _settings(tmp_path, connection_type="fake"), adapter_id="fake"
    )
    assert coverage.state_class == "stateless_local" and not coverage.files


def test_worker_and_identity_exclusion_and_partial_release(tmp_path):
    identity, _ = _fixture(tmp_path)
    config = _settings(tmp_path)
    held = declaration().local_exclusion(config, adapter_id="node")
    assert held.scope == "local_only" and held.method == "flock"
    rival = LocalExclusion("lxmf", note="test")
    with pytest.raises(ExclusionRefused):
        rival.retain_flock(identity, purpose="competing identity")
    held.close()
    identity.unlink()
    with pytest.raises(ExclusionRefused):
        declaration().local_exclusion(config, adapter_id="node")
    # The failed partial acquisition must not leak the worker flock.
    rival.retain_flock(tmp_path / "worker/.medre-worker.lock", purpose="worker")
    rival.close()


def test_symlinked_worker_identity_refused(tmp_path):
    identity, _ = _fixture(tmp_path)
    identity.unlink()
    identity.symlink_to(tmp_path / "external")
    with pytest.raises(ExclusionRefused):
        declaration().local_exclusion(_settings(tmp_path), adapter_id="node")


def test_explicit_worker_only_selection_preserves_fencing_but_not_full_installation(
    tmp_path,
):
    root = tmp_path / "state"
    identity, journal = _fixture(root)
    core_db = root / "medre.sqlite"
    with closing(sqlite3.connect(core_db)) as conn, conn:
        conn.execute("CREATE TABLE core(value TEXT)")
    # Explicit selected-file capture is still possible, but cannot be
    # advertised as a complete LXMF installation backup.
    plan = plan_from_coverages(
        root,
        [
            declared(
                provider="core",
                adapter_id="core",
                files=(DeclaredFile(core_db, "core_sqlite", "sqlite"),),
            ),
            declared(
                provider="lxmf",
                adapter_id="worker-only-selection",
                files=(
                    DeclaredFile(identity, "native_identity", "opaque"),
                    DeclaredFile(journal, "adapter_ledger", "sqlite"),
                ),
            ),
        ],
    )
    snapshot = tmp_path / "snapshot"
    create_stopped_snapshot(root, snapshot, plan.entries, runtime_stopped=True)
    verify_stopped_snapshot(snapshot)
    restored = tmp_path / "restored"
    restore_stopped_snapshot(snapshot, restored, runtime_stopped=True)
    assert (restored / "worker/.medre-identity").read_bytes() == identity.read_bytes()
    with closing(sqlite3.connect(restored / "worker/.medre-handoffs.sqlite")) as conn:
        assert dict(conn.execute("SELECT operation_id, status FROM handoffs")) == {
            "accepted": "accepted",
            "unknown": "unknown",
        }
    assert not (restored / "worker/.medre-worker.lock").exists()
    assert not (restored / "reticulum/storage/transport_identity").exists()
