"""Offline checks for the live smoke profile boundary."""

import pytest
from medre_lxmf_tests.helpers.live_reticulum_config import resolve_live_config_dir


def test_omitted_profile_preserves_generic_connectivity_smoke():
    assert resolve_live_config_dir("") is None


def test_explicit_profile_resolves_to_existing_directory(tmp_path, monkeypatch):
    directory = tmp_path / "reticulum"
    directory.mkdir()
    (directory / "config").write_text("[reticulum]\n  share_instance = No\n")
    monkeypatch.chdir(tmp_path)
    assert resolve_live_config_dir("reticulum") == str(directory.resolve())


@pytest.mark.parametrize(
    "kind", ["missing", "missing-config", "empty", "file", "blank"]
)
def test_invalid_explicit_profile_is_rejected_without_sdk(kind, tmp_path):
    directory = tmp_path / "profile"
    if kind in {"missing-config", "empty"}:
        directory.mkdir()
    if kind == "empty":
        (directory / "config").touch()
    if kind == "file":
        directory.write_text("not a directory")
    value = " " if kind == "blank" else str(directory)
    with pytest.raises(ValueError, match="LXMF_RETICULUM_CONFIG_DIR"):
        resolve_live_config_dir(value)
