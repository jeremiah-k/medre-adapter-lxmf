"""Native endpoint leases outlive session teardown and SDK persistence."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from medre.adapters.lxmf.native_worker import _retain_profile
from medre.adapters.lxmf.worker_state import WorkerProfile
from medre.config.adapters.lxmf import LxmfConfig


def test_profile_is_held_during_later_native_exit_callbacks(tmp_path):
    """A native flush cannot race a maintenance or replacement endpoint owner."""
    script = r"""
import atexit
import sys
from pathlib import Path
from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.native_worker import _retain_profile
from medre.adapters.lxmf.worker_state import WorkerProfile
from medre.config.adapters.lxmf import LxmfConfig

root = Path(sys.argv[1])
config = LxmfConfig('exit-test', storage_path=str(root))
_retain_profile(config, message_plane=True)
def native_flush():
    try:
        rival = WorkerProfile(config)
    except LxmfConnectionError:
        (root / 'flush-result').write_text('held')
    else:
        rival.close()
        (root / 'flush-result').write_text('released too early')
atexit.register(native_flush)
"""
    subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        check=True,
        timeout=15,
        env=os.environ.copy(),
    )
    assert (tmp_path / "flush-result").read_text() == "held"
    # Every descriptor has been released after the process and its callbacks exit.
    WorkerProfile(LxmfConfig("replacement", storage_path=str(tmp_path))).close()


def test_fake_control_probe_does_not_create_a_profile(tmp_path):
    """SDK-free control readiness has no native identity or durable journal."""
    profile = _retain_profile(
        LxmfConfig("fake-control", storage_path=str(tmp_path / "unused")),
        message_plane=False,
    )
    assert profile is None
    assert not (tmp_path / "unused").exists()


def test_real_control_probe_retains_the_endpoint_profile(monkeypatch):
    """Diagnostic SDK startup cannot bypass a message worker's owner leases."""
    from medre.adapters.lxmf import native_worker

    events = []

    class Profile:
        def retain_until_process_exit(self):
            pass

        def close(self):
            events.append("closed")

    profile = Profile()
    monkeypatch.setattr(native_worker, "WorkerProfile", lambda config: profile)
    monkeypatch.setattr(
        native_worker.atexit, "register", lambda callback: events.append(callback)
    )
    result = _retain_profile(
        LxmfConfig("real-control", connection_type="reticulum"), message_plane=False
    )
    assert result is profile
    assert events == [profile.close]


@pytest.mark.skipif(os.name != "posix", reason="POSIX flock descriptor semantics")
def test_python_cleanup_cannot_release_process_lifetime_leases(tmp_path):
    """Late native writers remain fenced after original Python streams close."""
    script = r"""
import sys
from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.native_worker import _retain_profile
from medre.adapters.lxmf.worker_state import WorkerProfile
from medre.config.adapters.lxmf import LxmfConfig
config = LxmfConfig('process-owner', storage_path=sys.argv[1])
profile = _retain_profile(config, message_plane=True)
profile.close()
try:
    rival = WorkerProfile(config)
except LxmfConnectionError:
    print('held')
else:
    rival.close()
    raise AssertionError('process lifetime lease released by Python cleanup')
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    assert result.stdout.strip() == "held"
    WorkerProfile(LxmfConfig("replacement", storage_path=str(tmp_path))).close()
