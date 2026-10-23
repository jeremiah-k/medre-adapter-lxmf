"""Opt-in native worker qualification against an explicitly selected RNode profile."""

import json
import os
import signal
from pathlib import Path

import msgspec
import pytest

from medre.adapters.lxmf.native_process import NativeWorker
from medre.config.adapters.lxmf import LxmfConfig

pytestmark = [pytest.mark.live, pytest.mark.hardware]


async def test_native_worker_restarts_preserve_profile_identity():
    """Reuse one persisted native identity while releasing its radio between workers.

    The private JSON fixture contains ``config`` (LxmfConfig fields) and an
    independently recorded ``expected_destination_hash``. The profile must have
    an explicit Reticulum directory, identity and storage. This qualifies native
    initialization and teardown only; it does not send or confirm messages.
    """
    fixture = os.environ.get("LXMF_WORKER_HARDWARE_PROFILE")
    if not fixture:
        pytest.skip("Select a private LXMF_WORKER_HARDWARE_PROFILE JSON fixture")
    settings = json.loads(Path(fixture).read_text())
    config = msgspec.convert(settings["config"], type=LxmfConfig)
    config.validate()
    assert config.connection_type == "reticulum"
    assert config.reticulum_config_dir and config.identity_path and config.storage_path
    assert Path(config.identity_path).is_file()
    assert (Path(config.reticulum_config_dir) / "config").is_file()
    expected = bytes.fromhex(settings["expected_destination_hash"])
    assert len(expected) == 16
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    for _ in range(2):
        worker = NativeWorker(config)
        try:
            ready = await worker.start()
            assert ready.connected and ready.router_running
            assert ready.destination_hash == expected
        finally:
            outcome = await worker.stop()
        assert outcome.returncode == 0
        assert outcome.acknowledged and not outcome.forced
        assert all(
            signal.getsignal(sig) is handler for sig, handler in handlers.items()
        )
