"""Qualify native constructors in an isolated worker with no radio interfaces."""

import importlib.util
import signal

import pytest

from medre.adapters.lxmf.native_process import NativeWorker
from medre.config.adapters.lxmf import LxmfConfig

pytestmark = pytest.mark.lxmf_sdk


async def test_native_readiness_and_shutdown_preserve_host_handlers(tmp_path):
    """The real pinned SDKs initialize outside the caller's process globals."""
    if (
        importlib.util.find_spec("RNS") is None
        or importlib.util.find_spec("LXMF") is None
    ):
        pytest.skip("Pinned native SDKs are unavailable")
    config_dir = tmp_path / "reticulum"
    config_dir.mkdir()
    (config_dir / "config").write_text(
        "[reticulum]\nshare_instance = No\nenable_transport = No\n"
        "[logging]\nloglevel = 1\n[interfaces]\n"
    )
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    worker = NativeWorker(
        LxmfConfig(
            "isolated-native",
            connection_type="reticulum",
            storage_path=str(tmp_path / "lxmf"),
            announce_interval_seconds=0,
            reticulum_config_dir=str(config_dir),
        ),
    )
    try:
        readiness = await worker.start()
        assert readiness.connected and readiness.router_running
        assert len(readiness.destination_hash) == 16
        assert readiness.diagnostics["mode"] == "reticulum"
        assert all(signal.getsignal(sig) is handler for sig, handler in before.items())
    finally:
        outcome = await worker.stop()
    assert outcome.returncode == 0
    assert outcome.acknowledged
    assert not outcome.forced
    assert all(signal.getsignal(sig) is handler for sig, handler in before.items())
