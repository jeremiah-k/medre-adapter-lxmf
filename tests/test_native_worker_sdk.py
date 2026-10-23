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


async def test_message_worker_persists_generated_identity_and_refuses_concurrent_owner(
    tmp_path,
):
    """Real SDK readiness preserves the native address across isolated lifetimes."""
    from medre.adapters.lxmf.errors import LxmfConnectionError, LxmfSendError
    from medre.adapters.lxmf.worker_session import WorkerSession

    config_dir = tmp_path / "reticulum"
    config_dir.mkdir()
    (config_dir / "config").write_text(
        "[reticulum]\nshare_instance = No\nenable_transport = No\n"
        "[logging]\nloglevel = 1\n[interfaces]\n"
    )
    config = LxmfConfig(
        "persistent-worker",
        connection_type="reticulum",
        storage_path=str(tmp_path / "lxmf"),
        announce_interval_seconds=0,
        reticulum_config_dir=str(config_dir),
        message_delay_seconds=0,
    )
    session = WorkerSession(config)
    await session.start()
    try:
        address = session._delivery_destination_hash
        assert address is not None and len(address) == 16
        identity = (tmp_path / "lxmf" / ".medre-identity").read_bytes()
        competing = WorkerSession(config)
        try:
            with pytest.raises(LxmfConnectionError, match="owned"):
                await competing.start()
        finally:
            await competing.stop()
        # A never-announced destination is a known pre-handoff refusal.
        with pytest.raises(LxmfSendError) as failure:
            await session.send_text("ee" * 16, "no native route")
        assert not failure.value.transient
        assert "recall identity" in str(failure.value)
        await session.refresh()
        assert session.connected
    finally:
        await session.stop()
    restarted = WorkerSession(config)
    try:
        await restarted.start()
        assert restarted._delivery_destination_hash == address
        assert (tmp_path / "lxmf" / ".medre-identity").read_bytes() == identity
        assert restarted.connected
    finally:
        await restarted.stop()
