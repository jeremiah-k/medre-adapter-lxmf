"""Pinned native initialization preserves its embedding process shutdown hooks."""

import subprocess
import sys

import pytest

pytestmark = pytest.mark.lxmf_sdk


_SCRIPT = """
import asyncio, signal, sys
from pathlib import Path
from medre.adapters.lxmf.session import LxmfSession
from medre.config.adapters.lxmf import LxmfConfig

root = Path(sys.argv[1])
config_dir = root / "reticulum"
config_dir.mkdir()
(config_dir / "config").write_text(
    "[reticulum]\\nshare_instance = No\\nenable_transport = No\\n"
    "[logging]\\nloglevel = 1\\n[interfaces]\\n"
)
def host_shutdown(signum, frame):
    pass
for signum in (signal.SIGINT, signal.SIGTERM):
    signal.signal(signum, host_shutdown)
before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
async def exercise():
    session = LxmfSession(
        LxmfConfig("native", connection_type="reticulum",
                   storage_path=str(root / "storage"), announce_interval_seconds=0),
        adapter_id="native", reticulum_config_dir=str(config_dir),
    )
    try:
        await session.start()
        assert all(signal.getsignal(signum) is handler for signum, handler in before.items())
    finally:
        await session.stop()
    assert all(signal.getsignal(signum) is handler for signum, handler in before.items())
asyncio.run(exercise())
"""


def test_pinned_sdk_start_stop_preserves_host_signal_handlers(tmp_path):
    """Use a child process so native singleton and atexit state cannot pollute pytest."""
    pytest.importorskip("RNS")
    pytest.importorskip("LXMF")
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
