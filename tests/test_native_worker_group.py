"""Qualify process-group identity independently of asyncio's SDK child reaping."""

import asyncio
import os
import signal
import sys

import pytest

from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.native_process import NativeWorker
from medre.adapters.lxmf.process_group import OwnedProcessGroup
from medre.config.adapters.lxmf import LxmfConfig

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX group ownership")


async def test_reaped_worker_keeps_group_id_reserved_until_final_signal(monkeypatch):
    """An exited SDK leader cannot release the ID used by group signalling."""
    worker = NativeWorker(LxmfConfig("reserved-group"))
    await worker.start()
    group = worker._group
    owner = group._process
    original = os.killpg
    observed = []

    def check_reservation(pgid, sig):
        assert pgid == owner.pid != worker.pid
        assert worker.returncode == 0
        assert os.getpgid(pgid) == os.getpgrp()
        # WNOWAIT proves this is our unreaped child, without releasing its PID.
        assert os.waitid(os.P_PID, pgid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None
        observed.append(pgid)
        return original(pgid, sig)

    monkeypatch.setattr(os, "killpg", check_reservation)
    result = await worker.stop()
    assert observed == [owner.pid]
    assert result.acknowledged and not result.forced and result.returncode == 0
    assert owner.returncode == 0
    with pytest.raises(LxmfConnectionError, match="ownership is unavailable"):
        group.kill()


async def test_dead_reservation_is_not_reaped_before_group_signal(monkeypatch):
    """Unexpected helper death retains the reserved PID through final signalling."""
    worker = NativeWorker(LxmfConfig("dead-reservation"))
    await worker.start()
    group = worker._group
    owner = group._process
    detach = group.detach
    original = os.killpg
    observed = []

    async def detach_then_die():
        await detach()
        os.kill(owner.pid, signal.SIGKILL)
        await asyncio.to_thread(os.waitid, os.P_PID, owner.pid, os.WEXITED | os.WNOWAIT)

    def check_reservation(pgid, sig):
        info = os.waitid(os.P_PID, pgid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
        assert info is not None and info.si_pid == owner.pid
        observed.append(pgid)
        return original(pgid, sig)

    monkeypatch.setattr(group, "detach", detach_then_die)
    monkeypatch.setattr(os, "killpg", check_reservation)
    with pytest.raises(LxmfConnectionError, match="owner exited unexpectedly"):
        await worker.stop()
    assert observed == [owner.pid]
    assert owner.returncode == -signal.SIGKILL
    assert worker.returncode == 0
    assert worker._stderr.done()


async def test_failed_sdk_spawn_retires_its_group_reservation(tmp_path):
    """SDK executable failure cannot strand the reservation helper."""
    worker = NativeWorker(
        LxmfConfig("failed-spawn"), _command=(str(tmp_path / "missing-executable"),)
    )
    with pytest.raises(FileNotFoundError):
        await worker.start()
    assert worker.pid is None
    assert worker._group._process.returncode is not None
    assert worker._group._retired


async def test_lifetime_pipe_eof_terminates_group_members():
    """Loss of the parent's control lease retires the private SDK group."""
    group = OwnedProcessGroup()
    await group.start()
    member = None
    try:
        member = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            process_group=group.pid,
        )
    finally:
        await group.close()
        if member is not None:
            await asyncio.wait_for(member.wait(), timeout=5)
    assert member.returncode == -signal.SIGKILL
    assert group._process.returncode == -signal.SIGKILL


async def test_unresponsive_group_owner_cannot_hang_worker_cleanup():
    """A stopped reservation child produces a bounded failure after reaping."""
    worker = NativeWorker(LxmfConfig("stopped-reservation"))
    await worker.start()
    owner = worker._group._process
    os.kill(owner.pid, signal.SIGSTOP)
    with pytest.raises(LxmfConnectionError):
        await asyncio.wait_for(worker.stop(), timeout=8)
    assert owner.returncode is not None
    assert worker.returncode == 0
    assert worker._stderr.done()


async def test_detached_group_owner_cannot_hang_final_reaping(monkeypatch):
    """The reservation's own exit deadline cannot depend on control cooperation."""
    worker = NativeWorker(LxmfConfig("stopped-detached-reservation"))
    await worker.start()
    group = worker._group
    owner = group._process
    detach = group.detach

    async def detach_then_stop():
        await detach()
        os.kill(owner.pid, signal.SIGSTOP)

    monkeypatch.setattr(group, "detach", detach_then_stop)
    with pytest.raises(LxmfConnectionError, match="exceeded its shutdown deadline"):
        await asyncio.wait_for(worker.stop(), timeout=8)
    assert owner.returncode == -signal.SIGKILL
    assert worker.returncode == 0
    assert worker._stderr.done()
