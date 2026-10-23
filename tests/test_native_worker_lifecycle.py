"""A native worker cannot take over host signals or escape lifecycle ownership."""

import asyncio
import io
import os
import re
import signal
import struct
import sys

import pytest
import msgspec

from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.native_process import NativeWorker
from medre.adapters.lxmf.worker_protocol import (
    MAX_FRAME_BYTES,
    Ready,
    Start,
    WorkerProtocolError,
    encode,
    read_sync,
)
from medre.config.adapters.lxmf import LxmfConfig
from medre_lxmf_tests.helpers.async_utils import wait_until

_CHILD = """
import os, signal, sys, time
from medre.adapters.lxmf.worker_protocol import Ready, Stopped, encode, read_sync
start = read_sync(sys.stdin.buffer)
mode = sys.argv[1]
if mode in {"blocked", "ignore-stop"}:
    signal.signal(signal.SIGINT, lambda *args: None)
    signal.signal(signal.SIGTERM, lambda *args: None)
if mode == "blocked":
    sys.stderr.write("constructor-entered\\n")
    sys.stderr.flush()
    time.sleep(60)
if mode == "exit":
    raise SystemExit(0)
if mode == "noisy":
    sys.stderr.write("x" * 1024 * 1024)
    sys.stderr.flush()
sys.stdout.buffer.write(encode(Ready(os.getpid(), True, True, None, {"reticulum_config_dir": start.reticulum_config_dir})))
sys.stdout.buffer.flush()
command = read_sync(sys.stdin.buffer)
if mode == "stop-budget":
    sys.stderr.write(f"session-budget:{command.timeout_seconds}\\n")
    sys.stderr.flush()
    time.sleep(command.timeout_seconds)
if mode == "ignore-stop":
    sys.stderr.write("stop-entered\\n")
    sys.stderr.flush()
    time.sleep(60)
if mode in {"ack-with-descendant", "escaped-stderr"}:
    import subprocess
    detached = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        start_new_session=mode == "escaped-stderr")
    if mode == "escaped-stderr":
        sys.stderr.write(f"escaped-pid:{detached.pid}\\n")
        sys.stderr.flush()
sys.stdout.buffer.write(encode(Stopped()))
sys.stdout.buffer.flush()
if mode == "ack-then-hang":
    sys.stderr.write("acknowledged-before-exit\\n")
    sys.stderr.flush()
    time.sleep(60)
"""


def _worker(mode="blocked", *, startup_timeout=5):
    """Use a real subprocess with deterministic native-constructor substitutes."""
    return NativeWorker(
        LxmfConfig("isolated"),
        startup_timeout_seconds=startup_timeout,
        _command=(sys.executable, "-c", _CHILD, mode),
    )


def test_start_frame_roundtrip_preserves_validated_configuration():
    """Typed decoding retains frozen configuration and rejects unknown tags."""
    config = LxmfConfig("native", default_channel=2)
    assert read_sync(io.BytesIO(encode(Start(config)))) == Start(config)
    assert read_sync(io.BytesIO()) is None


@pytest.mark.parametrize(
    "raw", [b"x", struct.pack("!I", MAX_FRAME_BYTES + 1), struct.pack("!I", 3) + b"x"]
)
def test_frame_bounds_reject_partial_or_oversized_data(raw):
    """Untrusted lengths cannot allocate arbitrary memory or imitate clean EOF."""
    with pytest.raises(WorkerProtocolError):
        read_sync(io.BytesIO(raw))


async def test_fake_native_session_completes_the_real_worker_lifecycle():
    """The production entry point uses the same framing without native dependencies."""
    worker = NativeWorker(LxmfConfig("fake-worker"))
    ready = await worker.start()
    assert isinstance(ready, Ready)
    assert ready.pid == worker.pid
    assert ready.connected and ready.router_running
    assert ready.diagnostics["mode"] == "fake"
    outcome = await worker.stop()
    assert outcome.returncode == 0
    assert outcome.acknowledged
    assert not outcome.forced
    assert await worker.stop() == outcome
    with pytest.raises(LxmfConnectionError, match="reused"):
        await worker.start()


async def test_blocked_constructor_timeout_reaps_worker_without_host_signal_changes():
    """A non-cooperative SDK constructor cannot hang startup or borrow host signals."""
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    worker = _worker()
    starting = asyncio.create_task(worker.start())
    assert await wait_until(
        lambda: b"constructor-entered" in worker._stderr_tail, timeout=3
    )
    assert worker._startup_deadline is not None
    worker._startup_deadline.reschedule(asyncio.get_running_loop().time())
    with pytest.raises(TimeoutError):
        await starting
    assert worker.pid is not None
    assert worker.returncode is not None
    assert (await worker.stop()).forced
    assert all(signal.getsignal(sig) is handler for sig, handler in before.items())


async def test_cancelled_initialization_reaps_worker_and_preserves_request_count():
    """Repeated parent cancellation cannot orphan a blocked native constructor."""
    worker = _worker()
    starting = asyncio.create_task(worker.start())
    assert await wait_until(
        lambda: b"constructor-entered" in worker._stderr_tail, timeout=3
    )
    for _ in range(3):
        starting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starting
    assert starting.cancelling() == 3
    assert worker.returncode is not None
    assert worker._spawn.done()
    assert worker._stderr.done()
    assert worker._stopping.done()


async def test_non_cooperative_stop_is_forced_and_reaped():
    """An SDK ignoring shutdown is killed without mistaking exit for an ACK."""
    worker = _worker("ignore-stop")
    await worker.start()
    outcome = await worker.stop(timeout=0.05)
    assert outcome.forced
    assert not outcome.acknowledged
    assert outcome.returncode is not None


async def test_shutdown_acknowledgement_survives_forced_process_reaping():
    """A clean Stop ACK and a forced process exit are independent facts."""
    worker = _worker("ack-then-hang", startup_timeout=12)
    await worker.start()
    outcome = await worker.stop(timeout=0.2)
    assert outcome.acknowledged
    assert outcome.forced
    assert outcome.returncode is not None


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX worker process groups")
async def test_forced_descendant_reaping_is_recorded_even_after_clean_leader_exit():
    """Process-group cleanup must not be reported as wholly cooperative."""
    worker = _worker("ack-with-descendant", startup_timeout=12)
    await worker.start()
    outcome = await worker.stop(timeout=1.0)
    assert outcome.acknowledged
    assert outcome.returncode == 0
    assert outcome.forced
    assert worker._stderr.done()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX detached process groups")
async def test_stop_bounds_stderr_drain_when_descendant_escapes_worker_group():
    """An escaped SDK descendant must not make parent cleanup indefinite."""
    worker = _worker("escaped-stderr", startup_timeout=12)
    await worker.start()
    stopping = asyncio.create_task(worker.stop(timeout=0.1))
    done, _ = await asyncio.wait({stopping}, timeout=2.0)
    try:
        assert stopping in done
        outcome = await stopping
        assert outcome.acknowledged
        assert worker._stderr.done()
    finally:
        marker = re.search(rb"escaped-pid:(\d+)", worker._stderr_tail)
        if marker is not None:
            try:
                os.kill(int(marker.group(1)), signal.SIGKILL)
            except ProcessLookupError:
                pass
        # Let the original implementation complete even if the bounded
        # deadline assertion fails, so the regression cannot leak a task.
        if not stopping.done():
            await asyncio.wait_for(stopping, timeout=5)


@pytest.mark.parametrize("timeout", [1.0, 5.0])
async def test_worker_session_stop_reserves_parent_ack_and_exit_time(timeout):
    """The worker's own cleanup may not consume the entire parent deadline."""
    worker = _worker("stop-budget")
    await worker.start()
    result = await worker.stop(timeout=timeout)
    assert result.acknowledged
    assert not result.forced
    match = re.search(rb"session-budget:([0-9.]+)", worker._stderr_tail)
    assert match is not None
    budget = float(match.group(1))
    assert 0 <= budget < timeout


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process group errors")
async def test_denied_process_group_cleanup_still_reaps_and_closes_pipes(monkeypatch):
    """Permission errors must not bypass the cleanup section's remaining work."""
    worker = _worker("ignore-stop")
    await worker.start()

    def denied(pid, sig):
        # Leave the leader alive so cleanup must exercise its direct fallback.
        raise PermissionError("permission denied")

    monkeypatch.setattr(os, "killpg", denied)
    with pytest.raises(LxmfConnectionError, match="group cleanup was denied"):
        await worker.stop(timeout=0.05)
    assert worker.returncode is not None
    assert worker._stderr.done()
    assert worker._stopping.done()


async def test_no_readiness_on_clean_early_exit():
    """Exit status zero cannot qualify native readiness without its typed frame."""
    worker = _worker("exit")
    with pytest.raises(LxmfConnectionError, match="without readiness"):
        await worker.start()
    assert worker.returncode == 0
    assert not (await worker.stop()).acknowledged


async def test_worker_stderr_is_drained_with_bounded_private_retention():
    """Verbose SDK output cannot block readiness or grow retained memory unbounded."""
    worker = _worker("noisy")
    try:
        await worker.start()
        assert len(worker._stderr_tail) <= 16384
    finally:
        result = await worker.stop()
    assert result.acknowledged
    assert not result.forced


@pytest.mark.parametrize("signal_name", ["SIGINT", "SIGTERM"])
def test_host_signal_during_blocked_worker_constructor_reaps_child(signal_name):
    """A real host shutdown signal cannot be consumed by the SDK process."""
    import json
    import subprocess

    script = """
import asyncio, json, signal, sys
from medre.adapters.lxmf.native_process import NativeWorker
from medre.config.adapters.lxmf import LxmfConfig

async def main():
    worker = NativeWorker(LxmfConfig("signal-host"),
        _command=(sys.executable, "-c", sys.argv[1], "blocked"))
    starting = asyncio.create_task(worker.start())
    seen = []
    signum = getattr(signal, sys.argv[2])
    previous = signal.getsignal(signum)
    def shutdown(received, frame):
        seen.append(received)
        starting.cancel()
    signal.signal(signum, shutdown)
    try:
        async with asyncio.timeout(5):
            while b"constructor-entered" not in worker._stderr_tail:
                await asyncio.sleep(0.01)
            signal.raise_signal(signum)
            try:
                await starting
            except asyncio.CancelledError:
                pass
        assert seen == [signum]
        assert worker.returncode is not None
        print(json.dumps({"host_signal_received": True, "worker_reaped": True}))
    finally:
        signal.signal(signum, previous)
        await worker.stop()

asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, "-c", script, _CHILD, signal_name],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "host_signal_received": True,
        "worker_reaped": True,
    }


async def test_cancelled_spawn_keeps_ownership_of_the_late_process(monkeypatch):
    """Cancelling before spawn returns cannot discard the child handle."""
    entered, release = asyncio.Event(), asyncio.Event()
    original = asyncio.create_subprocess_exec

    async def paused_spawn(*args, **kwargs):
        """Expose the process-creation ownership transfer without a timing guess."""
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", paused_spawn)
    worker = _worker()
    starting = asyncio.create_task(worker.start())
    await entered.wait()
    starting.cancel()
    assert await wait_until(lambda: worker._stopping is not None, timeout=1)
    assert not starting.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await starting
    assert worker.pid is not None
    assert worker.returncode is not None
    assert worker._spawn.done()
    assert worker._stderr.done()


@pytest.mark.parametrize(
    "payload",
    [
        {"type": "execute", "command": "private-marker"},
        {
            "type": "start",
            "config": {"adapter_id": "native"},
            "unknown": "private-marker",
        },
        {
            "type": "start",
            "config": {"adapter_id": "native", "unknown": "private-marker"},
        },
    ],
)
def test_unknown_control_and_configuration_fields_are_rejected_without_payloads(
    payload,
):
    """A permissive dataclass decoder cannot silently accept configuration drift."""
    encoded = msgspec.msgpack.encode(payload)
    with pytest.raises(WorkerProtocolError) as exc:
        read_sync(io.BytesIO(len(encoded).to_bytes(4, "big") + encoded))
    assert "private-marker" not in str(exc.value)


async def test_native_stdout_writes_cannot_corrupt_protocol_frames():
    """Native fd writes and Python logging both stay off the control pipe."""
    script = """
import os
from medre.adapters.lxmf.session import LxmfSession
from medre.adapters.lxmf.native_worker import main
original = LxmfSession.start
async def noisy_start(self, *args, **kwargs):
    os.write(1, b"native-fd-output\\n")
    print("python-sdk-output")
    await original(self, *args, **kwargs)
LxmfSession.start = noisy_start
main()
"""
    worker = NativeWorker(
        LxmfConfig("noisy-native"), _command=(sys.executable, "-c", script)
    )
    try:
        await worker.start()
    finally:
        outcome = await worker.stop()
    assert outcome.acknowledged
    assert b"native-fd-output" in worker._stderr_tail
    assert b"python-sdk-output" in worker._stderr_tail


async def test_cancellation_during_stop_defers_until_the_worker_is_reaped():
    """Repeated stop cancellation preserves cleanup and its exact caller depth."""
    worker = _worker("ignore-stop")
    await worker.start()
    stopping = asyncio.create_task(worker.stop())
    assert await wait_until(lambda: b"stop-entered" in worker._stderr_tail, timeout=3)
    for _ in range(3):
        stopping.cancel()
    assert worker._shutdown_deadline is not None
    worker._shutdown_deadline.reschedule(asyncio.get_running_loop().time())
    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert stopping.cancelling() == 3
    assert worker.returncode is not None
    assert worker._stopping.done()
    assert worker._stderr.done()


@pytest.mark.parametrize("override", [None, "explicit-config"])
async def test_worker_receives_configured_reticulum_directory(override):
    """Explicit constructor injection wins; config remains the normal source."""
    worker = NativeWorker(
        LxmfConfig("paths", reticulum_config_dir="configured-reticulum"),
        reticulum_config_dir=override,
        _command=(sys.executable, "-c", _CHILD, "ready"),
    )
    try:
        ready = await worker.start()
        assert ready.diagnostics["reticulum_config_dir"] == (
            override if override is not None else "configured-reticulum"
        )
    finally:
        await worker.stop()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX detached process groups")
async def test_cancelled_start_bounds_buffered_stdin_with_escaped_reader():
    """An escaped reader cannot keep buffered configuration pending during reap."""
    child = """
import subprocess, sys, time
p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
    stdout=subprocess.DEVNULL, start_new_session=True)
sys.stderr.write(f"escaped-pid:{p.pid}\\n")
sys.stderr.flush()
time.sleep(60)
"""
    worker = NativeWorker(
        LxmfConfig("buffered", origin_label="x" * 512_000),
        _command=(sys.executable, "-c", child),
        startup_timeout_seconds=12,
    )
    starting = asyncio.create_task(worker.start())
    try:
        assert await wait_until(
            lambda: b"escaped-pid:" in worker._stderr_tail, timeout=5
        )
        assert worker._process.stdin.transport.get_write_buffer_size() > 0
        starting.cancel()
        done, _ = await asyncio.wait({starting}, timeout=4)
        assert starting in done
        with pytest.raises(asyncio.CancelledError):
            await starting
        assert worker.returncode is not None
        assert worker._stderr.done()
        assert worker._process.stdin.transport.get_write_buffer_size() == 0
    finally:
        marker = re.search(rb"escaped-pid:(\d+)", worker._stderr_tail)
        if marker is not None:
            try:
                os.kill(int(marker.group(1)), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if not starting.done():
            starting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(starting, timeout=5)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process group errors")
@pytest.mark.parametrize("operation", ["start", "stop"])
async def test_cancelled_lifecycle_retains_cleanup_failure_as_cause(
    monkeypatch, operation
):
    """Caller cancellation survives a denied group signal after owned cleanup."""
    worker = _worker("blocked" if operation == "start" else "ignore-stop")
    if operation == "start":
        pending = asyncio.create_task(worker.start())
        marker = b"constructor-entered"
    else:
        await worker.start()
        pending = asyncio.create_task(worker.stop(timeout=12))
        marker = b"stop-entered"
    assert await wait_until(lambda: marker in worker._stderr_tail, timeout=5)

    def denied(pid, sig):
        raise PermissionError("permission denied")

    monkeypatch.setattr(os, "killpg", denied)
    pending.cancel()
    pending.cancel()
    if operation == "stop":
        worker._shutdown_deadline.reschedule(asyncio.get_running_loop().time())
    with pytest.raises(asyncio.CancelledError) as caught:
        await pending
    assert isinstance(caught.value.__cause__, LxmfConnectionError)
    assert pending.cancelling() == 2
    assert worker.returncode is not None
    assert worker._stderr.done()
    assert worker._stopping.done()
