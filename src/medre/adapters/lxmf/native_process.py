"""Own a native LXMF worker from spawn through readiness and final reaping."""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import signal
import subprocess
import sys
from dataclasses import dataclass

from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.worker_protocol import (
    Failed,
    Ready,
    Start,
    Stop,
    Stopped,
    encode,
    read_async,
)
from medre.config.adapters.lxmf import LxmfConfig


@dataclass(frozen=True)
class WorkerExit:
    """Process exit evidence, distinct from native message delivery evidence."""

    returncode: int | None
    forced: bool
    acknowledged: bool


class NativeWorker:
    """Bound SDK initialization without borrowing host process signal handlers.

    One instance owns one worker lifetime. This lifecycle module deliberately
    does not send messages or interpret routing, outbox or delivery facts.
    """

    def __init__(
        self,
        config: LxmfConfig,
        *,
        reticulum_config_dir: str | None = None,
        startup_timeout_seconds: float = 30.0,
        _command: tuple[str, ...] | None = None,
    ) -> None:
        config.validate()
        if (
            isinstance(startup_timeout_seconds, bool)
            or not math.isfinite(startup_timeout_seconds)
            or startup_timeout_seconds <= 0
        ):
            raise ValueError(
                "Native worker startup timeout must be finite and positive"
            )
        self._config = config
        self._reticulum_config_dir = (
            reticulum_config_dir
            if reticulum_config_dir is not None
            else config.reticulum_config_dir
        )
        self._startup_timeout = startup_timeout_seconds
        self._command = _command or (
            sys.executable,
            "-m",
            "medre.adapters.lxmf.native_worker",
        )
        self._process: asyncio.subprocess.Process | None = None
        self._spawn: asyncio.Task[asyncio.subprocess.Process] | None = None
        self._stderr: asyncio.Task[None] | None = None
        self._stopping: asyncio.Task[WorkerExit] | None = None
        self._ready: Ready | None = None
        self._startup_deadline: asyncio.Timeout | None = None
        self._shutdown_deadline: asyncio.Timeout | None = None
        self._stderr_tail = bytearray()

    @property
    def pid(self) -> int | None:
        """Return the owned process identity after spawn, including after exit."""
        return self._process.pid if self._process is not None else None

    @property
    def returncode(self) -> int | None:
        """Return an observed process exit code without inferring native delivery."""
        return self._process.returncode if self._process is not None else None

    async def _spawn_owned(self) -> asyncio.subprocess.Process:
        """Publish ownership before the cancellable start caller receives a child."""
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        process = await asyncio.create_subprocess_exec(
            *self._command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=sys.platform != "win32",
            creationflags=creationflags,
        )
        self._process = process
        self._stderr = asyncio.create_task(
            self._drain_stderr(process), name="lxmf-worker-stderr"
        )
        return process

    async def _drain_stderr(self, process: asyncio.subprocess.Process) -> None:
        """Drain SDK output with bounded private retention, never health/log echo."""
        if process.stderr is None:
            return
        while chunk := await process.stderr.read(4096):
            self._stderr_tail.extend(chunk)
            del self._stderr_tail[:-16384]

    async def start(self) -> Ready:
        """Return native readiness or reap failed/cancelled initialization first."""
        if self._spawn is not None or self._stopping is not None:
            raise LxmfConnectionError("Native worker lifetimes cannot be reused")
        self._spawn = asyncio.create_task(self._spawn_owned(), name="lxmf-worker-spawn")
        try:
            async with asyncio.timeout(self._startup_timeout) as deadline:
                self._startup_deadline = deadline
                process = await asyncio.shield(self._spawn)
                if process.stdin is None or process.stdout is None:
                    raise LxmfConnectionError("Native worker pipes are unavailable")
                process.stdin.write(
                    encode(Start(self._config, self._reticulum_config_dir))
                )
                await process.stdin.drain()
                reply = await read_async(process.stdout)
                if isinstance(reply, Failed):
                    raise LxmfConnectionError(reply.message)
                if (
                    not isinstance(reply, Ready)
                    or reply.pid != process.pid
                    or not reply.connected
                    or not reply.router_running
                    or self._stopping is not None
                ):
                    raise LxmfConnectionError("Native worker exited without readiness")
                self._ready = reply
                return reply
        except BaseException:
            await self.stop(timeout=2.0)
            raise

    async def stop(self, timeout: float = 5.0) -> WorkerExit:
        """Stop once and defer repeated caller cancellation until cleanup settles.

        A worker that has not announced readiness is killed immediately: an SDK
        constructor cannot be trusted to read a cooperative shutdown command.
        """
        if (
            isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or not 0 <= timeout <= 3600
        ):
            raise ValueError(
                "Native worker stop timeout must be finite and nonnegative"
            )
        if self._stopping is None:
            self._stopping = asyncio.create_task(
                self._stop_owned(timeout), name="lxmf-worker-stop"
            )
        cancelled: asyncio.CancelledError | None = None
        while not self._stopping.done():
            try:
                await asyncio.shield(self._stopping)
            except asyncio.CancelledError as exc:
                cancelled = exc
        result = self._stopping.result()
        if cancelled is not None:
            raise cancelled
        return result

    def _kill(self) -> None:
        """Kill the owned process group without invoking SDK signal handlers."""
        process = self._process
        if process is None:
            return
        with contextlib.suppress(ProcessLookupError):
            if sys.platform == "win32":
                if process.returncode is None:
                    process.kill()
            else:
                os.killpg(process.pid, signal.SIGKILL)

    async def _stop_owned(self, timeout: float) -> WorkerExit:
        """Own the spawn result, cooperative stop, forced stop and pipe drain."""
        if self._spawn is not None:
            with contextlib.suppress(Exception):
                await asyncio.shield(self._spawn)
        process = self._process
        if process is None:
            return WorkerExit(None, False, False)
        acknowledged = False
        forced = False
        try:
            if self._ready is not None and process.returncode is None and timeout > 0:
                assert process.stdin is not None and process.stdout is not None
                async with asyncio.timeout(timeout) as deadline:
                    self._shutdown_deadline = deadline
                    process.stdin.write(encode(Stop(timeout)))
                    await process.stdin.drain()
                    acknowledged = isinstance(await read_async(process.stdout), Stopped)
                    process.stdin.close()
                    await process.wait()
        except (TimeoutError, ConnectionError, OSError, RuntimeError):
            acknowledged = False
        finally:
            if process.returncode is None:
                forced = True
                self._kill()
            await process.wait()
            if process.stdin is not None:
                process.stdin.close()
                with contextlib.suppress(ConnectionError, OSError):
                    await process.stdin.wait_closed()
            # SDK descendants may inherit pipes. Retire the owned group before
            # waiting for EOF, even if its leader has already exited.
            self._kill()
            if self._stderr is not None:
                await self._stderr
        return WorkerExit(process.returncode, forced, acknowledged)
