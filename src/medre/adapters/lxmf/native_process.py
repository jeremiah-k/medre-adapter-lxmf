"""Own a native LXMF worker from spawn through readiness and final reaping."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, cast

from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.process_group import OwnedProcessGroup
from medre.adapters.lxmf.worker_environment import native_environment
from medre.adapters.lxmf.worker_channel import WorkerChannel
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


class _ProcessPipes(Protocol):
    """Describe asyncio's private transport seam absent from Process stubs."""

    _transport: asyncio.SubprocessTransport


class _ExitObservedProtocol(asyncio.SubprocessProtocol):
    """Observe leader exit independently of stream EOF while preserving stream flow control.

    Process.wait() can remain pending while a descendant retains a pipe. The
    public subprocess protocol exit event arrives when the leader is reaped;
    forwarding every other callback preserves asyncio's existing stream owner.
    """

    def __init__(
        self, delegate: asyncio.SubprocessProtocol, exited: Callable[[], None]
    ) -> None:
        self._delegate = delegate
        self._exited = exited

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Retain the stream protocol's transport initialization."""
        self._delegate.connection_made(transport)

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        """Feed the same bounded native protocol and diagnostic readers."""
        self._delegate.pipe_data_received(fd, data)

    def pipe_connection_lost(self, fd: int, exc: Exception | None) -> None:
        """Preserve EOF/error handling for each owned pipe."""
        self._delegate.pipe_connection_lost(fd, exc)

    def process_exited(self) -> None:
        """Record native exit before inherited pipes are required to close."""
        try:
            self._delegate.process_exited()
        finally:
            self._exited()

    def pause_writing(self) -> None:
        """Keep stdin drain backpressure attached to its original stream protocol."""
        self._delegate.pause_writing()

    def resume_writing(self) -> None:
        """Resume existing stdin waiters without introducing another writer."""
        self._delegate.resume_writing()

    def connection_lost(self, exc: Exception | None) -> None:
        """Complete the original stream teardown and its waiting callers."""
        self._delegate.connection_lost(exc)


@dataclass(frozen=True)
class WorkerExit:
    """Native cleanup ACK, forced group cleanup and leader exit are distinct."""

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
        message_plane: bool = False,
        on_failure: Callable[[], None] | None = None,
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
        self._message_plane = message_plane
        self._on_failure = on_failure
        self.channel: WorkerChannel | None = None
        self._failure_cleanup: asyncio.Task[None] | None = None
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
        self._group: OwnedProcessGroup | None = None
        self._spawn: asyncio.Task[asyncio.subprocess.Process] | None = None
        self._stderr: asyncio.Task[None] | None = None
        self._stopping: asyncio.Task[WorkerExit] | None = None
        self._ready: Ready | None = None
        self._startup_deadline: asyncio.Timeout | None = None
        self._shutdown_deadline: asyncio.Timeout | None = None
        self._stderr_tail = bytearray()
        self._leader_exit_seen = False

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
        process_group: int | None = None
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            self._group = OwnedProcessGroup()
            await self._group.start()
            process_group = self._group.pid
        process = await asyncio.create_subprocess_exec(
            *self._command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            process_group=process_group,
            creationflags=creationflags,
            env=native_environment(),
        )
        self._process = process
        transport = cast(_ProcessPipes, process)._transport
        protocol = transport.get_protocol()
        if isinstance(protocol, asyncio.SubprocessProtocol):
            transport.set_protocol(
                _ExitObservedProtocol(protocol, self._on_worker_exit)
            )
        elif process.returncode is None:
            raise LxmfConnectionError("Native worker exit observation is unavailable")
        # Exit can precede observer installation; the retained Process owns
        # its return code, so no PID probing or second child reaper is needed.
        if process.returncode is not None:
            self._on_worker_exit()
        self._stderr = asyncio.create_task(
            self._drain_stderr(process), name="lxmf-worker-stderr"
        )
        return process

    def _on_worker_exit(self) -> None:
        """Feed unexpected leader exit into the same fail-closed channel/cleanup owner."""
        if self._leader_exit_seen:
            return
        self._leader_exit_seen = True
        if self._stopping is not None or self._ready is None:
            return
        if self.channel is not None:
            self.channel._fail("Native worker process exited")
        else:
            self._schedule_failed_cleanup()

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
                    encode(
                        Start(
                            self._config,
                            self._reticulum_config_dir,
                            self._message_plane,
                        )
                    )
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
                    or process.returncode is not None
                ):
                    raise LxmfConnectionError("Native worker exited without readiness")
                self._ready = reply
                if self._message_plane:
                    self.channel = WorkerChannel(process, self._schedule_failed_cleanup)
                return reply
        except BaseException as startup_error:
            try:
                await self.stop(timeout=2.0)
            except Exception as cleanup_error:
                if isinstance(startup_error, asyncio.CancelledError):
                    raise startup_error from cleanup_error
                raise
            raise

    def _schedule_failed_cleanup(self) -> None:
        """Retain failure cleanup rather than leaving a dead generation unreaped."""
        if self._stopping is None and self._failure_cleanup is None:
            self._failure_cleanup = asyncio.create_task(
                self._cleanup_failed_channel(), name="lxmf-failed-generation"
            )
        if self._on_failure is not None:
            try:
                self._on_failure()
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "LXMF worker health observer failed (%s)", type(exc).__name__
                )

    async def _cleanup_failed_channel(self) -> None:
        """Contain cleanup errors; callers retain the channel's explicit failure."""
        with contextlib.suppress(Exception):
            await self.stop(timeout=0)

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
        try:
            while not self._stopping.done():
                try:
                    await asyncio.shield(self._stopping)
                except asyncio.CancelledError as exc:
                    cancelled = exc
            result = self._stopping.result()
        except BaseException as cleanup_error:
            if cancelled is not None:
                raise cancelled from cleanup_error
            raise
        if cancelled is not None:
            raise cancelled
        return result

    def _kill(self) -> bool:
        """Force an owned leader/group, reporting whether a signal was sent."""
        process = self._process
        if process is None:
            return False
        try:
            if sys.platform == "win32":
                if process.returncode is None:
                    process.kill()
                    return True
            else:
                assert self._group is not None
                return self._group.kill()
        except ProcessLookupError:
            pass
        return False

    async def _stop_owned(self, timeout: float) -> WorkerExit:
        """Own the spawn result, cooperative stop, forced stop and pipe drain."""
        if self._spawn is not None:
            with contextlib.suppress(Exception):
                await asyncio.shield(self._spawn)
        process = self._process
        if process is None:
            if self._group is not None:
                await self._group.close()
            return WorkerExit(None, False, False)
        ownership_error: Exception | None = None
        acknowledged = False
        forced = False
        group_cleanup_denied = False
        try:
            if self._ready is not None and process.returncode is None and timeout > 0:
                assert process.stdin is not None and process.stdout is not None
                async with asyncio.timeout(timeout) as deadline:
                    self._shutdown_deadline = deadline
                    # Session cleanup must finish before the parent's deadline:
                    # allow time for the acknowledgement and process exit.
                    session_budget = max(0.0, min(timeout * 0.8, timeout - 0.5))
                    if self.channel is not None:
                        acknowledged = await self.channel.request_stop(session_budget)
                    else:
                        process.stdin.write(encode(Stop(session_budget)))
                        await process.stdin.drain()
                        acknowledged = isinstance(
                            await read_async(process.stdout), Stopped
                        )
                    process.stdin.close()
                    await process.wait()
        except (TimeoutError, ConnectionError, OSError, RuntimeError):
            # A Stopped frame acknowledges native cleanup, not process exit.
            # Do not erase a received ACK if the process misses its exit
            # deadline or closes a pipe with an error afterwards.
            pass
        finally:
            if self._group is not None:
                try:
                    await self._group.detach()
                except Exception as exc:
                    ownership_error = exc
            if process.returncode is None:
                forced = True
            # Retire the whole POSIX group before waiting for pipe EOF. A
            # descendant may keep the pipes open after the leader exits.
            try:
                forced = self._kill() or forced
            except PermissionError:
                # A denied group signal must not skip closing the pipes or
                # reaping the leader. Never report unknown group cleanup as
                # a fully successful shutdown.
                group_cleanup_denied = True
                if process.returncode is None:
                    try:
                        process.kill()
                        forced = True
                    except ProcessLookupError:
                        pass
            if process.stdin is not None:
                # Cooperative exchange is over. Discard unsent configuration:
                # an escaped reader can retain stdin without ever draining it.
                # Cancelling wait_closed itself also cancels asyncio's shared
                # close future, causing late pipe callbacks to fail.
                if process.stdin.transport.get_write_buffer_size():
                    process.stdin.transport.abort()
                else:
                    process.stdin.close()
                with contextlib.suppress(ConnectionError, OSError, TimeoutError):
                    await asyncio.wait_for(
                        asyncio.shield(process.stdin.wait_closed()), timeout=1.0
                    )
            # Process has no public pipe-close method; keep its private
            # transport dependency confined to this typed lifecycle seam.
            transport = cast(_ProcessPipes, process)._transport
            try:
                if self._stderr is not None:
                    # Escaped descendants can retain stderr beyond group exit.
                    await asyncio.wait_for(self._stderr, timeout=1.0)
            except TimeoutError:
                pass
            finally:
                # Closing every owned read end is required even if stderr
                # draining fails. Detached native processes are outside the
                # group's ownership and cannot control parent pipe lifetime.
                for descriptor in (1, 2):
                    pipe = transport.get_pipe_transport(descriptor)
                    if pipe is not None:
                        pipe.close()
                try:
                    await process.wait()
                finally:
                    try:
                        if self._group is not None:
                            await self._group.close()
                    finally:
                        if self.channel is not None:
                            await self.channel.close()
        if ownership_error is not None:
            raise LxmfConnectionError(
                "Native worker group ownership release failed"
            ) from ownership_error
        if group_cleanup_denied:
            raise LxmfConnectionError("Native worker process-group cleanup was denied")
        return WorkerExit(process.returncode, forced, acknowledged)
