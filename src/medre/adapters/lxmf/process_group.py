"""Reserve a POSIX SDK group identity until its final signal has completed."""

from __future__ import annotations

import asyncio
import contextlib
import os
import select
import signal
import subprocess
import sys

from medre.adapters.lxmf.errors import LxmfConnectionError


class OwnedProcessGroup:
    """Retain an unreaped child PID while asyncio manages the SDK worker.

    The reservation child initially leads the SDK group, then moves into the
    host's group before cleanup. Its retained Popen handle is never polled or
    waited until group signalling is over. Thus even an unexpected child exit
    cannot release the numeric ID for another process group to reuse.
    """

    def __init__(self) -> None:
        self._process: subprocess.Popen[bytes] | None = None
        self._detached = False
        self._retired = False

    @property
    def pid(self) -> int:
        """Return the reserved group ID only while this instance owns it."""
        if self._process is None or self._retired:
            raise LxmfConnectionError("Native worker group ownership is unavailable")
        return self._process.pid

    async def start(self) -> None:
        """Create the reservation outside asyncio's automatic child reaper."""
        if self._process is not None or self._retired:
            raise LxmfConnectionError("Native worker group lifetimes cannot be reused")
        self._process = await asyncio.to_thread(
            subprocess.Popen,
            [
                sys.executable,
                "-m",
                "medre.adapters.lxmf.process_group",
                str(os.getpgrp()),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            process_group=0,
        )
        await self._acknowledge(None, b"R")

    async def _acknowledge(self, request: bytes | None, expected: bytes) -> None:
        """Bound a private control exchange without cancelling a blocked read."""
        process = self._process
        assert process is not None
        assert process.stdin is not None and process.stdout is not None
        write_fd = process.stdin.fileno()
        read_fd = process.stdout.fileno()

        def exchange() -> None:
            if request is not None:
                os.write(write_fd, request)
            readable, _, _ = select.select([read_fd], [], [], 2.0)
            if not readable or os.read(read_fd, 1) != expected:
                raise LxmfConnectionError(
                    "Native worker group control was not acknowledged"
                )

        try:
            await asyncio.to_thread(exchange)
        except OSError:
            raise LxmfConnectionError(
                "Native worker group control pipe failed"
            ) from None

    async def detach(self) -> None:
        """Leave only SDK processes in the group, keeping its ID reserved."""
        process = self._process
        if process is None or self._retired:
            raise LxmfConnectionError("Native worker group ownership is unavailable")
        if self._detached:
            return
        await self._acknowledge(b"D", b"D")
        self._detached = True

    def kill(self) -> bool:
        """Signal the SDK group while the reservation PID cannot be reused."""
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            return False
        return True

    async def close(self) -> None:
        """End the lifetime lease and reap only after all group signalling."""
        process = self._process
        if process is None or self._retired:
            return
        self._retired = True
        if process.stdin is not None:
            with contextlib.suppress(BrokenPipeError):
                process.stdin.close()
        try:
            try:
                returncode = await asyncio.to_thread(process.wait, timeout=2.0)
            except subprocess.TimeoutExpired:
                # The timed wait has completed without reaping this child.
                # Its PID remains owned through the final direct signal.
                process.kill()
                await asyncio.to_thread(process.wait)
                raise LxmfConnectionError(
                    "Native worker group owner exceeded its shutdown deadline"
                ) from None
        finally:
            if process.stdout is not None:
                process.stdout.close()
        expected = {0} if self._detached else {0, -signal.SIGKILL}
        if returncode not in expected:
            raise LxmfConnectionError("Native worker group owner exited unexpectedly")


def _hold_group() -> None:
    """Hold a private group ID and retire that group when its parent disappears."""
    group = os.getpgrp()
    parent_group = int(sys.argv[1])
    # Direct invocation without a private group must never signal the host.
    if group != os.getpid() or parent_group == group:
        raise SystemExit(2)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        os.write(1, b"R")
        command = os.read(0, 1)
        if command == b"D":
            os.setpgid(0, parent_group)
            os.write(1, b"D")
            # A private pipe is the lifetime lease, including parent death.
            if os.read(0, 1):
                raise SystemExit(2)
        elif command:
            raise SystemExit(2)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(group, signal.SIGKILL)


if __name__ == "__main__":
    _hold_group()
