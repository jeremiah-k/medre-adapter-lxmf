"""Own Reticulum/LXMF initialization and teardown inside a dedicated process.

The lifecycle entry point is private to the adapter. It does not accept code,
credentials in arguments, canonical events, routes, or durable delivery state.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
from dataclasses import asdict
from typing import BinaryIO

from medre.adapters.lxmf.worker_protocol import (
    Failed,
    Message,
    Ready,
    Start,
    Stop,
    Stopped,
    encode,
    read_sync,
)
from medre.core.observability.sanitization import sanitize_error


def _write(stream: BinaryIO, message: Message) -> None:
    """Publish a complete protocol frame independently of SDK stdout logging."""
    stream.write(encode(message))
    stream.flush()


async def _run(stream: BinaryIO) -> None:
    """Initialize one session and serve its explicit shutdown request."""
    # Import SDK-backed code only after stdout has been separated from IPC.
    from medre.adapters.lxmf.session import LxmfSession

    first = await asyncio.to_thread(read_sync, sys.stdin.buffer)
    if not isinstance(first, Start):
        raise RuntimeError("Native worker requires a start frame")
    first.config.validate()
    session = LxmfSession(
        first.config,
        first.config.adapter_id,
        reticulum_config_dir=first.reticulum_config_dir,
    )
    try:
        await session.start()
        _write(
            stream,
            Ready(
                pid=os.getpid(),
                connected=session.connected,
                router_running=session.router_running,
                destination_hash=session._delivery_destination_hash,
                diagnostics=asdict(session.diagnostics()),
            ),
        )
        command = await asyncio.to_thread(read_sync, sys.stdin.buffer)
        if command is None:
            return
        if not isinstance(command, Stop):
            raise RuntimeError("Native worker requires a stop frame")
        if not 0 <= command.timeout_seconds <= 3600:
            raise RuntimeError("Native worker stop deadline is invalid")
        await session.stop(timeout=command.timeout_seconds)
        _write(stream, Stopped())
    finally:
        await session.stop()


def main() -> None:
    """Keep SDK output off the protocol pipe and contain native process globals."""
    with os.fdopen(os.dup(sys.stdout.fileno()), "wb") as protocol:
        # Native libraries can bypass Python streams and write directly to fd 1.
        # Keep the original pipe on a private, non-inheritable descriptor and
        # redirect both native stdout and Python SDK output to drained stderr.
        os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
        sys.stdout = sys.stderr
        logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
        try:
            asyncio.run(_run(protocol))
        except Exception as exc:
            with contextlib.suppress(BrokenPipeError):
                _write(protocol, Failed(sanitize_error(str(exc))))
            raise SystemExit(1) from None


if __name__ == "__main__":
    main()
