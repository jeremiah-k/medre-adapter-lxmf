"""Bound native peer observations to the endpoint generation under qualification."""

from __future__ import annotations

import asyncio
import json
import uuid


async def command(process, **fields):
    """Send one local command without changing native transport state."""
    process.stdin.write((json.dumps(fields) + "\n").encode())
    await process.stdin.drain()


async def event(process, kind, *, timeout=20, **expected):
    """Match native facts, including a generation marker carried in announce data.

    Destination addresses survive restoration, so matching an address alone
    permits a queued pre-shutdown announce to masquerade as restored readiness.
    Every requested field must match; unrelated announces can be discarded but
    unexpected delivered payloads remain failures.
    """
    async with asyncio.timeout(timeout):
        while True:
            line = await process.stdout.readline()
            if not line:
                raise AssertionError("native peer exited before its expected event")
            observation = json.loads(line)
            if observation["event"] == "received" and kind != "received":
                raise AssertionError("unsolicited native peer delivery")
            if observation["event"] == kind and all(
                observation.get(key) == value for key, value in expected.items()
            ):
                return observation


async def inbound(queue, peer, session, *, timeout=20):
    """Preserve the delivery deadline and capture bounded native failure evidence.

    Diagnostics inspect the peer's last submitted message and existing links;
    they never resend, reset routing, or create a connection to make a test pass.
    The worker observation uses cached scalar diagnostics and admits no traffic.
    """
    try:
        return await asyncio.wait_for(queue.get(), timeout)
    except TimeoutError:
        token = uuid.uuid4().hex
        try:
            async with asyncio.timeout(2):
                await command(peer, command="status", token=token)
                status = await event(peer, "status", token=token)
            status.pop("token", None)
        except (TimeoutError, AssertionError, BrokenPipeError, ConnectionResetError):
            status = {"event": "status_unavailable"}
        diag = session.diagnostics()
        worker = {
            "connected": diag.connected,
            "router_running": diag.router_running,
            "pending_delivery_count": diag.pending_delivery_count,
        }
        raise AssertionError(
            "native inbound deadline exceeded; peer="
            + json.dumps(status, sort_keys=True)
            + "; worker="
            + json.dumps(worker, sort_keys=True)
        ) from None
