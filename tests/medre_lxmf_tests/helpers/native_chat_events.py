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


async def submit(peer, *, destination, content, timeout=20):
    """Wait for this peer submission's acceptance before its delivery deadline.

    Draining the control pipe proves only that a command was written. The
    native owner acknowledges after handle_outbound returns; that observation
    is neither recipient delivery nor permission for a second submission.
    """
    token = uuid.uuid4().hex
    await command(
        peer, command="send", destination=destination, content=content, token=token
    )
    await event(peer, "submitted", token=token, timeout=timeout)


async def outbound_received(peer, session, *, timeout=20):
    """Observe MEDRE -> peer delivery, without inspecting the peer's outbound link.

    The peer's ``status`` command tracks its own last sent message, not the
    message MEDRE sent to it. Reusing that status would misidentify the failed
    direction, so only the sending worker's own cached diagnostics are used.
    """
    try:
        return await event(peer, "received", timeout=timeout)
    except TimeoutError:
        diag = session.diagnostics()
        raise AssertionError(
            "native outbound recipient deadline exceeded; worker="
            + json.dumps(
                {
                    "connected": diag.connected,
                    "router_running": diag.router_running,
                    "pending_delivery_count": diag.pending_delivery_count,
                },
                sort_keys=True,
            )
        ) from None


def classify_inbound_timeout(peer: dict, worker: dict) -> str:
    """Classify the observed stage without inferring a network root cause.

    A known path or a successful local submission is not proof that a direct
    LXMF link became usable. Never turn a deadline into a retry instruction.
    """
    if peer.get("event") != "status":
        return "peer_observation_unavailable"
    if peer.get("attached") is False:
        return "peer_shared_service_disconnected"
    if worker.get("connected") is False or worker.get("router_running") is False:
        return "medre_native_router_unavailable"
    # The peer derives these facts from the pinned SDK's Link.ACTIVE, keeping
    # SDK imports and numeric state assumptions out of this SDK-free consumer.
    # An active backchannel is usable even without a cached destination path.
    usable_link = (
        peer.get("direct_link_active") is True
        or peer.get("backchannel_link_active") is True
    )
    if not usable_link:
        if peer.get("path_known") is False:
            return "destination_path_unavailable"
        return "native_link_unestablished"
    return "handoff_not_observed_at_recipient"


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
            "native inbound deadline exceeded; stage="
            + classify_inbound_timeout(status, worker)
            + "; peer="
            + json.dumps(status, sort_keys=True)
            + "; worker="
            + json.dumps(worker, sort_keys=True)
        ) from None
