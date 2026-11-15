"""Bound native peer observations to the endpoint generation under qualification."""

from __future__ import annotations

import asyncio
import json
import math
import time
import uuid
from typing import NoReturn

_RECOVERY_POLL_SECONDS = 1.0
_PENDING_MESSAGE_STATES = frozenset({"generating", "outbound", "sending", "sent"})


class NativePeerExitedError(AssertionError):
    """The native control stream ended before the requested observation."""


_TERMINAL_MESSAGE_STATES = frozenset({"delivered", "failed", "rejected", "cancelled"})


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
                raise NativePeerExitedError(
                    "native peer exited before its expected event"
                )
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


def _delivery_is_terminal(peer: dict[str, object]) -> bool:
    """Whether the peer's last submission has reached an LXMF terminal state."""
    return (
        peer.get("message_terminal") is True
        or peer.get("message_state_name") in _TERMINAL_MESSAGE_STATES
    )


def _usable_link(peer: dict[str, object]) -> bool:
    return (
        peer.get("direct_link_active") is True
        or peer.get("backchannel_link_active") is True
    )


def _participants_available(peer: dict[str, object], worker: dict[str, object]) -> bool:
    return (
        peer.get("event") == "status"
        and peer.get("attached") is True
        and worker.get("connected") is True
        and worker.get("router_running") is True
    )


def _announce_seen(peer: dict[str, object]) -> bool:
    """Report whether the peer observed a matching destination announce.

    An observed announce is the path-availability precursor: the pinned SDK
    registers the path from it on the router's next processing pass, so a
    fresh announce with an unknown path means path learning is in progress
    rather than dead. A missing announce leaves the strict refusal intact.
    """
    age = peer.get("matching_announce_age_seconds")
    return isinstance(age, (int, float)) and not isinstance(age, bool) and age >= 0


def recovery_is_available(peer: dict[str, object], worker: dict[str, object]) -> bool:
    """Grant extra observation only on complete, positive native recovery facts."""
    if not _participants_available(peer, worker):
        return False
    if (
        peer.get("path_known") is not True
        and not _usable_link(peer)
        and not _announce_seen(peer)
    ):
        return False
    state = peer.get("message_state_name")
    if not isinstance(state, str) or state not in _PENDING_MESSAGE_STATES:
        return False
    if peer.get("message_terminal") is not False:
        return False
    attempts = peer.get("delivery_attempts")
    limit = peer.get("max_delivery_attempts")
    return type(attempts) is int and type(limit) is int and 0 <= attempts < limit


def classify_inbound_timeout(peer: dict[str, object], worker: dict[str, object]) -> str:
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
    if peer.get("message_state_name") == "delivered":
        return "native_delivery_concluded_without_handoff"
    if _delivery_is_terminal(peer):
        return "native_delivery_terminal"
    attempts = peer.get("delivery_attempts")
    limit = peer.get("max_delivery_attempts")
    if isinstance(attempts, int) and isinstance(limit, int) and attempts >= limit:
        return "native_delivery_attempts_exhausted"
    # The peer derives these facts from the pinned SDK's Link.ACTIVE, keeping
    # SDK imports and numeric state assumptions out of this SDK-free consumer.
    # An active backchannel is usable even without a cached destination path.
    if not _usable_link(peer):
        if peer.get("path_known") is False:
            return "destination_path_unavailable"
        return "native_link_unestablished"
    return "handoff_not_observed_at_recipient"


async def _diagnose(peer, session) -> tuple[dict[str, object], dict[str, object]]:
    """Collect bounded peer and worker facts without admitting traffic."""
    token = uuid.uuid4().hex
    try:
        async with asyncio.timeout(2):
            await command(peer, command="status", token=token)
            status = await event(peer, "status", token=token)
        status.pop("token", None)
    except (TimeoutError, NativePeerExitedError, BrokenPipeError, ConnectionResetError):
        status = {"event": "status_unavailable"}
    diag = session.diagnostics()
    worker = {
        "connected": diag.connected,
        "router_running": diag.router_running,
        "pending_delivery_count": diag.pending_delivery_count,
    }
    return status, worker


def _raise_timeout(status: dict[str, object], worker: dict[str, object]) -> NoReturn:
    raise AssertionError(
        "native inbound deadline exceeded; stage="
        + classify_inbound_timeout(status, worker)
        + "; peer="
        + json.dumps(status, sort_keys=True)
        + "; worker="
        + json.dumps(worker, sort_keys=True)
    )


def _validated(
    item: dict[str, object], expected_content: str | None
) -> dict[str, object]:
    """Reject any delivery that is not the exact submission under qualification."""
    if expected_content is not None and item.get("content") != expected_content:
        raise AssertionError(
            "native inbound content mismatch; the observed delivery is not the "
            "exact submission under qualification"
        )
    return item


def _take_ready(queue) -> dict[str, object] | None:
    """Take an already-observed recipient delivery without waiting."""
    if queue.empty():
        return None
    return queue.get_nowait()


async def inbound(
    queue,
    peer,
    session,
    *,
    timeout: float = 20,
    recovery_seconds: float = 0,
    expected_content: str | None = None,
) -> dict[str, object]:
    """Preserve the delivery deadline and wait only for SDK-owned recovery.

    The first deadline is the ordinary qualification verdict. When it expires,
    the peer's unfinished attempt may still be inside the pinned SDK's retry
    and link-establishment cycle; the caller may then grant an explicit,
    SDK-derived recovery window. Terminal states, exhausted attempts, a path
    with no observed matching announce, or an unavailable worker end recovery
    at the next bounded status check.
    Sender delivery during an admitted window still needs exact recipient handoff.
    The complete receive budget includes diagnostic time. Diagnostics never resend,
    reset routing, or create a connection to make a test pass.
    """
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("delivery deadline must be finite and non-negative")
    if not math.isfinite(recovery_seconds) or not 0 <= recovery_seconds <= 120:
        raise ValueError("recovery window must be finite and between 0 and 120")
    if recovery_seconds and not isinstance(expected_content, str):
        raise ValueError("recovery requires exact expected content")
    healthy_deadline = time.monotonic() + timeout
    deadline = healthy_deadline + recovery_seconds
    item = _take_ready(queue)
    if item is not None:
        return _validated(item, expected_content)
    try:
        item = await asyncio.wait_for(queue.get(), timeout)
    except TimeoutError:
        pass
    if item is not None and time.monotonic() < healthy_deadline:
        return _validated(item, expected_content)

    # A successful wait can resume after its timer when scheduling is delayed.
    # Such a read needs the same admission as a callback arriving in diagnosis.
    status, worker = await _diagnose(peer, session)
    if time.monotonic() >= deadline:
        _raise_timeout(status, worker)
    if not recovery_seconds or not recovery_is_available(status, worker):
        _raise_timeout(status, worker)
    if item is not None:
        return _validated(item, expected_content)

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            status, worker = await _diagnose(peer, session)
            _raise_timeout(status, worker)
        ready = _take_ready(queue)
        if ready is not None:
            return _validated(ready, expected_content)
        try:
            item = await asyncio.wait_for(
                queue.get(), min(remaining, _RECOVERY_POLL_SECONDS)
            )
        except TimeoutError:
            status, worker = await _diagnose(peer, session)
            if time.monotonic() >= deadline:
                _raise_timeout(status, worker)
            # Sender DELIVERED can precede the parent's inbound callback.
            # It cannot grant recovery, but an admitted window can finish that
            # exact handoff. All other denied observations precede queue reads.
            handoff_pending = (
                _participants_available(status, worker)
                and status.get("message_state_name") == "delivered"
                and status.get("message_terminal") is True
            )
            if not handoff_pending and not recovery_is_available(status, worker):
                _raise_timeout(status, worker)
        else:
            if time.monotonic() >= deadline:
                status, worker = await _diagnose(peer, session)
                _raise_timeout(status, worker)
            return _validated(item, expected_content)
