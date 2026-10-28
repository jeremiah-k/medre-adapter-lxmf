"""Native restore observations must distinguish endpoint lifetimes."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from medre_lxmf_tests.helpers.native_chat_events import event, inbound


def _peer(*observations):
    """Provide a closed event stream with deterministic native observations."""
    reader = asyncio.StreamReader()
    for observation in observations:
        reader.feed_data((json.dumps(observation) + "\n").encode())
    reader.feed_eof()
    return SimpleNamespace(stdout=reader)


async def test_announce_wait_ignores_prior_endpoint_generation():
    """The same address cannot make a pre-shutdown announce prove readiness."""
    peer = _peer(
        {"event": "announce", "address": "same-contact", "display_name": "before"},
        {"event": "announce", "address": "other-contact", "display_name": "after"},
        {"event": "announce", "address": "same-contact", "display_name": "after"},
    )
    observed = await event(
        peer, "announce", address="same-contact", display_name="after"
    )
    assert observed["display_name"] == "after"


async def test_only_stale_announce_cannot_satisfy_restore_wait():
    """Queued historical facts fail instead of substituting for a fresh endpoint."""
    peer = _peer(
        {"event": "announce", "address": "same-contact", "display_name": "before"}
    )
    with pytest.raises(AssertionError, match="exited before its expected event"):
        await event(peer, "announce", address="same-contact", display_name="after")


async def test_announce_wait_does_not_discard_unexpected_delivery():
    """Readiness filtering cannot silently discard a duplicate delivered payload."""
    with pytest.raises(AssertionError, match="unsolicited native peer delivery"):
        await event(_peer({"event": "received"}), "announce", display_name="after")


async def test_inbound_deadline_records_native_state_without_resending():
    """A timeout reports native attempts and link state using one read-only command."""
    reader = asyncio.StreamReader()
    commands = []

    class Writer:
        def write(self, data):
            request = json.loads(data)
            commands.append(request["command"])
            reader.feed_data(
                (
                    json.dumps(
                        {
                            "event": "status",
                            "token": request["token"],
                            "message_state": 3,
                            "delivery_attempts": 1,
                            "direct_link_status": 2,
                        }
                    )
                    + "\n"
                ).encode()
            )

        async def drain(self):
            pass

    peer = SimpleNamespace(stdin=Writer(), stdout=reader)
    session = SimpleNamespace(
        diagnostics=lambda: SimpleNamespace(
            connected=True, router_running=True, pending_delivery_count=0
        )
    )
    with pytest.raises(
        AssertionError, match="native inbound deadline exceeded"
    ) as failure:
        await inbound(asyncio.Queue(), peer, session, timeout=0)
    assert commands == ["status"]
    assert '"delivery_attempts": 1' in str(failure.value)
    assert '"direct_link_status": 2' in str(failure.value)
    assert '"connected": true' in str(failure.value)


async def test_inbound_success_does_not_probe_native_state():
    """Successful receipt returns directly without any diagnostic command."""
    queue = asyncio.Queue()
    queue.put_nowait({"content": "expected"})
    assert await inbound(queue, None, None) == {"content": "expected"}


async def test_unavailable_peer_diagnostics_preserve_delivery_failure():
    """A dead peer cannot replace the failed receipt with a broken-pipe error."""

    class Writer:
        def write(self, _data):
            raise BrokenPipeError

    peer = SimpleNamespace(stdin=Writer())
    session = SimpleNamespace(
        diagnostics=lambda: SimpleNamespace(
            connected=False, router_running=False, pending_delivery_count=0
        )
    )
    with pytest.raises(
        AssertionError, match="native inbound deadline exceeded"
    ) as failure:
        await inbound(asyncio.Queue(), peer, session, timeout=0)
    assert "status_unavailable" in str(failure.value)
    assert '"connected": false' in str(failure.value)
