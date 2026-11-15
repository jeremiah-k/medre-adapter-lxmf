"""Recovery waits must be bounded by native SDK state, not elapsed luck."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from medre_lxmf_tests.helpers import native_chat_events, native_recovery


class _Session:
    def __init__(self, *, connected=True, router_running=True):
        self._connected = connected
        self._router_running = router_running

    def diagnostics(self):
        return SimpleNamespace(
            connected=self._connected,
            router_running=self._router_running,
            pending_delivery_count=0,
        )


def _status(**overrides):
    status = {
        "event": "status",
        "attached": True,
        "path_known": True,
        "message_state_name": "outbound",
        "message_terminal": False,
        "delivery_attempts": 1,
        "max_delivery_attempts": 5,
        "direct_link_active": False,
    }
    status.update(overrides)
    return status


def _patch_peer(monkeypatch, status):
    async def command(*args, **kwargs):
        return None

    async def event(*args, **kwargs):
        return status

    monkeypatch.setattr(native_chat_events, "command", command)
    monkeypatch.setattr(native_chat_events, "event", event)


async def test_recovery_accepts_the_exact_late_submission(monkeypatch):
    _patch_peer(monkeypatch, _status())
    queue = asyncio.Queue()

    async def command(*args, **kwargs):
        # This command is only sent after the healthy deadline expired.
        asyncio.get_running_loop().call_soon(queue.put_nowait, {"content": "exact"})

    monkeypatch.setattr(native_chat_events, "command", command)

    observed = await native_chat_events.inbound(
        queue,
        object(),
        _Session(),
        timeout=0,
        recovery_seconds=1,
        expected_content="exact",
    )

    assert observed == {"content": "exact"}


async def test_recovery_accepts_delivery_that_lands_during_diagnosis(monkeypatch):
    queue = asyncio.Queue()

    async def command(*args, **kwargs):
        queue.put_nowait({"content": "exact"})

    async def event(*args, **kwargs):
        return _status()

    monkeypatch.setattr(native_chat_events, "command", command)
    monkeypatch.setattr(native_chat_events, "event", event)

    observed = await native_chat_events.inbound(
        queue,
        object(),
        _Session(),
        timeout=0.01,
        recovery_seconds=1,
        expected_content="exact",
    )

    assert observed == {"content": "exact"}


async def test_recovery_fails_immediately_on_terminal_native_state(monkeypatch):
    _patch_peer(
        monkeypatch,
        _status(
            message_state_name="failed",
            message_terminal=True,
            delivery_attempts=5,
        ),
    )
    with pytest.raises(AssertionError, match="stage=native_delivery_terminal"):
        await native_chat_events.inbound(
            asyncio.Queue(),
            object(),
            _Session(),
            timeout=0.01,
            recovery_seconds=5,
            expected_content="exact",
        )


async def test_recovery_fails_on_exhausted_native_attempts(monkeypatch):
    _patch_peer(monkeypatch, _status(delivery_attempts=5))

    with pytest.raises(
        AssertionError, match="stage=native_delivery_attempts_exhausted"
    ):
        await native_chat_events.inbound(
            asyncio.Queue(),
            object(),
            _Session(),
            timeout=0.01,
            recovery_seconds=5,
            expected_content="exact",
        )


async def test_recovery_does_not_accept_a_different_message(monkeypatch):
    _patch_peer(monkeypatch, _status())
    queue = asyncio.Queue()
    queue.put_nowait({"content": "different"})

    with pytest.raises(AssertionError, match="content mismatch"):
        await native_chat_events.inbound(
            queue,
            object(),
            _Session(),
            timeout=0.01,
            expected_content="exact",
        )


def test_recovery_budget_covers_the_stale_link_cycle():
    pytest.importorskip("RNS")
    pytest.importorskip("LXMF")
    from LXMF.LXMRouter import LXMRouter

    establishment = native_recovery.establishment_seconds()
    extension = native_recovery.recovery_extension_seconds()

    # One stale-link retry wait plus one replacement establishment must fit
    # beyond the ordinary healthy deadline. A hard-coded 20-second budget
    # leaves no extension and fails this regression.
    assert extension >= LXMRouter.DELIVERY_RETRY_WAIT + establishment
    # The hosted failure delivered the same submission at 34.77 seconds.
    assert native_recovery.recovery_seconds() > 34.767872808
    assert native_recovery.recovery_seconds() <= 120
