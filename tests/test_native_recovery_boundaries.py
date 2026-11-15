"""Recovery requires current facts and cannot hide failed protocol boundaries."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from medre_lxmf_tests.helpers import native_chat_events as events


def status(**changes):
    result = dict(
        event="status",
        attached=True,
        path_known=True,
        message_state_name="outbound",
        message_terminal=False,
        delivery_attempts=1,
        max_delivery_attempts=5,
    )
    result.update(changes)
    return result


class Session:
    def diagnostics(self):
        return SimpleNamespace(
            connected=True, router_running=True, pending_delivery_count=0
        )


@pytest.mark.parametrize(
    "field",
    [
        "attached",
        "path_known",
        "message_state_name",
        "message_terminal",
        "delivery_attempts",
        "max_delivery_attempts",
    ],
)
def test_incomplete_native_status_never_grants_recovery(field):
    peer = status()
    peer.pop(field)
    assert not events.recovery_is_available(
        peer, dict(connected=True, router_running=True)
    )


@pytest.mark.parametrize(
    "changes",
    [
        dict(delivery_attempts=True),
        dict(delivery_attempts=-1),
        dict(max_delivery_attempts=None),
        dict(max_delivery_attempts=True),
        dict(max_delivery_attempts=0),
        dict(message_state_name="unknown"),
        dict(message_terminal=None),
        dict(attached=None),
    ],
)
def test_invalid_native_status_never_grants_recovery(changes):
    assert not events.recovery_is_available(
        status(**changes), dict(connected=True, router_running=True)
    )


@pytest.mark.parametrize("field", ["connected", "router_running"])
def test_worker_liveness_requires_positive_evidence(field):
    worker = dict(connected=True, router_running=True)
    worker.pop(field)
    assert not events.recovery_is_available(status(), worker)


@pytest.mark.parametrize("link", ["backchannel_link_active", "direct_link_active"])
def test_observed_active_link_does_not_require_a_cached_path(link):
    assert events.recovery_is_available(
        status(path_known=False, **{link: True}),
        dict(connected=True, router_running=True),
    )


async def test_unsolicited_delivery_cannot_be_hidden_by_matching_inbound(monkeypatch):
    queue = asyncio.Queue()
    stream = asyncio.StreamReader()
    stream.feed_data(
        (json.dumps(dict(event="received", content="unsolicited")) + "\n").encode()
    )
    peer = SimpleNamespace(stdout=stream)

    async def command(*args, **kwargs):
        queue.put_nowait(dict(content="exact"))

    monkeypatch.setattr(events, "command", command)
    with pytest.raises(AssertionError, match="unsolicited native peer delivery"):
        await events.inbound(
            queue,
            peer,
            Session(),
            timeout=0,
            recovery_seconds=1,
            expected_content="exact",
        )


async def test_peer_eof_remains_an_unavailable_observation(monkeypatch):
    stream = asyncio.StreamReader()
    stream.feed_eof()

    async def command(*args, **kwargs):
        pass

    monkeypatch.setattr(events, "command", command)
    with pytest.raises(AssertionError, match="stage=peer_observation_unavailable"):
        await events.inbound(
            asyncio.Queue(),
            SimpleNamespace(stdout=stream),
            Session(),
            timeout=0,
            recovery_seconds=1,
            expected_content="exact",
        )


async def test_recovery_requires_exact_content_identity():
    with pytest.raises(ValueError, match="exact expected content"):
        await events.inbound(
            asyncio.Queue(), object(), Session(), timeout=0, recovery_seconds=1
        )


async def test_delivery_after_final_deadline_diagnostic_cannot_pass(monkeypatch):
    queue = asyncio.Queue()
    clock = SimpleNamespace(now=0.0, waits=0, diagnoses=0)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        clock.waits += 1
        if clock.waits == 2:
            clock.now = 2.0
        raise TimeoutError

    async def diagnose(*args):
        clock.diagnoses += 1
        if clock.diagnoses == 2:
            queue.put_nowait(dict(content="exact"))
        return status(), dict(connected=True, router_running=True)

    monkeypatch.setattr(events, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    with pytest.raises(AssertionError, match="deadline exceeded"):
        await events.inbound(
            queue,
            object(),
            Session(),
            timeout=0,
            recovery_seconds=2,
            expected_content="exact",
        )
    assert clock.diagnoses == 2


async def test_diagnostic_time_counts_against_receive_ceiling(monkeypatch):
    queue = asyncio.Queue()
    clock = SimpleNamespace(now=0.0)

    async def diagnose(*args):
        clock.now = 3.0
        queue.put_nowait(dict(content="exact"))
        return status(), dict(connected=True, router_running=True)

    monkeypatch.setattr(events, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    with pytest.raises(AssertionError, match="deadline exceeded"):
        await events.inbound(
            queue,
            object(),
            Session(),
            timeout=0,
            recovery_seconds=2,
            expected_content="exact",
        )


async def test_terminal_transition_is_observed_before_later_delivery(monkeypatch):
    clock = SimpleNamespace(now=0.0, waits=0)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        clock.waits += 1
        if clock.waits == 1:
            raise TimeoutError
        # The sender fails at 0.5s; unrelated/late reception follows at 3s.
        if timeout >= 3:
            clock.now = 3
            return dict(content="exact")
        clock.now += timeout
        raise TimeoutError

    async def diagnose(*args):
        peer = (
            status()
            if clock.now < 0.5
            else status(message_state_name="failed", message_terminal=True)
        )
        return peer, dict(connected=True, router_running=True)

    monkeypatch.setattr(events, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    with pytest.raises(AssertionError, match="stage=native_delivery_terminal"):
        await events.inbound(
            asyncio.Queue(),
            object(),
            Session(),
            timeout=0,
            recovery_seconds=5,
            expected_content="exact",
        )
    assert clock.now < 3


async def test_delivered_sender_during_recovery_still_needs_recipient_handoff(
    monkeypatch,
):
    clock = SimpleNamespace(now=0.0, waits=0)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        clock.waits += 1
        if clock.waits == 1:
            raise TimeoutError
        if clock.now + timeout >= 1.5:
            clock.now = 1.5
            return dict(content="exact")
        clock.now += timeout
        raise TimeoutError

    async def diagnose(*args):
        peer = (
            status()
            if clock.now < 1
            else status(message_state_name="delivered", message_terminal=True)
        )
        return peer, dict(connected=True, router_running=True)

    monkeypatch.setattr(events, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    assert await events.inbound(
        asyncio.Queue(),
        object(),
        Session(),
        timeout=0,
        recovery_seconds=5,
        expected_content="exact",
    ) == dict(content="exact")


def test_already_delivered_sender_cannot_grant_a_recovery_window():
    assert not events.recovery_is_available(
        status(message_state_name="delivered", message_terminal=True),
        dict(connected=True, router_running=True),
    )


@pytest.mark.parametrize("phase_fraction", [0, 0.0625, 0.25, 0.75, 0.999999])
def test_recovery_budget_covers_tick_phases_and_nearly_full_second_handshake(
    phase_fraction,
):
    import math

    pytest.importorskip("LXMF")
    pytest.importorskip("RNS")
    from LXMF.LXMRouter import LXMRouter
    from medre_lxmf_tests.helpers import native_recovery

    tick = LXMRouter.PROCESSING_INTERVAL * LXMRouter.JOB_OUTBOUND_INTERVAL
    phase = phase_fraction * tick

    def next_tick(after):
        # attempt_due uses >, and the watchdog may close just after a tick.
        return phase + (math.floor((after - phase) / tick) + 1) * tick

    cleared = phase
    first_start = next_tick(cleared + LXMRouter.DELIVERY_RETRY_WAIT)
    timed_out = math.nextafter(
        first_start + native_recovery.establishment_seconds(), math.inf
    )
    failed_clear = next_tick(timed_out)
    second_start = next_tick(failed_clear + LXMRouter.DELIVERY_RETRY_WAIT)
    second_active = second_start + 0.99 * native_recovery.establishment_seconds()
    recipient_handoff = next_tick(second_active)
    assert recipient_handoff <= native_recovery.recovery_seconds()


@pytest.mark.parametrize("during_recovery", [False, True])
@pytest.mark.parametrize(
    "change",
    [
        {"message_state_name": "failed", "message_terminal": True},
        {"delivery_attempts": 5},
        {"attached": False},
        {"event": "status_unavailable"},
    ],
)
async def test_diagnosis_time_callback_cannot_override_denied_status(
    monkeypatch, during_recovery, change
):
    queue = asyncio.Queue()
    diagnoses = 0

    async def diagnose(*args):
        nonlocal diagnoses
        diagnoses += 1
        if diagnoses == (2 if during_recovery else 1):
            queue.put_nowait(dict(content="exact"))
            return status(**change), dict(connected=True, router_running=True)
        return status(), dict(connected=True, router_running=True)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    with pytest.raises(AssertionError, match="deadline exceeded"):
        await events.inbound(
            queue,
            object(),
            Session(),
            timeout=0,
            recovery_seconds=2,
            expected_content="exact",
        )


@pytest.mark.parametrize("during_recovery", [False, True])
async def test_diagnosis_time_delivered_callback_requires_admitted_window(
    monkeypatch, during_recovery
):
    queue = asyncio.Queue()
    diagnoses = 0

    async def diagnose(*args):
        nonlocal diagnoses
        diagnoses += 1
        if diagnoses == (2 if during_recovery else 1):
            queue.put_nowait(dict(content="exact"))
            return status(message_state_name="delivered", message_terminal=True), dict(
                connected=True, router_running=True
            )
        return status(), dict(connected=True, router_running=True)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    call = events.inbound(
        queue,
        object(),
        Session(),
        timeout=0,
        recovery_seconds=2,
        expected_content="exact",
    )
    if during_recovery:
        assert await call == dict(content="exact")
    else:
        with pytest.raises(AssertionError, match="concluded_without_handoff"):
            await call


@pytest.mark.parametrize("during_recovery", [False, True])
@pytest.mark.parametrize("queued", [False, True])
async def test_successful_or_queued_read_cannot_cross_receive_ceiling(
    monkeypatch, during_recovery, queued
):
    queue = asyncio.Queue()
    clock = SimpleNamespace(now=0.0, waits=0)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        clock.waits += 1
        if during_recovery and clock.waits == 1:
            clock.now = 1
            raise TimeoutError
        clock.now = 3
        if queued:
            queue.put_nowait(dict(content="exact"))
            raise TimeoutError
        return dict(content="exact")

    async def diagnose(*args):
        return status(), dict(connected=True, router_running=True)

    monkeypatch.setattr(events, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    with pytest.raises(AssertionError, match="deadline exceeded"):
        await events.inbound(
            queue,
            object(),
            Session(),
            timeout=1,
            recovery_seconds=1,
            expected_content="exact",
        )


async def test_late_healthy_read_requires_recovery_admission(monkeypatch):
    clock = SimpleNamespace(now=0.0)

    async def wait_for(awaitable, timeout):
        awaitable.close()
        clock.now = 1.5
        return dict(content="exact")

    async def diagnose(*args):
        return status(message_state_name="failed", message_terminal=True), dict(
            connected=True, router_running=True
        )

    monkeypatch.setattr(events, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(events, "asyncio", SimpleNamespace(wait_for=wait_for))
    monkeypatch.setattr(events, "_diagnose", diagnose)
    with pytest.raises(AssertionError, match="stage=native_delivery_terminal"):
        await events.inbound(
            asyncio.Queue(),
            object(),
            Session(),
            timeout=1,
            recovery_seconds=2,
            expected_content="exact",
        )
