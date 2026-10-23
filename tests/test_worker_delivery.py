"""Real private pipes exercise submission, inbound admission and exact-attempt facts."""

import asyncio
import sys
from unittest.mock import AsyncMock, patch

import pytest

from medre.adapters.lxmf.adapter import LxmfAdapter
from medre.adapters.lxmf.errors import LxmfHandoffUnknownError, LxmfSendError
from medre.adapters.lxmf.session import LxmfDeliveryState
from medre.adapters.lxmf.session_context import LxmfDeliveryContext
from medre.adapters.lxmf.worker_session import WorkerSession
from medre.config.adapters.lxmf import LxmfConfig
from medre_lxmf_tests.helpers.async_utils import wait_until
from test_lxmf_delivery_observations import _context, _rendering_result

# Execute the production worker loop with its SDK session replaced by a fake.
# Submission, journal, protocol, channel and parent callbacks remain real.
_CHILD = """
import asyncio, os, sys
from medre.adapters.lxmf.session import LxmfSession
original = LxmfSession.send_text
mode = sys.argv[1]
async def send(self, destination_hash, content, **kwargs):
    if mode == 'crash':
        os._exit(7)
    if mode == 'cancel':
        sys.stderr.write('native-entered\\n'); sys.stderr.flush()
        await asyncio.sleep(.1)
    native_id, state = await original(self, destination_hash, content, **kwargs)
    if mode != 'quiet':
        self._delivery_state_callback(native_id, 'delivered', kwargs['delivery_context'])
        self._message_callback({
            'source_hash': destination_hash, 'destination_hash': 'bb' * 16,
            'message_id': native_id, 'content': content, 'title': '',
            'timestamp': __import__('time').time(), 'fields': kwargs.get('fields') or {},
            'signature_validated': True, 'source_name': 'native peer',
        })
    if mode == 'overflow':
        for _ in range(70):
            self._message_callback({'content': content})
    return native_id, state
LxmfSession.send_text = send
from medre.adapters.lxmf.native_worker import main
main()
"""


def _session(mode="echo", storage_path=None):
    """Use native OS pipes and production framing while isolating radio prerequisites."""
    return WorkerSession(
        LxmfConfig(
            "lxmf-observation", message_delay_seconds=0, storage_path=storage_path
        ),
        _command=(sys.executable, "-c", _CHILD, mode),
    )


async def test_inbound_and_early_feedback_keep_native_fields_and_exact_parent_attempt(
    tmp_path,
):
    """A terminal fact before the handoff reply retains the immutable parent object."""
    inbound, feedback = AsyncMock(), AsyncMock()
    session = _session(storage_path=str(tmp_path))
    session.set_delivery_state_callback(feedback)
    await session.start(inbound)
    result = _rendering_result(
        event_id="event", channel="aa" * 16, plan_id="plan", outbox_id="outbox"
    )
    context = LxmfDeliveryContext(result.attempt_provenance, result.target_channel)
    try:
        native_id, state = await session.send_text(
            "aa" * 16, "hello", fields={42: b"bytes"}, delivery_context=context
        )
        assert native_id and state == LxmfDeliveryState.OUTBOUND
        assert await wait_until(
            lambda: feedback.await_count == 1 and inbound.await_count == 1, timeout=3
        )
        assert feedback.await_args.args == (native_id, "delivered", context)
        assert feedback.await_args.args[2] is context
        packet = inbound.await_args.args[0]
        assert packet["fields"] == {42: b"bytes"}
        assert packet["source_hash"] == "aa" * 16
        assert session.resolve_display_name("aa" * 16) == "native peer"
        await session.refresh()
        assert session.connected and session.diagnostics().pending_delivery_count == 0
    finally:
        await session.stop()
    assert session._worker.returncode == 0
    assert all(task.done() for task in session._consumers)


async def test_production_adapter_forwards_worker_events_and_feedback_to_core():
    """The real-mode facade routes plain packets and observations through core seams."""
    session = _session()
    adapter = LxmfAdapter(
        LxmfConfig(
            "lxmf-observation", connection_type="reticulum", storage_path="/unused"
        )
    )
    adapter._session = session
    feedback = AsyncMock()
    ctx = _context(feedback)
    with patch("medre.adapters.lxmf.adapter.HAS_LXMF", True):
        await adapter.start(ctx)
    try:
        result = _rendering_result(
            event_id="event", channel="aa" * 16, plan_id="plan", outbox_id="outbox"
        )
        handoff = await adapter.deliver(result)
        assert handoff.native_message_id
        assert await wait_until(
            lambda: feedback.await_count == 1 and ctx.publish_inbound.await_count == 1,
            timeout=3,
        )
        record = feedback.await_args.args[0]
        assert record.attempt_provenance is result.attempt_provenance
        assert record.native_message_id == handoff.native_message_id
        assert record.native_channel_id == result.target_channel
        assert record.state == "delivered"
        event = ctx.publish_inbound.await_args.args[0]
        assert event.payload["body"] == "hello"
        assert event.source_transport_id == "aa" * 16
    finally:
        await adapter.stop()


async def test_slow_inbound_does_not_starve_handoff_or_terminal_feedback():
    """Independent bounded dispatchers keep admission out of the reply reader."""
    entered, release = asyncio.Event(), asyncio.Event()

    async def inbound(packet):
        entered.set()
        await release.wait()

    feedback = AsyncMock()
    session = _session()
    session.set_delivery_state_callback(feedback)
    await session.start(inbound)
    try:
        async with asyncio.timeout(3):
            await session.send_text("aa" * 16, "hello", delivery_context=object())
            await entered.wait()
            assert await wait_until(lambda: feedback.await_count == 1, timeout=1)
        assert not release.is_set()
    finally:
        release.set()
        await session.stop()


async def test_worker_loss_after_submission_is_unknown_and_generation_is_reaped(
    tmp_path,
):
    """A crash cannot manufacture a handoff, negative confirmation or retry."""
    session = _session("crash", str(tmp_path))
    result = _rendering_result(
        event_id="event", channel="aa" * 16, plan_id="plan", outbox_id="outbox"
    )
    context = LxmfDeliveryContext(result.attempt_provenance, result.target_channel)
    callback = AsyncMock()
    session.set_delivery_state_callback(callback)
    await session.start()
    try:
        with pytest.raises(LxmfHandoffUnknownError, match="unknown"):
            await session.send_text("aa" * 16, "hello", delivery_context=context)
        assert await wait_until(
            lambda: session._worker.returncode is not None, timeout=3
        )
        assert not session.connected
        assert callback.await_count == 0
        with pytest.raises(LxmfSendError) as failure:
            await session.send_text("aa" * 16, "another")
        assert failure.value.transient
    finally:
        await session.stop()

    resumed = _session("quiet", str(tmp_path))
    await resumed.start()
    try:
        with pytest.raises(LxmfHandoffUnknownError):
            await resumed.send_text("aa" * 16, "hello", delivery_context=context)
        await resumed.refresh()
        assert resumed._diag.pending_delivery_count == 0
    finally:
        await resumed.stop()


async def test_cancelled_caller_retains_late_reply_and_native_feedback():
    """Cancellation cannot detach the reader or erase already-crossed attempt context."""
    feedback = AsyncMock()
    session = _session("cancel")
    session.set_delivery_state_callback(feedback)
    await session.start()
    context = object()
    sending = asyncio.create_task(
        session.send_text("aa" * 16, "hello", delivery_context=context)
    )
    try:
        assert await wait_until(
            lambda: b"native-entered" in session._worker._stderr_tail, timeout=3
        )
        sending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sending
        assert await wait_until(lambda: feedback.await_count == 1, timeout=3)
        assert feedback.await_args.args[2] is context
        assert session.connected
        await session.refresh()
        assert not session._worker.channel._pending
    finally:
        await session.stop()


async def test_exact_attempt_reuses_committed_handoff_without_resending(tmp_path):
    """A restarted worker consults durable handoff facts for the same core attempt."""
    result = _rendering_result(
        event_id="event", channel="aa" * 16, plan_id="plan", outbox_id="outbox"
    )
    context = LxmfDeliveryContext(result.attempt_provenance, result.target_channel)
    session = _session("quiet", str(tmp_path))
    await session.start()
    try:
        first = await session.send_text("aa" * 16, "hello", delivery_context=context)
    finally:
        await session.stop()
    session = _session("quiet", str(tmp_path))
    await session.start()
    try:
        assert (
            await session.send_text("aa" * 16, "hello", delivery_context=context)
            == first
        )
        await session.refresh()
        assert session._diag.pending_delivery_count == 0
        with pytest.raises(LxmfSendError) as failure:
            await session.send_text("aa" * 16, "different", delivery_context=context)
        assert not failure.value.transient
    finally:
        await session.stop()


async def test_repeated_stop_cancellation_waits_for_owned_cleanup():
    """Caller cancellation cannot leave native readers or callback consumers owned."""
    entered = asyncio.Event()

    async def blocked(packet):
        entered.set()
        await asyncio.Event().wait()

    session = _session()
    await session.start(blocked)
    await session.send_text("aa" * 16, "hello")
    await entered.wait()
    stopping = asyncio.create_task(session.stop(timeout=1))
    assert await wait_until(lambda: session._stopping is not None, timeout=1)
    stopping.cancel()
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert session._worker.returncode is not None
    assert session._stopping.done()
    assert not session._consumers
    assert session._worker.channel._reader.done()


async def test_spawned_worker_does_not_inherit_unrelated_secrets(monkeypatch):
    """The real spawn path applies the environment boundary before Python runs."""
    monkeypatch.setenv("MEDRE_ADAPTER__MATRIX__ACCESS_TOKEN", "must-not-cross")
    monkeypatch.setenv("SIMPLEX_PASSWORD", "must-not-cross")
    session = WorkerSession(
        LxmfConfig("environment"),
        _command=(
            sys.executable,
            "-c",
            "import os; assert 'MEDRE_ADAPTER__MATRIX__ACCESS_TOKEN' not in os.environ; "
            "assert 'SIMPLEX_PASSWORD' not in os.environ; "
            "from medre.adapters.lxmf.native_worker import main; main()",
        ),
    )
    try:
        await session.start()
        assert session.connected
    finally:
        await session.stop()


async def test_inbound_capacity_failure_reaps_generation_without_handoff_success():
    """An overloaded event queue fails closed instead of stranding the RPC reader."""
    blocked = asyncio.Event()

    async def slow_admission(packet):
        await blocked.wait()

    session = _session("overflow")
    await session.start(slow_admission)
    try:
        with pytest.raises(LxmfHandoffUnknownError):
            await session.send_text("aa" * 16, "hello")
        assert await wait_until(
            lambda: session._worker.returncode is not None, timeout=3
        )
        assert not session.connected
        assert session._worker.channel.failure is not None
        assert session._worker.channel.inbound.qsize() <= 64
    finally:
        blocked.set()
        await session.stop()
