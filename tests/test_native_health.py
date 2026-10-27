"""Owned worker failures publish local liveness without peers, sends or health polling."""

import asyncio
import os
import sys

import pytest
from dataclasses import replace

from medre.adapters.lxmf.adapter import LxmfAdapter
from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.worker_protocol import Status
from medre.adapters.lxmf.worker_session import WorkerSession
from medre.config.adapters.lxmf import LxmfConfig
from medre_lxmf_tests.helpers.async_utils import wait_until


async def start_adapter(tmp_path, make_adapter_context, reports):
    """Run production worker framing and OS pipes with the SDK-free native session."""
    config = LxmfConfig(
        "health-worker", storage_path=str(tmp_path), message_delay_seconds=0
    )
    adapter = LxmfAdapter(config)
    adapter._session = WorkerSession(config)
    ctx = make_adapter_context("health-worker")
    ctx.report_health = reports.append
    await adapter.start(ctx)
    return adapter, ctx


async def test_worker_exit_reports_even_when_event_queues_are_full(
    tmp_path, make_adapter_context
):
    """Passive EOF reaches health directly instead of depending on a queue sentinel."""
    reports = []
    adapter, _ = await start_adapter(tmp_path, make_adapter_context, reports)
    session = adapter._session
    worker = session._worker
    try:
        assert reports[-1].health == "healthy"
        for task in session._consumers:
            task.cancel()
        await asyncio.gather(*session._consumers, return_exceptions=True)
        for queue in (worker.channel.inbound, worker.channel.updates):
            while not queue.full():
                queue.put_nowait(None)
        worker._process.kill()
        assert await wait_until(lambda: reports[-1].health == "failed")
        assert adapter.diagnostics()["health_scope"] == "local_session_and_router"
        assert adapter.diagnostics()["peer_reachability"] == "unknown"
        assert not worker.channel._pending
    finally:
        await adapter.stop()


async def test_observer_failure_cannot_prevent_worker_reaping(
    tmp_path, make_adapter_context
):
    """The existing failure cleanup is scheduled before the optional observer runs."""
    reports = []
    adapter, ctx = await start_adapter(tmp_path, make_adapter_context, reports)
    worker = adapter._session._worker

    def refused(info):
        """Model a failing diagnostic consumer after native startup."""
        raise RuntimeError("observer unavailable")

    ctx.report_health = refused
    try:
        worker._process.kill()
        assert await wait_until(lambda: worker._failure_cleanup is not None)
        await asyncio.wait_for(worker._failure_cleanup, 5)
        assert worker.returncode is not None
        assert (await adapter.health_check()).health == "failed"
    finally:
        await adapter.stop()


async def test_explicit_status_timeout_and_recovery_publish_without_recursive_probe(
    tmp_path, make_adapter_context
):
    """Existing local status requests can update health; observers issue no extra RPCs."""
    reports = []
    adapter, _ = await start_adapter(tmp_path, make_adapter_context, reports)
    channel = adapter._session._worker.channel
    original = channel.exchange
    calls = 0

    async def exchange(request, *, timeout):
        """Time out one explicit local request while preserving a live worker."""
        nonlocal calls
        calls += 1
        if calls == 1:
            raise LxmfConnectionError("local status unavailable")
        return await original(request, timeout=timeout)

    channel.exchange = exchange
    try:
        assert (await adapter.health_check()).health == "degraded"
        assert reports[-1].health == "degraded"
        assert (await adapter.health_check()).health == "healthy"
        assert reports[-1].health == "healthy"
        assert calls == 2
    finally:
        await adapter.stop()


async def test_retired_worker_and_status_reply_cannot_publish_replacement_state(
    tmp_path, make_adapter_context
):
    """Both the worker callback and a delayed status response retain their generation."""
    reports = []
    adapter, ctx = await start_adapter(tmp_path, make_adapter_context, reports)
    session = adapter._session
    old = session._worker
    entered, release = asyncio.Event(), asyncio.Event()
    stale = replace(session._diag, router_running=False)

    async def delayed(request, *, timeout):
        """Keep a status result in flight across the owned stop/start boundary."""
        entered.set()
        await release.wait()
        return Status(request.request_id, stale)

    old.channel.exchange = delayed
    probe = asyncio.create_task(session.refresh())
    try:
        await entered.wait()
        await adapter.stop()
        await adapter.start(ctx)
        count = len(reports)
        old._on_failure()
        release.set()
        await probe
        assert len(reports) == count
        assert reports[-1].health == "healthy"
        assert adapter._session.router_running
    finally:
        release.set()
        await asyncio.gather(probe, return_exceptions=True)
        await adapter.stop()


@pytest.mark.skipif(
    os.name != "posix", reason="POSIX inherited-pipe process-group scenario"
)
async def test_leader_exit_reports_failed_while_descendant_retains_stdout(
    tmp_path, make_adapter_context
):
    """A worker leader's exit is observable before inherited stdout reaches EOF."""
    child = """
import os, subprocess, sys
protocol_stdout = os.dup(1)
from medre.adapters.lxmf.session import LxmfSession
original = LxmfSession.start
holder = None
async def start(self, *args, **kwargs):
    global holder
    await original(self, *args, **kwargs)
    holder = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
        stdin=subprocess.DEVNULL, stdout=protocol_stdout, stderr=subprocess.DEVNULL)
LxmfSession.start = start
from medre.adapters.lxmf.native_worker import main
main()
"""
    reports = []
    config = LxmfConfig(
        "exit-health", storage_path=str(tmp_path), message_delay_seconds=0
    )
    adapter = LxmfAdapter(config)
    adapter._session = WorkerSession(config, _command=(sys.executable, "-c", child))
    ctx = make_adapter_context("exit-health")
    ctx.report_health = reports.append
    await adapter.start(ctx)
    worker = adapter._session._worker
    try:
        assert reports[-1].health == "healthy"
        worker._process.kill()
        assert await wait_until(lambda: worker.returncode is not None)
        assert await wait_until(lambda: reports[-1].health == "failed")
        assert worker._failure_cleanup is not None
        await asyncio.wait_for(worker._failure_cleanup, 5)
        assert worker._failure_cleanup.done()
    finally:
        await adapter.stop()
