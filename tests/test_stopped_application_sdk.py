"""Real LXMF endpoint restore and chat exchange on a running shared Reticulum."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
import uuid
from contextlib import asynccontextmanager, closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.lxmf_sdk


async def _event(process, kind, *, address=None):
    """Wait for a concrete peer event rather than sleeping for native timing."""
    async with asyncio.timeout(20):
        while True:
            line = await process.stdout.readline()
            if not line:
                raise AssertionError("native peer exited before its expected event")
            event = json.loads(line)
            if event["event"] == "received" and kind != "received":
                raise AssertionError("unsolicited native peer delivery")
            if event["event"] == kind and (
                address is None or event.get("address") == address
            ):
                return event


async def _command(process, **command):
    """Send only local qualification commands to the independent native peer."""
    process.stdin.write((json.dumps(command) + "\n").encode())
    await process.stdin.drain()


@asynccontextmanager
async def _native(role, config, storage):
    """Reap the native service or peer and retain no subprocess transports."""
    helper = Path(__file__).parent / "medre_lxmf_tests/helpers/native_chat_peer.py"
    with tempfile.TemporaryFile() as errors:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(helper),
            role,
            str(config),
            str(storage),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=errors,
        )
        try:
            ready = await _event(process, "ready")
            yield process, ready
        finally:
            if process.returncode is None:
                try:
                    await _command(process, command="stop")
                    await asyncio.wait_for(process.communicate(), 15)
                except (TimeoutError, BrokenPipeError, ConnectionResetError):
                    if process.returncode is None:
                        process.kill()
                    await process.communicate()
            else:
                await process.communicate()
            errors.seek(0)
            assert process.returncode == 0, errors.read().decode(errors="replace")


async def test_shared_reticulum_chat_and_endpoint_restore(tmp_path, monkeypatch):
    """Restore the same LXMF contact while another chat peer and service stay live."""
    if sys.platform != "linux":
        pytest.skip("shared-service campaign is qualified on Linux")
    pytest.importorskip("RNS")
    pytest.importorskip("LXMF")

    from medre.adapters.lxmf.plugin import adapter_spec
    from medre.adapters.lxmf.worker_session import WorkerSession
    from medre.adapters.lxmf.worker_state import SubmissionRecord, WorkerProfile
    from medre.config.adapters.lxmf import LxmfConfig
    from medre.maintenance import stopped_declare as maintenance
    from medre.maintenance.stopped_snapshot import (
        restore_stopped_snapshot,
        verify_stopped_snapshot,
    )

    network = tmp_path / "independent-reticulum"
    network.mkdir()
    (network / "config").write_text(
        "[reticulum]\nshare_instance = Yes\nenable_transport = No\n"
        f"instance_name = medre-capture-{uuid.uuid4().hex}\n"
        "[logging]\nloglevel = 0\n[interfaces]\n"
    )
    root = tmp_path / "medre-state"
    config = LxmfConfig(
        "chat-bridge",
        connection_type="reticulum",
        storage_path=str(root / "endpoint"),
        reticulum_config_dir=str(network),
        stamp_cost=0,
        message_delay_seconds=0,
        announce_interval_seconds=0.2,
    )
    async with _native("service", network, tmp_path / "service") as (
        service,
        service_ready,
    ):
        assert service_ready["shared"]
        async with _native("peer", network, tmp_path / "chat-app") as (
            peer,
            peer_ready,
        ):
            assert peer_ready["attached"]
            inbound = asyncio.Queue()
            session = WorkerSession(config)
            try:
                await session.start(inbound.put_nowait)
                address = session._delivery_destination_hash.hex()
                await _event(peer, "announce", address=address)
                await _command(peer, command="announce")
                await _event(peer, "announced")
                await _command(
                    peer,
                    command="send",
                    destination=address,
                    content="Hello from an LXMF chat ☃",
                )
                assert (await asyncio.wait_for(inbound.get(), 20))[
                    "content"
                ] == "Hello from an LXMF chat ☃"
                await session.send_text(
                    peer_ready["address"], "[Alice/mesh] Reply through MEDRE ☃"
                )
                assert (await _event(peer, "received"))[
                    "content"
                ] == "[Alice/mesh] Reply through MEDRE ☃"
            finally:
                await session.stop()

            profile = WorkerProfile(config)
            try:
                profile.begin("accepted-sentinel", b"accepted")
                profile.finish(
                    "accepted-sentinel",
                    SubmissionRecord("accepted", "native-id", "outbound", None),
                )
                profile.begin("uncertain-sentinel", b"uncertain")
            finally:
                profile.close()
            core = root / "core.sqlite"
            with closing(sqlite3.connect(core)) as database, database:
                database.execute("CREATE TABLE retained(value TEXT)")
            monkeypatch.setattr(
                maintenance,
                "_prepare_instances",
                lambda *_: iter(
                    [("lxmf", config.adapter_id, "real", adapter_spec(), config)]
                ),
            )
            snapshot = tmp_path / "snapshot"
            plan = maintenance.capture_configured_stopped_state(
                SimpleNamespace(storage=SimpleNamespace(path=str(core))),
                SimpleNamespace(),
                common_root=root,
                snapshot=snapshot,
                runtime_stopped=True,
            )
            assert service.returncode is None and peer.returncode is None
            assert all(
                item.path.startswith("endpoint/") or item.path == "core.sqlite"
                for item in plan.entries
            )
            assert any(item.path.endswith(".ratchets") for item in plan.entries)
            verify_stopped_snapshot(snapshot)
            restored = tmp_path / "restored"
            restore_stopped_snapshot(snapshot, restored, runtime_stopped=True)
            assert (root / "endpoint/.medre-identity").read_bytes() == (
                restored / "endpoint/.medre-identity"
            ).read_bytes()
            resumed_config = replace(config, storage_path=str(restored / "endpoint"))
            held = WorkerProfile(resumed_config)
            try:
                assert held.begin("accepted-sentinel", b"accepted").status == "accepted"
                assert (
                    held.begin("uncertain-sentinel", b"uncertain").status == "unknown"
                )
            finally:
                held.close()
            resumed = WorkerSession(resumed_config)
            resumed_inbound = asyncio.Queue()
            try:
                await resumed.start(resumed_inbound.put_nowait)
                assert resumed._delivery_destination_hash.hex() == address
                await _event(peer, "announce", address=address)
                await _command(peer, command="announce")
                await _event(peer, "announced")
                await _command(
                    peer,
                    command="send",
                    destination=address,
                    content="The same contact after restore",
                )
                assert (await asyncio.wait_for(resumed_inbound.get(), 20))[
                    "content"
                ] == "The same contact after restore"
                await resumed.send_text(
                    peer_ready["address"], "A fresh reply after restore"
                )
                assert (await _event(peer, "received"))[
                    "content"
                ] == "A fresh reply after restore"
            finally:
                await resumed.stop()
