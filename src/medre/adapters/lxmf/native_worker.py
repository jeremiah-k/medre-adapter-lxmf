"""Own native initialization, normalization and submission inside a private process."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
from dataclasses import asdict, replace
from typing import Any, BinaryIO, Literal, cast

from medre.adapters.lxmf.errors import LxmfHandoffUnknownError, LxmfSendError
from medre.adapters.lxmf.worker_protocol import (
    DeliveryUpdate,
    Failed,
    GetStatus,
    Inbound,
    Message,
    Ready,
    Start,
    Status,
    Stop,
    Stopped,
    Submit,
    SubmitFailed,
    Submitted,
    WorkerProtocolError,
    _decode,
    encode,
    read_sync,
)
from medre.adapters.lxmf.worker_state import (
    SubmissionRecord,
    WorkerProfile,
    payload_digest,
)
from medre.core.observability.sanitization import sanitize_error


def _write(stream: BinaryIO, message: Message) -> None:
    """Publish a complete frame independently of SDK stdout logging."""
    stream.write(encode(message))
    stream.flush()


async def _run(stream: BinaryIO) -> None:
    """Serve closed native commands while retaining SDK and queue ownership."""
    from medre.adapters.lxmf.session import (
        LxmfDeliveryState,
        LxmfSession,
        LxmfSessionDiagnostics,
    )

    first = await asyncio.to_thread(read_sync, sys.stdin.buffer)
    if not isinstance(first, Start):
        raise RuntimeError("Native worker requires a start frame")
    first.config.validate()
    profile = WorkerProfile(first.config) if first.message_plane else None
    config = profile.config if profile is not None else first.config
    try:
        session = LxmfSession(
            config, config.adapter_id, reticulum_config_dir=first.reticulum_config_dir
        )
    except BaseException:
        if profile is not None:
            profile.close()
        raise
    output: asyncio.Queue[Message] = asyncio.Queue(maxsize=128)
    overflow = asyncio.Event()
    writer: asyncio.Task[None] | None = None

    def enqueue(message: Message) -> None:
        """Snapshot plain values before bounded output admission."""
        # Encoding here snapshots mutable SDK-normalized values before yielding.
        # The writer serializes a detached decoded frame, never an SDK object.
        try:
            frame = encode(message)
            snapshot_message = _decode(frame[4:])
        except WorkerProtocolError:
            if isinstance(message, Inbound):
                # SDK-normalized fields can contain untrusted remote values.
                # Losing that packet must not take down unrelated attempts.
                logging.getLogger(__name__).warning(
                    "Dropped inbound LXMF packet outside the native wire contract"
                )
                return
            overflow.set()
            return
        except Exception:
            overflow.set()
            return
        try:
            output.put_nowait(snapshot_message)
        except asyncio.QueueFull:
            overflow.set()

    def inbound(packet: dict[str, Any]) -> None:
        """Resolve local announce labels before forwarding normalized packets."""
        enriched = dict(packet)
        source = enriched.get("source_hash")
        if not enriched.get("source_name") and isinstance(source, str):
            name = session.resolve_display_name(source)
            if name:
                enriched["source_name"] = name
        enqueue(Inbound(enriched))

    def terminal(native_id: str, state: str, operation_id: object) -> None:
        """Forward only mapped native facts with their opaque operation identity."""
        if isinstance(operation_id, str) and state in {
            "delivered",
            "failed",
            "rejected",
            "cancelled",
        }:
            enqueue(
                DeliveryUpdate(
                    operation_id,
                    native_id,
                    cast(
                        Literal["delivered", "failed", "rejected", "cancelled"], state
                    ),
                )
            )

    def snapshot() -> LxmfSessionDiagnostics:
        """Copy safe native scalars with sanitized error descriptions."""
        diag = session.diagnostics()
        return replace(
            diag,
            last_error=sanitize_error(diag.last_error) if diag.last_error else None,
            last_announce_error=(
                sanitize_error(diag.last_announce_error)
                if diag.last_announce_error
                else None
            ),
        )

    async def write_frames() -> None:
        """Serialize output without blocking native callback dispatch."""
        while True:
            message = await output.get()
            try:
                await asyncio.to_thread(_write, stream, message)
            finally:
                output.task_done()

    async def submit(command: Submit) -> None:
        """Commit exact-attempt fences before acknowledging native acceptance."""
        assert profile is not None
        try:
            digest = payload_digest(
                (
                    command.destination_hash,
                    command.content,
                    command.title,
                    command.delivery_method,
                    command.fields,
                )
            )
            record = profile.begin(command.operation_id, digest)
            if record is None:
                try:
                    native_id, state = await session.send_text(
                        command.destination_hash,
                        command.content,
                        title=command.title,
                        delivery_method=command.delivery_method,
                        fields=command.fields,
                        delivery_context=command.operation_id,
                    )
                    record = SubmissionRecord("accepted", native_id, state.value, None)
                except LxmfHandoffUnknownError:
                    record = SubmissionRecord("unknown", None, "unmapped", None)
                except LxmfSendError as exc:
                    record = SubmissionRecord(
                        "retryable" if exc.transient else "rejected",
                        None,
                        "unmapped",
                        sanitize_error(str(exc)),
                    )
                # If this commit fails after SDK acceptance the dispatch fence
                # remains uncertain. Never send an acceptance that was not saved.
                profile.finish(command.operation_id, record)
            if record.status == "accepted":
                enqueue(
                    Submitted(
                        command.request_id,
                        record.native_id,
                        LxmfDeliveryState(record.state),
                    )
                )
            else:
                category: Literal["transient", "permanent", "unknown"] = (
                    "unknown"
                    if record.status == "unknown"
                    else "transient" if record.status == "retryable" else "permanent"
                )
                error = (
                    record.error
                    or f"LXMF handoff outcome unknown (operation {command.operation_id}); automatic retry suppressed"
                )
                enqueue(SubmitFailed(command.request_id, category, error))
        except LxmfSendError as exc:
            enqueue(
                SubmitFailed(
                    command.request_id,
                    "transient" if exc.transient else "permanent",
                    sanitize_error(str(exc)),
                )
            )
        except Exception:
            # Any storage/control failure may follow a native side effect.
            enqueue(
                SubmitFailed(
                    command.request_id,
                    "unknown",
                    f"LXMF handoff outcome unknown (operation {command.operation_id}); reconcile the native journal",
                )
            )

    read_task: asyncio.Task[Message | None] | None = None
    overflow_task: asyncio.Task[bool] | None = None
    try:
        await session.start(message_callback=inbound if first.message_plane else None)
        if first.message_plane:
            session.set_delivery_state_callback(terminal)
        _write(
            stream,
            Ready(
                os.getpid(),
                session.connected,
                session.router_running,
                session._delivery_destination_hash,
                asdict(snapshot()),
            ),
        )
        writer = asyncio.create_task(write_frames(), name="lxmf-native-writer")
        overflow_task = asyncio.create_task(overflow.wait())
        while True:
            read_task = asyncio.create_task(
                asyncio.to_thread(read_sync, sys.stdin.buffer)
            )
            done, _ = await asyncio.wait(
                {read_task, writer, overflow_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if overflow_task in done or writer in done:
                # Hard termination is owned by the parent. Exiting directly
                # prevents a blocked pipe writer/default executor from hanging.
                os._exit(3)
            command = read_task.result()
            if command is None:
                return
            if isinstance(command, Stop):
                if not 0 <= command.timeout_seconds <= 3600:
                    raise RuntimeError("Native stop deadline is invalid")
                await session.stop(timeout=command.timeout_seconds)
                await output.join()
                _write(stream, Stopped())
                return
            if not first.message_plane:
                raise RuntimeError("Native worker requires a stop frame")
            if isinstance(command, Submit):
                await submit(command)
            elif isinstance(command, GetStatus):
                enqueue(Status(command.request_id, snapshot()))
            else:
                raise RuntimeError("Unexpected native command direction")
    finally:
        for task in (read_task, writer, overflow_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (read_task, writer, overflow_task) if task is not None),
            return_exceptions=True,
        )
        try:
            await session.stop()
        finally:
            if profile is not None:
                profile.close()


def main() -> None:
    """Separate native stdout from protocol output before any SDK import."""
    with os.fdopen(os.dup(sys.stdout.fileno()), "wb") as protocol:
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
