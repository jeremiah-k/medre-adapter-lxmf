"""SDK-free parent session for native LXMF messages and exact-attempt feedback."""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import Any

import msgspec

from medre.adapters.lxmf.errors import (
    LxmfConnectionError,
    LxmfHandoffUnknownError,
    LxmfSendError,
)
from medre.adapters.lxmf.native_process import NativeWorker
from medre.adapters.lxmf.session import (
    DeliveryStateCallback,
    LxmfDeliveryState,
    LxmfSessionDiagnostics,
    MessageCallback,
)
from medre.adapters.lxmf.session_context import LxmfDeliveryContext
from medre.adapters.lxmf.worker_protocol import (
    GetStatus,
    Status,
    Submit,
    SubmitFailed,
    Submitted,
    WorkerProtocolError,
)
from medre.adapters.lxmf.worker_state import payload_digest
from medre.config.adapters.lxmf import LxmfConfig


@dataclass
class _ObservationContext:
    """Retain the exact parent object while its native result remains pending."""

    context: object | None
    digest: bytes
    native_id: str | None = None


class WorkerSession:
    """Expose normalized session operations without importing or exposing SDK state."""

    def __init__(
        self,
        config: LxmfConfig,
        *,
        reticulum_config_dir: str | None = None,
        _command: tuple[str, ...] | None = None,
    ) -> None:
        self._config = config
        self._reticulum_config_dir = reticulum_config_dir
        self._command = _command
        self._worker: NativeWorker | None = None
        self._started = False
        self._closing = False
        self._message_callback: MessageCallback | None = None
        self._delivery_callback: DeliveryStateCallback | None = None
        self._contexts: dict[str, _ObservationContext] = {}
        self._names: OrderedDict[str, str] = OrderedDict()
        self._consumers: list[asyncio.Task[None]] = []
        self._stopping: asyncio.Task[None] | None = None
        self._diag: LxmfSessionDiagnostics | None = None
        self._delivery_destination_hash: bytes | None = None
        self._last_error: str | None = None
        self._status_delayed = False
        self._logger = logging.getLogger(f"medre.adapters.{config.adapter_id}")

    @property
    def connected(self) -> bool:
        """Describe live local worker state; remote reachability remains unknown."""
        worker = self._worker
        return bool(
            self._started
            and not self._closing
            and worker
            and worker.channel
            and worker.channel.connected
            and self._diag
            and self._diag.connected
        )

    @property
    def router_running(self) -> bool:
        """Require the latest SDK router fact and a live parent channel."""
        return bool(self.connected and self._diag and self._diag.router_running)

    @property
    def status_delayed(self) -> bool:
        """Report a bounded status-probe timeout without killing active sends."""
        return self._status_delayed

    async def start(self, message_callback: MessageCallback | None = None) -> None:
        """Start a fresh generation and establish callbacks before consuming data."""
        if self._started:
            return
        if any(not task.done() for task in self._consumers):
            raise LxmfConnectionError("Previous native callbacks have not stopped")
        self._stopping = None
        self._contexts.clear()
        self._names.clear()
        self._closing = False
        self._last_error = None
        self._status_delayed = False
        self._message_callback = message_callback
        self._worker = NativeWorker(
            self._config,
            reticulum_config_dir=self._reticulum_config_dir,
            message_plane=True,
            _command=self._command,
        )
        ready = await self._worker.start()
        try:
            self._diag = msgspec.convert(ready.diagnostics, type=LxmfSessionDiagnostics)
        except Exception:
            await self._worker.stop()
            raise LxmfConnectionError(
                "Native readiness diagnostics are invalid"
            ) from None
        self._delivery_destination_hash = ready.destination_hash
        self._started = True
        self._consumers = [
            asyncio.create_task(self._inbound(), name="lxmf-worker-inbound"),
            asyncio.create_task(self._feedback(), name="lxmf-worker-feedback"),
        ]

    def set_delivery_state_callback(
        self, callback: DeliveryStateCallback | None
    ) -> None:
        """Register an in-process callback that retains core attempt authority."""
        self._delivery_callback = callback

    async def _inbound(self) -> None:
        """Apply bounded inbound backpressure separately from terminal observations."""
        assert self._worker and self._worker.channel
        queue = self._worker.channel.inbound
        while True:
            message = await queue.get()
            try:
                if message is None:
                    return
                source = message.packet.get("source_hash")
                name = message.packet.get("source_name")
                if isinstance(source, str) and isinstance(name, str) and name:
                    self._names[source] = name
                    self._names.move_to_end(source)
                    if len(self._names) > 1024:
                        self._names.popitem(last=False)
                if self._message_callback is not None:
                    result = self._message_callback(message.packet)
                    if inspect.isawaitable(result):
                        await result
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception("LXMF normalized inbound admission failed")
            finally:
                queue.task_done()
            if self._worker.channel.failure and queue.empty():
                return

    async def _feedback(self) -> None:
        """Echo provider facts only through the captured parent attempt context."""
        assert self._worker and self._worker.channel
        queue = self._worker.channel.updates
        while True:
            update = await queue.get()
            try:
                if update is None:
                    return
                context = self._contexts.get(update.operation_id)
                if context is None:
                    self._logger.debug("Ignored unmatched LXMF native delivery update")
                    if self._worker.channel.failure and queue.empty():
                        return
                    continue
                if (
                    context.native_id is not None
                    and context.native_id != update.native_message_id
                ):
                    self._worker.channel._fail("Contradictory native message identity")
                    return
                context.native_id = update.native_message_id
                if self._delivery_callback is not None:
                    result = self._delivery_callback(
                        update.native_message_id, update.state, context.context
                    )
                    if inspect.isawaitable(result):
                        await result
                self._contexts.pop(update.operation_id, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._logger.exception("LXMF native delivery observation failed")
            finally:
                queue.task_done()
            if self._worker.channel.failure and queue.empty():
                return

    async def send_text(
        self,
        destination_hash: str,
        content: str,
        *,
        title: str = "",
        delivery_method: str | None = None,
        fields: dict[int, Any] | None = None,
        delivery_context: object | None = None,
    ) -> tuple[str | None, LxmfDeliveryState]:
        """Return only acknowledged SDK handoff; uncertain outcomes never auto-retry."""
        worker = self._worker
        if not self.connected or worker is None or worker.channel is None:
            raise LxmfSendError(
                "Native worker is unavailable before submission", transient=True
            )
        operation_id = (
            payload_digest(
                msgspec.to_builtins(delivery_context.attempt_provenance)
            ).hex()
            if isinstance(delivery_context, LxmfDeliveryContext)
            else uuid.uuid4().hex
        )
        try:
            digest = payload_digest(
                (destination_hash, content, title, delivery_method, fields)
            )
        except (TypeError, ValueError, RecursionError):
            raise LxmfSendError(
                "Native payload violates the plain-data wire contract", transient=False
            ) from None
        record = self._contexts.get(operation_id)
        if record is None:
            if len(self._contexts) >= 1000:
                raise LxmfSendError(
                    "Native feedback capacity is full before submission", transient=True
                )
            record = _ObservationContext(delivery_context, digest)
            self._contexts[operation_id] = record
        elif record.digest != digest:
            raise LxmfSendError(
                "Attempt payload conflicts with its native handoff fence",
                transient=False,
            )
        try:
            response = await worker.channel.exchange(
                Submit(
                    uuid.uuid4().hex,
                    operation_id,
                    destination_hash,
                    content,
                    title,
                    delivery_method,
                    fields,
                ),
                timeout=max(30.0, self._config.message_delay_seconds + 30.0),
            )
            if isinstance(response, SubmitFailed):
                if response.category == "unknown":
                    raise LxmfHandoffUnknownError(response.error)
                self._contexts.pop(operation_id, None)
                raise LxmfSendError(
                    response.error, transient=response.category == "transient"
                )
            if not isinstance(response, Submitted):
                worker.channel._fail(
                    "Native submission returned the wrong response type"
                )
                raise LxmfHandoffUnknownError()
            if record.native_id is not None and response.native_message_id not in (
                None,
                record.native_id,
            ):
                worker.channel._fail("Contradictory native handoff identity")
                raise LxmfHandoffUnknownError()
            record.native_id = response.native_message_id
            return response.native_message_id, response.state
        except WorkerProtocolError:
            self._contexts.pop(operation_id, None)
            raise LxmfSendError(
                "Native payload violates the plain-data wire contract", transient=False
            ) from None
        except LxmfHandoffUnknownError as exc:
            self._last_error = str(exc)
            raise
        except LxmfSendError:
            self._contexts.pop(operation_id, None)
            raise

    async def refresh(self) -> None:
        """Refresh safe native diagnostics with an independent bounded probe."""
        if not self.connected or self._worker is None or self._worker.channel is None:
            return
        try:
            reply = await self._worker.channel.exchange(
                GetStatus(uuid.uuid4().hex), timeout=2
            )
            if not isinstance(reply, Status):
                self._worker.channel._fail(
                    "Native status returned the wrong response type"
                )
                return
            self._diag = reply.diagnostics
            self._status_delayed = False
        except (LxmfSendError, LxmfConnectionError):
            self._status_delayed = True

    def diagnostics(self) -> LxmfSessionDiagnostics:
        """Return cached native scalars without importing an SDK or probing peers."""
        diag = self._diag or LxmfSessionDiagnostics(
            False,
            False,
            False,
            0,
            None,
            0,
            0,
            None,
            None,
            None,
            0,
            self._config.connection_type,
            0,
            0,
            None,
        )
        failure = (
            self._worker.channel.failure
            if self._worker and self._worker.channel
            else None
        )
        return replace(
            diag,
            connected=self.connected,
            router_running=self.router_running,
            pending_delivery_count=len(self._contexts),
            last_error=self._last_error or failure or diag.last_error,
        )

    def resolve_display_name(self, source_hash: str) -> str | None:
        """Use only names resolved by the child's native announce cache."""
        return self._names.get(source_hash)

    async def stop(self, timeout: float = 5.0) -> None:
        """Retain native and callback cleanup through repeated caller cancellation."""
        self._closing = True
        if self._stopping is None:
            self._stopping = asyncio.create_task(
                self._stop_owned(timeout), name="lxmf-session-stop"
            )
        cancelled: asyncio.CancelledError | None = None
        try:
            while not self._stopping.done():
                try:
                    await asyncio.shield(self._stopping)
                except asyncio.CancelledError as exc:
                    cancelled = exc
            self._stopping.result()
        except BaseException as cleanup_error:
            if cancelled is not None:
                raise cancelled from cleanup_error
            raise
        if cancelled is not None:
            raise cancelled

    async def _stop_owned(self, timeout: float) -> None:
        """Reap one generation and boundedly finish its forwarded callbacks."""
        try:
            if self._worker is not None:
                await self._worker.stop(timeout=max(0, timeout * 0.75))
        finally:
            tasks = set(self._consumers)
            if tasks:
                _, pending = await asyncio.wait(tasks, timeout=max(0, timeout * 0.125))
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.wait(pending, timeout=max(0, timeout * 0.125))
                for task in tasks:
                    if task.done() and not task.cancelled():
                        task.exception()
            self._consumers = [task for task in tasks if not task.done()]
            for task in self._consumers:
                task.add_done_callback(
                    lambda finished: (
                        None if finished.cancelled() else finished.exception()
                    )
                )
            self._started = False
            self._message_callback = None
            self._delivery_callback = None
