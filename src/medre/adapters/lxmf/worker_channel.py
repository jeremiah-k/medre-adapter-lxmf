"""One bounded reader and serialized writer for the native message plane."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from medre.adapters.lxmf.errors import (
    LxmfConnectionError,
    LxmfHandoffUnknownError,
    LxmfSendError,
)
from medre.adapters.lxmf.worker_protocol import (
    DeliveryUpdate,
    Failed,
    Inbound,
    Request,
    Response,
    Status,
    Stop,
    Stopped,
    Submit,
    SubmitFailed,
    Submitted,
    WorkerProtocolError,
    encode,
    read_async,
)


class WorkerChannel:
    """Separate transport replies from callbacks so slow admission cannot deadlock RPC."""

    def __init__(
        self, process: asyncio.subprocess.Process, failed: Callable[[], None]
    ) -> None:
        self._process = process
        self._failed = failed
        self._write_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[Response]] = {}
        self.inbound: asyncio.Queue[Inbound | None] = asyncio.Queue(maxsize=64)
        self.updates: asyncio.Queue[DeliveryUpdate | None] = asyncio.Queue(maxsize=128)
        self._stopped: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self.failure: str | None = None
        self._closing = False
        self._reader = asyncio.create_task(self._read(), name="lxmf-native-reader")

    @property
    def connected(self) -> bool:
        """Report only live local process/channel state, never peer delivery."""
        return (
            self.failure is None
            and not self._closing
            and self._process.returncode is None
        )

    @staticmethod
    def _observe(future: asyncio.Future[Response]) -> None:
        """Retrieve late outcomes after a request caller was cancelled."""
        if not future.cancelled():
            future.exception()

    def _end_events(self) -> None:
        """Wake an idle dispatcher; a full queue already provides a wakeup."""
        for queue in (self.inbound, self.updates):
            if not queue.full():
                queue.put_nowait(None)

    def _fail(self, message: str) -> None:
        """Fail closed without converting possible submission into retryable failure."""
        if self.failure is not None:
            return
        self.failure = message
        if not self._stopped.done():
            self._stopped.set_result(False)
        for future in self._pending.values():
            if not future.done():
                future.set_exception(LxmfHandoffUnknownError())
        self._pending.clear()
        self._end_events()
        self._failed()

    async def _read(self) -> None:
        """Own every frame after Ready and reject contradictory/unknown replies."""
        assert self._process.stdout is not None
        try:
            while (message := await read_async(self._process.stdout)) is not None:
                if isinstance(message, (Submitted, SubmitFailed, Status)):
                    future = self._pending.pop(message.request_id, None)
                    if future is None or future.done():
                        raise WorkerProtocolError("Native reply has no pending request")
                    future.set_result(message)
                elif isinstance(message, Inbound):
                    self.inbound.put_nowait(message)
                elif isinstance(message, DeliveryUpdate):
                    self.updates.put_nowait(message)
                elif isinstance(message, Stopped):
                    if not self._closing:
                        raise WorkerProtocolError(
                            "Native worker stopped without request"
                        )
                    self._stopped.set_result(True)
                    self._end_events()
                    return
                elif isinstance(message, Failed):
                    self._fail(message.message)
                    return
                else:
                    raise WorkerProtocolError("Unexpected native message direction")
            self._fail("Native worker channel closed")
        except asyncio.CancelledError:
            raise
        except Exception:
            self._fail("Native worker channel failed or exceeded its event capacity")

    async def exchange(self, request: Request, *, timeout: float) -> Response:
        """Submit once; preserve uncertainty after bytes cross the private pipe."""
        frame = encode(request)
        if not self.connected:
            raise LxmfSendError(
                "Native worker is unavailable before submission", transient=True
            )
        if len(self._pending) >= 32:
            raise LxmfSendError(
                "Native request capacity is full before submission", transient=True
            )
        if request.request_id in self._pending:
            raise WorkerProtocolError("Native request identity was reused")
        future: asyncio.Future[Response] = asyncio.get_running_loop().create_future()
        future.add_done_callback(self._observe)
        self._pending[request.request_id] = future
        written = False
        try:
            async with asyncio.timeout(timeout):
                async with self._write_lock:
                    if not self.connected:
                        raise LxmfSendError(
                            "Native worker closed before submission", transient=True
                        )
                    assert self._process.stdin is not None
                    # A write can partially cross the pipe before raising.
                    written = True
                    self._process.stdin.write(frame)
                    await self._process.stdin.drain()
                return await asyncio.shield(future)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            if isinstance(request, Submit) and written:
                self._fail("Native handoff reply deadline expired")
                raise LxmfHandoffUnknownError() from None
            if isinstance(request, Submit):
                raise LxmfSendError(
                    "Native submission deadline expired before writing", transient=True
                ) from None
            raise LxmfConnectionError("Native status reply deadline expired") from None
        except (ConnectionError, OSError):
            self._fail("Native request pipe failed")
            if written:
                raise LxmfHandoffUnknownError() from None
            raise LxmfSendError(
                "Native worker closed before submission", transient=True
            ) from None
        finally:
            if not written or future.done():
                self._pending.pop(request.request_id, None)

    async def request_stop(self, timeout: float) -> bool:
        """Serialize the stop frame with submissions and await its separate ACK."""
        self._closing = True
        if self.failure is not None:
            return False
        async with self._write_lock:
            assert self._process.stdin is not None
            self._process.stdin.write(encode(Stop(timeout)))
            await self._process.stdin.drain()
        return bool(await asyncio.shield(self._stopped))

    async def close(self) -> None:
        """Retire the reader after owned process cleanup and settle pending callers."""
        self._closing = True
        if not self._stopped.done():
            self._stopped.set_result(False)
        for future in self._pending.values():
            if not future.done():
                future.set_exception(LxmfHandoffUnknownError())
        self._pending.clear()
        self._end_events()
        if not self._reader.done():
            self._reader.cancel()
        await asyncio.gather(self._reader, return_exceptions=True)
