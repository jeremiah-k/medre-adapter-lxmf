"""Bounded, typed frames for the adapter-owned native worker lifecycle."""

from __future__ import annotations

import asyncio
import math
from dataclasses import fields
from typing import Annotated, Any, BinaryIO, Literal, TypeAlias

import msgspec

from medre.config.adapters.lxmf import LxmfConfig
from medre.adapters.lxmf.session import LxmfDeliveryState, LxmfSessionDiagnostics

MAX_FRAME_BYTES = 1024 * 1024


class Start(msgspec.Struct, tag="start", frozen=True, forbid_unknown_fields=True):
    """Initialize one native profile; configuration travels over the private pipe."""

    config: LxmfConfig
    reticulum_config_dir: str | None = None
    message_plane: bool = False


class Stop(msgspec.Struct, tag="stop", frozen=True, forbid_unknown_fields=True):
    """Ask the worker to persist and release its session before exiting."""

    timeout_seconds: float


class Ready(msgspec.Struct, tag="ready", frozen=True, forbid_unknown_fields=True):
    """Native initialization completed; this is a readiness snapshot, not delivery."""

    pid: int
    connected: bool
    router_running: bool
    destination_hash: bytes | None
    diagnostics: dict[str, Any]


class Stopped(msgspec.Struct, tag="stopped", frozen=True, forbid_unknown_fields=True):
    """Native session cleanup completed; the parent must also reap process exit."""


class Failed(msgspec.Struct, tag="failed", frozen=True, forbid_unknown_fields=True):
    """A sanitized startup or shutdown failure; never an inferred delivery fact."""

    message: str


RequestId = Annotated[str, msgspec.Meta(min_length=1, max_length=96)]


class Submit(msgspec.Struct, tag="submit", frozen=True, forbid_unknown_fields=True):
    """Request one native submission; opaque operation identity fences retries."""

    request_id: RequestId
    operation_id: RequestId
    destination_hash: str
    content: str
    title: str = ""
    delivery_method: str | None = None
    fields: dict[int, Any] | None = None


class Submitted(
    msgspec.Struct, tag="submitted", frozen=True, forbid_unknown_fields=True
):
    """The SDK returned from native submission; this does not confirm delivery."""

    request_id: RequestId
    native_message_id: str | None
    state: LxmfDeliveryState


class SubmitFailed(
    msgspec.Struct, tag="submit_failed", frozen=True, forbid_unknown_fields=True
):
    """Distinguish known pre-handoff failure from an uncertain native outcome."""

    request_id: RequestId
    category: Literal["transient", "permanent", "unknown"]
    error: str


class GetStatus(
    msgspec.Struct, tag="get_status", frozen=True, forbid_unknown_fields=True
):
    """Read a fresh SDK-owned operational snapshot."""

    request_id: RequestId


class Status(msgspec.Struct, tag="status", frozen=True, forbid_unknown_fields=True):
    """A safe native snapshot, independent of any message delivery claim."""

    request_id: RequestId
    diagnostics: LxmfSessionDiagnostics


class Inbound(msgspec.Struct, tag="inbound", frozen=True, forbid_unknown_fields=True):
    """Normalized native values, including integer fields and opaque bytes."""

    packet: dict[str, Any]


class DeliveryUpdate(
    msgspec.Struct, tag="delivery_update", frozen=True, forbid_unknown_fields=True
):
    """A provider-emitted terminal fact correlated by its opaque operation ID."""

    operation_id: RequestId
    native_message_id: str
    state: Literal["delivered", "failed", "rejected", "cancelled"]


Request: TypeAlias = Submit | GetStatus
Response: TypeAlias = Submitted | SubmitFailed | Status
NativeEvent: TypeAlias = Inbound | DeliveryUpdate
Message: TypeAlias = (
    Start | Stop | Ready | Stopped | Failed | Request | Response | NativeEvent
)
_DECODER: msgspec.msgpack.Decoder[Message] = msgspec.msgpack.Decoder(Message)
_CONFIG_FIELDS = frozenset(field.name for field in fields(LxmfConfig))


class WorkerProtocolError(RuntimeError):
    """A worker frame is incomplete, oversized, or outside the closed vocabulary."""


def _validate_native_message(message: Message) -> None:
    """Reject executable/SDK objects and non-finite numbers at the native seam."""

    def native(value: object, depth: int = 0) -> None:
        if depth > 32:
            raise WorkerProtocolError("Native value nesting exceeds its limit")
        if value is None or type(value) in {str, bytes, int, bool}:
            return
        if type(value) is float and math.isfinite(value):
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                native(item, depth + 1)
            return
        if isinstance(value, dict):
            for key, item in value.items():
                if type(key) not in {str, int, bytes}:
                    raise WorkerProtocolError("Native mapping key is invalid")
                native(item, depth + 1)
            return
        raise WorkerProtocolError("Native value is outside the plain-data contract")

    if isinstance(message, Submit):
        native(message.fields)
    elif isinstance(message, Inbound):
        native(message.packet)


def encode(message: Message) -> bytes:
    """Encode one typed message without pickle or an executable object graph."""
    _validate_native_message(message)
    payload = msgspec.msgpack.encode(message)
    if not 0 < len(payload) <= MAX_FRAME_BYTES:
        raise WorkerProtocolError("Native worker frame exceeds its size limit")
    return len(payload).to_bytes(4, "big") + payload


def _size(header: bytes) -> int:
    """Validate the length before allocating or waiting for a payload."""
    size = int.from_bytes(header, "big")
    if not 0 < size <= MAX_FRAME_BYTES:
        raise WorkerProtocolError("Native worker frame length is invalid")
    return size


def _decode(payload: bytes) -> Message:
    """Reject malformed or unknown messages without exposing their contents."""
    try:
        # Dataclass decoding ignores unknown fields; retain its typed decoding
        # while enforcing the same closed configuration shape at this seam.
        raw = msgspec.msgpack.decode(payload)
        if isinstance(raw, dict) and raw.get("type") == "start":
            config = raw.get("config")
            if isinstance(config, dict) and not config.keys() <= _CONFIG_FIELDS:
                raise WorkerProtocolError(
                    "Native worker configuration fields are invalid"
                )
        message = _DECODER.decode(payload)
        _validate_native_message(message)
        return message
    except msgspec.DecodeError:
        raise WorkerProtocolError("Native worker frame is invalid") from None


def read_sync(stream: BinaryIO) -> Message | None:
    """Read a worker-side frame, distinguishing clean EOF from truncation."""

    def exact(size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = stream.read(size - len(chunks))
            if not chunk:
                break
            chunks.extend(chunk)
        return bytes(chunks)

    header = exact(4)
    if not header:
        return None
    if len(header) != 4:
        raise WorkerProtocolError("Native worker frame header is truncated")
    size = _size(header)
    payload = exact(size)
    if len(payload) != size:
        raise WorkerProtocolError("Native worker frame payload is truncated")
    return _decode(payload)


async def read_async(stream: asyncio.StreamReader) -> Message | None:
    """Read a parent-side frame with the same bounds and EOF rules."""
    try:
        header = await stream.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise WorkerProtocolError("Native worker frame header is truncated") from None
    try:
        payload = await stream.readexactly(_size(header))
    except asyncio.IncompleteReadError:
        raise WorkerProtocolError("Native worker frame payload is truncated") from None
    return _decode(payload)
