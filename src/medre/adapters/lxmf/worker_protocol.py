"""Bounded, typed frames for the adapter-owned native worker lifecycle."""

from __future__ import annotations

import asyncio
from dataclasses import fields
from typing import Any, BinaryIO, TypeAlias

import msgspec

from medre.config.adapters.lxmf import LxmfConfig

MAX_FRAME_BYTES = 1024 * 1024


class Start(msgspec.Struct, tag="start", frozen=True, forbid_unknown_fields=True):
    """Initialize one native profile; configuration travels over the private pipe."""

    config: LxmfConfig
    reticulum_config_dir: str | None = None


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


Message: TypeAlias = Start | Stop | Ready | Stopped | Failed
_DECODER: msgspec.msgpack.Decoder[Message] = msgspec.msgpack.Decoder(Message)
_CONFIG_FIELDS = frozenset(field.name for field in fields(LxmfConfig))


class WorkerProtocolError(RuntimeError):
    """A worker frame is incomplete, oversized, or outside the closed vocabulary."""


def encode(message: Message) -> bytes:
    """Encode one typed message without pickle or an executable object graph."""
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
        return _DECODER.decode(payload)
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
