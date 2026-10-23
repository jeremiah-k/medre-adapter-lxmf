"""Private wire values and persistent native handoff fences preserve uncertainty."""

import io
import os
import subprocess
import sys
from dataclasses import replace

import pytest

from medre.adapters.lxmf.errors import LxmfConnectionError, LxmfSendError
from medre.adapters.lxmf.worker_environment import native_environment
from medre.adapters.lxmf.worker_protocol import (
    DeliveryUpdate,
    Inbound,
    Submit,
    WorkerProtocolError,
    encode,
    read_sync,
)
from medre.adapters.lxmf.worker_state import (
    WorkerProfile,
    SubmissionRecord,
    payload_digest,
)
from medre.config.adapters.lxmf import LxmfConfig


def test_native_fields_roundtrip_preserves_integer_keys_and_bytes():
    """Framing cannot JSON-coerce LXMF fields or opaque metadata."""
    fields = {1: b"opaque", 2: {b"key": [42, b"bytes", {3: "text"}]}}
    command = Submit("request", "operation", "ab" * 16, "hello", fields=fields)
    assert read_sync(io.BytesIO(encode(command))) == command
    packet = Inbound({"fields": fields, "content": "hello"})
    assert read_sync(io.BytesIO(encode(packet))) == packet
    update = DeliveryUpdate("operation", "native-id", "delivered")
    assert read_sync(io.BytesIO(encode(update))) == update


@pytest.mark.parametrize("value", [object(), float("nan"), float("inf"), {1.1: "key"}])
def test_non_native_field_values_are_rejected_before_submission(value):
    """SDK objects and non-finite payloads cannot enter the private protocol."""
    with pytest.raises(WorkerProtocolError):
        encode(Submit("r", "o", "ab" * 16, "hello", fields={1: value}))


def test_native_field_depth_is_bounded():
    """A recursive payload fails before encoding or queue admission."""
    recursive = []
    recursive.append(recursive)
    with pytest.raises(WorkerProtocolError, match="nesting"):
        encode(Inbound({"fields": recursive}))


def test_journal_fences_interrupted_and_accepted_attempts_across_restart(tmp_path):
    """A durable reservation never invites retransmission after an ambiguous exit."""
    config = LxmfConfig("fenced", storage_path=str(tmp_path))
    digest = payload_digest(("destination", "body", {2: b"value", 1: "text"}))
    assert digest == payload_digest(("destination", "body", {1: "text", 2: b"value"}))
    profile = WorkerProfile(config)
    try:
        assert profile.begin("interrupted", digest) is None
        assert profile.begin("accepted", digest) is None
        accepted = SubmissionRecord("accepted", "native-id", "outbound", None)
        profile.finish("accepted", accepted)
        with pytest.raises(LxmfConnectionError, match="owned"):
            WorkerProfile(config)
    finally:
        profile.close()
    resumed = WorkerProfile(config)
    try:
        assert resumed.begin("interrupted", digest).status == "unknown"
        assert resumed.begin("accepted", digest) == accepted
        with pytest.raises(LxmfSendError, match="conflicts"):
            resumed.begin("accepted", payload_digest("different content"))
        assert resumed.begin("distinct-attempt", digest) is None
    finally:
        resumed.close()
    if os.name != "nt":
        assert (tmp_path / ".medre-handoffs.sqlite").stat().st_mode & 0o777 == 0o600
    assert b"body" not in (tmp_path / ".medre-handoffs.sqlite").read_bytes()


def test_explicit_identity_ownership_is_shared_across_storage_roots(tmp_path):
    """Two queues cannot concurrently announce the same private identity."""
    identity = tmp_path / "identity"
    identity.write_bytes(b"unchanged-key")
    config = LxmfConfig(
        "identity",
        connection_type="reticulum",
        identity_path=str(identity),
        storage_path=str(tmp_path / "a"),
    )
    profile = WorkerProfile(config)
    try:
        with pytest.raises(LxmfConnectionError, match="owned"):
            WorkerProfile(replace(config, storage_path=str(tmp_path / "b")))
        assert identity.read_bytes() == b"unchanged-key"
    finally:
        profile.close()
    restarted = WorkerProfile(config)
    restarted.close()


def test_native_environment_excludes_other_adapter_credentials(monkeypatch):
    """Worker inheritance keeps OS/Python configuration without unrelated secrets."""
    monkeypatch.setenv("MEDRE_ADAPTER__MATRIX__ACCESS_TOKEN", "private-token")
    monkeypatch.setenv("SIMPLEX_PASSWORD", "private-password")
    monkeypatch.setenv("SSL_CERT_FILE", "/private/lab-ca.pem")
    env = native_environment()
    assert "MEDRE_ADAPTER__MATRIX__ACCESS_TOKEN" not in env
    assert "SIMPLEX_PASSWORD" not in env
    assert env["SSL_CERT_FILE"] == "/private/lab-ca.pem"
    assert env["PATH"] == os.environ["PATH"]


def test_importing_real_adapter_does_not_load_native_sdks():
    """Constructing the parent facade leaves native globals inside the worker."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from medre.adapters.lxmf.adapter import LxmfAdapter
from medre.config.adapters.lxmf import LxmfConfig
from medre.adapters.lxmf.worker_session import WorkerSession
adapter = LxmfAdapter(LxmfConfig('lazy', connection_type='reticulum', storage_path='/unused'))
assert isinstance(adapter.session, WorkerSession)
assert 'RNS' not in sys.modules and 'LXMF' not in sys.modules
""",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_retryable_handoff_can_reenter_dispatch_without_reusing_uncertain_fence(
    tmp_path,
):
    """A documented pre-handoff transient failure is safe to resubmit."""
    profile = WorkerProfile(LxmfConfig("retryable", storage_path=str(tmp_path)))
    digest = payload_digest(("destination", "text"))
    try:
        assert profile.begin("same-attempt", digest) is None
        profile.finish(
            "same-attempt", SubmissionRecord("retryable", None, "unmapped", "offline")
        )
        assert profile.begin("same-attempt", digest) is None
        # An interrupted second dispatch is *not* safe to send again.
        assert profile.begin("same-attempt", digest).status == "unknown"
        profile.finish(
            "same-attempt", SubmissionRecord("accepted", "native", "outbound", None)
        )
        assert profile.begin("same-attempt", digest).native_id == "native"
    finally:
        profile.close()
