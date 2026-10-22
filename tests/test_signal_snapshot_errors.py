"""Fail closed before SDK startup if host signal ownership cannot be recorded."""

import logging
import signal
from unittest.mock import MagicMock

import pytest

from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.session import _preserve_runtime_signals


def test_partial_snapshot_cannot_launch_sdk_or_lose_host_shutdown_handler(monkeypatch):
    """SIGTERM lookup failure must not discard an already-saved SIGINT."""
    host_handler = object()
    inspected = []

    def getsignal(signum):
        inspected.append(signum)
        if signum == signal.SIGTERM:
            raise ValueError("test: missing SIGTERM")
        return host_handler

    restore = MagicMock()
    monkeypatch.setattr(signal, "getsignal", getsignal)
    monkeypatch.setattr(signal, "signal", restore)
    invoked = False
    with pytest.raises(LxmfConnectionError, match="Cannot preserve runtime signal"):
        with _preserve_runtime_signals(logging.getLogger(__name__), "radio"):
            invoked = True
    assert inspected == [signal.SIGINT, signal.SIGTERM]
    assert not invoked
    restore.assert_not_called()


def test_restore_failure_attempts_both_handlers_and_fails_closed(monkeypatch):
    """A partially restored constructor cannot report successful startup."""
    host_handler = object()
    attempted = []
    monkeypatch.setattr(signal, "getsignal", lambda _: host_handler)

    def restore(signum, handler):
        attempted.append(signum)
        if signum == signal.SIGTERM:
            raise OSError("test: rejected restoration")

    monkeypatch.setattr(signal, "signal", restore)
    with pytest.raises(LxmfConnectionError, match="Could not restore runtime"):
        with _preserve_runtime_signals(logging.getLogger(__name__), "radio"):
            pass
    assert attempted == [signal.SIGINT, signal.SIGTERM]
