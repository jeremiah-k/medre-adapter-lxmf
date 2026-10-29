"""Native link stage evidence must never be mistaken for delivery authority."""

from __future__ import annotations

import pytest
from types import SimpleNamespace

from medre_lxmf_tests.helpers import native_chat_events
from medre_lxmf_tests.helpers.native_chat_events import classify_inbound_timeout


@pytest.mark.parametrize(
    ("peer", "worker", "expected"),
    [
        ({"event": "status_unavailable"}, {}, "peer_observation_unavailable"),
        (
            {"event": "status", "attached": False},
            {},
            "peer_shared_service_disconnected",
        ),
        (
            {"event": "status", "attached": True, "path_known": False},
            {},
            "destination_path_unavailable",
        ),
        (
            {
                "event": "status",
                "attached": True,
                "path_known": True,
                "direct_link_status": 0,
                "message_state": 1,
            },
            {"connected": True, "router_running": True},
            "native_link_unestablished",
        ),
        (
            {"event": "status", "attached": True, "path_known": True},
            {"connected": False},
            "medre_native_router_unavailable",
        ),
        (
            {
                "event": "status",
                "attached": True,
                "path_known": True,
                "direct_link_status": 2,
                "direct_link_active": True,
            },
            {"connected": True, "router_running": True},
            "handoff_not_observed_at_recipient",
        ),
    ],
)
def test_stage_classification_is_observational(peer, worker, expected):
    before = dict(peer), dict(worker)
    assert classify_inbound_timeout(peer, worker) == expected
    assert (peer, worker) == before


async def test_outbound_timeout_has_directional_evidence_not_peer_send_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def timeout(*args: object, **kwargs: object) -> None:
        raise TimeoutError

    class Session:
        def diagnostics(self) -> SimpleNamespace:
            return SimpleNamespace(
                connected=True, router_running=True, pending_delivery_count=1
            )

    monkeypatch.setattr(native_chat_events, "event", timeout)
    with pytest.raises(
        AssertionError, match="native outbound recipient deadline exceeded"
    ):
        await native_chat_events.outbound_received(object(), Session())


def test_campaign_does_not_count_sdk_skips_as_native_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess
    import sys

    from scripts import qualify_native_link

    monkeypatch.setattr(sys, "argv", ["qualify_native_link.py", "--runs", "1"])
    monkeypatch.setattr(
        qualify_native_link.importlib.util, "find_spec", lambda _module: object()
    )
    monkeypatch.setattr(
        qualify_native_link.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, stdout="2 skipped in 0.05s\n"
        ),
    )
    assert qualify_native_link.main() == 1


def test_campaign_requires_both_native_directions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess
    import sys

    from scripts import qualify_native_link

    monkeypatch.setattr(sys, "argv", ["qualify_native_link.py", "--runs", "1"])
    monkeypatch.setattr(
        qualify_native_link.importlib.util, "find_spec", lambda _module: object()
    )
    monkeypatch.setattr(
        qualify_native_link.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args, 0, stdout="2 passed in 1.05s\n"
        ),
    )
    assert qualify_native_link.main() == 0


@pytest.mark.parametrize("status", [None, 0, 1, 3, 4, 99])
def test_absent_or_nonactive_links_do_not_imply_recipient_handoff(status):
    """Missing, pending, handshaking, stale and closed links are not usable."""
    peer = {
        "event": "status",
        "attached": True,
        "path_known": True,
        "direct_link_status": status,
        "direct_link_active": False,
        "backchannel_link_active": False,
    }
    worker = {"connected": True, "router_running": True}
    assert classify_inbound_timeout(peer, worker) == "native_link_unestablished"


@pytest.mark.parametrize("path_known", [False, True])
@pytest.mark.parametrize("direct_status", [None, 1])
def test_active_backchannel_counts_as_observed_usable_link(path_known, direct_status):
    """An active backchannel is not hidden by absent direct-link/path evidence."""
    peer = {
        "event": "status",
        "attached": True,
        "path_known": path_known,
        "direct_link_status": direct_status,
        "direct_link_active": False,
        "backchannel_link_status": 2,
        "backchannel_link_active": True,
    }
    assert (
        classify_inbound_timeout(peer, {"connected": True, "router_running": True})
        == "handoff_not_observed_at_recipient"
    )
