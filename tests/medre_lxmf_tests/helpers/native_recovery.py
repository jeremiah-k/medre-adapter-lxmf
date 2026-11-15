"""Derive the native restore recovery budget from the pinned SDK's own cycle.

The peer is an independent LXMF application. After MEDRE stops, it may keep a
closed direct link to the restored destination. On the next submission
``LXMRouter.process_outbound`` clears that stale link, requests the path, and
schedules the replacement attempt one ``DELIVERY_RETRY_WAIT`` later. Each
replacement link request then owns the initiator establishment allowance that
``RNS.Link`` derives from the first-hop and per-hop timeouts. A single lost
handshake therefore costs retry wait + establishment timeout, and the SDK
schedules another retry at the same cadence.

The local qualification path is a zero-hop shared instance. Its baseline
initiator allowance combines the SDK's default first-hop timeout with one
per-hop unit. Polling headroom also covers local serialization/dispatch; this
is a bounded qualification allowance, not a real-time SDK guarantee.
"""

from __future__ import annotations

HEALTHY_DEADLINE_SECONDS = 20.0


def establishment_seconds() -> float:
    """Return the initiator allowance for a zero-hop local shared-instance path."""
    from RNS.Link import Link
    from RNS.Reticulum import Reticulum

    return Reticulum.DEFAULT_PER_HOP_TIMEOUT + Link.ESTABLISHMENT_TIMEOUT_PER_HOP


def recovery_seconds() -> float:
    """Budget two replacement opportunities and the tick-gated transitions.

    Allow a polling turn for the initial stale-link clear, each replacement
    start, failed-handshake cleanup, and recipient dispatch. Only budgeting
    one turn can expire before the second handshake's allowance finishes.
    The native attempt limit remains authoritative; time is not a resend grant.
    """
    from LXMF.LXMRouter import LXMRouter

    establishment = establishment_seconds()
    outbound_tick = LXMRouter.PROCESSING_INTERVAL * LXMRouter.JOB_OUTBOUND_INTERVAL
    return 2 * LXMRouter.DELIVERY_RETRY_WAIT + 2 * establishment + 5 * outbound_tick


def recovery_extension_seconds() -> float:
    """Return the recovery window beyond the ordinary healthy deadline."""
    return max(0.0, recovery_seconds() - HEALTHY_DEADLINE_SECONDS)
