"""Parent-owned immutable correlation for LXMF terminal delivery facts."""

from dataclasses import dataclass

from medre.core.events.delivery import DeliveryAttemptProvenance


@dataclass(frozen=True)
class LxmfDeliveryContext:
    """Keep lifecycle provenance in the parent; only an opaque key crosses IPC."""

    attempt_provenance: DeliveryAttemptProvenance
    native_channel_id: str | None
