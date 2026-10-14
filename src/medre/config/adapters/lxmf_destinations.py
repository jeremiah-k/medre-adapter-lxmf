"""Pure adapter-owned destination-address validation."""

import re

from medre.config.routes import RouteDestinationConfig


def validate_destination(destination: RouteDestinationConfig) -> None:
    """Reject addresses outside this transport's addressing contract."""
    if destination.destination_hash is None:
        raise ValueError(
            "destination_hash is required for kind 'lxmf_destination' and must be a 32-hex-character LXMF destination hash"
        )
    if not re.fullmatch(r"[0-9a-fA-F]{32}", destination.destination_hash):
        raise ValueError(
            "destination_hash must be a 32-hex-character LXMF destination hash (16 bytes)"
        )
