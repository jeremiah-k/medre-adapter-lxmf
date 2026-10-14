"""Native address rules are owned and enforced by the installed adapter."""

import pytest
from medre.adapter_registry import get_destination_spec
from medre.config.errors import ConfigValidationError
from medre.config.routes import RouteDestinationConfig


def test_registered_native_destination_validates() -> None:
    assert get_destination_spec("lxmf_destination") is not None
    destination = RouteDestinationConfig.from_dict(
        "native",
        {
            "kind": "lxmf_destination",
            **{"destination_hash": "0123456789abcdef0123456789abcdef"},
        },
        field_name="dest_destination",
        section_path="routes.native",
    )
    assert destination.kind == "lxmf_destination"


@pytest.mark.parametrize(
    "address",
    [
        {},
        {"destination_hash": "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
        {"destination_hash": "abc"},
    ],
)
def test_native_destination_rejects_invalid_address(address: dict) -> None:
    with pytest.raises(ConfigValidationError):
        RouteDestinationConfig.from_dict(
            "native",
            {"kind": "lxmf_destination", **address},
            field_name="dest_destination",
            section_path="routes.native",
        )
