"""Router cleanup supports each owned announce-registry representation."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from medre.adapters.lxmf.session import LxmfSession
from medre.config.adapters.lxmf import LxmfConfig


@pytest.mark.parametrize("bucketed", [False, True])
def test_teardown_deregisters_only_owned_announce_handlers(bucketed):
    owner = SimpleNamespace(exit_handler=Mock())
    unrelated = SimpleNamespace(lxmrouter=object())
    delivery = SimpleNamespace(lxmrouter=owner)
    propagation = SimpleNamespace(lxmrouter=owner)
    registry = (
        {b"delivery": [delivery, unrelated], b"propagation": [propagation]}
        if bucketed
        else [delivery, unrelated, propagation]
    )
    transport = SimpleNamespace(
        announce_handlers=registry, deregister_announce_handler=Mock()
    )
    session = LxmfSession(config=LxmfConfig(adapter_id="test"), adapter_id="test")
    session._router = owner
    session._rns_transport = transport

    session._teardown_sdk()

    assert [
        c.args[0] for c in transport.deregister_announce_handler.call_args_list
    ] == [delivery, propagation]
    owner.exit_handler.assert_called_once_with()
