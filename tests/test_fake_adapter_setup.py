"""Fake LXMF setup enforces the same configuration contract as native setup."""

import pytest

from medre.adapters.fakes.lxmf import FakeLxmfAdapter
from medre.config.adapters.lxmf import LxmfConfig, LxmfConfigError


def test_fake_adapter_rejects_invalid_prefix_at_setup():
    with pytest.raises(LxmfConfigError):
        FakeLxmfAdapter(LxmfConfig(adapter_id="test", relay_prefix="{unsupported}"))
