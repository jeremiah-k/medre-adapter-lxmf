"""SDK construction cannot take process shutdown authority from the runtime."""

import signal
from unittest.mock import MagicMock

import pytest

from medre.adapters.lxmf.errors import LxmfConnectionError
from medre.adapters.lxmf.session import LxmfSession
from medre.config.adapters.lxmf import LxmfConfig


@pytest.mark.parametrize("fail_at", [None, "reticulum", "router"])
async def test_both_sdk_constructors_preserve_runtime_signal_handlers(
    monkeypatch, tmp_path, fail_at
):
    """Restore original handlers on success and either constructor's failure."""
    original = {signal.SIGINT: object(), signal.SIGTERM: object()}
    handlers = dict(original)
    monkeypatch.setattr(signal, "getsignal", handlers.__getitem__)
    monkeypatch.setattr(signal, "signal", handlers.__setitem__)
    rns, lxmf = MagicMock(), MagicMock()
    rns.Reticulum.get_instance.return_value = None

    def construct_reticulum(*args, **kwargs):
        """Model the pinned Reticulum constructor's process signal ownership."""
        handlers.update({key: "rns-handler" for key in original})
        if fail_at == "reticulum":
            raise RuntimeError("Reticulum initialization failed")
        return MagicMock()

    def construct_router(*args, **kwargs):
        """Verify Reticulum's ownership was retired before initializing LXMF."""
        assert handlers == original
        handlers.update({key: "lxmf-handler" for key in original})
        if fail_at == "router":
            raise RuntimeError("LXMF initialization failed")
        return MagicMock()

    rns.Reticulum.side_effect = construct_reticulum
    lxmf.LXMRouter.side_effect = construct_router
    monkeypatch.setattr("medre.adapters.lxmf.session.HAS_LXMF", True)
    monkeypatch.setattr(
        "medre.adapters.lxmf.session._require_lxmf", lambda: (rns, lxmf)
    )
    session = LxmfSession(
        LxmfConfig("radio", storage_path=str(tmp_path / "router")),
        adapter_id="radio",
    )
    if fail_at:
        with pytest.raises(LxmfConnectionError):
            await session._connect_real()
    else:
        await session._connect_real()
    assert handlers == original
