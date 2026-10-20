"""Adapter-owned runtime configuration wrapper for lxmf."""

from dataclasses import dataclass
from typing import Any, Self

from medre.config.adapters.lxmf import LxmfConfig
from medre.config.model import GenericAdapterRuntimeConfig


@dataclass(frozen=True)
class LxmfRuntimeConfig(GenericAdapterRuntimeConfig):
    """Validated runtime configuration for one lxmf instance."""

    config: LxmfConfig | None = None

    @classmethod
    def from_dict(cls, instance_name: str, data: dict[str, Any]) -> Self:
        return cls.from_transport_dict(
            instance_name, data, transport="lxmf", config_cls=LxmfConfig
        )
