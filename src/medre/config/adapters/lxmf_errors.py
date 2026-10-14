"""Configuration validation error for the lxmf adapter."""

from medre.config.adapters.errors import AdapterConfigError


class LxmfConfigError(AdapterConfigError):
    """Invalid lxmf adapter configuration."""
