"""Validate a selected live Reticulum profile without opening the SDK or radio."""

from pathlib import Path


def resolve_live_config_dir(value: str) -> str | None:
    """An omitted profile allows ordinary (not RF-qualified) connectivity smoke."""
    if value == "":
        return None
    if not value.strip():
        raise ValueError("LXMF_RETICULUM_CONFIG_DIR must not be blank")
    try:
        directory = Path(value).expanduser().resolve(strict=True)
        config = directory / "config"
        if not directory.is_dir() or not config.is_file() or config.stat().st_size == 0:
            raise ValueError(
                "LXMF_RETICULUM_CONFIG_DIR must contain a nonempty config file"
            )
    except (OSError, RuntimeError) as exc:
        raise ValueError(
            "LXMF_RETICULUM_CONFIG_DIR does not resolve to a profile"
        ) from exc
    return str(directory)
