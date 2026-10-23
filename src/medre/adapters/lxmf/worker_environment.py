"""Explicit runtime environment passed to LXMF-owned subprocesses."""

from __future__ import annotations

import os

_KEYS = frozenset(
    {
        "PATH",
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "PROGRAMDATA",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "SYSTEMDRIVE",
        "HOMEDRIVE",
        "HOMEPATH",
        "TEMP",
        "TMP",
        "TMPDIR",
        "LANG",
        "LANGUAGE",
        "TZ",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "XDG_CACHE_HOME",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONPYCACHEPREFIX",
        "PYTHONNOUSERSITE",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONWARNINGS",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    }
)


def native_environment() -> dict[str, str]:
    """Retain interpreter/SDK paths and locale without other adapters' secrets."""
    selected = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in _KEYS or key.upper().startswith("LC_")
    }
    selected["PYTHONUNBUFFERED"] = "1"
    return selected
