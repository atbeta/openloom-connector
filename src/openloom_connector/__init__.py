"""
openloom-connector — file-based adapter for OpenLoom.

Bridge any storage backend to OpenLoom by implementing two methods:
``upload`` and ``download``. OpenLoom does the rest.
"""

from __future__ import annotations

from .base import Connector, FileEntry
from .config import (
    OPENLOOM_LISTEN_HOST,
    OPENLOOM_LISTEN_PATH,
    OPENLOOM_LISTEN_PORT,
    OPENLOOM_LISTENER_URL,
    ConnectorConfig,
    load_config,
)
from .runner import Runner

__version__ = "0.3.0"

__all__ = [
    "Connector",
    "FileEntry",
    "ConnectorConfig",
    "OPENLOOM_LISTEN_HOST",
    "OPENLOOM_LISTEN_PATH",
    "OPENLOOM_LISTEN_PORT",
    "OPENLOOM_LISTENER_URL",
    "load_config",
    "Runner",
]
