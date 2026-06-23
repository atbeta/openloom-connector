"""
openloom-connector — file-based adapter for OpenLoom.

Bridge any storage backend to OpenLoom by implementing two methods:
``upload`` and ``download``. OpenLoom does the rest.
"""

from __future__ import annotations

from .base import Connector, FileEntry
from .config import ConnectorConfig, OutboundWebhookConfig, load_config
from .runner import Runner

__version__ = "0.2.0"

__all__ = [
    "Connector",
    "FileEntry",
    "ConnectorConfig",
    "OutboundWebhookConfig",
    "load_config",
    "Runner",
]
