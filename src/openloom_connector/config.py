"""YAML config loader for the connector."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .base import Connector


@dataclass(frozen=True)
class WebhookSource:
    """OpenLoom webhook target configuration."""

    url: str
    signing_secret: str = ""
    source: str = "generic"


@dataclass(frozen=True)
class ConnectorConfig:
    """Resolved connector configuration."""

    connector_class: type[Connector]
    connector_kwargs: dict[str, Any] = field(default_factory=dict)
    openloom_url: str = "http://127.0.0.1:55413"
    webhook: WebhookSource = field(
        default_factory=lambda: WebhookSource(url=""),
    )
    inbox_dir: str = "/inbox"
    outbox_dir: str = "/outbox"
    archive_dir: str = ""
    poll_interval_seconds: int = 10
    state_path: Path | None = None
    task_prefix: str = "task-"
    result_suffix: str = ".result"


def load_config(path: str | Path) -> ConnectorConfig:
    """Load a connector config from a YAML file.

    Expected YAML structure::

        openloom:
          url: http://127.0.0.1:55413
          source: generic

        connector:
          class: my_module.MyConnector
          kwargs:
            api_url: https://example.com
            token: xxx

        paths:
          inbox: /tasks/incoming
          outbox: /tasks/results
          archive: /tasks/archive  # optional

        poll_interval_seconds: 10

        state_path: .openloom-connector/state.json  # optional
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Config must be a YAML mapping")

    connector_spec = raw.get("connector") or {}
    if not isinstance(connector_spec, dict):
        raise ValueError("'connector' must be a mapping")
    class_path = connector_spec.get("class")
    if not class_path:
        raise ValueError("'connector.class' is required (e.g. my_pkg.MyConnector)")
    cls = _import_class(class_path)
    if not (isinstance(cls, type) and issubclass(cls, Connector)):
        raise TypeError(f"{class_path!r} must subclass Connector")

    kwargs = dict(connector_spec.get("kwargs") or {})
    if not isinstance(kwargs, dict):
        raise ValueError("'connector.kwargs' must be a mapping")

    openloom = raw.get("openloom") or {}
    if not isinstance(openloom, dict):
        raise ValueError("'openloom' must be a mapping")
    openloom_url = str(openloom.get("url") or "http://127.0.0.1:55413").rstrip("/")

    paths = raw.get("paths") or {}
    if not isinstance(paths, dict):
        raise ValueError("'paths' must be a mapping")
    inbox_dir = str(paths.get("inbox") or "/inbox")
    outbox_dir = str(paths.get("outbox") or "/outbox")
    archive_dir = str(paths.get("archive") or "")

    poll_interval = int(raw.get("poll_interval_seconds") or 10)

    task_prefix = str(raw.get("task_prefix") or "task-")
    result_suffix = str(raw.get("result_suffix") or ".result")

    webhook_url = str(openloom.get("webhook_url") or f"{openloom_url}/api/webhooks/generic")
    webhook = WebhookSource(
        url=webhook_url,
        signing_secret=str(openloom.get("signing_secret") or ""),
        source=str(openloom.get("source") or "generic"),
    )

    state_raw = raw.get("state_path")
    state_path = Path(state_raw).expanduser() if state_raw else None

    return ConnectorConfig(
        connector_class=cls,
        connector_kwargs=kwargs,
        openloom_url=openloom_url,
        webhook=webhook,
        inbox_dir=inbox_dir,
        outbox_dir=outbox_dir,
        archive_dir=archive_dir,
        poll_interval_seconds=poll_interval,
        state_path=state_path,
        task_prefix=task_prefix,
        result_suffix=result_suffix,
    )


def _import_class(dotted: str) -> type:
    """Import a class from a dotted path like ``my_pkg.module.MyClass``."""
    module_path, _, class_name = dotted.rpartition(".")
    if not module_path:
        raise ImportError(
            f"Invalid dotted path: {dotted!r} (expected MODULE.CLASS)",
        )
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name, None)
    if cls is None:
        raise ImportError(f"Class {class_name!r} not found in module {module_path!r}")
    return cls  # type: ignore[return-value]
