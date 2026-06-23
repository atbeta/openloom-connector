"""YAML config loader for the connector.

The connector's config is intentionally minimal. Every field below
maps to behaviour the *integrator* cares about — which storage
backend, which inbox/outbox paths, how often to poll. The webhook
listener (where OpenLoom pushes completion events) is **not**
configurable: it always binds to ``127.0.0.1:55414/listener/openloom``
so the integrator never has to copy the URL from OpenLoom's
perspective into the connector's YAML.

OpenLoom itself, on the other hand, is the part that decides where
to send its outbound events — it stays an open webhook emitter,
configured by ``OPENLOOM_NOTIFY_WEBHOOK_URLS``. The connector just
makes sure the listening side is predictable.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .base import Connector

# Hardcoded listener address. OpenLoom should be configured with
# ``OPENLOOM_NOTIFY_WEBHOOK_URLS`` pointing at this URL. Keeping the
# value constant means there's exactly one place in the system that
# has to know where the receiver lives.
OPENLOOM_LISTEN_HOST = "127.0.0.1"
OPENLOOM_LISTEN_PORT = 55414
OPENLOOM_LISTEN_PATH = "/listener/openloom"
OPENLOOM_LISTENER_URL = (
    f"http://{OPENLOOM_LISTEN_HOST}:{OPENLOOM_LISTEN_PORT}{OPENLOOM_LISTEN_PATH}"
)


@dataclass(frozen=True)
class ConnectorConfig:
    """Resolved connector configuration.

    The connector only ever talks to one OpenLoom (``openloom_url``,
    which the connector uses to POST new task files). It listens for
    completion events at the constant ``OPENLOOM_LISTENER_URL`` — that
    side is intentionally not configurable here.
    """

    connector_class: type[Connector]
    connector_kwargs: dict[str, Any] = field(default_factory=dict)
    openloom_url: str = "http://127.0.0.1:55413"
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

        connector:
          class: my_module.MyConnector
          kwargs:
            api_url: https://example.com
            token: xxx

        openloom:
          url: http://127.0.0.1:55413   # optional; default shown

        paths:
          inbox: /tasks/incoming         # optional; default /inbox
          outbox: /tasks/results         # optional; default /outbox
          archive: /tasks/archive        # optional

        poll_interval_seconds: 10         # optional
        task_prefix: task-                 # optional
        state_path: .openloom-connector/state.json   # optional

    See ``OPENLOOM_LISTENER_URL`` for the address the connector
    listens on for OpenLoom's outbound events.
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

    state_raw = raw.get("state_path")
    state_path = Path(state_raw).expanduser() if state_raw else None

    return ConnectorConfig(
        connector_class=cls,
        connector_kwargs=kwargs,
        openloom_url=openloom_url,
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
