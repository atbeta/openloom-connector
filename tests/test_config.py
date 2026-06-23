"""Tests for ConnectorConfig YAML loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from openloom_connector import Connector
from openloom_connector.config import (
    OPENLOOM_LISTEN_HOST,
    OPENLOOM_LISTEN_PATH,
    OPENLOOM_LISTEN_PORT,
    OPENLOOM_LISTENER_URL,
    load_config,
)


class DummyConnector(Connector):
    def __init__(self, foo: str = "", bar: int = 0) -> None:
        self.foo = foo
        self.bar = bar

    def list_inbox(self): return []
    def download(self, path): return None
    def upload(self, path, content): pass
    def delete_inbox(self, path): pass


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "openloom-connector.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def test_load_minimal_config(tmp_path: Path) -> None:
    """The minimum YAML — connector.class and nothing else — must work.

    The listener URL is hardcoded so the integrator doesn't need any
    webhook / outbound / signing fields; this test pins that contract.
    """
    p = _write(tmp_path, """
connector:
  class: test_config.DummyConnector
""")
    cfg = load_config(p)
    assert cfg.connector_class is DummyConnector
    assert cfg.connector_kwargs == {}
    assert cfg.openloom_url == "http://127.0.0.1:55413"
    assert cfg.inbox_dir == "/inbox"
    assert cfg.outbox_dir == "/outbox"
    assert cfg.archive_dir == ""
    assert cfg.poll_interval_seconds == 10


def test_listener_url_is_hardcoded() -> None:
    """The constant module attributes document the listener
    address. If anyone tries to make these configurable, this test
    fails first so the intent is captured in CI."""
    assert OPENLOOM_LISTEN_HOST == "127.0.0.1"
    assert OPENLOOM_LISTEN_PORT == 55414
    assert OPENLOOM_LISTEN_PATH == "/listener/openloom"
    assert OPENLOOM_LISTENER_URL == "http://127.0.0.1:55414/listener/openloom"


def test_load_full_config(tmp_path: Path) -> None:
    p = _write(tmp_path, f"""
openloom:
  url: http://loom:9000/

connector:
  class: test_config.DummyConnector
  kwargs:
    foo: hello
    bar: 42

paths:
  inbox: /tasks/incoming
  outbox: /tasks/results
  archive: /tasks/archive

poll_interval_seconds: 30

state_path: {tmp_path}/state.json
""")
    cfg = load_config(p)
    assert cfg.openloom_url == "http://loom:9000"
    assert cfg.connector_class is DummyConnector
    assert cfg.connector_kwargs == {"foo": "hello", "bar": 42}
    assert cfg.inbox_dir == "/tasks/incoming"
    assert cfg.outbox_dir == "/tasks/results"
    assert cfg.archive_dir == "/tasks/archive"
    assert cfg.poll_interval_seconds == 30
    assert cfg.state_path == tmp_path / "state.json"


def test_missing_class(tmp_path: Path) -> None:
    p = _write(tmp_path, """
connector:
  kwargs:
    foo: bar
""")
    with pytest.raises(ValueError, match="'connector.class' is required"):
        load_config(p)


def test_class_must_subclass_connector(tmp_path: Path) -> None:
    class NotAConnector:
        pass

    import test_config as _tc
    _tc.NotAConnector = NotAConnector  # type: ignore[attr-defined]

    p = _write(tmp_path, """
connector:
  class: test_config.NotAConnector
""")
    with pytest.raises(TypeError, match="must subclass Connector"):
        load_config(p)


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.yaml")


def test_invalid_yaml_path(tmp_path: Path) -> None:
    p = _write(tmp_path, "- a\n- b\n")
    with pytest.raises(ValueError, match="YAML mapping"):
        load_config(p)


def test_webhook_signing_secret_ignored(tmp_path: Path) -> None:
    """Old configs sometimes still carry ``signing_secret`` from when
    the connector signed outbound requests. We no longer sign (the
    receiver is a private localhost channel) and we silently drop the
    field rather than error — old files keep working without edit."""
    p = _write(tmp_path, """
openloom:
  signing_secret: leftover-from-old-config

connector:
  class: test_config.DummyConnector
""")
    cfg = load_config(p)
    assert cfg.openloom_url == "http://127.0.0.1:55413"


def test_outbound_webhook_block_ignored(tmp_path: Path) -> None:
    """The previous ``outbound_webhook`` block is also silently dropped —
    the listener is always on now. Keeping the loader tolerant means
    old config files keep loading without churn."""
    p = _write(tmp_path, """
connector:
  class: test_config.DummyConnector

outbound_webhook:
  enabled: false
  host: 0.0.0.0
  port: 9001
  path: /events
""")
    cfg = load_config(p)
    # No exception, defaults intact.
    assert cfg.inbox_dir == "/inbox"
