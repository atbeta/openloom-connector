"""Tests for ConnectorConfig YAML loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from openloom_connector import Connector
from openloom_connector.config import load_config


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
    p = _write(tmp_path, """
connector:
  class: test_config.DummyConnector
""")
    cfg = load_config(p)
    assert cfg.connector_class is DummyConnector
    assert cfg.connector_kwargs == {}
    assert cfg.openloom_url == "http://127.0.0.1:55413"
    assert cfg.webhook.url == "http://127.0.0.1:55413/api/webhooks/generic"
    assert cfg.inbox_dir == "/inbox"
    assert cfg.outbox_dir == "/outbox"
    assert cfg.archive_dir == ""
    assert cfg.poll_interval_seconds == 10


def test_load_full_config(tmp_path: Path) -> None:
    p = _write(tmp_path, f"""
openloom:
  url: http://loom:9000/
  signing_secret: s3cret

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
    assert cfg.webhook.signing_secret == "s3cret"
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

    # Re-import the class into the test module so the dotted path resolves
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


def test_custom_source(tmp_path: Path) -> None:
    p = _write(tmp_path, """
openloom:
  source: github

connector:
  class: test_config.DummyConnector
""")
    cfg = load_config(p)
    assert cfg.webhook.source == "github"
    assert cfg.webhook.url.endswith("/api/webhooks/generic")
