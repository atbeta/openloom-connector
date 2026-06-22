"""Tests for the Connector ABC + FileEntry dataclass."""

from __future__ import annotations

import pytest

from openloom_connector import Connector, FileEntry


def test_connector_is_abstract() -> None:
    with pytest.raises(TypeError):
        Connector()  # type: ignore[abstract]


def test_file_entry_defaults() -> None:
    e = FileEntry(path="/x/y.txt")
    assert e.path == "/x/y.txt"
    assert e.name == ""
    assert e.size == 0


def test_file_entry_frozen() -> None:
    e = FileEntry(path="/x")
    with pytest.raises(Exception):
        e.path = "/tampered"  # type: ignore[misc]


def test_minimal_subclass_implementation() -> None:
    class Minimal(Connector):
        def list_inbox(self):
            return [FileEntry(path="/x.json")]
        def download(self, path):
            return b"{}"
        def upload(self, path, content):
            pass
        def delete_inbox(self, path):
            pass

    c = Minimal()
    assert c.list_inbox()[0].path == "/x.json"
    assert c.download("/x.json") == b"{}"
