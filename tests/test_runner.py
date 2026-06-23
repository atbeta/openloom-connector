"""Tests for the Runner — poll cycle, push to OpenLoom, result write-back."""

from __future__ import annotations

import io
import json
from typing import Any

import httpx
import pytest
import respx

from openloom_connector import Connector, FileEntry
from openloom_connector.config import ConnectorConfig, WebhookSource
from openloom_connector.runner import Runner, _parse_docx, _parse_spec


class InMemoryConnector(Connector):
    """Connector backed by an in-memory dict — for tests."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def list_inbox(self) -> list[FileEntry]:
        prefix = self.inbox_dir.rstrip("/") + "/"
        return [
            FileEntry(path=p, name=p.rsplit("/", 1)[-1], size=len(b))
            for p, b in self.files.items() if p.startswith(prefix)
        ]

    def download(self, path: str) -> bytes | None:
        return self.files.get(path)

    def upload(self, path: str, content: bytes) -> None:
        self.files[path] = content

    def delete_inbox(self, path: str) -> None:
        self.files.pop(path, None)


def _config(connector_class: type[Connector] = InMemoryConnector, **kwargs: Any) -> ConnectorConfig:
    poll = kwargs.pop("poll_interval_seconds", 1)
    inbox = kwargs.pop("inbox_dir", "/inbox")
    outbox = kwargs.pop("outbox_dir", "/outbox")
    archive = kwargs.pop("archive_dir", "")
    secret = kwargs.pop("signing_secret", "")
    prefix = kwargs.pop("task_prefix", "task-")
    return ConnectorConfig(
        connector_class=connector_class,
        connector_kwargs=kwargs,
        openloom_url="http://loom:55413",
        webhook=WebhookSource(url="http://loom:55413/api/webhooks/generic", signing_secret=secret),
        inbox_dir=inbox,
        outbox_dir=outbox,
        archive_dir=archive,
        poll_interval_seconds=poll,
        task_prefix=prefix,
    )


def _build_docx(rows: list[tuple[str, str]]) -> bytes:
    """Build a minimal docx with a single table containing *rows*."""
    from docx import Document

    doc = Document()
    table = doc.add_table(rows=0, cols=2)
    for k, v in rows:
        row = table.add_row().cells
        row[0].text = k
        row[1].text = v
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


# ── _parse_spec ──────────────────────────────────────────────────────────


def test_parse_json() -> None:
    assert _parse_spec(b'{"goal":"x"}', "task-t.json") == {"goal": "x"}


def test_parse_yaml() -> None:
    assert _parse_spec(b"goal: y\n", "task-t.yaml") == {"goal": "y"}


def test_parse_invalid_returns_none() -> None:
    assert _parse_spec(b"not json", "task-t.json") is None
    assert _parse_spec(b":invalid: yaml:", "task-t.yaml") is None
    assert _parse_spec(b"not a dict", "task-t.json") is None


def test_parse_docx_basic() -> None:
    raw = _build_docx([
        ("goal", "fix login CSS"),
        ("workspace", "/Users/me/proj"),
        ("name", "CSS Fix"),
    ])
    spec = _parse_spec(raw, "task-t.docx")
    assert spec == {
        "goal": "fix login CSS",
        "workspace": "/Users/me/proj",
        "name": "CSS Fix",
    }


def test_parse_docx_session_id_and_metadata() -> None:
    raw = _build_docx([
        ("goal", "continue review"),
        ("sessionId", "ses_abc"),
        ("branch", "main"),
        ("priority", "high"),
    ])
    spec = _parse_spec(raw, "task-t.docx")
    assert spec is not None
    assert spec["goal"] == "continue review"
    assert spec["sessionId"] == "ses_abc"
    assert spec["metadata"] == {"branch": "main", "priority": "high"}


def test_parse_docx_empty_returns_none() -> None:
    raw = _build_docx([])
    assert _parse_docx(raw) is None


def test_parse_docx_ignores_empty_rows() -> None:
    raw = _build_docx([("", ""), ("goal", "x"), ("", "y")])
    spec = _parse_docx(raw)
    assert spec == {"goal": "x"}


# ── prefix filtering ──────────────────────────────────────────────────────


def test_poll_skips_files_without_prefix() -> None:
    """Non-task files (no `task-` prefix) are ignored — the inbox may
    contain arbitrary docs uploaded by teammates."""
    conn = InMemoryConnector()
    conn.files["/inbox/random-doc.json"] = json.dumps({"goal": "x"}).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 0


def test_poll_custom_prefix() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/loom-fix.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config(task_prefix="loom-"))
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 1


# ── poll cycle ───────────────────────────────────────────────────────────


def test_poll_dispatches_new_files() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-task1.json"] = json.dumps({
        "goal": "fix bug", "workspace": "/p",
    }).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"ok": True, "taskId": "task_abc"}),
        )
        runner._poll_once()

    assert "task_abc" in runner._task_to_file
    assert "/inbox/task-task1.json" in runner._seen


def test_poll_skips_seen_files() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()
        runner._poll_once()

    assert route.call_count == 1


def test_poll_skips_files_without_goal() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-bad.json"] = json.dumps({"name": "no goal", "workspace": "/p"}).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 0
    assert "/inbox/task-bad.json" in runner._seen


def test_poll_skips_files_without_workspace_or_session() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "no workspace"}).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 0


def test_poll_accepts_session_id_without_workspace() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({
        "goal": "continue", "sessionId": "ses_xyz",
    }).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 1
    body = json.loads(route.calls.last.request.content)
    assert body["sessionId"] == "ses_xyz"


def test_poll_skips_non_task_extensions() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-readme.txt"] = b"text file"
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 0


def test_poll_dispatches_docx() -> None:
    conn = InMemoryConnector()
    raw = _build_docx([("goal", "from docx"), ("workspace", "/p")])
    conn.files["/inbox/task-from-docx.docx"] = raw
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "task_docx"}),
        )
        runner._poll_once()

    assert route.call_count == 1
    body = json.loads(route.calls.last.request.content)
    assert body["goal"] == "from docx"


# ── write_result (json input) ────────────────────────────────────────────


def test_write_result_uploads_and_deletes() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config())
    runner._connector = conn
    runner._task_to_file["task_xyz"] = "/inbox/task-t.json"
    runner._seen.add("/inbox/task-t.json")

    runner.write_result("task_xyz", "t", "completed", {"summary": "ok"})

    out_files = [p for p in conn.files if p.startswith("/outbox/")]
    assert out_files == ["/outbox/task-t.result.json"]
    result = json.loads(conn.files["/outbox/task-t.result.json"])
    assert result["task_id"] == "task_xyz"
    assert result["status"] == "completed"
    assert result["data"] == {"summary": "ok"}
    assert "/inbox/task-t.json" not in conn.files


def test_write_result_with_archive() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config(archive_dir="/archive"))
    runner._connector = conn
    runner._task_to_file["task_xyz"] = "/inbox/task-t.json"

    runner.write_result("task_xyz", "t", "completed", {})

    assert "/inbox/task-t.json" not in conn.files  # deleted
    archive_files = [p for p in conn.files if p.startswith("/archive/")]
    assert archive_files == ["/archive/task-t.json"]


def test_write_result_unknown_task_noop() -> None:
    conn = InMemoryConnector()
    runner = Runner(_config())
    runner._connector = conn

    runner.write_result("nonexistent_task", "t", "completed", {})

    assert conn.files == {}


# ── write_result (docx input → docx output) ─────────────────────────────


def test_write_result_docx_round_trip() -> None:
    conn = InMemoryConnector()
    raw = _build_docx([("goal", "from docx"), ("workspace", "/p")])
    conn.files["/inbox/task-doc.docx"] = raw
    runner = Runner(_config(archive_dir="/archive"))
    runner._connector = conn
    runner._task_to_file["task_doc"] = "/inbox/task-doc.docx"

    runner.write_result("task_doc", "doc task", "completed", {"summary": "done"})

    # Result should be a docx
    out_files = [p for p in conn.files if p.startswith("/outbox/")]
    assert out_files == ["/outbox/task-doc.result.docx"]
    result_bytes = conn.files["/outbox/task-doc.result.docx"]
    assert result_bytes[:2] == b"PK"  # docx is a zip

    # Parse it back
    from docx import Document
    doc = Document(io.BytesIO(result_bytes))
    rows = {doc.tables[0].rows[i].cells[0].text: doc.tables[0].rows[i].cells[1].text
            for i in range(len(doc.tables[0].rows))}
    assert rows["task_id"] == "task_doc"
    assert rows["status"] == "completed"
    assert rows["task_name"] == "doc task"
    assert rows["data.summary"] == "done"

    # Archive + delete
    assert "/inbox/task-doc.docx" not in conn.files
    assert "/archive/task-doc.docx" in conn.files


# ── webhook signing ─────────────────────────────────────────────────────


def test_webhook_signs_when_secret_set() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config(signing_secret="topsecret"))
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    sig = route.calls.last.request.headers.get("X-OpenLoom-Signature-256")
    assert sig is not None
    assert sig.startswith("sha256=")
    assert len(sig) == len("sha256=") + 64


def test_webhook_no_signature_when_no_secret() -> None:
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config())
    runner._connector = conn

    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert "X-OpenLoom-Signature-256" not in route.calls.last.request.headers


# ── run loop ─────────────────────────────────────────────────────────────


async def test_run_loop_stops_on_signal() -> None:
    conn = InMemoryConnector()
    runner = Runner(_config(poll_interval_seconds=0))
    runner._connector = conn

    async def stop_soon() -> None:
        import asyncio
        await asyncio.sleep(0.05)
        runner.stop()

    import asyncio
    await asyncio.gather(runner.run(), stop_soon())


def test_push_ignores_system_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    """If HTTP_PROXY is set, the connector must still POST directly to
    127.0.0.1. Regression test for the 'Content Filter - Access Denied'
    symptom caused by httpx honouring system proxy env vars."""
    conn = InMemoryConnector()
    conn.files["/inbox/task-t.json"] = json.dumps({"goal": "x", "workspace": "/p"}).encode()
    runner = Runner(_config())
    runner._connector = conn

    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:9999")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:9999")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.invalid:9999")

    # respx mocks the loopback address, not the proxy. If trust_env is on,
    # httpx routes through the proxy and respx sees nothing. If trust_env
    # is off, the mock intercepts the request directly.
    with respx.mock:
        route = respx.post("http://loom:55413/api/webhooks/generic").mock(
            return_value=httpx.Response(200, json={"taskId": "t1"}),
        )
        runner._poll_once()

    assert route.call_count == 1
