"""
Runner — polls a Connector, pushes tasks to OpenLoom, writes back results.

Does not import from OpenLoom internals. Talks to OpenLoom only through:
- HTTP POST to ``/api/webhooks/{source}`` (push task)
- (Future) SSE on ``/api/events`` (listen for task completion)

Task file conventions:
- File name must start with ``task_prefix`` (default ``task-``); non-task
  files (e.g. random docs uploaded to the inbox) are ignored.
- Supported formats: ``.json``, ``.yaml``, ``.yml``, ``.docx``.
- For ``.docx``, the spec is read from a 2-column table (field | value).
- Results are written in the same format as the input (``task-X.json`` →
  ``task-X.result.json``, ``task-X.docx`` → ``task-X.result.docx``).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import logging
import time
from pathlib import PurePosixPath
from typing import Any

import httpx

from .base import Connector, FileEntry
from .config import ConnectorConfig

_logger = logging.getLogger("openloom_connector.runner")

# File extensions that can carry a task spec.
_TASK_EXTENSIONS = {".json", ".yaml", ".yml", ".docx"}

# Map file extension → result suffix (extension).
_RESULT_SUFFIX_BY_EXT = {
    ".json": ".result.json",
    ".yaml": ".result.yaml",
    ".yml": ".result.yml",
    ".docx": ".result.docx",
}


class Runner:
    """Poll-and-forward runner — orchestrates a Connector with OpenLoom."""

    def __init__(self, config: ConnectorConfig) -> None:
        self._config = config
        self._connector: Connector = config.connector_class(**config.connector_kwargs)
        self._seen: set[str] = set()
        self._task_to_file: dict[str, str] = {}
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        """Run the polling loop until ``stop()`` is called."""
        _logger.info(
            "connector started — polling every %ds, prefix=%r",
            self._config.poll_interval_seconds,
            self._config.task_prefix,
        )
        _logger.info("openloom url: %s", self._config.openloom_url)
        _logger.info("webhook:      %s", self._config.webhook.url)
        _logger.info("inbox:        %s", self._config.inbox_dir)
        _logger.info("archive:      %s", self._config.archive_dir or "(disabled)")

        while not self._stopped.is_set():
            try:
                self._poll_once()
            except Exception:
                _logger.exception("poll cycle failed")

            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=self._config.poll_interval_seconds,
                )
            except TimeoutError:
                pass

    # ── poll cycle ──────────────────────────────────────────────────────

    def _poll_once(self) -> None:
        """One polling cycle — list inbox, dispatch unseen task files."""
        entries = self._connector.list_inbox()
        for entry in entries:
            if entry.path in self._seen:
                continue
            name = PurePosixPath(entry.path).name
            if not name.startswith(self._config.task_prefix):
                continue
            ext = PurePosixPath(entry.path).suffix.lower()
            if ext not in _TASK_EXTENSIONS:
                continue
            self._dispatch(entry)

    def _dispatch(self, entry: FileEntry) -> None:
        """Download one file, push to OpenLoom, register for completion."""
        content = self._connector.download(entry.path)
        if content is None:
            return
        spec = _parse_spec(content, entry.path)
        if spec is None:
            _logger.warning("skipping %s — not a valid task spec", entry.path)
            self._seen.add(entry.path)
            return
        goal = str(spec.get("goal") or "").strip()
        if not goal:
            _logger.warning("skipping %s — missing 'goal'", entry.path)
            self._seen.add(entry.path)
            return
        workspace = str(spec.get("workspace") or spec.get("cwd") or "").strip()
        session_id = str(spec.get("sessionId") or spec.get("session_id") or "").strip()
        if not workspace and not session_id:
            _logger.warning(
                "skipping %s — need workspace or sessionId", entry.path,
            )
            self._seen.add(entry.path)
            return

        task_id = self._push_to_openloom(spec, entry)
        if task_id:
            self._seen.add(entry.path)
            self._task_to_file[task_id] = entry.path
            _logger.info(
                "pushed task %s from %s (workspace=%r sessionId=%r)",
                task_id, entry.path, workspace, session_id,
            )

    def _push_to_openloom(self, spec: dict[str, Any], entry: FileEntry) -> str | None:
        """POST task spec to OpenLoom webhook. Return task_id or None."""
        url = self._config.webhook.url
        if not url:
            _logger.error("webhook url not configured")
            return None
        payload = json.dumps(spec).encode()
        headers = {"Content-Type": "application/json"}
        if self._config.webhook.signing_secret:
            sig = _sign_payload(self._config.webhook.signing_secret, payload)
            headers["X-OpenLoom-Signature-256"] = f"sha256={sig}"

        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, content=payload, headers=headers)
        except httpx.HTTPError as exc:
            _logger.error("POST %s failed: %s", url, exc)
            return None

        if resp.status_code >= 400:
            _logger.error(
                "POST %s returned %s: %s", url, resp.status_code, resp.text[:200],
            )
            return None
        try:
            data = resp.json()
        except Exception:
            return None
        return data.get("taskId") or data.get("task_id")

    # ── result write-back ───────────────────────────────────────────────

    def write_result(
        self,
        task_id: str,
        task_name: str,
        status: str,
        data: dict[str, Any],
    ) -> None:
        """Write a task result back to the connector's outbox.

        The result is written in the same format as the input file
        (``task-X.json`` → ``task-X.result.json`` etc.).
        """
        source_file = self._task_to_file.pop(task_id, None)
        if source_file is None:
            return

        source_ext = PurePosixPath(source_file).suffix.lower()
        result_suffix = _RESULT_SUFFIX_BY_EXT.get(source_ext, ".result.json")
        source_stem = _strip_prefix(
            PurePosixPath(source_file).name,
            self._config.task_prefix,
        )
        source_stem = PurePosixPath(source_stem).stem
        out_name = f"{self._config.task_prefix}{source_stem}{result_suffix}"
        out_path = f"{self._config.outbox_dir}/{out_name}"

        result = {
            "schema_version": "1.0",
            "task_id": task_id,
            "task_name": task_name,
            "status": status,
            "timestamp": time.time(),
            "data": data,
        }

        try:
            content = _render_result(result, source_ext)
            self._connector.upload(out_path, content)
            _logger.info("wrote result to %s", out_path)
        except Exception:
            _logger.exception("upload failed for %s", out_path)
            return

        # Archive + delete the consumed input file
        if self._config.archive_dir:
            archive_path = f"{self._config.archive_dir}/{PurePosixPath(source_file).name}"
            try:
                original = self._connector.download(source_file)
                if original is not None:
                    self._connector.upload(archive_path, original)
            except Exception:
                _logger.exception("archive failed for %s", archive_path)

        try:
            self._connector.delete_inbox(source_file)
        except Exception:
            _logger.exception("delete_inbox failed for %s", source_file)


# ── spec parsing ──────────────────────────────────────────────────────────


def _parse_spec(raw: bytes, filepath: str) -> dict[str, Any] | None:
    """Parse a task file (json/yaml/docx) into a normalized spec dict."""
    ext = PurePosixPath(filepath).suffix.lower()
    try:
        if ext == ".json":
            data = json.loads(raw)
            return data if isinstance(data, dict) else None
        if ext in (".yaml", ".yml"):
            import yaml
            data = yaml.safe_load(raw)
            return data if isinstance(data, dict) else None
        if ext == ".docx":
            return _parse_docx(raw)
    except Exception:
        return None
    return None


def _parse_docx(raw: bytes) -> dict[str, Any] | None:
    """Parse a docx task file: first table with 2 columns (field | value).

    Recognized field names (case-insensitive): ``goal``, ``workspace``,
    ``sessionId``, ``session_id``, ``name``. Any other field goes into
    ``metadata``.
    """
    from docx import Document  # python-docx

    doc = Document(io.BytesIO(raw))
    tables = doc.tables
    if not tables:
        return None

    spec: dict[str, Any] = {}
    metadata: dict[str, Any] = {}
    for row in tables[0].rows:
        if len(row.cells) < 2:
            continue
        key = row.cells[0].text.strip().lower()
        value = row.cells[1].text.strip()
        if not key:
            continue
        if key in ("goal", "workspace", "cwd", "name", "title", "sessionid", "session_id"):
            if key == "sessionid":
                key = "sessionId"
            spec[key] = value
        else:
            metadata[key] = value

    if metadata:
        spec["metadata"] = metadata
    return spec or None


# ── result rendering ──────────────────────────────────────────────────────


def _render_result(result: dict[str, Any], source_ext: str) -> bytes:
    """Render a result in the same format as the input."""
    if source_ext == ".json":
        return json.dumps(result, indent=2, ensure_ascii=False).encode()
    if source_ext in (".yaml", ".yml"):
        import yaml
        return yaml.safe_dump(result, allow_unicode=True).encode()
    if source_ext == ".docx":
        return _render_docx_result(result).read()
    return json.dumps(result, indent=2).encode()


def _render_docx_result(result: dict[str, Any]) -> io.BytesIO:
    """Render a result as a 2-column docx table for human reading."""
    from docx import Document

    doc = Document()
    doc.add_heading("OpenLoom Task Result", level=1)

    table = doc.add_table(rows=0, cols=2)
    table.style = "Light List Accent 1"
    for key, value in _flatten_for_docx(result):
        row = table.add_row().cells
        row[0].text = str(key)
        row[1].text = str(value)

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf


def _flatten_for_docx(result: dict[str, Any]) -> list[tuple[str, str]]:
    """Flatten a result dict into (key, str_value) rows for the docx table."""
    flat: list[tuple[str, str]] = []
    for key in ("schema_version", "task_id", "task_name", "status"):
        if key in result:
            flat.append((key, str(result[key])))
    if "timestamp" in result:
        from datetime import UTC, datetime
        ts = result["timestamp"]
        flat.append(("timestamp", str(ts)))
        flat.append(("timestamp_iso", datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")))
    data = result.get("data") or {}
    if isinstance(data, dict):
        for k, v in data.items():
            flat.append((f"data.{k}", str(v)))
    elif data:
        flat.append(("data", str(data)))
    return flat


# ── helpers ───────────────────────────────────────────────────────────────


def _sign_payload(secret: str, payload: bytes) -> str:
    return hmac.new(
        secret.encode("utf-8"), payload, hashlib.sha256,
    ).hexdigest()


def _strip_prefix(name: str, prefix: str) -> str:
    return name[len(prefix):] if name.startswith(prefix) else name
