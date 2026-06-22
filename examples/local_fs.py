"""
LocalFilesystemConnector — example Connector for local files.

Use this as a template for writing your own. The 4 methods are
all you need; the runner handles everything else.
"""

from __future__ import annotations

from pathlib import Path

from openloom_connector import Connector, FileEntry


class LocalFilesystemConnector(Connector):
    """Reads from a local inbox/ directory, writes to outbox/."""

    def __init__(self, base_dir: str) -> None:
        self._base = Path(base_dir).expanduser().resolve()

    def _resolve(self, path: str) -> Path:
        """Resolve *path* under base_dir if not already absolute."""
        p = Path(path)
        if not p.is_absolute():
            p = self._base / p
        return p

    def list_inbox(self) -> list[FileEntry]:
        inbox = self._base / self.inbox_dir.lstrip("/")
        if not inbox.exists():
            return []
        entries: list[FileEntry] = []
        for p in sorted(inbox.iterdir()):
            if p.is_file():
                entries.append(FileEntry(path=str(p), name=p.name, size=p.stat().st_size))
        return entries

    def download(self, path: str) -> bytes | None:
        p = self._resolve(path)
        if not p.exists():
            return None
        return p.read_bytes()

    def upload(self, path: str, content: bytes) -> None:
        p = self._resolve(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)

    def delete_inbox(self, path: str) -> None:
        p = self._resolve(path)
        if p.exists():
            p.unlink()
