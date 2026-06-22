"""Connector ABC — the only interface an integrator implements."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass(frozen=True)
class FileEntry:
    """A file in the storage backend."""

    path: str
    name: str = ""
    size: int = 0


class Connector(ABC):
    """Abstract storage connector — implement ``upload`` and ``download``.

    Lifecycle:
      1. Runner calls ``list_inbox()`` periodically (poll interval from config).
      2. For each file, runner reads via ``download(path)``, parses as
         YAML/JSON task spec, and calls OpenLoom ``POST /api/webhooks/generic``.
      3. Runner subscribes to OpenLoom webhook for task completion events.
      4. On completion, runner calls ``upload(outbox_path, result_bytes)``.
      5. After upload succeeds, runner calls ``delete_inbox(path)`` to
         clean up the consumed input file.

    Implementations only deal with raw bytes and path strings; OpenLoom
    integration (webhook signing, polling, task parsing, result formatting)
    is handled by the framework.
    """

    inbox_dir: str = "/inbox"
    outbox_dir: str = "/outbox"
    archive_dir: str = ""

    @abstractmethod
    def list_inbox(self) -> list[FileEntry]:
        """List files waiting in the inbox."""
        ...

    @abstractmethod
    def download(self, path: str) -> bytes | None:
        """Download file contents. Return ``None`` if not found."""
        ...

    @abstractmethod
    def upload(self, path: str, content: bytes) -> None:
        """Upload *content* to *path* in the outbox."""
        ...

    @abstractmethod
    def delete_inbox(self, path: str) -> None:
        """Delete a consumed inbox file. No-op if missing."""
        ...


@dataclass
class _Unused:  # silence "field unused" warnings if user subclasses
    _placeholder: dict[str, str] = field(default_factory=dict)
