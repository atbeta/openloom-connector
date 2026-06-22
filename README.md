# openloom-connector

File-based adapter for [OpenLoom](https://github.com/atbeta/openloom) — bridge any storage backend by implementing four methods. OpenLoom handles polling, task creation, and result write-back; you only deal with `list`, `download`, `upload`, `delete`.

Typical use: your team writes task files (`.json` / `.yaml` / `.docx`) to a shared drive (phone-friendly, behind any firewall). The connector polls, converts the file to an OpenLoom webhook, and writes the result back when the task completes.

## What you implement vs what the framework handles

| You write | The framework handles |
|-----------|----------------------|
| 4 methods (`list_inbox` / `download` / `upload` / `delete_inbox`) | YAML config parsing, polling timer, YAML/JSON/docx parsing, HMAC signing, result rendering (same format as input), archiving, error logging |
| ~30 lines for typical REST storage | Everything else |

Built-in support for `.json`, `.yaml`, `.yml`, and **`.docx`** (two-column table). Result files mirror the input format.

## Install

Pick **one** of the methods below.

### Option A — Install from GitHub (recommended, always current)

```bash
uv tool install git+https://github.com/atbeta/openloom-connector.git
```

For a specific branch or tag:

```bash
uv tool install git+https://github.com/atbeta/openloom-connector.git@v0.1.0
uv tool install git+https://github.com/atbeta/openloom-connector.git@main
```

For SSH (if your GitHub account uses key auth):

```bash
uv tool install git+ssh://git@github.com/atbeta/openloom-connector.git
```

After install, the `openloom-connector` command is available globally. Re-run with `--reinstall` after pulling changes.

### Option B — Install from a local clone (for active development)

```bash
git clone https://github.com/atbeta/openloom-connector.git
cd openloom-connector
uv tool install --editable .
```

The `--editable` flag means edits to the source are immediately effective — no reinstall needed. Use this when you're iterating on your `Connector` subclass.

### Option C — Run directly from a clone (no install)

```bash
git clone https://github.com/atbeta/openloom-connector.git
cd openloom-connector
uv run openloom-connector run -c my-config.yaml
```

The `uv run` prefix sets up the venv on first invocation and runs the CLI in it. No global install needed.

### Option D — Pin as a dependency of another project

In your project's `pyproject.toml`:

```toml
[project]
dependencies = [
  "openloom-connector @ git+https://github.com/atbeta/openloom-connector.git",
]
```

Then `uv sync` and `import openloom_connector`.

### What the package does NOT do

- It is **not on PyPI**. We don't publish this to a public index — it's an internal adapter, not a general-purpose library.
- It does not auto-update. Run the install command again to refresh.

## Quick start

### 1. Initialize a config template

```bash
openloom-connector init
```

This writes `openloom-connector.yaml` with all the fields explained.

### 2. Implement a `Connector` subclass

```python
# my_connector.py
import httpx
from openloom_connector import Connector, FileEntry

class MyCloudConnector(Connector):
    def __init__(self, api_url: str, token: str) -> None:
        self._api = api_url
        self._h = {"Authorization": f"Bearer {token}"}

    def list_inbox(self) -> list[FileEntry]:
        r = httpx.get(f"{self._api}/files", params={"dir": self.inbox_dir}, headers=self._h)
        return [FileEntry(path=f["path"], name=f["name"], size=f["size"]) for f in r.json()]

    def download(self, path: str) -> bytes | None:
        r = httpx.get(f"{self._api}/file", params={"path": path}, headers=self._h)
        return r.content if r.status_code == 200 else None

    def upload(self, path: str, content: bytes) -> None:
        httpx.put(f"{self._api}/file", params={"path": path}, content=content, headers=self._h)

    def delete_inbox(self, path: str) -> None:
        httpx.delete(f"{self._api}/file", params={"path": path}, headers=self._h)
```

### 3. Fill in the YAML

```yaml
openloom:
  url: http://127.0.0.1:55413

connector:
  class: my_connector.MyCloudConnector
  kwargs:
    api_url: https://example.com
    token: xxx

paths:
  inbox: /inbox
  outbox: /outbox
  archive: /archive        # optional

poll_interval_seconds: 10
```

### 4. Validate and run

```bash
openloom-connector validate --config openloom-connector.yaml
openloom-connector run --config openloom-connector.yaml
```

## Task file format

| Format | How it's parsed |
|--------|-----------------|
| `.json` | Standard JSON, must contain at least `goal` and `workspace` (or `sessionId`) |
| `.yaml` / `.yml` | Standard YAML, same fields as JSON |
| `.docx` | First 2-column table in the document; recognized keys: `goal`, `workspace` (or `cwd`), `sessionId` (or `session_id`), `name` (or `title`); other keys go into `metadata` |

Only files whose name starts with `task-` (configurable via `task_prefix`) are processed. Other files in the inbox are ignored — your storage can hold any other documents.

### JSON example

```json
{
  "goal": "fix the login page CSS",
  "workspace": "/Users/me/project",
  "name": "CSS Fix",
  "sessionId": "ses_existing_xyz"
}
```

### docx example

| goal | fix the login page CSS |
|------|----------------------|
| workspace | /Users/me/project |
| name | CSS Fix |
| sessionId | ses_existing_xyz |

## Lifecycle

```
inbox/task-001.json  ─poll (every 10s)→  download → parse → POST /api/webhooks/generic
                                                                    ↓ OpenLoom creates task
                                                                    ↓ agent runs
                                                                    ↓ task completes
outbox/task-001.result.json  ←upload─  format result
inbox/task-001.json  ─delete─  consumed input cleared
                         (also archived to /archive if configured)
```

## Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `openloom.url` | `http://127.0.0.1:55413` | OpenLoom server URL |
| `openloom.source` | `generic` | Webhook source name (matches `@register_source` on the OpenLoom side) |
| `openloom.signing_secret` | `""` | Optional HMAC secret for signing the inbound webhook |
| `connector.class` | — | **Required.** Dotted path to your `Connector` subclass |
| `connector.kwargs` | `{}` | Passed to the connector's `__init__` |
| `paths.inbox` | `/inbox` | Where to watch for incoming task files |
| `paths.outbox` | `/outbox` | Where to write result files |
| `paths.archive` | `""` | Optional: archive consumed input files here |
| `poll_interval_seconds` | `10` | How often to poll the inbox |
| `task_prefix` | `task-` | Files must start with this prefix to be processed |
| `state_path` | `null` | Optional: path for connector-side state (e.g. cursor) |

## CLI reference

```bash
openloom-connector init                          # write starter template
openloom-connector validate -c path/to/yaml      # validate config
openloom-connector run -c path/to/yaml           # start polling loop
openloom-connector run -c path/to/yaml -v        # with DEBUG logging
```

## Architecture

`openloom-connector` does **not** import from `openloom`. It talks to OpenLoom only through HTTP:

- `POST /api/webhooks/{source}` — push tasks
- (Future) `GET /api/events` SSE — listen for completion events

This keeps the connector lightweight and decoupled from OpenLoom internals. You can run multiple connectors (different backends) against one OpenLoom server.

## Development

```bash
git clone https://github.com/atbeta/openloom-connector.git
cd openloom-connector
uv sync --group dev
uv run pytest -q
uv run ruff check src tests
```

## License

MIT
