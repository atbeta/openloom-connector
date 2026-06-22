# openloom-connector

Bridge any storage backend to [OpenLoom](https://github.com/atbeta/openloom) by implementing just two methods — `upload` and `download`. OpenLoom handles polling, task creation, and result write-back.

## Install

```bash
pip install openloom-connector
```

Or from source:

```bash
git clone https://github.com/atbeta/openloom-connector
cd openloom-connector
pip install -e .
```

## Quick start

### 1. Initialize a config template

```bash
openloom-connector init
```

This writes `openloom-connector.yaml` with all the fields explained.

### 2. Implement a Connector subclass

You only need 4 methods. Here's the full implementation for a local filesystem (also in `examples/local_fs.py`):

```python
# my_connector.py
from pathlib import Path
from openloom_connector import Connector, FileEntry

class LocalConnector(Connector):
    def __init__(self, base_dir: str) -> None:
        self._base = Path(base_dir)

    def list_inbox(self) -> list[FileEntry]:
        return [
            FileEntry(path=str(p), name=p.name, size=p.stat().st_size)
            for p in sorted((self._base / "inbox").iterdir())
            if p.is_file()
        ]

    def download(self, path: str) -> bytes | None:
        p = Path(path)
        return p.read_bytes() if p.exists() else None

    def upload(self, path: str, content: bytes) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)

    def delete_inbox(self, path: str) -> None:
        Path(path).unlink(missing_ok=True)
```

### 3. Fill in the YAML

```yaml
openloom:
  url: http://127.0.0.1:55413

connector:
  class: my_connector.LocalConnector
  kwargs:
    base_dir: /var/loom

paths:
  inbox: inbox
  outbox: outbox
  archive: archive        # optional

poll_interval_seconds: 10
```

### 4. Validate and run

```bash
openloom-connector validate --config openloom-connector.yaml
openloom-connector run --config openloom-connector.yaml
```

## How it works

```
Your storage                              openloom-connector                    OpenLoom
───────────                              ─────────────────                    ────────
inbox/task1.json ──poll (every 10s)──>   download(task1.json)  ──POST /api/webhooks/generic──>  create task
                                          parse YAML/JSON
                                          extract goal/workspace
                                          record task_id
                                                                                          agent runs...
                                                                                          task completes
                                          <──webhook/SSE──                          write result
outbox/task1.result.json <──upload───    format JSON result
                                          delete inbox/task1.json
                                          (archive if configured)
```

## Task file format

Drop a YAML or JSON file in your inbox:

```json
{
  "goal": "fix the login page CSS bug",
  "workspace": "/Users/me/my-project",
  "name": "Login CSS Fix"
}
```

Only `goal` is required. The runner parses `.json`, `.yaml`, `.yml` files; other extensions are ignored.

## Writing your own Connector

For S3, WebDAV, FTP, your custom HTTP API, etc.:

```python
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

Then point your YAML at it:

```yaml
connector:
  class: my_pkg.MyCloudConnector
  kwargs:
    api_url: https://my-cloud.example.com
    token: xxx
```

## Security

If you set `openloom.signing_secret` in the YAML, every push to OpenLoom includes an HMAC-SHA256 signature in `X-OpenLoom-Signature-256`. OpenLoom's outbound webhook receiver can verify it (see OpenLoom docs for the verification helper).

## Config reference

| Field | Default | Description |
|-------|---------|-------------|
| `openloom.url` | `http://127.0.0.1:55413` | OpenLoom server URL |
| `openloom.source` | `generic` | Webhook source name (matches `@register_source` on OpenLoom side) |
| `openloom.signing_secret` | `""` | Optional HMAC secret |
| `connector.class` | — | **Required.** Dotted path to your Connector subclass |
| `connector.kwargs` | `{}` | Passed to the connector's `__init__` |
| `paths.inbox` | `/inbox` | Where to watch for incoming tasks |
| `paths.outbox` | `/outbox` | Where to write results |
| `paths.archive` | `""` | Optional: archive consumed input files |
| `poll_interval_seconds` | `10` | How often to poll the inbox |
| `state_path` | `null` | Optional: path for connector-side state (e.g. cursor) |

## CLI reference

```bash
openloom-connector init                         # write starter template
openloom-connector validate -c path/to/yaml     # validate config
openloom-connector run -c path/to/yaml          # start polling loop
openloom-connector run -c path/to/yaml -v       # with DEBUG logging
```

## Architecture

```
openloom-connector does NOT import from openloom.
It talks to OpenLoom only through HTTP:
  - POST /api/webhooks/{source}   (push tasks)
  - GET  /api/events              (SSE — listen for completion events, future)
```

This keeps the connector lightweight and decoupled from OpenLoom internals. You can run multiple connectors (different backends) against one OpenLoom server.

## License

MIT
