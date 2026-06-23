"""openloom-connector CLI entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from . import __version__
from .config import (
    OPENLOOM_LISTENER_URL,
    load_config,
)
from .runner import Runner


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="openloom-connector",
        description=(
            "Bridge any storage backend to OpenLoom via upload/download. "
            "Configure a YAML + implement a Connector subclass to start."
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging")

    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Start the polling loop")
    run_p.add_argument(
        "--config", "-c", default="openloom-connector.yaml",
        help="Path to YAML config (default: openloom-connector.yaml)",
    )

    val_p = sub.add_parser("validate", help="Validate the config file and exit")
    val_p.add_argument(
        "--config", "-c", default="openloom-connector.yaml",
        help="Path to YAML config (default: openloom-connector.yaml)",
    )

    init_p = sub.add_parser("init", help="Write a starter template YAML")
    init_p.add_argument(
        "target", nargs="?", default="openloom-connector.yaml",
        help="Path to write template to (default: openloom-connector.yaml)",
    )

    args = parser.parse_args()
    _configure_logging(args.verbose)

    if args.command == "init":
        _write_template(args.target)
        return

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        print("Run `openloom-connector init` to create a starter template.", file=sys.stderr)
        sys.exit(1)

    config = load_config(config_path)

    if args.command == "validate":
        print(f"OK — connector: {config.connector_class.__name__}")
        print(f"    openloom:   {config.openloom_url}")
        print(f"    listener:   {OPENLOOM_LISTENER_URL}")
        print(f"    inbox:      {config.inbox_dir}")
        print(f"    outbox:     {config.outbox_dir}")
        print(f"    poll:       {config.poll_interval_seconds}s")
        return

    print(f"openloom-connector {__version__}")
    runner = Runner(config)

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, runner.stop)
            except NotImplementedError:
                pass
        await runner.run()

    asyncio.run(_main())


_TEMPLATE = """\
# openloom-connector config — minimum you need to fill in.
#
# The connector always listens for OpenLoom's outbound webhook events
# at http://127.0.0.1:55414/listener/openloom — you do not need to
# configure this. Just point OpenLoom's OPENLOOM_NOTIFY_WEBHOOK_URLS
# at that URL when you start it.

connector:
  class: my_package.MyConnector
  kwargs:
    # api_url: https://example.com
    # token: xxx

paths:
  inbox: /inbox                 # where to watch for incoming task files
  outbox: /outbox               # where to write result files
  archive: ""                   # optional: archive consumed input files here

poll_interval_seconds: 10

# state_path: .openloom-connector/state.json   # optional — local connector state
"""


def _write_template(target: str) -> None:
    out = Path(target)
    if out.exists():
        print(f"Already exists: {out}", file=sys.stderr)
        sys.exit(1)
    out.write_text(_TEMPLATE, encoding="utf-8")
    print(f"Wrote {out}")
    print("Next: edit the YAML and implement a Connector subclass.")


if __name__ == "__main__":
    main()
