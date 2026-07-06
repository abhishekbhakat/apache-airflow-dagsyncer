"""Command-line interface for apache-airflow-dagsyncer."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from airflow.dagsyncer.client import PushError, push
from airflow.dagsyncer.parse import ParseError

log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="apache-airflow-dagsyncer",
        description="Ship serialized dags from a data-plane DagProcessor to a control-plane listener.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    push = subparsers.add_parser(
        "push",
        help="Parse a dag bundle locally and push the manifest to a listener",
    )
    push.add_argument("--bundle-path", required=True, help="Path to the dag bundle to parse")
    push.add_argument("--bundle-name", required=True, help="Bundle name reported to the control plane")
    push.add_argument("--listener-url", required=True, help="Base URL of the control-plane listener")
    push.add_argument("--token", required=True, help="Bearer token for the listener")

    listen = subparsers.add_parser(
        "listen",
        help="Run the control-plane listener (requires the [listen] extra)",
    )
    listen.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    listen.add_argument("--port", type=int, default=8793, help="Bind port (default: 8793)")
    listen.add_argument(
        "--token",
        default=os.environ.get("DAGSYNCER_TOKEN", ""),
        help="Bearer token clients must present (default: DAGSYNCER_TOKEN env var)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    if args.command == "push":
        try:
            return push(args.bundle_path, args.bundle_name, args.listener_url, args.token)
        except (ParseError, PushError) as exc:
            log.error("%s", exc)
            return 1
    if args.command == "listen":
        if not args.token:
            log.error("--token or DAGSYNCER_TOKEN is required")
            return 2
        try:
            from airflow.dagsyncer.ingest import ingest_manifest
        except ImportError:
            log.error("listen requires apache-airflow; install apache-airflow-dagsyncer[listen]")
            return 2
        from airflow.dagsyncer.server import serve

        serve(args.host, args.port, args.token, ingest_manifest)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
