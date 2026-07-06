"""Command-line interface for apache-airflow-dagsyncer."""

from __future__ import annotations

import argparse
import logging
import sys

from airflow.dagsyncer.client import PushError, push
from airflow.dagsyncer.parse import ParseError

log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="apache-airflow-dagsyncer",
        description=(
            "Ship serialized dags from a data-plane DagProcessor to the control-plane "
            "listener plugin mounted in the Airflow api-server."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    push = subparsers.add_parser(
        "push",
        help="Parse a dag bundle locally and push the manifest to a listener",
    )
    push.add_argument("--bundle-path", required=True, help="Path to the dag bundle to parse")
    push.add_argument("--bundle-name", required=True, help="Bundle name reported to the control plane")
    push.add_argument(
        "--listener-url", required=True, help="Base URL of the control-plane Airflow api-server"
    )
    push.add_argument("--token", required=True, help="Bearer token for the listener")

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
    return 1


if __name__ == "__main__":
    sys.exit(main())
