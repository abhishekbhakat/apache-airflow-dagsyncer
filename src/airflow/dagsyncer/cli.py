"""Command-line interface for apache-airflow-dagsyncer."""

import argparse
import logging
import sys

from airflow.dagsyncer.syncer import sync_dags


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apache-airflow-dagsyncer",
        description="Sync DAG files from a source location into an Airflow dags folder.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync = subparsers.add_parser("sync", help="Sync DAGs from source to destination")
    sync.add_argument("--source", required=True, help="Source directory containing DAG files")
    sync.add_argument("--dest", required=True, help="Destination Airflow dags folder")
    sync.add_argument("--dry-run", action="store_true", help="Show what would be synced without copying")

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    if args.command == "sync":
        return sync_dags(source=args.source, dest=args.dest, dry_run=args.dry_run)
    return 1


if __name__ == "__main__":
    sys.exit(main())
