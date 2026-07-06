"""Core sync logic for apache-airflow-dagsyncer."""

from __future__ import annotations

import filecmp
import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


def sync_dags(source: str, dest: str, dry_run: bool = False) -> int:
    """
    Sync python DAG files from ``source`` into ``dest``.

    Copies new and changed ``.py`` files, preserving relative layout.
    Returns 0 on success, 1 on error.
    """
    src_dir = Path(source).expanduser().resolve()
    dest_dir = Path(dest).expanduser().resolve()

    if not src_dir.is_dir():
        log.error("Source directory does not exist: %s", src_dir)
        return 1

    if not dry_run:
        dest_dir.mkdir(parents=True, exist_ok=True)

    synced = 0
    for src_file in sorted(src_dir.rglob("*.py")):
        rel = src_file.relative_to(src_dir)
        dest_file = dest_dir / rel
        if dest_file.exists() and filecmp.cmp(src_file, dest_file, shallow=False):
            continue
        if dry_run:
            log.info("Would sync %s -> %s", rel, dest_file)
        else:
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest_file)
            log.info("Synced %s -> %s", rel, dest_file)
        synced += 1

    log.info("%d file(s) %s", synced, "would be synced" if dry_run else "synced")
    return 0
