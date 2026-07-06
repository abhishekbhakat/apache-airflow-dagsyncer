"""
Run a one-shot DagProcessor parse and extract the results.

The parse runs the ``airflow`` CLI in an isolated ``AIRFLOW_HOME`` backed by a
local SQLite database. Extraction reads that database with stdlib ``sqlite3``,
so this module never imports Airflow itself.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import subprocess
import zlib
from pathlib import Path

from airflow.dagsyncer.protocol import ImportErrorEntry, Manifest, SerializedDagEntry

log = logging.getLogger(__name__)


class ParseError(RuntimeError):
    """Raised when the one-shot DagProcessor run fails."""


def _airflow_env(airflow_home: Path, bundle_path: Path, bundle_name: str) -> dict[str, str]:
    bundle_config = [
        {
            "name": bundle_name,
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(bundle_path)},
        }
    ]
    env = os.environ.copy()
    env.update(
        {
            "AIRFLOW_HOME": str(airflow_home),
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{airflow_home / 'dagsyncer.db'}",
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(bundle_config),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "False",
            "AIRFLOW__LOGGING__LOGGING_LEVEL": "WARNING",
        }
    )
    return env


def _run(cmd: list[str], env: dict[str, str]) -> None:
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise ParseError(f"Command {cmd[0]} {cmd[1]} failed (exit {result.returncode}):\n{result.stderr}")


def airflow_version(env: dict[str, str] | None = None) -> str:
    """Return the Airflow version of the local ``airflow`` executable."""
    result = subprocess.run(
        ["airflow", "version"], env=env or os.environ.copy(), capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise ParseError(f"'airflow version' failed (exit {result.returncode}):\n{result.stderr}")
    return result.stdout.strip().splitlines()[-1]


def run_dag_processor_once(airflow_home: Path, bundle_path: Path, bundle_name: str) -> Path:
    """Parse ``bundle_path`` once with the DagProcessor; return the SQLite DB path."""
    airflow_home.mkdir(parents=True, exist_ok=True)
    env = _airflow_env(airflow_home, bundle_path, bundle_name)
    _run(["airflow", "db", "migrate"], env)
    _run(["airflow", "dag-processor", "--num-runs", "1", "--bundle-name", bundle_name], env)
    return airflow_home / "dagsyncer.db"


def _decode_dag_data(data: str | None, data_compressed: bytes | None) -> dict[str, object]:
    if data is not None:
        loaded = json.loads(data)
    elif data_compressed is not None:
        loaded = json.loads(zlib.decompress(data_compressed))
    else:
        raise ParseError("serialized_dag row has neither data nor data_compressed")
    if not isinstance(loaded, dict):
        raise ParseError("serialized dag data is not a JSON object")
    return loaded


def extract_manifest(db_path: Path, bundle_name: str, airflow_version_str: str) -> Manifest:
    """Build a :class:`Manifest` from the SQLite DB produced by the parse run."""
    dags: list[SerializedDagEntry] = []
    import_errors: list[ImportErrorEntry] = []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT sd.dag_id, sd.data, sd.data_compressed, sd.dag_hash
            FROM serialized_dag sd
            JOIN (
                SELECT dag_id, MAX(last_updated) AS latest
                FROM serialized_dag GROUP BY dag_id
            ) newest ON newest.dag_id = sd.dag_id AND newest.latest = sd.last_updated
            ORDER BY sd.dag_id
            """
        ).fetchall()
        for dag_id, data, data_compressed, dag_hash in rows:
            payload = _decode_dag_data(data, data_compressed)
            dag_section = payload.get("dag")
            fileloc = dag_section.get("fileloc", "") if isinstance(dag_section, dict) else ""
            dags.append(
                SerializedDagEntry(dag_id=dag_id, fileloc=str(fileloc), dag_hash=dag_hash, data=payload)
            )
        error_rows = conn.execute(
            "SELECT filename, stacktrace FROM import_error WHERE bundle_name = ? ORDER BY filename",
            (bundle_name,),
        ).fetchall()
        import_errors = [
            ImportErrorEntry(filename=filename or "", stacktrace=stacktrace or "")
            for filename, stacktrace in error_rows
        ]

    bundle_version = hashlib.sha256("\n".join(d.dag_hash for d in dags).encode()).hexdigest()[:16]
    return Manifest(
        airflow_version=airflow_version_str,
        bundle_name=bundle_name,
        bundle_version=bundle_version,
        dags=dags,
        import_errors=import_errors,
    )
