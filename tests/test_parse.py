"""Tests for airflow.dagsyncer.parse extraction logic."""

from __future__ import annotations

import json
import sqlite3
import zlib
from pathlib import Path

import pytest

from airflow.dagsyncer.parse import ParseError, _decode_dag_data, extract_manifest

SCHEMA = """
CREATE TABLE serialized_dag (
    id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    data TEXT,
    data_compressed BLOB,
    dag_hash TEXT NOT NULL,
    last_updated TEXT NOT NULL
);
CREATE TABLE import_error (
    id INTEGER PRIMARY KEY,
    filename TEXT,
    bundle_name TEXT,
    stacktrace TEXT
);
"""


def dag_payload(dag_id: str) -> str:
    return json.dumps({"dag": {"dag_id": dag_id, "fileloc": f"/dags/{dag_id}.py"}})


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "dagsyncer.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
        conn.executemany(
            "INSERT INTO serialized_dag VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("u1", "dag_a", dag_payload("dag_a"), None, "hash-a1", "2026-01-01"),
                ("u2", "dag_a", dag_payload("dag_a"), None, "hash-a2", "2026-02-01"),
                ("u3", "dag_b", None, zlib.compress(dag_payload("dag_b").encode()), "hash-b", "2026-01-01"),
            ],
        )
        conn.executemany(
            "INSERT INTO import_error (filename, bundle_name, stacktrace) VALUES (?, ?, ?)",
            [
                ("broken.py", "my-bundle", "boom"),
                ("other.py", "other-bundle", "not ours"),
            ],
        )
    return path


class TestExtractManifest:
    def test_latest_serialized_dag_per_dag_id(self, db_path: Path) -> None:
        manifest = extract_manifest(db_path, "my-bundle", "3.2.0")
        hashes = {d.dag_id: d.dag_hash for d in manifest.dags}
        assert hashes == {"dag_a": "hash-a2", "dag_b": "hash-b"}

    def test_decompresses_compressed_data(self, db_path: Path) -> None:
        manifest = extract_manifest(db_path, "my-bundle", "3.2.0")
        dag_b = next(d for d in manifest.dags if d.dag_id == "dag_b")
        assert dag_b.fileloc == "/dags/dag_b.py"
        dag_section = dag_b.data["dag"]
        assert isinstance(dag_section, dict)
        assert dag_section["dag_id"] == "dag_b"

    def test_import_errors_filtered_by_bundle(self, db_path: Path) -> None:
        manifest = extract_manifest(db_path, "my-bundle", "3.2.0")
        assert [e.filename for e in manifest.import_errors] == ["broken.py"]

    def test_manifest_metadata(self, db_path: Path) -> None:
        manifest = extract_manifest(db_path, "my-bundle", "3.2.0")
        assert manifest.airflow_version == "3.2.0"
        assert manifest.bundle_name == "my-bundle"
        assert len(manifest.bundle_version) == 16

    def test_bundle_version_deterministic(self, db_path: Path) -> None:
        first = extract_manifest(db_path, "my-bundle", "3.2.0")
        second = extract_manifest(db_path, "my-bundle", "3.2.0")
        assert first.bundle_version == second.bundle_version


class TestDecodeDagData:
    def test_rejects_row_without_data(self) -> None:
        with pytest.raises(ParseError, match="neither"):
            _decode_dag_data(None, None)

    def test_rejects_non_object_data(self) -> None:
        with pytest.raises(ParseError, match="not a JSON object"):
            _decode_dag_data("[1]", None)
