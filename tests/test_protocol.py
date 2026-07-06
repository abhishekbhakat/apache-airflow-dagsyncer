"""Tests for airflow.dagsyncer.protocol."""

from __future__ import annotations

import json

import pytest

from airflow.dagsyncer.protocol import (
    PROTOCOL_VERSION,
    ImportErrorEntry,
    Manifest,
    ManifestError,
    SerializedDagEntry,
)


def make_manifest() -> Manifest:
    return Manifest(
        airflow_version="3.2.0",
        bundle_name="my-dags",
        bundle_version="v1-abc123",
        dags=[
            SerializedDagEntry(
                dag_id="example",
                fileloc="/dags/example.py",
                dag_hash="deadbeef",
                data={"dag": {"dag_id": "example"}},
            )
        ],
        import_errors=[ImportErrorEntry(filename="/dags/broken.py", stacktrace="boom")],
    )


class TestManifestRoundtrip:
    def test_roundtrip(self) -> None:
        manifest = make_manifest()
        assert Manifest.from_json(manifest.to_json()) == manifest

    def test_empty_manifest_roundtrip(self) -> None:
        manifest = Manifest(airflow_version="3.2.0", bundle_name="b", bundle_version="v")
        restored = Manifest.from_json(manifest.to_json())
        assert restored == manifest
        assert restored.dags == []
        assert restored.import_errors == []

    def test_to_json_is_plain_json(self) -> None:
        payload = json.loads(make_manifest().to_json())
        assert payload["protocol_version"] == PROTOCOL_VERSION
        assert payload["dags"][0]["dag_id"] == "example"


class TestManifestValidation:
    def test_rejects_non_object_payload(self) -> None:
        with pytest.raises(ManifestError, match="JSON object"):
            Manifest.from_json("[1, 2]")

    def test_rejects_missing_protocol_version(self) -> None:
        with pytest.raises(ManifestError, match="protocol version"):
            Manifest.from_json("{}")

    def test_rejects_wrong_protocol_version(self) -> None:
        payload = json.loads(make_manifest().to_json())
        payload["protocol_version"] = PROTOCOL_VERSION + 1
        with pytest.raises(ManifestError, match="Unsupported protocol version"):
            Manifest.from_json(json.dumps(payload))

    def test_rejects_missing_required_field(self) -> None:
        payload = json.loads(make_manifest().to_json())
        del payload["bundle_name"]
        with pytest.raises(ManifestError, match="Malformed manifest"):
            Manifest.from_json(json.dumps(payload))

    def test_rejects_malformed_dag_entry(self) -> None:
        payload = json.loads(make_manifest().to_json())
        payload["dags"][0].pop("dag_hash")
        with pytest.raises(ManifestError, match="Malformed manifest"):
            Manifest.from_json(json.dumps(payload))

    def test_rejects_unknown_dag_entry_field(self) -> None:
        payload = json.loads(make_manifest().to_json())
        payload["dags"][0]["surprise"] = True
        with pytest.raises(ManifestError, match="Malformed manifest"):
            Manifest.from_json(json.dumps(payload))

    def test_rejects_invalid_json(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            Manifest.from_json("not json")
