"""
Wire protocol for syncer -> listener communication.

The syncer (data plane) posts a full :class:`Manifest` per dag bundle to the
listener (control plane). The manifest is authoritative for the bundle: dags
absent from it are deactivated on the control plane.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

PROTOCOL_VERSION = 1

MANIFEST_ENDPOINT = "/dagsyncer/v1/manifest"
HEALTH_ENDPOINT = "/dagsyncer/v1/health"
AUTH_HEADER = "Authorization"


@dataclass
class SerializedDagEntry:
    """One serialized dag as produced by the DagProcessor."""

    dag_id: str
    fileloc: str
    dag_hash: str
    data: dict[str, Any]


@dataclass
class ImportErrorEntry:
    """An import error raised while parsing a dag file."""

    filename: str
    stacktrace: str


@dataclass
class Manifest:
    """Full parse result for one dag bundle deployment."""

    airflow_version: str
    bundle_name: str
    bundle_version: str
    dags: list[SerializedDagEntry] = field(default_factory=list)
    import_errors: list[ImportErrorEntry] = field(default_factory=list)
    protocol_version: int = PROTOCOL_VERSION

    def to_json(self) -> str:
        """Serialize the manifest to a JSON string."""
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, payload: str) -> Manifest:
        """Deserialize a manifest from a JSON string."""
        raw = json.loads(payload)
        if not isinstance(raw, dict):
            raise ManifestError("Manifest payload must be a JSON object")
        protocol_version = raw.get("protocol_version")
        if protocol_version != PROTOCOL_VERSION:
            raise ManifestError(
                f"Unsupported protocol version: {protocol_version!r} (expected {PROTOCOL_VERSION})"
            )
        try:
            return cls(
                airflow_version=raw["airflow_version"],
                bundle_name=raw["bundle_name"],
                bundle_version=raw["bundle_version"],
                dags=[SerializedDagEntry(**d) for d in raw.get("dags", [])],
                import_errors=[ImportErrorEntry(**e) for e in raw.get("import_errors", [])],
                protocol_version=protocol_version,
            )
        except (KeyError, TypeError) as exc:
            raise ManifestError(f"Malformed manifest payload: {exc}") from exc


class ManifestError(ValueError):
    """Raised when a manifest payload is malformed or unsupported."""
