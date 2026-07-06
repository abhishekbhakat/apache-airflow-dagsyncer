"""HTTP client that pushes manifests to the control-plane listener."""

from __future__ import annotations

import logging
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from airflow.dagsyncer.parse import airflow_version, extract_manifest, run_dag_processor_once
from airflow.dagsyncer.protocol import MANIFEST_ENDPOINT, Manifest

log = logging.getLogger(__name__)


class PushError(RuntimeError):
    """Raised when the listener rejects a manifest or is unreachable."""


def post_manifest(listener_url: str, token: str, manifest: Manifest, timeout: float = 60.0) -> str:
    """POST ``manifest`` to the listener; return the response body."""
    url = listener_url.rstrip("/") + MANIFEST_ENDPOINT
    request = urllib.request.Request(
        url,
        data=manifest.to_json().encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body: str = response.read().decode()
            return body
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise PushError(f"Listener rejected manifest: HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise PushError(f"Listener unreachable at {url}: {exc.reason}") from exc


def push(bundle_path: str, bundle_name: str, listener_url: str, token: str) -> int:
    """Parse a bundle locally and push the manifest to the listener."""
    version = airflow_version()
    with tempfile.TemporaryDirectory(prefix="dagsyncer-") as tmp:
        db_path = run_dag_processor_once(Path(tmp), Path(bundle_path).resolve(), bundle_name)
        manifest = extract_manifest(db_path, bundle_name, version)
    log.info(
        "Pushing manifest: bundle=%s version=%s dags=%d import_errors=%d",
        manifest.bundle_name,
        manifest.bundle_version,
        len(manifest.dags),
        len(manifest.import_errors),
    )
    body = post_manifest(listener_url, token, manifest)
    log.info("Listener response: %s", body)
    return 0
