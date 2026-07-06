"""
Airflow plugin mounting the dagsyncer listener into the api-server.

Follows the edge3 provider pattern: the listener is a FastAPI sub-app
registered via ``AirflowPlugin.fastapi_apps``, so it inherits the
api-server's port, TLS termination, and deployment story.

Requires apache-airflow (the ``[listen]`` extra).
"""

from __future__ import annotations

import hmac
import json
import logging
import sys
from typing import Any

from airflow.configuration import conf
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Header, HTTPException, Request

from airflow.dagsyncer.ingest import VersionSkewError, ingest_manifest
from airflow.dagsyncer.protocol import PROTOCOL_VERSION, Manifest, ManifestError

log = logging.getLogger(__name__)

RUNNING_ON_APISERVER = (len(sys.argv) > 1 and sys.argv[1] in ["api-server"]) or (
    len(sys.argv) > 2 and sys.argv[2].endswith("airflow-core/src/airflow/api_fastapi/main.py")
)


def _check_token(authorization: str | None) -> None:
    secret = conf.get("dagsyncer", "api_secret_key", fallback="")
    if not secret:
        raise HTTPException(503, "dagsyncer api_secret_key is not configured on the control plane")
    expected = f"Bearer {secret}"
    if authorization is None or not hmac.compare_digest(authorization.encode(), expected.encode()):
        raise HTTPException(401, "missing or invalid token")


def create_dagsyncer_api_app() -> FastAPI:
    """Create the FastAPI sub-app served under ``/dagsyncer``."""
    app = FastAPI(
        title="Airflow DagSyncer API",
        description=(
            "Listener for apache-airflow-dagsyncer. Data-plane syncers POST full "
            "bundle manifests to ``/dagsyncer/v1/manifest``; dags are ingested into "
            "the control-plane DB through Airflow's own dag parsing update path."
        ),
    )

    @app.get("/v1/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "protocol_version": PROTOCOL_VERSION}

    @app.post("/v1/manifest")
    async def post_manifest(
        request: Request, authorization: str | None = Header(default=None)
    ) -> dict[str, Any]:
        _check_token(authorization)
        raw = await request.body()
        try:
            manifest = Manifest.from_json(raw.decode())
        except (ManifestError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HTTPException(400, f"malformed manifest: {exc}") from exc
        try:
            return ingest_manifest(manifest)
        except VersionSkewError as exc:
            raise HTTPException(409, str(exc)) from exc
        except Exception as exc:
            log.exception("Manifest ingestion failed")
            raise HTTPException(503, f"ingestion failed: {exc}") from exc

    return app


class DagSyncerPlugin(AirflowPlugin):
    """Mounts the dagsyncer listener API into the Airflow api-server."""

    name = "dagsyncer"
    if RUNNING_ON_APISERVER:
        fastapi_apps = [
            {
                "app": create_dagsyncer_api_app(),
                "url_prefix": "/dagsyncer",
                "name": "DagSyncer API",
            }
        ]
