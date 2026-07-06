"""
Control-plane listener: HTTP server that ingests manifests.

Stdlib HTTP server; Airflow is only needed by the ingest layer. The ingest
callable is injected so the server itself stays testable without Airflow.
"""

from __future__ import annotations

import hmac
import json
import logging
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from airflow.dagsyncer.protocol import (
    HEALTH_ENDPOINT,
    MANIFEST_ENDPOINT,
    PROTOCOL_VERSION,
    Manifest,
    ManifestError,
)

log = logging.getLogger(__name__)

IngestCallable = Callable[[Manifest], dict[str, Any]]

MAX_BODY_BYTES = 512 * 1024 * 1024


def build_handler(token: str, ingest: IngestCallable) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to an auth token and an ingest callable."""

    class ListenerHandler(BaseHTTPRequestHandler):
        def _respond(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            header = self.headers.get("Authorization", "")
            expected = f"Bearer {token}"
            return hmac.compare_digest(header.encode(), expected.encode())

        def do_GET(self) -> None:
            if self.path == HEALTH_ENDPOINT:
                self._respond(200, {"status": "ok", "protocol_version": PROTOCOL_VERSION})
                return
            self._respond(404, {"error": "not found"})

        def do_POST(self) -> None:
            if self.path != MANIFEST_ENDPOINT:
                self._respond(404, {"error": "not found"})
                return
            if not self._authorized():
                self._respond(401, {"error": "missing or invalid token"})
                return
            length = int(self.headers.get("Content-Length", 0))
            if length <= 0 or length > MAX_BODY_BYTES:
                self._respond(400, {"error": "invalid content length"})
                return
            raw = self.rfile.read(length)
            try:
                manifest = Manifest.from_json(raw.decode())
            except (ManifestError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                self._respond(400, {"error": f"malformed manifest: {exc}"})
                return
            try:
                summary = ingest(manifest)
            except Exception as exc:
                if type(exc).__name__ == "VersionSkewError":
                    self._respond(409, {"error": str(exc)})
                    return
                log.exception("Manifest ingestion failed")
                self._respond(503, {"error": f"ingestion failed: {exc}"})
                return
            self._respond(200, summary)

        def log_message(self, format: str, *args: Any) -> None:
            log.info("%s - %s", self.address_string(), format % args)

    return ListenerHandler


def serve(host: str, port: int, token: str, ingest: IngestCallable) -> None:
    """Run the listener until interrupted."""
    server = ThreadingHTTPServer((host, port), build_handler(token, ingest))
    log.info("Listener serving on %s:%d", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Listener shutting down")
    finally:
        server.server_close()
