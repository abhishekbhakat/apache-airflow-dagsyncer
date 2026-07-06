"""Tests for airflow.dagsyncer.client against a local HTTP server."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from airflow.dagsyncer.client import PushError, post_manifest
from airflow.dagsyncer.protocol import MANIFEST_ENDPOINT, Manifest

received: list[dict[str, object]] = []


class FakeListener(BaseHTTPRequestHandler):
    status = 200

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        received.append(
            {
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "payload": json.loads(body),
            }
        )
        self.send_response(self.status)
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def listener_url() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), FakeListener)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    received.clear()
    FakeListener.status = 200
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def make_manifest() -> Manifest:
    return Manifest(airflow_version="3.2.0", bundle_name="b", bundle_version="v1")


class TestPostManifest:
    def test_posts_manifest_with_bearer_token(self, listener_url: str) -> None:
        body = post_manifest(listener_url, "secret", make_manifest())
        assert json.loads(body) == {"ok": True}
        assert received[0]["path"] == MANIFEST_ENDPOINT
        assert received[0]["auth"] == "Bearer secret"
        payload = received[0]["payload"]
        assert isinstance(payload, dict)
        assert payload["bundle_name"] == "b"

    def test_raises_on_http_error(self, listener_url: str) -> None:
        FakeListener.status = 401
        with pytest.raises(PushError, match="HTTP 401"):
            post_manifest(listener_url, "bad-token", make_manifest())

    def test_raises_when_unreachable(self) -> None:
        with pytest.raises(PushError, match="unreachable"):
            post_manifest("http://127.0.0.1:1", "t", make_manifest())
