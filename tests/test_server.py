"""Tests for airflow.dagsyncer.server with a fake ingest callable."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from http.server import ThreadingHTTPServer
from typing import Any

import pytest

from airflow.dagsyncer.protocol import HEALTH_ENDPOINT, MANIFEST_ENDPOINT, Manifest
from airflow.dagsyncer.server import build_handler

TOKEN = "secret-token"

ingested: list[Manifest] = []


class FakeVersionSkew(Exception):
    pass


FakeVersionSkew.__name__ = "VersionSkewError"


def ok_ingest(manifest: Manifest) -> dict[str, Any]:
    ingested.append(manifest)
    return {"dags_upserted": len(manifest.dags)}


def skew_ingest(manifest: Manifest) -> dict[str, Any]:
    raise FakeVersionSkew("version mismatch")


def broken_ingest(manifest: Manifest) -> dict[str, Any]:
    raise RuntimeError("db down")


def start_server(ingest: Any) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(TOKEN, ingest))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}"


@pytest.fixture
def url() -> Iterator[str]:
    ingested.clear()
    server, base = start_server(ok_ingest)
    yield base
    server.shutdown()


def request(
    base: str, path: str, body: bytes | None = None, token: str = TOKEN
) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(
        base + path,
        data=body,
        headers={"Authorization": f"Bearer {token}"},
        method="POST" if body is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def make_body() -> bytes:
    return Manifest(airflow_version="3.2.0", bundle_name="b", bundle_version="v1").to_json().encode()


class TestListener:
    def test_health(self, url: str) -> None:
        status, payload = request(url, HEALTH_ENDPOINT)
        assert status == 200
        assert payload["status"] == "ok"

    def test_ingests_valid_manifest(self, url: str) -> None:
        status, payload = request(url, MANIFEST_ENDPOINT, make_body())
        assert status == 200
        assert payload == {"dags_upserted": 0}
        assert ingested[0].bundle_name == "b"

    def test_rejects_bad_token(self, url: str) -> None:
        status, payload = request(url, MANIFEST_ENDPOINT, make_body(), token="wrong")
        assert status == 401

    def test_rejects_malformed_manifest(self, url: str) -> None:
        status, payload = request(url, MANIFEST_ENDPOINT, b'{"nope": 1}')
        assert status == 400
        assert "malformed manifest" in payload["error"]

    def test_unknown_path_404(self, url: str) -> None:
        status, _ = request(url, "/api/v1/nope", make_body())
        assert status == 404

    def test_version_skew_409(self) -> None:
        server, base = start_server(skew_ingest)
        try:
            status, payload = request(base, MANIFEST_ENDPOINT, make_body())
        finally:
            server.shutdown()
        assert status == 409
        assert "version mismatch" in payload["error"]

    def test_ingest_failure_503(self) -> None:
        server, base = start_server(broken_ingest)
        try:
            status, payload = request(base, MANIFEST_ENDPOINT, make_body())
        finally:
            server.shutdown()
        assert status == 503
