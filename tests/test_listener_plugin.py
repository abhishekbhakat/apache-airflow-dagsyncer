"""Tests for the listener plugin FastAPI routes."""

from __future__ import annotations

from typing import Any

import pytest
from airflow.configuration import conf as airflow_conf
from fastapi.testclient import TestClient

from airflow.dagsyncer import listener_plugin
from airflow.dagsyncer.ingest import VersionSkewError
from airflow.dagsyncer.protocol import Manifest

TOKEN = "secret-token"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(
        airflow_conf,
        "get",
        lambda *a, **kw: TOKEN if a[:2] == ("dagsyncer", "api_secret_key") else "",
    )
    return TestClient(listener_plugin.create_dagsyncer_api_app())


def make_body() -> str:
    return Manifest(airflow_version="3.2.0", bundle_name="b", bundle_version="v1").to_json()


def post(client: TestClient, body: str, token: str = TOKEN) -> Any:
    return client.post("/v1/manifest", content=body, headers={"Authorization": f"Bearer {token}"})


class TestListenerRoutes:
    def test_health(self, client: TestClient) -> None:
        response = client.get("/v1/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_ingests_valid_manifest(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[Manifest] = []

        def fake_ingest(manifest: Manifest) -> dict[str, Any]:
            seen.append(manifest)
            return {"dags_upserted": len(manifest.dags)}

        monkeypatch.setattr(listener_plugin, "ingest_manifest", fake_ingest)
        response = post(client, make_body())
        assert response.status_code == 200
        assert response.json() == {"dags_upserted": 0}
        assert seen[0].bundle_name == "b"

    def test_rejects_bad_token(self, client: TestClient) -> None:
        assert post(client, make_body(), token="wrong").status_code == 401

    def test_rejects_missing_token(self, client: TestClient) -> None:
        assert client.post("/v1/manifest", content=make_body()).status_code == 401

    def test_unconfigured_secret_503(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(airflow_conf, "get", lambda *a, **kw: "")
        client = TestClient(listener_plugin.create_dagsyncer_api_app())
        response = post(client, make_body())
        assert response.status_code == 503
        assert "not configured" in response.json()["detail"]

    def test_rejects_malformed_manifest(self, client: TestClient) -> None:
        response = post(client, '{"nope": 1}')
        assert response.status_code == 400
        assert "malformed manifest" in response.json()["detail"]

    def test_version_skew_409(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        def skew(manifest: Manifest) -> dict[str, Any]:
            raise VersionSkewError("version mismatch")

        monkeypatch.setattr(listener_plugin, "ingest_manifest", skew)
        response = post(client, make_body())
        assert response.status_code == 409

    def test_ingest_failure_503(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        def broken(manifest: Manifest) -> dict[str, Any]:
            raise RuntimeError("db down")

        monkeypatch.setattr(listener_plugin, "ingest_manifest", broken)
        assert post(client, make_body()).status_code == 503
