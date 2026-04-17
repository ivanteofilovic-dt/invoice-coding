"""Smoke tests for FastAPI app (no GCP calls)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from invoice_processing.api.app import app


def test_health_endpoint() -> None:
    client = TestClient(app)
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.json()
    assert "ok" in data
    assert "gcp_project_set" in data
