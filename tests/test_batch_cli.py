"""Tests for batch extraction CLI."""

from __future__ import annotations

import pytest


def test_batch_cli_requires_gcp_project(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.setenv("GCS_BUCKET", "b")
    monkeypatch.setenv("BQ_DATASET", "d")
    from invoice_processing import batch_cli

    with pytest.raises(SystemExit) as exc:
        batch_cli.main()
    assert exc.value.code == 1


def test_batch_cli_requires_gcs_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    monkeypatch.setenv("BQ_DATASET", "d")
    from invoice_processing import batch_cli

    with pytest.raises(SystemExit) as exc:
        batch_cli.main()
    assert exc.value.code == 1
