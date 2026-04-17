"""Tests for BigQuery schema helpers."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from unittest.mock import MagicMock

from invoice_processing.bq_invoice_extractions import (
    ensure_invoice_extractions_table,
    invoice_extractions_schema,
    load_ndjson_jsonl_files,
    load_ndjson_rows,
)


def test_invoice_extractions_schema_top_level_names() -> None:
    fields = {f.name for f in invoice_extractions_schema()}
    assert "gcs_uri" in fields
    assert "supplier" in fields
    assert "tax_lines" in fields
    assert "invoice_lines" in fields
    assert "document_totals" in fields
    assert "extras" in fields


def test_ensure_invoice_extractions_table_creates_dataset_and_table() -> None:
    client = MagicMock()
    client.project = "proj"
    ref = ensure_invoice_extractions_table(
        client, "mydataset", "t1", project_id="proj", location="EU"
    )
    assert ref == "proj.mydataset.t1"
    client.create_dataset.assert_called_once()
    client.create_table.assert_called_once()


def test_load_ndjson_rows_calls_load_table_from_file() -> None:
    client = MagicMock()
    job = MagicMock()
    client.load_table_from_file.return_value = job
    rows = [{"gcs_uri": "gs://b/a.pdf", "extras": None}]
    load_ndjson_rows(client, "proj.ds.t", rows, location="EU")
    client.load_table_from_file.assert_called_once()
    job.result.assert_called_once()


def test_load_ndjson_rows_logs_job_errors_on_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    client = MagicMock()
    job = MagicMock()
    job.errors = [{"message": "bad row", "reason": "invalid"}]
    job.result.side_effect = RuntimeError("bq failed")
    client.load_table_from_file.return_value = job
    rows = [{"gcs_uri": "gs://b/a.pdf", "extras": None}]
    with pytest.raises(RuntimeError, match="bq failed"):
        load_ndjson_rows(client, "proj.ds.t", rows, location="EU")
    assert "bad row" in caplog.text


def test_load_ndjson_jsonl_files(tmp_path: Path) -> None:
    client = MagicMock()
    job = MagicMock()
    client.load_table_from_file.return_value = job
    f1 = tmp_path / "a.jsonl"
    f1.write_text('{"gcs_uri":"gs://b/a.pdf"}\n', encoding="utf-8")
    f2 = tmp_path / "b.jsonl"
    f2.write_text('{"gcs_uri":"gs://b/b.pdf"}\n', encoding="utf-8")
    load_ndjson_jsonl_files(client, "proj.ds.t", [f1, f2], location="EU")
    assert client.load_table_from_file.call_count == 2
    assert job.result.call_count == 2
