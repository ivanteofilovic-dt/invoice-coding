"""Tests for BigQuery schema helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

from invoice_processing.bq_invoice_extractions import (
    ensure_invoice_extractions_table,
    invoice_extractions_schema,
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
