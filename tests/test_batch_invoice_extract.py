"""Tests for batch extraction helpers and orchestration (mocked GCP)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from google.genai.types import JobState

from invoice_processing.batch_invoice_extract import (
    BatchExtractConfig,
    batch_output_jsonl_paths_to_bq_rows,
    chunk_list,
    extraction_payload_to_bq_row,
    gcs_uri_from_batch_request,
    parse_batch_output_line,
    parse_model_json_text,
    run_batch_extract,
)


def test_chunk_list() -> None:
    assert chunk_list([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]
    assert chunk_list(["a"], 10) == [["a"]]


def test_chunk_list_rejects_non_positive() -> None:
    with pytest.raises(ValueError):
        chunk_list([1], 0)


def test_parse_model_json_text_strips_fence() -> None:
    raw = '```json\n{"issue_date": "2025-09-24"}\n```'
    assert parse_model_json_text(raw) == {"issue_date": "2025-09-24"}


def test_gcs_uri_from_batch_request() -> None:
    req = {
        "contents": [
            {
                "parts": [
                    {"text": "hi"},
                    {"fileData": {"fileUri": "gs://b/o.pdf", "mimeType": "application/pdf"}},
                ]
            }
        ]
    }
    assert gcs_uri_from_batch_request(req) == "gs://b/o.pdf"


def test_parse_batch_output_line_success() -> None:
    payload = {"issue_date": "2025-09-24", "invoice_number": "F80386", "tax_lines": []}
    line_obj = {
        "status": "",
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": "x"},
                        {
                            "fileData": {
                                "fileUri": "gs://bucket/doc.pdf",
                                "mimeType": "application/pdf",
                            }
                        },
                    ],
                }
            ]
        },
        "response": {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": json.dumps(payload)}],
                        "role": "model",
                    }
                }
            ]
        },
    }
    uri, out, err = parse_batch_output_line(json.dumps(line_obj))
    assert err is None
    assert uri == "gs://bucket/doc.pdf"
    assert out == payload


def test_parse_batch_output_line_status_error() -> None:
    line_obj = {
        "status": "Bad Request: something",
        "request": {
            "contents": [
                {
                    "parts": [
                        {
                            "fileData": {
                                "fileUri": "gs://bucket/bad.pdf",
                                "mimeType": "application/pdf",
                            }
                        }
                    ]
                }
            ]
        },
    }
    uri, out, err = parse_batch_output_line(json.dumps(line_obj))
    assert out is None
    assert uri == "gs://bucket/bad.pdf"
    assert err is not None


def test_batch_output_jsonl_paths_to_bq_rows(tmp_path: Path) -> None:
    payload = {"issue_date": "2025-09-24", "invoice_number": "F80386", "tax_lines": []}
    line_obj = {
        "status": "",
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "fileData": {
                                "fileUri": "gs://bucket/doc.pdf",
                                "mimeType": "application/pdf",
                            }
                        },
                    ],
                }
            ]
        },
        "response": {
            "candidates": [
                {
                    "content": {
                        "parts": [{"text": json.dumps(payload)}],
                        "role": "model",
                    }
                }
            ]
        },
    }
    f = tmp_path / "out.jsonl"
    f.write_text(json.dumps(line_obj) + "\n", encoding="utf-8")
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows, errors = batch_output_jsonl_paths_to_bq_rows(
        [f], model_id="gemini-test", batch_job_name="jobs/1", extracted_at=ts
    )
    assert errors == []
    assert len(rows) == 1
    assert rows[0]["gcs_uri"] == "gs://bucket/doc.pdf"
    assert rows[0]["model_id"] == "gemini-test"
    assert rows[0]["batch_job_name"] == "jobs/1"
    assert rows[0]["invoice_number"] == "F80386"


def test_extraction_payload_to_bq_row_numeric_strings() -> None:
    ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    row = extraction_payload_to_bq_row(
        {
            "issue_date": "2025-09-24",
            "tax_lines": [{"tax_category": "S", "tax_rate_percent": 25, "taxable_amount": 100.5}],
            "invoice_lines": [
                {
                    "line_number": 1,
                    "quantity": 2,
                    "net_unit_price": 10.25,
                    "line_amount": 20.5,
                }
            ],
            "document_totals": {"rounding_amount": -0.25},
            "extras": {"raw_footer": "x"},
        },
        gcs_uri="gs://b/a.pdf",
        model_id="gemini-test",
        batch_job_name="projects/p/locations/l/batchPredictionJobs/1",
        extracted_at=ts,
    )
    assert row["gcs_uri"] == "gs://b/a.pdf"
    assert row["tax_lines"][0]["tax_rate_percent"] == "25"
    assert row["invoice_lines"][0]["quantity"] == "2"
    assert row["document_totals"]["rounding_amount"] == "-0.25"


@patch("invoice_processing.batch_invoice_extract.load_ndjson_rows")
@patch("invoice_processing.batch_invoice_extract.read_all_batch_output_lines")
@patch("invoice_processing.batch_invoice_extract.wait_batch_job")
@patch("invoice_processing.batch_invoice_extract.upload_text_blob")
def test_run_batch_extract_happy_path(
    mock_upload: MagicMock,
    mock_wait: MagicMock,
    mock_read_lines: MagicMock,
    mock_load: MagicMock,
) -> None:
    mock_upload.return_value = "gs://bucket/staging/in.jsonl"

    job = MagicMock()
    job.name = "projects/p/locations/us-central1/batchPredictionJobs/99"
    job.state = JobState.JOB_STATE_SUCCEEDED
    mock_wait.return_value = job

    extraction = {
        "issue_date": "2025-09-24",
        "invoice_number": "N1",
        "tax_lines": [],
        "invoice_lines": [],
        "extras": {},
    }
    out_line = {
        "status": "",
        "request": {
            "contents": [
                {
                    "parts": [
                        {"text": "p"},
                        {
                            "fileData": {
                                "fileUri": "gs://bucket/historical/invoices/x.pdf",
                                "mimeType": "application/pdf",
                            }
                        },
                    ]
                }
            ]
        },
        "response": {
            "candidates": [
                {"content": {"parts": [{"text": json.dumps(extraction)}]}}
            ]
        },
    }
    mock_read_lines.return_value = [json.dumps(out_line)]

    sc = MagicMock()
    bucket = MagicMock()
    sc.bucket.return_value = bucket
    pdf_blob = MagicMock()
    pdf_blob.name = "historical/invoices/x.pdf"
    bucket.list_blobs.return_value = [pdf_blob]

    gc = MagicMock()
    create_job = MagicMock()
    create_job.name = job.name
    create_job.state = JobState.JOB_STATE_PENDING
    gc.batches.create.return_value = create_job

    bqc = MagicMock()

    cfg = BatchExtractConfig(
        project_id="proj",
        vertex_location="us-central1",
        gcs_bucket="bucket",
        invoice_prefix="historical/invoices",
        batch_staging_prefix="batch",
        gemini_model="gemini-2.5-flash",
        bq_dataset="ds",
        bq_table="invoice_extractions",
        bq_location="US",
        max_invoices_per_job=500,
        poll_interval_seconds=0.0,
    )
    summary = run_batch_extract(
        cfg,
        storage_client=sc,
        genai_client=gc,
        bq_client=bqc,
        sleep_fn=lambda _: None,
    )

    assert len(summary.pdf_uris) == 1
    assert summary.rows_loaded == 1
    assert not summary.parse_errors
    gc.batches.create.assert_called_once()
    mock_load.assert_called_once()
    args, kwargs = mock_load.call_args
    assert args[0] is bqc
    loaded_rows = args[2]
    assert len(loaded_rows) == 1
    assert loaded_rows[0]["invoice_number"] == "N1"
