"""Tests for batch JSONL request building."""

from __future__ import annotations

import json

from invoice_processing.invoice_extraction_schema import (
    build_batch_jsonl_line,
    invoice_response_json_schema,
)


def test_invoice_response_json_schema_has_expected_roots() -> None:
    s = invoice_response_json_schema()
    props = s["properties"]
    for key in (
        "issue_date",
        "supplier",
        "tax_lines",
        "invoice_lines",
        "document_totals",
        "extras",
    ):
        assert key in props


def test_build_batch_jsonl_line_contains_pdf_and_schema() -> None:
    line = build_batch_jsonl_line("gs://bucket/historical/invoices/x.pdf")
    obj = json.loads(line)
    assert "request" in obj
    req = obj["request"]
    gc = req["generationConfig"]
    assert gc["responseMimeType"] == "application/json"
    assert "responseJsonSchema" in gc
    parts = req["contents"][0]["parts"]
    texts = [p for p in parts if "text" in p]
    files = [p for p in parts if "fileData" in p]
    assert texts and files
    assert files[0]["fileData"]["fileUri"] == "gs://bucket/historical/invoices/x.pdf"
    assert files[0]["fileData"]["mimeType"] == "application/pdf"
