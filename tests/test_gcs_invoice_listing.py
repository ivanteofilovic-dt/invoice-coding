"""Tests for GCS invoice PDF listing."""

from __future__ import annotations

from unittest.mock import MagicMock

from invoice_processing.gcs_invoice_listing import list_invoice_pdf_uris


def test_list_invoice_pdf_uris_filters_and_prefixes() -> None:
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket

    b1 = MagicMock()
    b1.name = "historical/invoices/2026/a.pdf"
    b2 = MagicMock()
    b2.name = "historical/invoices/notes.txt"
    b3 = MagicMock()
    b3.name = "historical/invoices/b.PDF"
    bucket.list_blobs.return_value = [b1, b2, b3]

    uris = list_invoice_pdf_uris("my-bucket", "historical/invoices", client=client)

    assert uris == [
        "gs://my-bucket/historical/invoices/2026/a.pdf",
        "gs://my-bucket/historical/invoices/b.PDF",
    ]
    bucket.list_blobs.assert_called_once()
    _, kwargs = bucket.list_blobs.call_args
    assert kwargs["prefix"] == "historical/invoices"


def test_list_invoice_pdf_uris_strips_prefix_slashes() -> None:
    client = MagicMock()
    bucket = MagicMock()
    client.bucket.return_value = bucket
    bucket.list_blobs.return_value = []

    list_invoice_pdf_uris("b", "  pfx/  ", client=client)
    assert bucket.list_blobs.call_args[1]["prefix"] == "pfx"
