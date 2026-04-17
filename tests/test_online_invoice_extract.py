"""Tests for UI extraction projection."""

from __future__ import annotations

from invoice_processing.online_invoice_extract import extraction_payload_to_ui_extraction


def test_extraction_payload_to_ui_extraction() -> None:
    payload = {
        "issue_date": "2024-06-01",
        "invoice_number": "INV-1",
        "currency_code": "SEK",
        "supplier": {"legal_name": "Acme AB", "name": "Acme"},
        "invoice_lines": [
            {
                "line_number": 1,
                "item_name": "Widget",
                "item_number": "W1",
                "line_amount": "99.5",
            }
        ],
        "delivery": {"delivery_date": "2024-06-10"},
    }
    ui = extraction_payload_to_ui_extraction(payload, document_id="doc-1")
    assert ui["document_id"] == "doc-1"
    assert ui["supplier"] == "Acme AB"
    assert ui["invoice_number"] == "INV-1"
    assert ui["currency"] == "SEK"
    assert ui["periodization_hint"] == "2024-06-10"
    assert len(ui["lines"]) == 1
    assert ui["lines"][0]["join_key"] == "L1|W1"
    assert ui["lines"][0]["amount"] == "99.5"
