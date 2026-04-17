"""Tests for rag_suggestions row → API DTO mapping."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from invoice_processing.bq_rag_suggestions import (
    insert_suggestion_row,
    row_to_api_detail,
    row_to_api_list_item,
)


def test_row_to_api_detail() -> None:
    row = {
        "suggestion_id": "sid1",
        "created_at": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "gcs_uri": "gs://b/a.pdf",
        "document_id": "a.pdf",
        "status": "Needs Review",
        "final_confidence": 0.7,
        "extraction": {"document_id": "a.pdf", "lines": []},
        "suggestion": {
            "journal_lines": [{"account": "1", "cost_center": "2"}],
            "confidence": 0.8,
            "rationale": "Because history",
        },
        "neighbors": [{"rank": 1}],
        "confidence_meta": {"neighbor_count": 1},
    }
    d = row_to_api_detail(row)
    assert d["suggestion_id"] == "sid1"
    assert d["status"] == "Needs Review"
    assert d["final_confidence"] == 0.7
    assert d["neighbors"] == [{"rank": 1}]


def test_row_to_api_list_item_preview() -> None:
    row = {
        "suggestion_id": "x",
        "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "gcs_uri": None,
        "document_id": "d",
        "status": "Auto-Posted",
        "final_confidence": 0.9,
        "extraction": None,
        "suggestion": {
            "journal_lines": [{"account": "A"}],
            "confidence": 0.9,
            "rationale": "x" * 200,
        },
        "neighbors": None,
        "confidence_meta": None,
    }
    item = row_to_api_list_item(row)
    assert item["suggestion_id"] == "x"
    assert len(item["rationale_preview"]) <= 165
    assert item["journal_lines_preview"][0]["account"] == "A"


def test_insert_suggestion_row_serializes_neighbors_as_json_string() -> None:
    """Streaming insert must not pass a Python list for a JSON column (BQ: non-repeated field)."""
    client = MagicMock()
    client.insert_rows_json = MagicMock(return_value=[])
    insert_suggestion_row(
        client,
        "proj.ds.rag_suggestions",
        {
            "suggestion_id": "s1",
            "created_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "neighbors": [{"rank": 1}],
            "extraction": {"document_id": "a"},
            "suggestion": {"confidence": 0.5},
            "confidence_meta": {"k": 1},
        },
    )
    client.insert_rows_json.assert_called_once()
    row = client.insert_rows_json.call_args[0][1][0]
    assert row["neighbors"] == '[{"rank": 1}]'
    assert row["extraction"] == '{"document_id": "a"}'
    assert isinstance(row["suggestion"], str)
