"""Tests for coding suggestion normalization."""

from __future__ import annotations

from invoice_processing.invoice_coding_schema import normalize_coding_suggestion


def test_normalize_coding_suggestion_clamps_confidence() -> None:
    out = normalize_coding_suggestion(
        {
            "journal_lines": [{"account": "1"}],
            "confidence": 9,
            "rationale": "ok",
        }
    )
    assert out["confidence"] == 1.0
    assert len(out["journal_lines"]) == 1


def test_normalize_empty_journal_lines() -> None:
    out = normalize_coding_suggestion({"confidence": None, "rationale": None})
    assert out["journal_lines"] == []
    assert out["confidence"] == 0.0
