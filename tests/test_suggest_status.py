"""Tests for status derivation from confidence and neighbors."""

from __future__ import annotations

from invoice_processing.suggest_status import derive_status_and_final_confidence


def test_no_neighbors_anomaly() -> None:
    st, fc = derive_status_and_final_confidence(0.9, [])
    assert st == "Anomaly Flagged"
    assert fc < 0.5


def test_auto_posted_high_conf_and_gl() -> None:
    neighbors = [
        {
            "similarity": 0.5,
            "training": {"account": "5000", "cost_center": "D1"},
            "gl_lines_preview": [],
        }
    ]
    st, fc = derive_status_and_final_confidence(0.9, neighbors)
    assert st == "Auto-Posted"
    assert fc >= 0.85


def test_needs_review_no_gl() -> None:
    neighbors = [{"similarity": 0.9, "training": {}, "gl_lines_preview": []}]
    st, _fc = derive_status_and_final_confidence(0.95, neighbors)
    assert st == "Needs Review"
