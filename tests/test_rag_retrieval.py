"""Tests for RAG neighbor mapping and BigQuery value materialization."""

from __future__ import annotations

from invoice_processing.rag_retrieval import (
    _materialize_bq_value,
    bq_row_to_neighbor_record,
    cosine_distance_to_similarity,
)


def test_cosine_distance_to_similarity() -> None:
    assert cosine_distance_to_similarity(None) is None
    assert cosine_distance_to_similarity(0.0) == 1.0
    assert cosine_distance_to_similarity(1.0) == 0.0
    assert cosine_distance_to_similarity(2.0) == 0.0


def test_bq_row_to_neighbor_record_with_gl() -> None:
    row = {
        "gcs_uri": "gs://b/hist/x.pdf",
        "distance": 0.2,
        "gl_lines_recent": [
            {
                "booking_date": "2024-01-02",
                "period": "202401",
                "account": "5000",
                "hfm_account": "H1",
                "gl_line_description": "Line A",
                "department": "D1",
                "product": "P1",
                "net_accounted": "100.00",
                "transaction_type_name": "INV",
            }
        ],
        "gl_line_count": 3,
        "net_accounted_sum": 100,
    }
    rec = bq_row_to_neighbor_record(row, rank=1)
    assert rec["rank"] == 1
    assert rec["document_id"] == "gs://b/hist/x.pdf"
    assert rec["cosine_distance"] == 0.2
    assert rec["similarity"] == 0.8
    assert rec["training"]["account"] == "5000"
    assert rec["training"]["cost_center"] == "D1"
    assert len(rec["gl_lines_preview"]) >= 1


def test_materialize_nested() -> None:
    class FakeRow:
        def keys(self):
            return ["a"]

        def __getitem__(self, k: str):
            if k == "a":
                return {"nested": 1}
            raise KeyError

    out = _materialize_bq_value(FakeRow())
    assert out == {"a": {"nested": 1}}
