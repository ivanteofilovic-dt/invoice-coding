"""Tests for BigQuery invoice embedding SQL builders and table helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from invoice_processing.bq_invoice_embeddings import (
    EMBED_TEXT_VERSION,
    DEFAULT_OUTPUT_DIMENSIONALITY,
    IVF_VECTOR_INDEX_MIN_ROW_COUNT,
    _assert_ident,
    _sanitize_precomputed_embedding_row,
    build_backfill_embeddings_insert_sql,
    build_create_remote_embedding_model_ddl,
    build_create_vector_index_ddl,
    build_embeddings_table_health_sql,
    build_invoice_embed_text_view_ddl,
    build_invoice_gl_context_view_ddl,
    build_rag_neighbors_with_gl_sql,
    build_rag_neighbors_with_gl_stored_embedding_sql,
    build_vector_search_by_gcs_uri_sql,
    build_vector_search_by_stored_embedding_sql,
    ensure_invoice_embeddings_table,
    invoice_embedding_inner_select_sql,
    invoice_embeddings_schema,
)


def test_assert_ident_rejects_dotted_name() -> None:
    with pytest.raises(ValueError, match="Invalid BigQuery identifier"):
        _assert_ident("foo.bar")


def test_assert_ident_accepts_hyphenated_project_style_id() -> None:
    assert _assert_ident("my-gcp-project-123") == "my-gcp-project-123"


def test_invoice_embedding_inner_select_contains_supplier_and_lines() -> None:
    sql = invoice_embedding_inner_select_sql("`p`.`d`.`invoice_extractions`")
    assert "supplier.legal_name" in sql
    assert "UNNEST(e.invoice_lines)" in sql
    assert "FROM `p`.`d`.`invoice_extractions` AS e" in sql


def test_build_invoice_embed_text_view_ddl_contains_version() -> None:
    sql = build_invoice_embed_text_view_ddl(
        project_id="myproj",
        dataset_id="ds1",
        extractions_table_id="invoice_extractions",
        view_id="v_invoice_embed_text",
    )
    assert "CREATE OR REPLACE VIEW `myproj.ds1.v_invoice_embed_text`" in sql
    assert EMBED_TEXT_VERSION in sql
    assert "content_hash" in sql


def test_build_invoice_gl_context_view_ddl_groups_keys() -> None:
    sql = build_invoice_gl_context_view_ddl(
        project_id="p",
        dataset_id="d",
        gl_table_id="gl_lines",
        view_id="v_invoice_gl_context",
    )
    assert "CREATE OR REPLACE VIEW `p.d.v_invoice_gl_context`" in sql
    assert "invoice_key_norm" in sql
    assert "supplier_number_norm" in sql
    assert "FROM `p.d.gl_lines`" in sql


def test_build_backfill_contains_generate_embedding_and_insert() -> None:
    sql = build_backfill_embeddings_insert_sql(
        project_id="p",
        dataset_id="d",
        remote_model_id="m1",
    )
    assert "INSERT INTO `p.d.invoice_embeddings`" in sql
    assert "AI.GENERATE_EMBEDDING" in sql
    assert "RETRIEVAL_DOCUMENT" in sql
    assert str(DEFAULT_OUTPUT_DIMENSIONALITY) in sql
    assert "WHERE g.embedding IS NOT NULL" in sql
    assert "ARRAY_LENGTH(g.embedding) > 0" in sql


def test_ivf_vector_index_min_row_count_is_5000() -> None:
    assert IVF_VECTOR_INDEX_MIN_ROW_COUNT == 5000


def test_build_embeddings_table_health_sql() -> None:
    sql = build_embeddings_table_health_sql(
        project_id="p",
        dataset_id="d",
        table_id="invoice_embeddings",
    )
    assert "FROM `p.d.invoice_embeddings`" in sql
    assert "rows_with_vectors" in sql


def test_build_vector_search_stored_has_no_ai_generate() -> None:
    sql = build_vector_search_by_stored_embedding_sql(
        project_id="p",
        dataset_id="d",
        query_gcs_uri="gs://b/x.pdf",
        top_k=5,
    )
    assert "AI.GENERATE_EMBEDDING" not in sql
    assert "VECTOR_SEARCH" in sql
    assert "FROM `p.d.invoice_embeddings`" in sql


def test_build_rag_stored_uses_stored_vector_search() -> None:
    sql = build_rag_neighbors_with_gl_stored_embedding_sql(
        project_id="p",
        dataset_id="d",
        query_gcs_uri="gs://b/a.pdf",
    )
    assert "AI.GENERATE_EMBEDDING" not in sql
    assert "WITH hits AS" in sql


def test_sanitize_precomputed_embedding_row() -> None:
    row = {
        "gcs_uri": "gs://b/a.pdf",
        "embedding": [0.0, 0.25],
        "embed_text_version": "v1",
        "extras_ignored": None,
    }
    out = _sanitize_precomputed_embedding_row(row)
    assert out["embedding"] == [0.0, 0.25]
    assert "extras_ignored" not in out


def test_build_vector_search_uses_retrieval_query_and_cosine() -> None:
    sql = build_vector_search_by_gcs_uri_sql(
        project_id="p",
        dataset_id="d",
        query_gcs_uri="gs://b/x.pdf",
        top_k=5,
    )
    assert "RETRIEVAL_QUERY" in sql
    assert "VECTOR_SEARCH" in sql
    assert "COSINE" in sql
    assert "gs://b/x.pdf" in sql
    assert "LIMIT 5" in sql


def test_build_rag_neighbors_wraps_hits_subquery() -> None:
    sql = build_rag_neighbors_with_gl_sql(
        project_id="p",
        dataset_id="d",
        query_gcs_uri="gs://b/a.pdf",
        top_k=3,
    )
    assert "WITH hits AS" in sql
    assert "LEFT JOIN `p.d.v_invoice_gl_context` AS g" in sql


def test_build_create_remote_model_ddl() -> None:
    sql = build_create_remote_embedding_model_ddl(
        project_id="p",
        dataset_id="d",
        model_id="emb_model",
        connection_region="us-central1",
        connection_id="c1",
        endpoint="text-embedding-004",
    )
    assert "CREATE OR REPLACE MODEL `p.d.emb_model`" in sql
    assert "REMOTE WITH CONNECTION `p.us-central1.c1`" in sql
    assert "text-embedding-004" in sql


def test_build_create_vector_index_ddl() -> None:
    sql = build_create_vector_index_ddl(
        project_id="p",
        dataset_id="d",
        table_id="invoice_embeddings",
        index_id="idx1",
    )
    assert "CREATE VECTOR INDEX" in sql
    assert "`p.d.idx1`" in sql
    assert "index_type = 'IVF'" in sql
    assert "ivf_options = '{\"num_lists\": 100}'" in sql


def test_invoice_embeddings_schema_has_embedding_array() -> None:
    names = {f.name for f in invoice_embeddings_schema()}
    assert "embedding" in names
    assert "gcs_uri" in names


def test_ensure_invoice_embeddings_table_creates_dataset_and_table() -> None:
    client = MagicMock()
    client.project = "proj"
    ref = ensure_invoice_embeddings_table(
        client,
        "mydataset",
        "invoice_embeddings",
        project_id="proj",
        location="EU",
    )
    assert ref == "proj.mydataset.invoice_embeddings"
    client.create_dataset.assert_called_once()
    client.create_table.assert_called_once()
