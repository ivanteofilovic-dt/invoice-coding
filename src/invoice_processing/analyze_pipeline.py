"""End-to-end: GCS upload → extract → BQ row → RAG neighbors → Gemini coding → optional persist."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any

from google import genai
from google.cloud import storage
from google.cloud.bigquery import Client as BigQueryClient

from invoice_processing.bq_invoice_extractions import (
    ensure_invoice_extractions_table,
    load_ndjson_rows,
)
from invoice_processing.bq_rag_suggestions import (
    ensure_rag_suggestions_table,
    insert_suggestion_row,
)
from invoice_processing.invoice_coding_schema import suggest_coding
from invoice_processing.online_invoice_extract import extract_invoice_from_gcs_pdf
from invoice_processing.rag_retrieval import (
    RagRetrievalConfig,
    fetch_rag_neighbors,
    neighbors_for_llm_context,
)
from invoice_processing.suggest_status import derive_status_and_final_confidence


def upload_pdf_to_gcs(
    bucket: str,
    object_name: str,
    pdf_bytes: bytes,
    *,
    client: storage.Client | None = None,
) -> str:
    c = client or storage.Client()
    blob = c.bucket(bucket).blob(object_name)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    return f"gs://{bucket}/{object_name}"


def run_analyze_pdf(
    pdf_bytes: bytes,
    filename: str,
    *,
    persist: bool,
    project_id: str,
    vertex_location: str,
    gemini_model: str,
    gcs_bucket: str,
    new_invoice_prefix: str,
    bq_dataset: str,
    bq_extractions_table: str,
    bq_location: str,
    rag_top_k: int,
    embedding_output_dim: int,
    embed_text_view: str,
    embeddings_table: str,
    gl_context_view: str,
    remote_model: str,
    rag_suggestions_table: str,
    genai_client: genai.Client,
    bq_client: BigQueryClient,
    storage_client: storage.Client | None = None,
) -> dict[str, Any]:
    """Run full pipeline; returns an ``AnalyzeResponse``-shaped dict."""
    safe_name = filename.strip().replace("\\", "/").split("/")[-1] or "invoice.pdf"
    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"
    run_part = uuid.uuid4().hex[:12]
    object_name = f"{new_invoice_prefix.strip().strip('/')}/{run_part}_{safe_name}"
    gcs_uri = upload_pdf_to_gcs(
        gcs_bucket, object_name, pdf_bytes, client=storage_client
    )

    ext = extract_invoice_from_gcs_pdf(
        genai_client, model_id=gemini_model, gcs_uri=gcs_uri
    )
    ext_table_ref = ensure_invoice_extractions_table(
        bq_client,
        bq_dataset,
        bq_extractions_table,
        project_id=project_id,
        location=bq_location,
    )
    load_ndjson_rows(bq_client, ext_table_ref, [ext.bq_row], location=bq_location)

    rag_cfg = RagRetrievalConfig(
        project_id=project_id,
        dataset_id=bq_dataset,
        extractions_table=bq_extractions_table,
        embed_text_view=embed_text_view,
        embeddings_table=embeddings_table,
        gl_context_view=gl_context_view,
        remote_model=remote_model,
        output_dimensionality=embedding_output_dim,
        bq_location=bq_location,
        top_k=rag_top_k,
    )
    neighbors = fetch_rag_neighbors(bq_client, rag_cfg, gcs_uri)
    n_ctx = neighbors_for_llm_context(neighbors)
    coding = suggest_coding(
        genai_client,
        model_id=gemini_model,
        current_extraction=ext.payload,
        neighbors_context=n_ctx,
    )
    model_conf = float(coding.get("confidence") or 0.0)
    status, final_conf = derive_status_and_final_confidence(model_conf, neighbors)
    confidence_meta: dict[str, Any] = {
        "model_confidence": model_conf,
        "neighbor_count": len(neighbors),
        "max_neighbor_similarity": max(
            (n.get("similarity") or 0 for n in neighbors), default=None
        ),
    }
    suggestion_id = uuid.uuid4().hex
    document_id = ext.ui_extraction.get("document_id") or safe_name

    if persist:
        sug_ref = ensure_rag_suggestions_table(
            bq_client,
            bq_dataset,
            rag_suggestions_table,
            project_id=project_id,
            location=bq_location,
        )
        insert_suggestion_row(
            bq_client,
            sug_ref,
            {
                "suggestion_id": suggestion_id,
                "created_at": datetime.now(timezone.utc),
                "gcs_uri": gcs_uri,
                "document_id": document_id,
                "status": status,
                "final_confidence": final_conf,
                "extraction": ext.ui_extraction,
                "suggestion": coding,
                "neighbors": neighbors,
                "confidence_meta": confidence_meta,
            },
        )

    return {
        "extraction": ext.ui_extraction,
        "suggestion": coding,
        "neighbors": neighbors,
        "confidence_meta": confidence_meta,
        "final_confidence": final_conf,
        "status": status,
        "suggestion_id": suggestion_id if persist else None,
        "gcs_uri": gcs_uri,
    }


def read_settings_from_env() -> dict[str, Any]:
    """Load string/int settings from environment (shared by CLI and API)."""
    return {
        "project_id": os.environ.get("GCP_PROJECT", "").strip(),
        "vertex_location": os.environ.get("VERTEX_LOCATION", "us-central1").strip(),
        "gemini_model": os.environ.get("VERTEX_GEMINI_MODEL", "gemini-2.5-flash").strip(),
        "gcs_bucket": os.environ.get("GCS_BUCKET", "").strip(),
        "new_invoice_prefix": os.environ.get(
            "GCS_NEW_INVOICE_PREFIX", "new_invoices"
        ).strip(),
        "bq_dataset": os.environ.get("BQ_DATASET", "").strip(),
        "bq_extractions_table": os.environ.get(
            "BQ_TABLE", "invoice_extractions"
        ).strip(),
        "bq_location": os.environ.get("BQ_LOCATION", "US").strip(),
        "rag_top_k": int(os.environ.get("RAG_TOP_K", "10")),
        "embedding_output_dim": int(
            os.environ.get("BQ_EMBEDDING_OUTPUT_DIMENSIONALITY", "768")
        ),
        "embed_text_view": os.environ.get(
            "BQ_INVOICE_EMBED_TEXT_VIEW", "v_invoice_embed_text"
        ).strip(),
        "embeddings_table": os.environ.get(
            "BQ_INVOICE_EMBEDDINGS_TABLE", "invoice_embeddings"
        ).strip(),
        "gl_context_view": os.environ.get(
            "BQ_INVOICE_GL_CONTEXT_VIEW", "v_invoice_gl_context"
        ).strip(),
        "remote_model": os.environ.get(
            "BQ_EMBEDDING_REMOTE_MODEL", "bqml_invoice_embedding"
        ).strip(),
        "rag_suggestions_table": os.environ.get(
            "BQ_RAG_SUGGESTIONS_TABLE", "rag_suggestions"
        ).strip(),
        "embedding_model_display": os.environ.get(
            "BQ_EMBEDDING_ENDPOINT", "text-embedding-004"
        ).strip(),
        "confidence_high": float(os.environ.get("CONFIDENCE_HIGH_THRESHOLD", "0.85")),
        "confidence_low": float(os.environ.get("CONFIDENCE_LOW_THRESHOLD", "0.5")),
    }
