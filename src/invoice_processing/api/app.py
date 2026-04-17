"""FastAPI application: health, config, stats, suggestions, analyze."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, NotFound

from invoice_processing.analyze_pipeline import read_settings_from_env, run_analyze_pdf
from invoice_processing.batch_invoice_extract import make_genai_client
from invoice_processing.bq_rag_suggestions import (
    get_suggestion_by_id,
    list_suggestions,
    row_to_api_detail,
    row_to_api_list_item,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="AnkReg RAG API", version="0.1.0")


def _safe_count(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    *,
    location: str,
) -> int | None:
    tid = f"`{project_id}.{dataset_id}.{table_id}`"
    sql = f"SELECT COUNT(*) AS c FROM {tid}"
    try:
        rows = list(client.query(sql, location=location).result())
        if not rows:
            return None
        return int(rows[0]["c"])
    except (BadRequest, NotFound) as e:
        logger.info("count skip %s: %s", tid, e)
        return None
    except Exception:
        logger.exception("count failed for %s", tid)
        return None


@app.get("/api/health")
def api_health() -> dict[str, Any]:
    s = read_settings_from_env()
    return {
        "ok": bool(s["project_id"]),
        "gcp_project_set": bool(s["project_id"]),
        "gcs_bucket_set": bool(s["gcs_bucket"]),
    }


@app.get("/api/config")
def api_config() -> dict[str, Any]:
    s = read_settings_from_env()
    return {
        "gcp_project": s["project_id"] or None,
        "gcp_region": s["vertex_location"],
        "bq_dataset": s["bq_dataset"] or "",
        "gemini_model": s["gemini_model"],
        "embedding_model": s["embedding_model_display"],
        "rag_top_k": s["rag_top_k"],
        "rag_neighbors_per_line": s["rag_top_k"],
        "confidence_high_threshold": s["confidence_high"],
        "confidence_low_threshold": s["confidence_low"],
        "vector_search_backend": "bigquery_vector_search",
    }


@app.get("/api/stats")
def api_stats() -> dict[str, Any]:
    s = read_settings_from_env()
    if not s["project_id"] or not s["bq_dataset"]:
        return {
            "configured": False,
            "counts": {},
            "error": "GCP_PROJECT and BQ_DATASET must be set",
        }
    client = bigquery.Client(project=s["project_id"], location=s["bq_location"])
    loc = s["bq_location"]
    emb = _safe_count(
        client,
        s["project_id"],
        s["bq_dataset"],
        s["embeddings_table"],
        location=loc,
    )
    ext = _safe_count(
        client,
        s["project_id"],
        s["bq_dataset"],
        s["bq_extractions_table"],
        location=loc,
    )
    gl = _safe_count(client, s["project_id"], s["bq_dataset"], "gl_lines", location=loc)
    sug = _safe_count(
        client,
        s["project_id"],
        s["bq_dataset"],
        s["rag_suggestions_table"],
        location=loc,
    )
    return {
        "configured": True,
        "counts": {
            "invoice_line_embeddings": emb,
            "invoice_embeddings": emb,
            "rag_suggestions": sug,
            "invoice_extractions": ext,
            "gl_lines": gl,
        },
        "error": None,
    }


@app.get("/api/suggestions")
def api_suggestions(limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    s = read_settings_from_env()
    if not s["project_id"] or not s["bq_dataset"]:
        raise HTTPException(status_code=400, detail="BQ not configured")
    client = bigquery.Client(project=s["project_id"], location=s["bq_location"])
    try:
        rows = list_suggestions(
            client,
            project_id=s["project_id"],
            dataset_id=s["bq_dataset"],
            table_id=s["rag_suggestions_table"],
            limit=limit,
            location=s["bq_location"],
        )
    except (GoogleAPICallError, BadRequest, NotFound) as e:
        logger.info("suggestions list unavailable: %s", e)
        return {"items": []}
    return {"items": [row_to_api_list_item(r) for r in rows]}


@app.get("/api/suggestions/{suggestion_id}")
def api_suggestion_detail(suggestion_id: str) -> dict[str, Any]:
    s = read_settings_from_env()
    if not s["project_id"] or not s["bq_dataset"]:
        raise HTTPException(status_code=400, detail="BQ not configured")
    client = bigquery.Client(project=s["project_id"], location=s["bq_location"])
    try:
        row = get_suggestion_by_id(
            client,
            project_id=s["project_id"],
            dataset_id=s["bq_dataset"],
            suggestion_id=suggestion_id,
            table_id=s["rag_suggestions_table"],
            location=s["bq_location"],
        )
    except (GoogleAPICallError, BadRequest, NotFound) as e:
        logger.info("suggestion get failed: %s", e)
        raise HTTPException(status_code=404, detail="suggestion not found") from e
    if not row:
        raise HTTPException(status_code=404, detail="suggestion not found")
    return row_to_api_detail(row)


@app.post("/api/analyze")
async def api_analyze(
    file: UploadFile = File(...),
    persist: bool = Query(True),
) -> dict[str, Any]:
    s = read_settings_from_env()
    if not s["project_id"] or not s["gcs_bucket"] or not s["bq_dataset"]:
        raise HTTPException(
            status_code=400,
            detail="Set GCP_PROJECT, GCS_BUCKET, and BQ_DATASET",
        )
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")
    name = file.filename or "invoice.pdf"
    gc = make_genai_client(s["project_id"], s["vertex_location"])
    bq = bigquery.Client(project=s["project_id"], location=s["bq_location"])
    try:
        result = run_analyze_pdf(
            raw,
            name,
            persist=persist,
            project_id=s["project_id"],
            vertex_location=s["vertex_location"],
            gemini_model=s["gemini_model"],
            gcs_bucket=s["gcs_bucket"],
            new_invoice_prefix=s["new_invoice_prefix"],
            bq_dataset=s["bq_dataset"],
            bq_extractions_table=s["bq_extractions_table"],
            bq_location=s["bq_location"],
            rag_top_k=s["rag_top_k"],
            embedding_output_dim=s["embedding_output_dim"],
            embed_text_view=s["embed_text_view"],
            embeddings_table=s["embeddings_table"],
            gl_context_view=s["gl_context_view"],
            remote_model=s["remote_model"],
            rag_suggestions_table=s["rag_suggestions_table"],
            genai_client=gc,
            bq_client=bq,
        )
    except Exception as e:
        logger.exception("analyze failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
    return {
        "extraction": result["extraction"],
        "suggestion": result["suggestion"],
        "neighbors": result["neighbors"],
        "confidence_meta": result["confidence_meta"],
        "final_confidence": result["final_confidence"],
        "status": result["status"],
    }
