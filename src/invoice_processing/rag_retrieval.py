"""Run BigQuery VECTOR_SEARCH + GL context and map rows for the API / Gemini."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from invoice_processing.bq_invoice_embeddings import (
    DEFAULT_OUTPUT_DIMENSIONALITY,
    build_rag_neighbors_with_gl_sql,
)


def _materialize_bq_value(value: Any) -> Any:
    """Convert BigQuery ``Row`` / nested structures to plain JSON types."""
    if value is None:
        return None
    if isinstance(value, list):
        return [_materialize_bq_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _materialize_bq_value(v) for k, v in value.items()}
    if hasattr(value, "keys") and callable(getattr(value, "keys")) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        try:
            return {k: _materialize_bq_value(value[k]) for k in value.keys()}
        except (TypeError, KeyError, ValueError):
            return value
    return value


@dataclass(frozen=True)
class RagRetrievalConfig:
    project_id: str
    dataset_id: str
    extractions_table: str = "invoice_extractions"
    embed_text_view: str = "v_invoice_embed_text"
    embeddings_table: str = "invoice_embeddings"
    gl_context_view: str = "v_invoice_gl_context"
    remote_model: str = "bqml_invoice_embedding"
    output_dimensionality: int = DEFAULT_OUTPUT_DIMENSIONALITY
    bq_location: str = "US"
    top_k: int = 10


def cosine_distance_to_similarity(distance: float | None) -> float | None:
    """BQ ``VECTOR_SEARCH`` cosine *distance* is in ``[0, 2]`` for normalized vectors.

    Map to a loose ``[0, 1]`` similarity for UI: ``max(0, 1 - distance)``.
    """
    if distance is None:
        return None
    try:
        d = float(distance)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, 1.0 - d))


def _gl_line_to_training_snippet(gl: dict[str, Any]) -> dict[str, Any]:
    """One GL struct row (from ``gl_lines_recent``) → ``TrainingSnippet``-like dict."""
    return {
        "account": gl.get("account"),
        "cost_center": gl.get("department"),
        "product_code": gl.get("product"),
        "ic": gl.get("ic"),
        "project": gl.get("project"),
        "gl_system": gl.get("gl_system"),
        "reserve": gl.get("reserve"),
        "line_description": gl.get("gl_line_description"),
        "line_amount": gl.get("net_accounted"),
        "currency": None,
        "posting_date": gl.get("booking_date"),
        "invoice_date": None,
        "invoice_number": None,
        "supplier": None,
        "join_key": None,
        "hfm_account": gl.get("hfm_account"),
        "period": gl.get("period"),
        "transaction_type_name": gl.get("transaction_type_name"),
    }


def bq_row_to_neighbor_record(
    row: dict[str, Any],
    *,
    rank: int,
) -> dict[str, Any]:
    """Map one BigQuery result row to API ``NeighborRecord`` (plain dict)."""
    gcs_uri = str(row.get("gcs_uri") or "")
    dist = row.get("distance")
    dist_f: float | None
    try:
        dist_f = float(dist) if dist is not None else None
    except (TypeError, ValueError):
        dist_f = None
    sim = cosine_distance_to_similarity(dist_f)

    gl_recent = row.get("gl_lines_recent")
    training: dict[str, Any] | None = None
    if isinstance(gl_recent, list) and gl_recent:
        first = gl_recent[0]
        if isinstance(first, dict):
            training = _gl_line_to_training_snippet(first)
            training["join_key"] = f"{gcs_uri}#gl0"
    elif row.get("gl_line_count") or row.get("net_accounted_sum"):
        training = {
            "join_key": f"{gcs_uri}#summary",
            "line_description": f"GL lines={row.get('gl_line_count')}, net_sum={row.get('net_accounted_sum')}",
        }

    return {
        "rank": rank,
        "join_key": gcs_uri or f"unknown-{rank}",
        "invoice_line_id": f"{gcs_uri}#doc",
        "document_id": gcs_uri,
        "line_index": 0,
        "cosine_distance": dist_f,
        "similarity": sim,
        "training": training,
        "query_line_index": None,
        "gcs_uri": gcs_uri,
        "gl_line_count": row.get("gl_line_count"),
        "gl_lines_preview": [
            _gl_line_to_training_snippet(x)
            for x in (gl_recent or [])[:5]
            if isinstance(x, dict)
        ],
    }


def neighbors_for_llm_context(records: list[dict[str, Any]]) -> str:
    """Compact text block for the coding model."""
    parts: list[str] = []
    for rec in records:
        parts.append(
            f"- rank={rec.get('rank')} gcs_uri={rec.get('document_id')} "
            f"cosine_distance={rec.get('cosine_distance')} similarity={rec.get('similarity')}"
        )
        prev = rec.get("gl_lines_preview") or []
        if isinstance(prev, list) and prev:
            for i, gl in enumerate(prev[:3]):
                if isinstance(gl, dict):
                    parts.append(
                        f"    gl[{i}]: account={gl.get('account')} dept={gl.get('cost_center')} "
                        f"product={gl.get('product_code')} ic={gl.get('ic')} "
                        f"project={gl.get('project')} system={gl.get('gl_system')} "
                        f"reserve={gl.get('reserve')} period={gl.get('period')} "
                        f"net={gl.get('line_amount')} memo={gl.get('line_description')}"
                    )
        elif rec.get("training") and isinstance(rec["training"], dict):
            t = rec["training"]
            parts.append(
                f"    gl: account={t.get('account')} dept={t.get('cost_center')} "
                f"product={t.get('product_code')} ic={t.get('ic')} "
                f"project={t.get('project')} system={t.get('gl_system')} "
                f"reserve={t.get('reserve')} period={t.get('period')} net={t.get('line_amount')}"
            )
    return "\n".join(parts) if parts else "(no neighbors)"


def fetch_rag_neighbors(
    client: BigQueryClient,
    cfg: RagRetrievalConfig,
    query_gcs_uri: str,
) -> list[dict[str, Any]]:
    """Execute RAG SQL and return ``NeighborRecord`` dicts."""
    sql = build_rag_neighbors_with_gl_sql(
        project_id=cfg.project_id,
        dataset_id=cfg.dataset_id,
        query_gcs_uri=query_gcs_uri,
        embed_text_view_id=cfg.embed_text_view,
        embeddings_table_id=cfg.embeddings_table,
        gl_context_view_id=cfg.gl_context_view,
        remote_model_id=cfg.remote_model,
        top_k=cfg.top_k,
        output_dimensionality=cfg.output_dimensionality,
    )
    rows = client.query(sql, location=cfg.bq_location).result()
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        raw = _materialize_bq_value(dict(row))
        out.append(bq_row_to_neighbor_record(raw, rank=i))
    return out
