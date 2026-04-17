"""BigQuery table for persisted RAG coding suggestions (JSON payloads)."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from google.cloud.bigquery import Client as BigQueryClient

_DEFAULT_TABLE = "rag_suggestions"


def rag_suggestions_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("suggestion_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("gcs_uri", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("final_confidence", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("extraction", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("suggestion", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("neighbors", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("confidence_meta", "JSON", mode="NULLABLE"),
    ]


def ensure_rag_suggestions_table(
    client: BigQueryClient,
    dataset_id: str,
    table_id: str = _DEFAULT_TABLE,
    *,
    project_id: str | None = None,
    location: str = "US",
) -> str:
    pid = project_id or client.project
    dataset_ref = bigquery.Dataset(f"{pid}.{dataset_id}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)
    table_ref = f"{pid}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=rag_suggestions_schema())
    client.create_table(table, exists_ok=True)
    return table_ref


def insert_suggestion_row(
    client: BigQueryClient,
    table_ref: str,
    row: dict[str, Any],
    *,
    location: str = "US",
) -> str:
    """Insert one suggestion row; returns ``suggestion_id``."""
    _ = location  # streaming insert is global; kept for API symmetry
    sid = str(row.get("suggestion_id") or uuid.uuid4().hex)
    created = row.get("created_at")
    if isinstance(created, datetime):
        created_s = created.astimezone(timezone.utc).isoformat()
    elif isinstance(created, str) and created.strip():
        created_s = created.strip()
    else:
        created_s = datetime.now(timezone.utc).isoformat()

    errors = client.insert_rows_json(
        table_ref,
        [
            {
                "suggestion_id": sid,
                "created_at": created_s,
                "gcs_uri": row.get("gcs_uri"),
                "document_id": row.get("document_id"),
                "status": row.get("status"),
                "final_confidence": row.get("final_confidence"),
                "extraction": row.get("extraction"),
                "suggestion": row.get("suggestion"),
                "neighbors": row.get("neighbors"),
                "confidence_meta": row.get("confidence_meta"),
            }
        ],
    )
    if errors:
        logger.error("insert_rows_json errors: %s", errors)
        msg = f"BigQuery insert failed: {errors}"
        raise RuntimeError(msg)
    return sid


def list_suggestions(
    client: BigQueryClient,
    *,
    project_id: str,
    dataset_id: str,
    table_id: str = _DEFAULT_TABLE,
    limit: int = 50,
    location: str = "US",
) -> list[dict[str, Any]]:
    tid = f"`{project_id}.{dataset_id}.{table_id}`"
    sql = f"""
SELECT
  suggestion_id,
  created_at,
  gcs_uri,
  document_id,
  status,
  final_confidence,
  extraction,
  suggestion,
  neighbors,
  confidence_meta
FROM {tid}
ORDER BY created_at DESC
LIMIT {int(limit)}
""".strip()
    return [dict(r) for r in client.query(sql, location=location).result()]


def get_suggestion_by_id(
    client: BigQueryClient,
    *,
    project_id: str,
    dataset_id: str,
    suggestion_id: str,
    table_id: str = _DEFAULT_TABLE,
    location: str = "US",
) -> dict[str, Any] | None:
    sid = suggestion_id.replace("'", "''")
    tid = f"`{project_id}.{dataset_id}.{table_id}`"
    sql = f"""
SELECT
  suggestion_id,
  created_at,
  gcs_uri,
  document_id,
  status,
  final_confidence,
  extraction,
  suggestion,
  neighbors,
  confidence_meta
FROM {tid}
WHERE suggestion_id = '{sid}'
LIMIT 1
""".strip()
    rows = list(client.query(sql, location=location).result())
    if not rows:
        return None
    return dict(rows[0])


def _json_field(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def row_to_api_detail(row: dict[str, Any]) -> dict[str, Any]:
    """BQ row → ``SuggestionDetailResponse``-shaped dict."""
    extraction = _json_field(row.get("extraction"))
    suggestion = _json_field(row.get("suggestion")) or {
        "journal_lines": [],
        "confidence": 0.0,
        "rationale": "",
    }
    neighbors = _json_field(row.get("neighbors")) or []
    meta = _json_field(row.get("confidence_meta")) or {}
    fc = row.get("final_confidence")
    try:
        final_c = float(fc) if fc is not None else float(suggestion.get("confidence") or 0.0)
    except (TypeError, ValueError):
        final_c = 0.0
    created = row.get("created_at")
    created_s = created.isoformat() if hasattr(created, "isoformat") else str(created or "")
    return {
        "suggestion_id": row.get("suggestion_id"),
        "document_id": row.get("document_id"),
        "gcs_uri": row.get("gcs_uri"),
        "created_at": created_s,
        "suggestion": suggestion,
        "final_confidence": final_c,
        "confidence_meta": meta if isinstance(meta, dict) else {},
        "status": row.get("status") or "Unknown",
        "neighbors": neighbors if isinstance(neighbors, list) else [],
        "extraction": extraction if isinstance(extraction, dict) else None,
    }


def row_to_api_list_item(row: dict[str, Any]) -> dict[str, Any]:
    """BQ row → ``SuggestionListItem``-shaped dict."""
    detail = row_to_api_detail(row)
    sug = detail["suggestion"]
    rationale = str(sug.get("rationale") or "")
    preview = rationale[:160] + ("…" if len(rationale) > 160 else "")
    jl = sug.get("journal_lines") if isinstance(sug, dict) else []
    lines = jl if isinstance(jl, list) else []
    return {
        "suggestion_id": detail["suggestion_id"],
        "document_id": detail["document_id"],
        "gcs_uri": detail["gcs_uri"],
        "confidence": float(sug.get("confidence") or 0.0) if isinstance(sug, dict) else 0.0,
        "status": detail["status"],
        "created_at": detail["created_at"],
        "rationale_preview": preview,
        "journal_lines_preview": lines[:3] if lines else [],
    }
