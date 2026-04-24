"""Synchronous Vertex Gemini extraction for a single invoice PDF (interactive / API)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from google import genai
from google.genai import types

from invoice_processing.batch_invoice_extract import (
    extraction_payload_to_bq_row,
    make_genai_client,
    parse_model_json_text,
)
from invoice_processing.invoice_extraction_schema import (
    EXTRACTION_SYSTEM_INSTRUCTION,
    invoice_response_json_schema,
)


@dataclass
class OnlineExtractResult:
    """Structured extraction for BigQuery load and API/UI projection."""

    gcs_uri: str
    payload: dict[str, Any]
    bq_row: dict[str, Any]
    ui_extraction: dict[str, Any]


def _supplier_display(supplier: dict[str, Any] | None) -> str | None:
    if not supplier or not isinstance(supplier, dict):
        return None
    return (
        supplier.get("legal_name")
        or supplier.get("name")
        or supplier.get("company_legal_id")
        or None
    )


def extraction_payload_to_ui_extraction(
    payload: dict[str, Any],
    *,
    document_id: str,
) -> dict[str, Any]:
    """Map model extraction JSON to the frontend ``Extraction`` shape (plain dicts)."""
    supplier = payload.get("supplier")
    if not isinstance(supplier, dict):
        supplier = None
    lines_raw = payload.get("invoice_lines")
    lines: list[dict[str, Any]] = []
    if isinstance(lines_raw, list):
        for il in lines_raw:
            if not isinstance(il, dict):
                continue
            ln = il.get("line_number")
            try:
                line_index = int(ln) if ln is not None else len(lines)
            except (TypeError, ValueError):
                line_index = len(lines)
            item_no = il.get("item_number") or ""
            join_key = f"L{line_index}|{item_no}"
            amt = il.get("line_amount")
            lines.append(
                {
                    "line_index": line_index,
                    "description": il.get("item_name") or il.get("line_note"),
                    "amount": str(amt) if amt is not None else None,
                    "join_key": join_key,
                }
            )
    delivery = payload.get("delivery")
    period_hint = None
    if isinstance(delivery, dict) and delivery.get("delivery_date"):
        period_hint = str(delivery.get("delivery_date"))
    return {
        "document_id": document_id,
        "supplier": _supplier_display(supplier),
        "invoice_number": payload.get("invoice_number"),
        "invoice_date": payload.get("issue_date"),
        "currency": payload.get("currency_code"),
        "periodization_hint": period_hint,
        "lines": lines,
    }


def _extract_with_pdf_parts(
    client: genai.Client,
    *,
    model_id: str,
    pdf_parts: list[types.Part],
    bq_gcs_uri: str,
    ui_document_id: str,
    batch_job_name: str | None = None,
) -> OnlineExtractResult:
    """Shared Gemini JSON extraction; ``bq_gcs_uri`` is stored in BigQuery (may be ``inline://…``)."""
    schema = invoice_response_json_schema()
    user_text = (
        f"{EXTRACTION_SYSTEM_INSTRUCTION}\n\n"
        "Extract all invoice fields from this PDF into JSON per the schema."
    )
    parts: list[types.Part] = [types.Part(text=user_text), *pdf_parts]
    config = types.GenerateContentConfig(
        temperature=0.1,
        max_output_tokens=8192,
        response_mime_type="application/json",
        response_json_schema=schema,
    )
    resp = client.models.generate_content(
        model=model_id,
        contents=[types.Content(role="user", parts=parts)],
        config=config,
    )
    candidate = resp.candidates[0] if resp.candidates else None
    finish_reason = str(getattr(candidate, "finish_reason", "")) if candidate else ""
    if finish_reason and "MAX_TOKENS" in finish_reason.upper():
        msg = (
            f"Extraction model response was truncated (finish_reason={finish_reason}). "
            "The PDF may be too large; try splitting it or increasing max_output_tokens."
        )
        raise RuntimeError(msg)
    text = (resp.text or "").strip()
    if not text:
        msg = "empty model response for extraction"
        raise RuntimeError(msg)
    payload = parse_model_json_text(text)
    if not isinstance(payload, dict):
        msg = "model extraction is not a JSON object"
        raise RuntimeError(msg)
    job_name = batch_job_name or f"online-{uuid.uuid4().hex[:16]}"
    extracted_at = datetime.now(timezone.utc)
    bq_row = extraction_payload_to_bq_row(
        payload,
        gcs_uri=bq_gcs_uri,
        model_id=model_id,
        batch_job_name=job_name,
        extracted_at=extracted_at,
    )
    ui_extraction = extraction_payload_to_ui_extraction(
        payload, document_id=ui_document_id
    )
    return OnlineExtractResult(
        gcs_uri=bq_gcs_uri,
        payload=payload,
        bq_row=bq_row,
        ui_extraction=ui_extraction,
    )


def extract_invoice_from_gcs_pdf(
    client: genai.Client,
    *,
    model_id: str,
    gcs_uri: str,
    batch_job_name: str | None = None,
) -> OnlineExtractResult:
    """Run one ``generate_content`` call against a PDF already in GCS (``gs://`` URI)."""
    doc_id = gcs_uri.rsplit("/", 1)[-1] if "/" in gcs_uri else gcs_uri
    return _extract_with_pdf_parts(
        client,
        model_id=model_id,
        pdf_parts=[types.Part.from_uri(file_uri=gcs_uri, mime_type="application/pdf")],
        bq_gcs_uri=gcs_uri,
        ui_document_id=doc_id,
        batch_job_name=batch_job_name,
    )


def extract_invoice_from_pdf_bytes(
    client: genai.Client,
    *,
    model_id: str,
    pdf_bytes: bytes,
    document_uri: str,
    display_filename: str,
    batch_job_name: str | None = None,
) -> OnlineExtractResult:
    """Extract from in-memory PDF (no GCS). ``document_uri`` is stored as ``gcs_uri`` in BigQuery (e.g. ``inline://…``)."""
    return _extract_with_pdf_parts(
        client,
        model_id=model_id,
        pdf_parts=[
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        ],
        bq_gcs_uri=document_uri,
        ui_document_id=display_filename,
        batch_job_name=batch_job_name,
    )


def make_inline_document_uri(*, run_id: str, filename: str) -> str:
    """Stable logical URI for BQ / RAG when no bucket (not a real ``gs://`` object)."""
    safe = filename.strip().replace("\\", "/").split("/")[-1] or "invoice.pdf"
    return f"inline://{run_id}/{safe}"


def default_genai_client(project_id: str, vertex_location: str) -> genai.Client:
    """Shared Vertex client factory (same settings as batch pipeline)."""
    return make_genai_client(project_id, vertex_location)
