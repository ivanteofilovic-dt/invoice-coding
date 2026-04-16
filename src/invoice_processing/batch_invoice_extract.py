"""Orchestrate GCS PDF listing, Vertex Gemini batch JSONL, and BigQuery loads."""

from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from google import genai
from google.cloud import storage
from google.genai.types import CreateBatchJobConfig, HttpOptions, JobState

from invoice_processing.bq_invoice_extractions import (
    ensure_invoice_extractions_table,
    load_ndjson_rows,
)
from invoice_processing.gcs_invoice_listing import list_invoice_pdf_uris
from invoice_processing.invoice_extraction_schema import build_batch_jsonl_line

if TYPE_CHECKING:
    from google.cloud.bigquery import Client as BigQueryClient


def chunk_list[T](items: list[T], chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        msg = "chunk_size must be positive"
        raise ValueError(msg)
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _json_numeric_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _normalize_tax_lines(raw: Any) -> list[dict[str, Any]] | None:
    if not raw:
        return None
    if not isinstance(raw, list):
        return None
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "tax_category": row.get("tax_category"),
                "tax_rate_percent": _json_numeric_str(row.get("tax_rate_percent")),
                "taxable_amount": _json_numeric_str(row.get("taxable_amount")),
                "tax_amount": _json_numeric_str(row.get("tax_amount")),
            }
        )
    return out or None


def _normalize_invoice_lines(raw: Any) -> list[dict[str, Any]] | None:
    if not raw:
        return None
    if not isinstance(raw, list):
        return None
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        ln = row.get("line_number")
        try:
            line_number = int(ln) if ln is not None else None
        except (TypeError, ValueError):
            line_number = None
        out.append(
            {
                "line_number": line_number,
                "item_number": row.get("item_number"),
                "item_name": row.get("item_name"),
                "quantity": _json_numeric_str(row.get("quantity")),
                "unit_code": row.get("unit_code"),
                "net_unit_price": _json_numeric_str(row.get("net_unit_price")),
                "tax_details": row.get("tax_details"),
                "allowance_charge": row.get("allowance_charge"),
                "line_amount": _json_numeric_str(row.get("line_amount")),
                "line_note": row.get("line_note"),
                "orderline_reference": row.get("orderline_reference"),
            }
        )
    return out or None


def _normalize_document_totals(raw: Any) -> dict[str, Any] | None:
    if not raw or not isinstance(raw, dict):
        return None
    return {
        "rounding_amount": _json_numeric_str(raw.get("rounding_amount")),
        "total_amount_excl_tax": _json_numeric_str(raw.get("total_amount_excl_tax")),
        "total_tax_amount": _json_numeric_str(raw.get("total_tax_amount")),
        "total_amount_incl_tax": _json_numeric_str(raw.get("total_amount_incl_tax")),
    }


def extraction_payload_to_bq_row(
    payload: dict[str, Any],
    *,
    gcs_uri: str,
    model_id: str,
    batch_job_name: str,
    extracted_at: datetime,
) -> dict[str, Any]:
    """Map model JSON (schema-shaped) to a BigQuery NDJSON row dict."""
    extras = payload.get("extras")
    if extras is not None and not isinstance(extras, dict):
        extras = {"_raw": extras}

    row: dict[str, Any] = {
        "gcs_uri": gcs_uri,
        "model_id": model_id,
        "batch_job_name": batch_job_name,
        "extracted_at": extracted_at.replace(tzinfo=timezone.utc).isoformat(),
        "issue_date": payload.get("issue_date"),
        "due_date": payload.get("due_date"),
        "invoice_number": payload.get("invoice_number"),
        "buyer_order_number": payload.get("buyer_order_number"),
        "currency_code": payload.get("currency_code"),
        "header_note": payload.get("header_note"),
        "supplier": payload.get("supplier"),
        "customer": payload.get("customer"),
        "delivery": payload.get("delivery"),
        "tax_lines": _normalize_tax_lines(payload.get("tax_lines")),
        "payment": payload.get("payment"),
        "invoice_lines": _normalize_invoice_lines(payload.get("invoice_lines")),
        "document_totals": _normalize_document_totals(payload.get("document_totals")),
        "extras": extras,
    }
    return row


_CODE_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


def parse_model_json_text(text: str) -> dict[str, Any]:
    """Parse JSON from model output, stripping optional markdown fences."""
    cleaned = text.strip()
    cleaned = _CODE_FENCE.sub("", cleaned).strip()
    return json.loads(cleaned)


def gcs_uri_from_batch_request(request: dict[str, Any]) -> str | None:
    for content in request.get("contents") or []:
        for part in content.get("parts") or []:
            fd = part.get("fileData") or part.get("file_data")
            if isinstance(fd, dict) and fd.get("fileUri"):
                return str(fd["fileUri"])
    return None


def response_text_from_batch_output(obj: dict[str, Any]) -> str | None:
    resp = obj.get("response") or {}
    cands = resp.get("candidates") or []
    if not cands:
        return None
    parts = (cands[0].get("content") or {}).get("parts") or []
    for p in parts:
        if isinstance(p, dict) and p.get("text"):
            return str(p["text"])
    return None


def parse_batch_output_line(
    line: str,
) -> tuple[str | None, dict[str, Any] | None, str | None]:
    """Parse one Vertex batch output JSONL line.

    Returns ``(gcs_uri, extraction_payload, error)`` — at most one of payload/error.
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        return None, None, f"invalid json: {e}"
    err = (obj.get("status") or "").strip()
    if err:
        req = obj.get("request") or {}
        uri = gcs_uri_from_batch_request(req) if isinstance(req, dict) else None
        return uri, None, err
    req = obj.get("request") or {}
    if not isinstance(req, dict):
        return None, None, "missing request"
    gcs_uri = gcs_uri_from_batch_request(req)
    text = response_text_from_batch_output(obj)
    if not text:
        return gcs_uri, None, "empty response"
    try:
        payload = parse_model_json_text(text)
    except (json.JSONDecodeError, ValueError) as e:
        return gcs_uri, None, f"model json: {e}"
    if not isinstance(payload, dict):
        return gcs_uri, None, "model output not an object"
    return gcs_uri, payload, None


def upload_text_blob(
    bucket_name: str,
    object_name: str,
    data: str,
    *,
    client: storage.Client | None = None,
    content_type: str = "application/jsonl",
) -> str:
    """Upload UTF-8 text to GCS; returns ``gs://`` URI."""
    c = client or storage.Client()
    bucket = c.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket_name}/{object_name}"


def read_jsonl_lines_from_gcs_uri(
    gcs_uri: str,
    *,
    client: storage.Client | None = None,
) -> list[str]:
    """Read a single JSONL object blob from ``gs://``."""
    if not gcs_uri.startswith("gs://"):
        msg = f"Expected gs:// URI, got {gcs_uri!r}"
        raise ValueError(msg)
    rest = gcs_uri[5:]
    bucket_name, _, path = rest.partition("/")
    c = client or storage.Client()
    blob = c.bucket(bucket_name).blob(path)
    raw = blob.download_as_bytes()
    text = raw.decode("utf-8")
    return [ln for ln in text.splitlines() if ln.strip()]


def list_output_jsonl_uris(
    bucket_name: str,
    prefix: str,
    *,
    client: storage.Client | None = None,
) -> list[str]:
    """List ``gs://`` URIs for JSONL blobs under ``prefix`` (Vertex batch output)."""
    c = client or storage.Client()
    b = c.bucket(bucket_name)
    out: list[str] = []
    for blob in b.list_blobs(prefix=prefix):
        name_lower = blob.name.lower()
        if blob.name.endswith(".jsonl") or "prediction" in name_lower:
            out.append(f"gs://{bucket_name}/{blob.name}")
    return sorted(set(out))


def read_all_batch_output_lines(
    bucket_name: str,
    output_prefix: str,
    *,
    client: storage.Client | None = None,
) -> list[str]:
    """Read and concatenate JSONL lines from all output blobs under a prefix."""
    c = client or storage.Client()
    uris = list_output_jsonl_uris(bucket_name, output_prefix, client=c)
    lines: list[str] = []
    for uri in uris:
        lines.extend(read_jsonl_lines_from_gcs_uri(uri, client=c))
    return lines


@dataclass
class BatchExtractConfig:
    project_id: str
    vertex_location: str
    gcs_bucket: str
    invoice_prefix: str
    batch_staging_prefix: str
    gemini_model: str
    bq_dataset: str
    bq_table: str
    bq_location: str = "US"
    max_invoices_per_job: int = 500
    poll_interval_seconds: float = 15.0


@dataclass
class BatchExtractSummary:
    """Aggregate result of a :func:`run_batch_extract` call."""

    pdf_uris: list[str]
    batch_job_names: list[str]
    rows_loaded: int
    parse_errors: list[str]


def make_genai_client(project_id: str, location: str) -> genai.Client:
    return genai.Client(
        vertexai=True,
        project=project_id,
        location=location,
        http_options=HttpOptions(api_version="v1"),
    )


def wait_batch_job(
    client: genai.Client,
    job_name: str,
    *,
    poll_interval_seconds: float = 15.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Any:
    ended = {
        JobState.JOB_STATE_SUCCEEDED,
        JobState.JOB_STATE_FAILED,
        JobState.JOB_STATE_CANCELLED,
        JobState.JOB_STATE_PAUSED,
    }
    while True:
        job = client.batches.get(name=job_name)
        if job.state in ended:
            return job
        sleep_fn(poll_interval_seconds)


def run_batch_extract(
    cfg: BatchExtractConfig,
    *,
    storage_client: storage.Client | None = None,
    genai_client: genai.Client | None = None,
    bq_client: BigQueryClient | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> BatchExtractSummary:
    """List PDFs, run Vertex batch job(s), parse output, append rows to BigQuery."""
    from google.cloud import bigquery

    sc = storage_client or storage.Client()
    gc = genai_client or make_genai_client(cfg.project_id, cfg.vertex_location)
    bqc = bq_client or bigquery.Client(project=cfg.project_id)

    pdf_uris = list_invoice_pdf_uris(cfg.gcs_bucket, cfg.invoice_prefix, client=sc)
    if not pdf_uris:
        ensure_invoice_extractions_table(
            bqc,
            cfg.bq_dataset,
            cfg.bq_table,
            project_id=cfg.project_id,
            location=cfg.bq_location,
        )
        return BatchExtractSummary(
            pdf_uris=[],
            batch_job_names=[],
            rows_loaded=0,
            parse_errors=[],
        )

    run_id = uuid.uuid4().hex[:12]
    base = cfg.batch_staging_prefix.strip().strip("/")
    staging_base = f"{base}/{run_id}" if base else run_id

    batch_job_names: list[str] = []
    parse_errors: list[str] = []
    rows: list[dict[str, Any]] = []
    extracted_at = datetime.now(timezone.utc)

    for chunk_idx, chunk in enumerate(chunk_list(pdf_uris, cfg.max_invoices_per_job)):
        lines = "\n".join(build_batch_jsonl_line(u) for u in chunk) + "\n"
        input_object = f"{staging_base}/input_{chunk_idx}.jsonl"
        input_uri = upload_text_blob(
            cfg.gcs_bucket, input_object, lines, client=sc, content_type="application/jsonl"
        )
        output_prefix = f"{staging_base}/out_{chunk_idx}/"
        job = gc.batches.create(
            model=cfg.gemini_model,
            src=input_uri,
            config=CreateBatchJobConfig(
                display_name=f"invoice-extract-{run_id}-{chunk_idx}",
                dest=f"gs://{cfg.gcs_bucket}/{output_prefix}",
            ),
        )
        if not job.name:
            msg = "batch job missing name"
            raise RuntimeError(msg)
        batch_job_names.append(job.name)
        job = wait_batch_job(
            gc,
            job.name,
            poll_interval_seconds=cfg.poll_interval_seconds,
            sleep_fn=sleep_fn,
        )
        if job.state != JobState.JOB_STATE_SUCCEEDED:
            msg = f"batch job failed: {job.name} state={job.state!r} error={job.error!r}"
            raise RuntimeError(msg)

        out_lines = read_all_batch_output_lines(
            cfg.gcs_bucket, output_prefix, client=sc
        )
        for ln in out_lines:
            gcs_uri, payload, err = parse_batch_output_line(ln)
            if err:
                parse_errors.append(f"{gcs_uri or '?'}: {err}")
                continue
            assert gcs_uri and payload
            rows.append(
                extraction_payload_to_bq_row(
                    payload,
                    gcs_uri=gcs_uri,
                    model_id=cfg.gemini_model,
                    batch_job_name=job.name,
                    extracted_at=extracted_at,
                )
            )
    table_ref = ensure_invoice_extractions_table(
        bqc,
        cfg.bq_dataset,
        cfg.bq_table,
        project_id=cfg.project_id,
        location=cfg.bq_location,
    )
    if rows:
        load_ndjson_rows(bqc, table_ref, rows, location=cfg.bq_location)
    return BatchExtractSummary(
        pdf_uris=pdf_uris,
        batch_job_names=batch_job_names,
        rows_loaded=len(rows),
        parse_errors=parse_errors,
    )
