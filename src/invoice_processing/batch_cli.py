"""CLI: list invoice PDFs in GCS, run Vertex Gemini batch extraction, load BigQuery."""

from __future__ import annotations

import os
import sys

from invoice_processing.batch_invoice_extract import BatchExtractConfig, run_batch_extract


def _require(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"error: set {name}", file=sys.stderr)
        sys.exit(1)
    return v


def main() -> None:
    project_id = _require("GCP_PROJECT")
    vertex_location = os.environ.get("VERTEX_LOCATION", "us-central1").strip()
    gcs_bucket = _require("GCS_BUCKET")
    invoice_prefix = os.environ.get("GCS_INVOICE_PREFIX", "historical/invoices").strip()
    batch_staging = os.environ.get("GCS_BATCH_STAGING_PREFIX", "batch").strip()
    gemini_model = os.environ.get("VERTEX_GEMINI_MODEL", "gemini-2.5-flash").strip()
    bq_dataset = _require("BQ_DATASET")
    bq_table = os.environ.get("BQ_TABLE", "invoice_extractions").strip()
    bq_location = os.environ.get("BQ_LOCATION", "US").strip()
    max_per = int(os.environ.get("BATCH_MAX_INVOICES_PER_JOB", "500"))

    cfg = BatchExtractConfig(
        project_id=project_id,
        vertex_location=vertex_location,
        gcs_bucket=gcs_bucket,
        invoice_prefix=invoice_prefix,
        batch_staging_prefix=batch_staging,
        gemini_model=gemini_model,
        bq_dataset=bq_dataset,
        bq_table=bq_table,
        bq_location=bq_location,
        max_invoices_per_job=max_per,
    )
    summary = run_batch_extract(cfg)
    print(
        f"pdfs={len(summary.pdf_uris)} jobs={len(summary.batch_job_names)} "
        f"rows_loaded={summary.rows_loaded} parse_errors={len(summary.parse_errors)}"
    )
    for j in summary.batch_job_names:
        print(f"  job: {j}")
    for e in summary.parse_errors[:20]:
        print(f"  parse_error: {e}")
    if len(summary.parse_errors) > 20:
        print(f"  ... and {len(summary.parse_errors) - 20} more")


if __name__ == "__main__":
    main()
