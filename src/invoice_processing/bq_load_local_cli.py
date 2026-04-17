"""CLI: load local NDJSON (.jsonl) files into the invoice extractions BigQuery table."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from google.cloud import bigquery

from invoice_processing.batch_invoice_extract import batch_output_jsonl_paths_to_bq_rows
from invoice_processing.bq_invoice_extractions import load_ndjson_jsonl_files, load_ndjson_rows

logger = logging.getLogger(__name__)


def _require(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"error: set {name}", file=sys.stderr)
        sys.exit(1)
    return v


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Load local newline-delimited JSON files into BigQuery "
        "(invoice_extractions schema). Uses GCP_PROJECT, BQ_DATASET, "
        "BQ_TABLE (default invoice_extractions), BQ_LOCATION (default US)."
    )
    parser.add_argument(
        "--from-batch-output",
        action="store_true",
        help="Input files are Vertex batch JSONL (request/response per line), "
        "not flat table rows — parse and load like the batch pipeline.",
    )
    parser.add_argument(
        "jsonl_files",
        nargs="+",
        help="Paths to .jsonl files (one load job per file unless --from-batch-output)",
    )
    args = parser.parse_args()

    project_id = _require("GCP_PROJECT")
    bq_dataset = _require("BQ_DATASET")
    bq_table = os.environ.get("BQ_TABLE", "invoice_extractions").strip()
    bq_location = os.environ.get("BQ_LOCATION", "US").strip()
    table_ref = f"{project_id}.{bq_dataset}.{bq_table}"

    client = bigquery.Client(project=project_id, location=bq_location)
    if args.from_batch_output:
        model_id = os.environ.get("VERTEX_GEMINI_MODEL", "local-import").strip()
        batch_job_name = os.environ.get("BATCH_JOB_NAME", "local-jsonl").strip()
        rows, parse_errors = batch_output_jsonl_paths_to_bq_rows(
            args.jsonl_files,
            model_id=model_id,
            batch_job_name=batch_job_name,
        )
        for e in parse_errors[:50]:
            logger.warning("%s", e)
        if len(parse_errors) > 50:
            logger.warning("... and %s more parse errors", len(parse_errors) - 50)
        if not rows:
            print("error: no rows to load (fix parse errors or check files)", file=sys.stderr)
            sys.exit(1)
        job = load_ndjson_rows(client, table_ref, rows, location=bq_location)
        logger.info(
            "Load job %s finished (%s rows from batch output)",
            job.job_id,
            job.output_rows,
        )
    else:
        jobs = load_ndjson_jsonl_files(
            client, table_ref, args.jsonl_files, location=bq_location
        )
        for path, job in zip(args.jsonl_files, jobs, strict=True):
            logger.info(
                "Load job %s finished (%s rows from %s)",
                job.job_id,
                job.output_rows,
                path,
            )


if __name__ == "__main__":
    main()
