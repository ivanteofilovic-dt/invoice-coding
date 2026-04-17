"""CLI: load local NDJSON (.jsonl) files into the invoice extractions BigQuery table."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from google.cloud import bigquery

from invoice_processing.bq_invoice_extractions import load_ndjson_jsonl_files

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
        "jsonl_files",
        nargs="+",
        help="Paths to .jsonl files (one load job per file)",
    )
    args = parser.parse_args()

    project_id = _require("GCP_PROJECT")
    bq_dataset = _require("BQ_DATASET")
    bq_table = os.environ.get("BQ_TABLE", "invoice_extractions").strip()
    bq_location = os.environ.get("BQ_LOCATION", "US").strip()
    table_ref = f"{project_id}.{bq_dataset}.{bq_table}"

    client = bigquery.Client(project=project_id, location=bq_location)
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
