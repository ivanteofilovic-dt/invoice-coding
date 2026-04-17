"""CLI: load local GL TSV files into BigQuery (filtered gl_lines schema)."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from google.cloud import bigquery

from invoice_processing.bq_gl_lines import (
    collect_gl_txt_files,
    ensure_gl_lines_table,
    load_gl_txt_paths,
)

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
        description="Load local GL tab-separated exports into BigQuery. "
        "Uses GCP_PROJECT, BQ_DATASET, BQ_GL_TABLE (default gl_lines), "
        "BQ_LOCATION (default US), DATA_ROOT (default data). "
        "Optional GL_FILE_ENCODING: if unset, UTF-8 is tried first then cp1252 / Latin-1."
    )
    parser.add_argument(
        "txt_files",
        nargs="*",
        help="Paths to GL .txt files (default: all *.txt under DATA_ROOT/GL)",
    )
    parser.add_argument(
        "--write-disposition",
        choices=("TRUNCATE", "APPEND"),
        default="TRUNCATE",
        help="WRITE_TRUNCATE replaces table contents; WRITE_APPEND adds rows.",
    )
    args = parser.parse_args()

    project_id = _require("GCP_PROJECT")
    bq_dataset = _require("BQ_DATASET")
    bq_table = os.environ.get("BQ_GL_TABLE", "gl_lines").strip()
    bq_location = os.environ.get("BQ_LOCATION", "US").strip()
    data_root = Path(os.environ.get("DATA_ROOT", "data")).expanduser().resolve()
    gl_encoding = os.environ.get("GL_FILE_ENCODING", "").strip() or None

    if args.txt_files:
        paths = [Path(p).expanduser().resolve() for p in args.txt_files]
    else:
        paths = collect_gl_txt_files(data_root / "GL")

    if not paths:
        print(
            "error: no GL .txt files (pass paths or place *.txt under DATA_ROOT/GL)",
            file=sys.stderr,
        )
        sys.exit(1)

    for p in paths:
        if not p.is_file():
            print(f"error: not a file: {p}", file=sys.stderr)
            sys.exit(1)

    disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if args.write_disposition == "TRUNCATE"
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    table_ref = f"{project_id}.{bq_dataset}.{bq_table}"
    client = bigquery.Client(project=project_id, location=bq_location)
    ensure_gl_lines_table(
        client, bq_dataset, bq_table, project_id=project_id, location=bq_location
    )
    job = load_gl_txt_paths(
        client,
        table_ref,
        paths,
        write_disposition=disposition,
        location=bq_location,
        file_encoding=gl_encoding,
    )
    logger.info(
        "Load job %s finished (%s output rows) -> %s",
        job.job_id,
        job.output_rows,
        table_ref,
    )


if __name__ == "__main__":
    main()
