"""Recreate the v_invoice_gl_context BigQuery view.

Usage:
    python scripts/recreate_gl_context_view.py

Required env vars (same as the API):
    GCP_PROJECT   - GCP project ID
    BQ_DATASET    - BigQuery dataset ID

Optional env vars:
    BQ_LOCATION   - BigQuery location (default: US)
    GL_TABLE      - GL lines table name (default: gl_lines)
    GL_VIEW       - View name to create/replace (default: v_invoice_gl_context)
"""

from __future__ import annotations

import os
import sys

from google.cloud import bigquery

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from invoice_processing.bq_invoice_embeddings import build_invoice_gl_context_view_ddl


def main() -> None:
    project = os.environ.get("GCP_PROJECT", "").strip()
    dataset = os.environ.get("BQ_DATASET", "").strip()
    location = os.environ.get("BQ_LOCATION", "US").strip()
    gl_table = os.environ.get("GL_TABLE", "gl_lines").strip()
    gl_view = os.environ.get("GL_VIEW", "v_invoice_gl_context").strip()

    if not project or not dataset:
        print("error: GCP_PROJECT and BQ_DATASET must be set", file=sys.stderr)
        sys.exit(1)

    ddl = build_invoice_gl_context_view_ddl(
        project_id=project,
        dataset_id=dataset,
        gl_table_id=gl_table,
        view_id=gl_view,
    )

    print(f"Recreating view  : {project}.{dataset}.{gl_view}")
    print(f"Source GL table  : {project}.{dataset}.{gl_table}")
    print(f"BQ location      : {location}")
    print()
    print("--- DDL ---")
    print(ddl)
    print("-----------")
    print()

    client = bigquery.Client(project=project)
    job = client.query(ddl, location=location)
    job.result()

    print(f"Done. View {gl_view!r} recreated successfully.")


if __name__ == "__main__":
    main()
