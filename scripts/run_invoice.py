"""Run the full pipeline on a single invoice PDF and print the result.

Usage (from project root):
    uv run python scripts/run_invoice.py path/to/invoice.pdf

Options:
    --persist       Persist the suggestion into the BQ rag_suggestions table
                    (default: dry-run, no BQ write)

Environment variables required:
    GCP_PROJECT, BQ_DATASET
    (all other vars are optional / have defaults, same as the API)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Allow running as `python scripts/run_invoice.py` from project root.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from google.cloud import bigquery

from invoice_processing.analyze_pipeline import read_settings_from_env, run_analyze_pdf
from invoice_processing.batch_invoice_extract import make_genai_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full AnkReg pipeline on a single invoice PDF"
    )
    parser.add_argument("pdf", help="Path to the invoice PDF file")
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Persist the suggestion into the BQ rag_suggestions table (default: dry-run)",
    )
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        logger.error("File not found: %s", pdf_path)
        sys.exit(1)

    settings = read_settings_from_env()
    if not settings["project_id"] or not settings["bq_dataset"]:
        logger.error("GCP_PROJECT and BQ_DATASET must be set")
        sys.exit(1)

    gc = make_genai_client(settings["project_id"], settings["vertex_location"])
    bq = bigquery.Client(
        project=settings["project_id"], location=settings["bq_location"]
    )

    logger.info("Running pipeline on: %s  (persist=%s)", pdf_path.name, args.persist)
    result = run_analyze_pdf(
        pdf_path.read_bytes(),
        pdf_path.name,
        persist=args.persist,
        project_id=settings["project_id"],
        vertex_location=settings["vertex_location"],
        gemini_model=settings["gemini_model"],
        gcs_bucket=settings["gcs_bucket"],
        new_invoice_prefix=settings["new_invoice_prefix"],
        bq_dataset=settings["bq_dataset"],
        bq_extractions_table=settings["bq_extractions_table"],
        bq_location=settings["bq_location"],
        rag_top_k=settings["rag_top_k"],
        embedding_output_dim=settings["embedding_output_dim"],
        embed_text_view=settings["embed_text_view"],
        embeddings_table=settings["embeddings_table"],
        gl_context_view=settings["gl_context_view"],
        remote_model=settings["remote_model"],
        rag_suggestions_table=settings["rag_suggestions_table"],
        genai_client=gc,
        bq_client=bq,
    )

    timings = (result.get("confidence_meta") or {}).get("timings_seconds", {})
    suggestion = result.get("suggestion") or {}
    extraction = result.get("extraction") or {}

    print()
    print("=" * 52)
    print("  Invoice:", extraction.get("invoice_number", "—"))
    print("  Vendor: ", extraction.get("vendor_name", "—"))
    print("  Status: ", result.get("status", "—"))
    print(f"  Confidence: {result.get('final_confidence', 0):.0%}")
    print()
    print("  Journal lines:")
    for line in suggestion.get("journal_lines") or []:
        print(
            f"    account={line.get('account')}  cost_center={line.get('cost_center')}"
            f"  amount={line.get('amount')}  {line.get('description', '')}"
        )
    print()
    print("  Timings (seconds):")
    for step, secs in timings.items():
        if step != "total":
            print(f"    {step:<30} {secs:.2f}s")
    print(f"    {'total':<30} {timings.get('total', 0):.2f}s")
    print("=" * 52)
    print()
    print("Full result JSON:")
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
