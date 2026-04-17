"""CLI: ``ankrag serve`` (FastAPI) and ``ankrag suggest`` (headless PDF run)."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from google.cloud import bigquery

from invoice_processing.analyze_pipeline import read_settings_from_env, run_analyze_pdf
from invoice_processing.batch_invoice_extract import make_genai_client

logger = logging.getLogger(__name__)


def _cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run(
        "invoice_processing.api.app:app",
        host=args.host,
        port=args.port,
        log_level="info",
    )


def _cmd_suggest(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    s = read_settings_from_env()
    if not s["project_id"] or not s["bq_dataset"]:
        print("error: set GCP_PROJECT and BQ_DATASET (GCS_BUCKET optional)", file=sys.stderr)
        sys.exit(1)
    path = Path(args.local_pdf)
    if not path.is_file():
        print(f"error: not a file: {path}", file=sys.stderr)
        sys.exit(1)
    raw = path.read_bytes()
    gc = make_genai_client(s["project_id"], s["vertex_location"])
    bq = bigquery.Client(project=s["project_id"], location=s["bq_location"])
    try:
        result = run_analyze_pdf(
            raw,
            path.name,
            persist=args.persist,
            project_id=s["project_id"],
            vertex_location=s["vertex_location"],
            gemini_model=s["gemini_model"],
            gcs_bucket=s["gcs_bucket"],
            new_invoice_prefix=s["new_invoice_prefix"],
            bq_dataset=s["bq_dataset"],
            bq_extractions_table=s["bq_extractions_table"],
            bq_location=s["bq_location"],
            rag_top_k=s["rag_top_k"],
            embedding_output_dim=s["embedding_output_dim"],
            embed_text_view=s["embed_text_view"],
            embeddings_table=s["embeddings_table"],
            gl_context_view=s["gl_context_view"],
            remote_model=s["remote_model"],
            rag_suggestions_table=s["rag_suggestions_table"],
            genai_client=gc,
            bq_client=bq,
        )
    except Exception:
        logger.exception("suggest failed")
        raise
    print(json.dumps(result, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="AnkReg RAG API / headless suggest")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="Run FastAPI on :8000 (default)")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    p_sug = sub.add_parser("suggest", help="Run analyze on a local PDF, print JSON")
    p_sug.add_argument("--local-pdf", required=True, help="Path to invoice PDF")
    p_sug.add_argument(
        "--persist",
        action="store_true",
        help="Append row to rag_suggestions in BigQuery",
    )
    p_sug.set_defaults(func=_cmd_suggest)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
