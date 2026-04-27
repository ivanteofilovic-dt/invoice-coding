"""Evaluate GL coding prediction accuracy for PDFs in data/eval.

For each PDF:
  1. Run the full analyze pipeline (Gemini extraction + RAG + Gemini coding)
  2. Match the extracted invoice_number against the local GL ground-truth files
  3. Compare every predicted journal-line field to the GL values
  4. Report per-category accuracy and write results to JSON

Usage (from project root):
    uv run python scripts/eval_accuracy.py

Options:
    --eval-dir PATH     Directory with eval PDF files     (default: data/eval)
    --gl-dir   PATH     Directory with GL *.txt files     (default: data/GL)
    --out      PATH     Output JSON results file          (default: data/eval/results.json)
    --no-persist        Skip persisting predictions to BQ rag_suggestions table

Environment variables required:
    GCP_PROJECT, BQ_DATASET
    (all other AnkReg vars are optional / have defaults, same as the API)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Allow running as `python scripts/eval_accuracy.py` from project root.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from google.cloud import bigquery

from invoice_processing.analyze_pipeline import read_settings_from_env, run_analyze_pdf
from invoice_processing.batch_invoice_extract import make_genai_client
from invoice_processing.bq_gl_lines import collect_gl_txt_files, iter_filtered_gl_rows

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Predicted field → GL column for accuracy evaluation
FIELD_MAP: dict[str, str] = {
    "account": "ACCOUNT",
    "cost_center": "DEPARTMENT",
    "product_code": "PRODUCT",
    "ic": "IC",
    "project": "PROJECT",
    "gl_system": "SYSTEM",
    "reserve": "RESERVE",
}


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _norm(value: str | None) -> str:
    """Strip whitespace and uppercase for case-insensitive comparison."""
    if not value:
        return ""
    return str(value).strip().upper()


# ---------------------------------------------------------------------------
# Ground-truth loading
# ---------------------------------------------------------------------------

def load_gl_ground_truth(gl_dir: Path) -> dict[str, dict[str, set[str]]]:
    """Parse GL *.txt files and build a lookup by normalised INVOICE_NUM.

    Returns
    -------
    {invoice_num: {GL_COLUMN: {value, ...}, ...}}
    """
    gt: dict[str, dict[str, set[str]]] = {}
    files = collect_gl_txt_files(gl_dir)
    if not files:
        logger.warning("No GL .txt files found in %s", gl_dir)
        return gt

    for path in files:
        logger.info("Loading GL data: %s", path.name)
        for row in iter_filtered_gl_rows(path):
            inv_num = _norm(row.get("INVOICE_NUM"))
            if not inv_num:
                continue
            if inv_num not in gt:
                gt[inv_num] = {col: set() for col in FIELD_MAP.values()}
            for gl_col in FIELD_MAP.values():
                val = _norm(row.get(gl_col))
                if val:
                    gt[inv_num][gl_col].add(val)

    logger.info("Loaded %d unique invoice numbers from GL data", len(gt))
    return gt


def find_gt_entry(
    extracted_invoice_num: str | None,
    gt: dict[str, dict[str, set[str]]],
) -> tuple[str | None, dict[str, set[str]] | None]:
    """Find the ground-truth record for an extracted invoice number.

    Tries exact match first, then substring containment (to handle minor
    formatting differences between the PDF and the GL export).

    Returns ``(matched_key, fields_dict)`` or ``(None, None)`` on no match.
    """
    if not extracted_invoice_num:
        return None, None

    norm = _norm(extracted_invoice_num)

    if norm in gt:
        return norm, gt[norm]

    for key in gt:
        if norm in key or key in norm:
            logger.debug("Fuzzy match: '%s' ↔ '%s'", norm, key)
            return key, gt[key]

    return None, None


# ---------------------------------------------------------------------------
# Per-invoice comparison
# ---------------------------------------------------------------------------

FieldResult = bool | None  # True=correct, False=wrong, None=no GT value to compare


def compare_prediction(
    suggestion: dict,
    gt_fields: dict[str, set[str]],
) -> dict[str, FieldResult]:
    """Check each coding field in the suggestion against GT.

    An invoice is considered *correct* for a field when ANY predicted
    journal-line value appears in the set of GT values for that field.
    Returns ``None`` for fields where the GT has no non-empty values.
    """
    journal_lines: list[dict] = suggestion.get("journal_lines") or []
    results: dict[str, FieldResult] = {}

    for pred_field, gl_col in FIELD_MAP.items():
        gt_vals = gt_fields.get(gl_col, set())
        if not gt_vals:
            results[pred_field] = None
            continue

        predicted_vals = {
            _norm(jl.get(pred_field))
            for jl in journal_lines
            if _norm(jl.get(pred_field))
        }

        results[pred_field] = bool(predicted_vals & gt_vals) if predicted_vals else False

    return results


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline_for_pdf(
    pdf_path: Path,
    settings: dict,
    gc,
    bq: bigquery.Client,
    *,
    persist: bool,
) -> dict:
    """Run the full AnkReg pipeline for one PDF and return the raw result dict."""
    pdf_bytes = pdf_path.read_bytes()
    return run_analyze_pdf(
        pdf_bytes,
        pdf_path.name,
        persist=persist,
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


# ---------------------------------------------------------------------------
# Accuracy aggregation
# ---------------------------------------------------------------------------

def aggregate_accuracy(
    invoice_results: list[dict],
) -> dict[str, dict]:
    """Compute per-category accuracy over all invoices that had a GL match."""
    stats: dict[str, dict[str, int]] = {
        field: {"correct": 0, "total": 0} for field in FIELD_MAP
    }

    for result in invoice_results:
        for field, correct in result.get("field_results", {}).items():
            if correct is not None:
                stats[field]["total"] += 1
                if correct:
                    stats[field]["correct"] += 1

    return {
        field: {
            "correct": s["correct"],
            "total": s["total"],
            "accuracy_pct": (
                round(100.0 * s["correct"] / s["total"], 1) if s["total"] else None
            ),
        }
        for field, s in stats.items()
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate AnkReg GL coding accuracy against local GL ground truth"
    )
    parser.add_argument(
        "--eval-dir", default="data/eval", help="Directory containing eval PDF files"
    )
    parser.add_argument(
        "--gl-dir", default="data/GL", help="Directory containing GL *.txt files"
    )
    parser.add_argument(
        "--out",
        default="data/eval/results.json",
        help="Output path for detailed JSON results",
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Skip persisting predictions into the rag_suggestions BQ table",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Evaluate at most N PDFs (alphabetical order). Useful for quick smoke-tests.",
    )
    args = parser.parse_args()

    eval_dir = Path(args.eval_dir)
    gl_dir = Path(args.gl_dir)
    out_path = Path(args.out)

    if not eval_dir.exists():
        logger.error("Eval directory not found: %s", eval_dir)
        sys.exit(1)

    pdf_files = sorted(eval_dir.glob("*.pdf"))
    if not pdf_files:
        logger.error("No PDF files found in %s", eval_dir)
        sys.exit(1)

    if args.limit is not None:
        pdf_files = pdf_files[: args.limit]
        logger.info("Limiting evaluation to %d PDF file(s) (--limit)", len(pdf_files))
    else:
        logger.info("Found %d PDF file(s) to evaluate", len(pdf_files))

    # Load ground truth
    gt = load_gl_ground_truth(gl_dir)
    if not gt:
        logger.error("No GL ground truth loaded — check %s", gl_dir)
        sys.exit(1)

    # Load pipeline settings from environment
    settings = read_settings_from_env()
    if not settings["project_id"] or not settings["bq_dataset"]:
        logger.error(
            "GCP_PROJECT and BQ_DATASET must be set (same env vars as the API server)"
        )
        sys.exit(1)

    gc = make_genai_client(settings["project_id"], settings["vertex_location"])
    bq = bigquery.Client(
        project=settings["project_id"], location=settings["bq_location"]
    )

    persist = not args.no_persist
    invoice_results: list[dict] = []

    for i, pdf_path in enumerate(pdf_files, 1):
        logger.info("[%d/%d] %s", i, len(pdf_files), pdf_path.name)

        try:
            pipeline_result = run_pipeline_for_pdf(
                pdf_path, settings, gc, bq, persist=persist
            )
        except Exception as exc:
            logger.exception("  Pipeline error: %s", exc)
            invoice_results.append(
                {
                    "filename": pdf_path.name,
                    "error": str(exc),
                    "invoice_number_extracted": None,
                    "invoice_number_matched": None,
                    "field_results": {},
                    "model_confidence": None,
                    "final_confidence": None,
                    "status": None,
                }
            )
            continue

        extraction = pipeline_result.get("extraction") or {}
        suggestion = pipeline_result.get("suggestion") or {}
        inv_num_extracted = extraction.get("invoice_number")

        matched_key, gt_fields = find_gt_entry(inv_num_extracted, gt)

        if gt_fields is None:
            logger.warning(
                "  No GL match for invoice_number=%r (file: %s)",
                inv_num_extracted,
                pdf_path.name,
            )
            invoice_results.append(
                {
                    "filename": pdf_path.name,
                    "error": None,
                    "invoice_number_extracted": inv_num_extracted,
                    "invoice_number_matched": None,
                    "field_results": {},
                    "model_confidence": (
                        pipeline_result.get("confidence_meta") or {}
                    ).get("model_confidence"),
                    "final_confidence": pipeline_result.get("final_confidence"),
                    "status": pipeline_result.get("status"),
                }
            )
            continue

        field_results = compare_prediction(suggestion, gt_fields)

        correct_fields = [f for f, v in field_results.items() if v is True]
        wrong_fields = [f for f, v in field_results.items() if v is False]
        logger.info(
            "  invoice=%s → GL=%s | correct=%s | wrong=%s",
            inv_num_extracted,
            matched_key,
            correct_fields,
            wrong_fields,
        )

        invoice_results.append(
            {
                "filename": pdf_path.name,
                "error": None,
                "invoice_number_extracted": inv_num_extracted,
                "invoice_number_matched": matched_key,
                "field_results": field_results,
                "model_confidence": (
                    pipeline_result.get("confidence_meta") or {}
                ).get("model_confidence"),
                "final_confidence": pipeline_result.get("final_confidence"),
                "status": pipeline_result.get("status"),
            }
        )

    # Aggregate
    accuracy = aggregate_accuracy(invoice_results)
    matched_count = sum(
        1 for r in invoice_results if r.get("invoice_number_matched") is not None
    )
    error_count = sum(1 for r in invoice_results if r.get("error"))

    summary = {
        "total_invoices": len(invoice_results),
        "matched_to_gl": matched_count,
        "unmatched": len(invoice_results) - matched_count - error_count,
        "errors": error_count,
        "accuracy_by_category": accuracy,
        "invoices": invoice_results,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    logger.info("Results written → %s", out_path)

    # Console summary
    print()
    print("=" * 48)
    print("  GL Coding Accuracy Evaluation")
    print("=" * 48)
    print(f"  Invoices processed : {len(invoice_results)}")
    print(f"  Matched to GL data : {matched_count}")
    print(f"  Unmatched (no GT)  : {len(invoice_results) - matched_count - error_count}")
    print(f"  Pipeline errors    : {error_count}")
    print()
    print(f"  {'Category':<14}  {'Correct':>7}  {'Total':>6}  {'Accuracy':>9}")
    print("  " + "-" * 42)
    for field, stats in accuracy.items():
        acc_str = (
            f"{stats['accuracy_pct']}%"
            if stats["accuracy_pct"] is not None
            else "  N/A"
        )
        print(
            f"  {field:<14}  {stats['correct']:>7}  {stats['total']:>6}  {acc_str:>9}"
        )
    print("=" * 48)
    print(f"  Full results → {out_path}")
    print()


if __name__ == "__main__":
    main()
