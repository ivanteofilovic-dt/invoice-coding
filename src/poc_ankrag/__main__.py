"""Command-line helpers for the invoice coding POC."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from poc_ankrag.config import PipelineConfig
from poc_ankrag.sql_assets import SQL_SCRIPTS, iter_rendered_sql, load_sql_script, render_sql


def main() -> None:
    parser = argparse.ArgumentParser(description="Automated invoice coding POC helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_sql = subparsers.add_parser("list-sql", help="List available BigQuery SQL scripts")
    list_sql.set_defaults(func=_list_sql)

    render = subparsers.add_parser("render-sql", help="Render SQL scripts with environment config")
    render.add_argument("--script", choices=SQL_SCRIPTS, help="Render only one script")
    render.add_argument("--output-dir", type=Path, help="Write rendered SQL files to this directory")
    render.set_defaults(func=_render_sql)

    predict_pdf = subparsers.add_parser("predict-pdf", help="Extract and code an invoice PDF")
    predict_pdf.add_argument("pdf_path", type=Path, help="Path to the invoice PDF")
    predict_pdf.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    predict_pdf.set_defaults(func=_predict_pdf)

    args = parser.parse_args()
    args.func(args)


def _list_sql(_: argparse.Namespace) -> None:
    for script_name in SQL_SCRIPTS:
        print(script_name)


def _render_sql(args: argparse.Namespace) -> None:
    config = PipelineConfig.from_env()
    replacements = config.sql_replacements()

    if args.script:
        scripts = [(args.script, render_sql(load_sql_script(args.script), replacements))]
    else:
        scripts = iter_rendered_sql(replacements)

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for script_name, sql in scripts:
            (args.output_dir / script_name).write_text(sql, encoding="utf-8")
    else:
        for script_name, sql in scripts:
            print(f"-- {script_name}")
            print(sql)
            print()


def _predict_pdf(args: argparse.Namespace) -> None:
    from poc_ankrag.cloud_clients import BigQueryCodingHistoryStore, GeminiJSONClient
    from poc_ankrag.pipeline import run_invoice_pdf_coding

    if not args.pdf_path.is_file():
        raise FileNotFoundError(f"Invoice PDF not found: {args.pdf_path}")

    config = PipelineConfig.from_env()
    result = run_invoice_pdf_coding(
        args.pdf_path.read_bytes(),
        gemini=GeminiJSONClient(),
        store=BigQueryCodingHistoryStore(config),
        config=config,
    )

    payload = {
        "invoice": result.invoice.to_prompt_dict(),
        "predictions": [
            {
                "line_id": line.line_id,
                "coding": prediction.to_dimensions(),
                "estimated_accuracy": prediction.confidence,
                "reasoning_summary": prediction.reasoning_summary,
            }
            for line, prediction in zip(result.invoice.lines, result.predictions, strict=True)
        ],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main()
