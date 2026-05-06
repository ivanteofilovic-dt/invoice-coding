"""Command-line helpers for the invoice coding POC."""

from __future__ import annotations

import argparse
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


if __name__ == "__main__":
    main()
