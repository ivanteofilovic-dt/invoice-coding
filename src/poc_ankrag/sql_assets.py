"""Utilities for loading and rendering checked-in BigQuery SQL scripts."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

SQL_SCRIPTS = (
    "01_create_gl_coding_history.sql",
    "02_embedding_pipeline.sql",
    "03_search_and_vendor_context.sql",
    "04_ic_lookup.sql",
    "05_prediction_audit_and_evaluation.sql",
)


def load_sql_script(script_name: str) -> str:
    """Load a SQL script from the repository sql directory."""

    if script_name not in SQL_SCRIPTS:
        raise ValueError(f"Unknown SQL script: {script_name}")
    return (SQL_DIR / script_name).read_text(encoding="utf-8")


def render_sql(script: str, replacements: dict[str, str]) -> str:
    """Render simple {{PLACEHOLDER}} tokens without introducing a template dependency."""

    rendered = script
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    return rendered


def iter_rendered_sql(replacements: dict[str, str]) -> list[tuple[str, str]]:
    return [
        (script_name, render_sql(load_sql_script(script_name), replacements))
        for script_name in SQL_SCRIPTS
    ]


def package_files_available() -> bool:
    """Return whether package resources can be inspected in the current runtime."""

    return resources.files("poc_ankrag") is not None
