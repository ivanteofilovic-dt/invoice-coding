"""JSON schema and prompts for Gemini invoice → GL coding suggestions."""

from __future__ import annotations

import json
from typing import Any

from google import genai
from google.genai import types

from invoice_processing.batch_invoice_extract import parse_model_json_text

CODING_SYSTEM_INSTRUCTION = (
    "You are an accounts-payable GL coding assistant for Nordic telecom procurement (AnkReg). "
    "You receive the current invoice extraction JSON and similar historical invoices with "
    "their actual GL lines. Propose journal lines consistent with those historical patterns "
    "(account, department/product, periodization). Use null for unknown fields. "
    "confidence must be a number between 0 and 1 reflecting how well the historical evidence "
    "supports the proposal. rationale must be a short factual explanation citing neighbor similarity."
)


def coding_response_json_schema() -> dict[str, Any]:
    """JSON schema for structured coding output (aligned with frontend ``CodingSuggestion``)."""
    journal_line = {
        "type": "object",
        "properties": {
            "account": {"type": "string"},
            "cost_center": {"type": "string"},
            "product_code": {"type": "string"},
            "ic": {"type": "string"},
            "project": {"type": "string"},
            "gl_system": {"type": "string"},
            "reserve": {"type": "string"},
            "debit": {"type": "string"},
            "credit": {"type": "string"},
            "currency": {"type": "string"},
            "periodization_start": {"type": "string"},
            "periodization_end": {"type": "string"},
            "memo": {"type": "string"},
        },
    }
    line_pred = {
        "type": "object",
        "properties": {
            "line_index": {"type": "integer"},
            "journal_line": journal_line,
            "confidence": {"type": "number"},
        },
        "required": ["line_index", "journal_line", "confidence"],
    }
    return {
        "type": "object",
        "properties": {
            "journal_lines": {
                "type": "array",
                "items": journal_line,
                "description": "Suggested GL journal lines (1–5 typical)",
            },
            "confidence": {
                "type": "number",
                "description": "Overall confidence 0–1",
            },
            "rationale": {"type": "string"},
            "line_predictions": {
                "type": "array",
                "items": line_pred,
            },
        },
        "required": ["journal_lines", "confidence", "rationale"],
    }


def build_coding_user_prompt(
    current_extraction: dict[str, Any],
    neighbors_context: str,
) -> str:
    body = {
        "current_invoice_extraction": current_extraction,
        "similar_historical_cases_text": neighbors_context,
    }
    return (
        "Propose GL coding for the current invoice. Ground accounts/departments/products/periods "
        "in the similar historical GL lines when possible.\n\n"
        f"{json.dumps(body, ensure_ascii=False, indent=2)}"
    )


def normalize_coding_suggestion(raw: dict[str, Any]) -> dict[str, Any]:
    """Ensure required keys exist for API consumers."""
    jl = raw.get("journal_lines")
    if not isinstance(jl, list):
        jl = []
    conf = raw.get("confidence")
    try:
        conf_f = float(conf) if conf is not None else 0.0
    except (TypeError, ValueError):
        conf_f = 0.0
    conf_f = max(0.0, min(1.0, conf_f))
    rationale = raw.get("rationale")
    if not isinstance(rationale, str):
        rationale = str(rationale or "")
    line_preds = raw.get("line_predictions")
    if not isinstance(line_preds, list):
        line_preds = None
    return {
        "journal_lines": jl,
        "confidence": conf_f,
        "rationale": rationale,
        "line_predictions": line_preds,
    }


def suggest_coding(
    client: genai.Client,
    *,
    model_id: str,
    current_extraction: dict[str, Any],
    neighbors_context: str,
) -> dict[str, Any]:
    """Call Gemini with JSON schema for ``CodingSuggestion``."""
    schema = coding_response_json_schema()
    user_text = build_coding_user_prompt(current_extraction, neighbors_context)
    config = types.GenerateContentConfig(
        system_instruction=CODING_SYSTEM_INSTRUCTION,
        temperature=0.15,
        max_output_tokens=8192,
        response_mime_type="application/json",
        response_json_schema=schema,
    )
    resp = client.models.generate_content(
        model=model_id,
        contents=[types.Content(role="user", parts=[types.Part(text=user_text)])],
        config=config,
    )
    # Detect token-limit truncation before trying to parse — truncated JSON always
    # fails with an "Unterminated string" / unexpected-end error.
    candidate = resp.candidates[0] if resp.candidates else None
    finish_reason = str(getattr(candidate, "finish_reason", "")) if candidate else ""
    if finish_reason and "MAX_TOKENS" in finish_reason.upper():
        msg = (
            f"Coding model response was truncated (finish_reason={finish_reason}). "
            "Reduce prompt size or increase max_output_tokens."
        )
        raise RuntimeError(msg)
    text = (resp.text or "").strip()
    if not text:
        msg = "empty model response for coding"
        raise RuntimeError(msg)
    payload = parse_model_json_text(text)
    if not isinstance(payload, dict):
        msg = "coding output is not a JSON object"
        raise RuntimeError(msg)
    return normalize_coding_suggestion(payload)
