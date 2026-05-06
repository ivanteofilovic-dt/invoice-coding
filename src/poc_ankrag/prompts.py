"""Gemini prompt construction for extraction and final coding."""

from __future__ import annotations

import json
from typing import Any, Iterable

from poc_ankrag.models import ExtractedInvoice, HistoricalExample, InvoiceLine, VendorCodingSummary


EXTRACTION_SCHEMA: dict[str, Any] = {
    "vendor": "string",
    "invoice_number": "string",
    "invoice_date": "YYYY-MM-DD",
    "currency": "string",
    "lines": [
        {
            "line_id": "string",
            "description": "string",
            "quantity": "number|null",
            "amount": "number",
            "tax_amount": "number|null",
        }
    ],
}

PREDICTION_SCHEMA: dict[str, str] = {
    "ACCOUNT": "string",
    "DEPARTMENT": "string",
    "PRODUCT": "string",
    "IC": "string",
    "PROJECT": "string",
    "SYSTEM": "string",
    "RESERVE": "string",
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def build_extraction_prompt(invoice_text: str) -> str:
    """Build the invoice-to-JSON extraction prompt for Gemini."""

    return "\n".join(
        [
            "Extract structured invoice data from the invoice text.",
            "",
            "Rules:",
            "- Preserve Swedish text exactly where possible.",
            "- Do not infer GL coding dimensions.",
            "- Return only strict JSON matching the schema.",
            "- Keep invoice references and OCR artifacts in description when no better line description exists.",
            "",
            "Schema:",
            _json_dumps(EXTRACTION_SCHEMA),
            "",
            "Invoice text:",
            invoice_text,
        ]
    )


def build_prediction_prompt(
    invoice: ExtractedInvoice,
    line: InvoiceLine,
    historical_examples: Iterable[HistoricalExample],
    vendor_summary: Iterable[VendorCodingSummary],
    resolved_ic: str,
) -> str:
    """Build the final coding prompt with retrieval and vendor statistics evidence."""

    invoice_line_payload = {
        "invoice": {
            "vendor": invoice.vendor,
            "invoice_number": invoice.invoice_number,
            "invoice_date": invoice.invoice_date.isoformat() if invoice.invoice_date else None,
            "currency": invoice.currency,
        },
        "line": line.to_prompt_dict(),
    }

    examples_payload = [example.to_prompt_dict() for example in historical_examples]
    summary_payload = [summary.to_prompt_dict() for summary in vendor_summary]

    return "\n".join(
        [
            "You are coding an invoice line to General Ledger dimensions.",
            "",
            "Use the historical examples and vendor statistics as evidence. Prefer consistent historical vendor usage when the invoice line is ambiguous. Do not invent dimensions that are not supported by the examples or vendor summary.",
            "",
            f"The IC value is rule-resolved and must be copied exactly: {resolved_ic}",
            "",
            "Return only strict JSON with this schema:",
            _json_dumps(PREDICTION_SCHEMA),
            "",
            "Invoice line:",
            _json_dumps(invoice_line_payload),
            "",
            "Top 20 similar historical GL lines:",
            _json_dumps(examples_payload),
            "",
            "Vendor coding summary:",
            _json_dumps(summary_payload),
        ]
    )
