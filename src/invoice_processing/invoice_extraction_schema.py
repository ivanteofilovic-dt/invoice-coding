"""JSON schema and prompts for Vertex Gemini batch invoice extraction."""

from __future__ import annotations

import json
from typing import Any

EXTRACTION_SYSTEM_INSTRUCTION = (
    "You are an accounts-payable extraction engine. Read the attached invoice PDF "
    "and return one JSON object matching the response schema. Use ISO dates "
    "YYYY-MM-DD where applicable. Use null for unknown fields. Preserve printed "
    "tax labels in tax_details strings. Put any unmapped text blocks in extras."
)


def invoice_response_json_schema() -> dict[str, Any]:
    """JSON Schema (draft-style) for structured JSON responses (``responseJsonSchema``)."""
    contact = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "email": {"type": "string"},
        },
    }
    party = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "address_line": {"type": "string"},
            "postal_code": {"type": "string"},
            "city": {"type": "string"},
            "country_code": {"type": "string"},
            "endpoint_id": {"type": "string"},
            "party_identification": {"type": "string"},
            "company_legal_id": {"type": "string"},
            "legal_name": {"type": "string"},
            "vat_number": {"type": "string"},
            "tax_status": {"type": "string"},
            "contact": contact,
        },
    }
    tax_line = {
        "type": "object",
        "properties": {
            "tax_category": {"type": "string"},
            "tax_rate_percent": {"type": "number"},
            "taxable_amount": {"type": "number"},
            "tax_amount": {"type": "number"},
        },
    }
    invoice_line = {
        "type": "object",
        "properties": {
            "line_number": {"type": "integer"},
            "item_number": {"type": "string"},
            "item_name": {"type": "string"},
            "quantity": {"type": "number"},
            "unit_code": {"type": "string"},
            "net_unit_price": {"type": "number"},
            "tax_details": {"type": "string"},
            "allowance_charge": {"type": "string"},
            "line_amount": {"type": "number"},
            "line_note": {"type": "string"},
            "orderline_reference": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "properties": {
            "issue_date": {"type": "string", "description": "YYYY-MM-DD or null"},
            "due_date": {"type": "string"},
            "invoice_number": {"type": "string"},
            "buyer_order_number": {"type": "string"},
            "currency_code": {"type": "string"},
            "header_note": {"type": "string"},
            "supplier": party,
            "customer": party,
            "delivery": {
                "type": "object",
                "properties": {
                    "delivery_date": {"type": "string"},
                    "delivery_location": {"type": "string"},
                },
            },
            "tax_lines": {"type": "array", "items": tax_line},
            "payment": {
                "type": "object",
                "properties": {
                    "payment_terms": {"type": "string"},
                    "payment_means_code": {"type": "string"},
                    "instruction_id": {"type": "string"},
                    "account_number": {"type": "string"},
                    "financial_institution_branch": {"type": "string"},
                    "financial_institution_name": {"type": "string"},
                    "payment_id": {"type": "string"},
                },
            },
            "invoice_lines": {"type": "array", "items": invoice_line},
            "document_totals": {
                "type": "object",
                "properties": {
                    "rounding_amount": {"type": "number"},
                    "total_amount_excl_tax": {"type": "number"},
                    "total_tax_amount": {"type": "number"},
                    "total_amount_incl_tax": {"type": "number"},
                },
            },
            "extras": {"type": "object"},
        },
    }


def build_batch_jsonl_line(pdf_gcs_uri: str) -> str:
    """One JSONL line: Vertex batch ``request`` with PDF + JSON-only generation config."""
    schema = invoice_response_json_schema()
    user_text = (
        f"{EXTRACTION_SYSTEM_INSTRUCTION}\n\n"
        "Extract all invoice fields from this PDF into JSON per the schema."
    )
    request_body: dict[str, Any] = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": user_text},
                    {
                        "fileData": {
                            "fileUri": pdf_gcs_uri,
                            "mimeType": "application/pdf",
                        }
                    },
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 8192,
            "responseMimeType": "application/json",
            "responseJsonSchema": schema,
        },
    }
    return json.dumps({"request": request_body}, ensure_ascii=False)
