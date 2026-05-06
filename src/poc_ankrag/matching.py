"""Text preparation and fallback detection for retrieval queries."""

from __future__ import annotations

import re

INVOICE_REFERENCE_PATTERNS = (
    re.compile(r"^\s*(invoice|inv|faktura|fakturanr|ref|reference|ocr)[\s:#-]*[a-z0-9-]+\s*$", re.I),
    re.compile(r"^\s*[a-z]{0,4}[-_/]?\d{5,}\s*$", re.I),
)

GENERIC_DESCRIPTION_WORDS = {
    "",
    "invoice",
    "inv",
    "faktura",
    "fakturanr",
    "reference",
    "ref",
    "ocr",
    "payment",
    "betalning",
}


def normalize_vendor(value: str) -> str:
    """Normalize vendor names for deterministic lookup keys."""

    return " ".join(value.casefold().strip().split())


def is_generic_line_description(description: str | None) -> bool:
    """Return True when a line description is too weak for semantic matching."""

    if description is None:
        return True

    normalized = " ".join(description.strip().split())
    if normalized.casefold() in GENERIC_DESCRIPTION_WORDS:
        return True

    if len(normalized) < 4:
        return True

    if any(pattern.match(normalized) for pattern in INVOICE_REFERENCE_PATTERNS):
        return True

    alpha_chars = sum(character.isalpha() for character in normalized)
    digit_chars = sum(character.isdigit() for character in normalized)
    return digit_chars >= 5 and alpha_chars <= 3


def build_embedding_content(
    vendor: str,
    line_description: str | None,
    *,
    vendor_only: bool | None = None,
) -> str:
    """Build the exact embedding text shape used by historical GL rows."""

    use_vendor_only = (
        is_generic_line_description(line_description)
        if vendor_only is None
        else vendor_only
    )
    description = "" if use_vendor_only else (line_description or "")
    return f"{vendor} | {description} | {description}"
