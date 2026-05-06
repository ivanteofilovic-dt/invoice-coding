"""Shared data structures for invoice extraction and coding prediction."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any


CodingDimensions = dict[str, str]


@dataclass(frozen=True)
class InvoiceLine:
    """One extracted invoice line item."""

    line_id: str
    description: str
    amount: Decimal
    quantity: Decimal | None = None
    tax_amount: Decimal | None = None

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "line_id": self.line_id,
            "description": self.description,
            "quantity": str(self.quantity) if self.quantity is not None else None,
            "amount": str(self.amount),
            "tax_amount": str(self.tax_amount) if self.tax_amount is not None else None,
        }


@dataclass(frozen=True)
class ExtractedInvoice:
    """Strict invoice object expected from Gemini extraction."""

    vendor: str
    invoice_number: str
    invoice_date: date | None
    currency: str
    lines: list[InvoiceLine] = field(default_factory=list)

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "vendor": self.vendor,
            "invoice_number": self.invoice_number,
            "invoice_date": self.invoice_date.isoformat() if self.invoice_date else None,
            "currency": self.currency,
            "lines": [line.to_prompt_dict() for line in self.lines],
        }


@dataclass(frozen=True)
class HistoricalExample:
    """A similar historical GL line returned by BigQuery VECTOR_SEARCH."""

    historical_row_id: str
    supplier_customer_name: str
    gl_line_description: str
    hfm_descriptions: str
    account: str
    department: str
    product: str
    ic: str
    project: str
    system: str
    reserve: str
    amount: Decimal | None
    posting_date: date | None
    distance: float

    def to_prompt_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["amount"] = str(self.amount) if self.amount is not None else None
        payload["posting_date"] = self.posting_date.isoformat() if self.posting_date else None
        return payload


@dataclass(frozen=True)
class VendorCodingSummary:
    """Aggregated historical coding context for one vendor."""

    vendor: str
    account: str
    department: str
    product: str
    system: str
    reserve: str
    usage_count: int
    vendor_usage_share: float
    total_abs_amount: Decimal | None
    avg_abs_amount: Decimal | None
    most_recent_posting_date: date | None

    def to_prompt_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["total_abs_amount"] = (
            str(self.total_abs_amount) if self.total_abs_amount is not None else None
        )
        payload["avg_abs_amount"] = (
            str(self.avg_abs_amount) if self.avg_abs_amount is not None else None
        )
        payload["most_recent_posting_date"] = (
            self.most_recent_posting_date.isoformat()
            if self.most_recent_posting_date
            else None
        )
        return payload


@dataclass(frozen=True)
class CodingPrediction:
    """Final strict coding dimensions returned by Gemini and IC rules."""

    account: str
    department: str
    product: str
    ic: str
    project: str
    system: str
    reserve: str
    confidence: float | None = None
    reasoning_summary: str | None = None

    def to_dimensions(self) -> CodingDimensions:
        return {
            "ACCOUNT": self.account,
            "DEPARTMENT": self.department,
            "PRODUCT": self.product,
            "IC": self.ic,
            "PROJECT": self.project,
            "SYSTEM": self.system,
            "RESERVE": self.reserve,
        }
