"""FastAPI application for the invoice coding POC."""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from functools import lru_cache
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from poc_ankrag.cloud_clients import BigQueryCodingHistoryStore, GeminiJSONClient
from poc_ankrag.config import PipelineConfig
from poc_ankrag.models import CodingPrediction, ExtractedInvoice, InvoiceLine
from poc_ankrag.pipeline import InvoiceCodingResult, run_invoice_pdf_coding

MAX_UPLOAD_BYTES = 10 * 1024 * 1024


class HistoricalLineResponse(BaseModel):
    date: str | None = None
    desc: str
    account: str
    dept: str
    similarity: str | None = None


class InvoiceLineResponse(BaseModel):
    id: str
    description: str
    quantity: float | None = None
    unit_price: float | None = Field(default=None, alias="unitPrice")
    total: float
    coding: dict[str, str]
    confidence: float | None = None
    reasoning: str | None = None
    historical_lines: list[HistoricalLineResponse] = Field(default_factory=list, alias="historicalLines")

    model_config = ConfigDict(populate_by_name=True)


class InvoiceCodingResponse(BaseModel):
    vendor: str
    invoice_number: str = Field(alias="invoiceNumber")
    date: str | None
    total_amount: str = Field(alias="totalAmount")
    currency: str
    source_file_name: str | None = Field(default=None, alias="sourceFileName")
    line_items: list[InvoiceLineResponse] = Field(alias="lineItems")

    model_config = ConfigDict(populate_by_name=True)


class HealthResponse(BaseModel):
    status: str


app = FastAPI(
    title="Automated Invoice Coding API",
    description="HTTP API for Gemini extraction, BigQuery evidence retrieval, and GL prediction.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("FRONTEND_ORIGINS", "http://localhost:5173").split(","),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/api/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.post("/api/invoices/code", response_model=InvoiceCodingResponse)
async def code_invoice(file: UploadFile = File(...)) -> InvoiceCodingResponse:
    if not _is_pdf_upload(file):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Only PDF invoices are supported by the current Gemini extraction pipeline.",
        )

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file is empty.")
    if len(pdf_bytes) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Invoice PDF must be 10 MB or smaller.",
        )

    try:
        result = await run_in_threadpool(_run_pdf_coding, pdf_bytes)
    except KeyError as exc:
        missing_name = exc.args[0]
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Backend is missing required environment variable: {missing_name}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Invoice coding failed: {exc}",
        ) from exc

    return _to_response(result.invoice, result.predictions, source_file_name=file.filename)


@app.get("/api/demo-invoice", response_model=InvoiceCodingResponse)
def demo_invoice() -> InvoiceCodingResponse:
    return _demo_response()


def _is_pdf_upload(file: UploadFile) -> bool:
    filename = (file.filename or "").lower()
    return file.content_type == "application/pdf" or filename.endswith(".pdf")


@lru_cache(maxsize=1)
def _clients() -> tuple[PipelineConfig, GeminiJSONClient, BigQueryCodingHistoryStore]:
    config = PipelineConfig.from_env()
    return config, GeminiJSONClient(config), BigQueryCodingHistoryStore(config)


def _run_pdf_coding(pdf_bytes: bytes) -> InvoiceCodingResult:
    config, gemini, store = _clients()
    return run_invoice_pdf_coding(pdf_bytes, gemini=gemini, store=store, config=config)


def _to_response(
    invoice: ExtractedInvoice,
    predictions: list[CodingPrediction],
    *,
    source_file_name: str | None = None,
) -> InvoiceCodingResponse:
    return InvoiceCodingResponse(
        vendor=invoice.vendor,
        invoiceNumber=invoice.invoice_number,
        date=invoice.invoice_date.isoformat() if invoice.invoice_date else None,
        totalAmount=_format_amount(sum((line.amount for line in invoice.lines), Decimal("0"))),
        currency=invoice.currency,
        sourceFileName=source_file_name,
        lineItems=[
            _line_to_response(line, prediction)
            for line, prediction in zip(invoice.lines, predictions, strict=True)
        ],
    )


def _line_to_response(line: InvoiceLine, prediction: CodingPrediction) -> InvoiceLineResponse:
    quantity = _decimal_to_float(line.quantity)
    total = _decimal_to_float(line.amount) or 0.0
    unit_price = total / quantity if quantity else None
    return InvoiceLineResponse(
        id=line.line_id,
        description=line.description,
        quantity=quantity,
        unitPrice=unit_price,
        total=total,
        coding=prediction.to_dimensions(),
        confidence=prediction.confidence,
        reasoning=prediction.reasoning_summary,
        historicalLines=[],
    )


def _decimal_to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _format_amount(value: Decimal) -> str:
    return f"{value:,.2f}"


def _demo_response() -> InvoiceCodingResponse:
    demo_payload: dict[str, Any] = {
        "vendor": "Exclusive Networks Sweden AB",
        "invoiceNumber": "INSE010035371",
        "date": date(2026, 3, 15).isoformat(),
        "totalAmount": "3,413.36",
        "currency": "SEK",
        "sourceFileName": "INSE010035371_ExclusiveNetworks.pdf",
        "lineItems": [
            {
                "id": "line-1",
                "description": "FC-10-S12FP-247-02-12 Firewall appliance",
                "quantity": 2,
                "unitPrice": 1281.68,
                "total": 2563.36,
                "coding": {
                    "ACCOUNT": "40190",
                    "DEPARTMENT": "F82250",
                    "PRODUCT": "F800000",
                    "IC": "00",
                    "PROJECT": "000000",
                    "SYSTEM": "000000",
                    "RESERVE": "F80000000000",
                },
                "confidence": 0.98,
                "reasoning": (
                    "High confidence prediction based on strong historical precedent. "
                    "The vendor and Fortinet product code have repeatedly mapped to hardware coding."
                ),
                "historicalLines": [
                    {
                        "date": "2026-02-10",
                        "desc": "FC-10-S12FP-247-02-12 Firewall",
                        "account": "40190",
                        "dept": "F82250",
                        "similarity": "99%",
                    },
                    {
                        "date": "2026-01-22",
                        "desc": "FC-10-F108F-247-02-12 Firewall app.",
                        "account": "40190",
                        "dept": "F82250",
                        "similarity": "92%",
                    },
                ],
            },
            {
                "id": "line-2",
                "description": "Installation Services & Setup - Remote",
                "quantity": 1,
                "unitPrice": 850.0,
                "total": 850.0,
                "coding": {
                    "ACCOUNT": "60110",
                    "DEPARTMENT": "F82250",
                    "PRODUCT": "F800000",
                    "IC": "00",
                    "PROJECT": "000000",
                    "SYSTEM": "000000",
                    "RESERVE": "F80000000000",
                },
                "confidence": 0.82,
                "reasoning": (
                    "Prediction based on semantic matching for installation services tied to the "
                    "same vendor and department context as the primary hardware purchase."
                ),
                "historicalLines": [
                    {
                        "date": "2026-01-22",
                        "desc": "Consulting - setup",
                        "account": "60110",
                        "dept": "F82250",
                        "similarity": "88%",
                    }
                ],
            },
        ],
    }
    return InvoiceCodingResponse.model_validate(demo_payload)
