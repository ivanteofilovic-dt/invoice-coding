"""End-to-end orchestration for automated invoice coding."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from poc_ankrag.config import PipelineConfig
from poc_ankrag.ic_resolver import HistoricalICUsage, VendorICMapping, resolve_ic
from poc_ankrag.matching import build_embedding_content, is_generic_line_description
from poc_ankrag.models import CodingPrediction, ExtractedInvoice, HistoricalExample, VendorCodingSummary
from poc_ankrag.prompts import build_extraction_prompt, build_pdf_extraction_prompt, build_prediction_prompt

REQUIRED_DIMENSIONS = ("ACCOUNT", "DEPARTMENT", "PRODUCT", "IC", "PROJECT", "SYSTEM", "RESERVE")


@dataclass(frozen=True)
class SimilarLineSearch:
    line_id: str
    vendor: str
    line_description: str
    embedding_content: str
    vendor_only: bool


class GeminiClient(Protocol):
    """Minimal Gemini interface used by the pipeline."""

    def generate_json(self, prompt: str, *, model: str) -> dict:
        """Return a JSON object produced by Gemini."""


class GeminiPDFClient(GeminiClient, Protocol):
    """Gemini interface for extracting invoice data from PDFs."""

    def generate_json_from_pdf(
        self,
        prompt: str,
        *,
        pdf_bytes: bytes,
        model: str,
        mime_type: str = "application/pdf",
    ) -> dict:
        """Return a JSON object produced from a PDF plus prompt."""


class CodingHistoryStore(Protocol):
    """BigQuery-backed retrieval and persistence interface."""

    def search_similar_lines(
        self,
        *,
        line_id: str,
        vendor: str,
        line_description: str,
        embedding_content: str,
        vendor_only: bool,
        top_k: int = 20,
    ) -> list[HistoricalExample]:
        """Run embedding generation plus BigQuery VECTOR_SEARCH."""

    def search_similar_lines_batch(
        self,
        searches: list[SimilarLineSearch],
        *,
        top_k: int = 20,
    ) -> dict[str, list[HistoricalExample]]:
        """Run embedding generation plus BigQuery VECTOR_SEARCH for multiple lines."""

    def fetch_vendor_summary(self, vendor: str, *, limit: int = 50) -> list[VendorCodingSummary]:
        """Fetch vendor-level ACCOUNT and DEPARTMENT statistics."""

    def fetch_vendor_ic_mappings(self, vendor: str) -> list[VendorICMapping]:
        """Fetch active deterministic IC mappings for the vendor."""

    def fetch_historical_ic_usage(self, vendor: str) -> list[HistoricalICUsage]:
        """Fetch historical IC majority candidates for the vendor."""

    def save_prediction(self, prediction: "PredictionRecord") -> None:
        """Persist prediction evidence and output for auditability."""

    def save_predictions(self, predictions: list["PredictionRecord"]) -> None:
        """Persist multiple prediction records for auditability."""


@dataclass(frozen=True)
class PredictionRecord:
    invoice: ExtractedInvoice
    line_id: str
    prediction: CodingPrediction
    vector_example_row_ids: list[str]
    prompt_version: str
    extraction_model: str
    prediction_model: str


@dataclass(frozen=True)
class InvoiceCodingResult:
    invoice: ExtractedInvoice
    predictions: list[CodingPrediction]
    historical_examples_by_line_id: dict[str, list[HistoricalExample]]


def parse_coding_prediction(payload: dict, *, resolved_ic: str) -> CodingPrediction:
    """Validate Gemini's strict JSON coding response."""

    missing = [key for key in REQUIRED_DIMENSIONS if key not in payload]
    if missing:
        raise ValueError(f"Gemini prediction missing dimensions: {', '.join(missing)}")

    if resolved_ic and str(payload["IC"]) != resolved_ic:
        raise ValueError("Gemini prediction did not copy the rule-resolved IC value")

    return CodingPrediction(
        account=str(payload["ACCOUNT"]),
        department=str(payload["DEPARTMENT"]),
        product=str(payload["PRODUCT"]),
        ic=str(payload["IC"]),
        project=str(payload["PROJECT"]),
        system=str(payload["SYSTEM"]),
        reserve=str(payload["RESERVE"]),
        confidence=float(payload["confidence"]) if "confidence" in payload else None,
        reasoning_summary=str(payload["reasoning_summary"])
        if "reasoning_summary" in payload
        else None,
    )


def run_invoice_coding(
    invoice_text: str,
    *,
    gemini: GeminiClient,
    store: CodingHistoryStore,
    config: PipelineConfig,
) -> list[CodingPrediction]:
    """Extract an invoice, retrieve evidence, resolve IC, and predict coding dimensions."""

    invoice = extract_invoice_from_text(invoice_text, gemini=gemini, config=config)
    return code_extracted_invoice(invoice, gemini=gemini, store=store, config=config)


def run_invoice_pdf_coding(
    pdf_bytes: bytes,
    *,
    gemini: GeminiPDFClient,
    store: CodingHistoryStore,
    config: PipelineConfig,
) -> InvoiceCodingResult:
    """Extract an attached PDF invoice and predict coding dimensions for each line."""

    invoice = extract_invoice_from_pdf(pdf_bytes, gemini=gemini, config=config)
    predictions, historical_examples_by_line_id = _code_extracted_invoice_with_evidence(
        invoice,
        gemini=gemini,
        store=store,
        config=config,
    )
    return InvoiceCodingResult(
        invoice=invoice,
        predictions=predictions,
        historical_examples_by_line_id=historical_examples_by_line_id,
    )


def extract_invoice_from_text(
    invoice_text: str,
    *,
    gemini: GeminiClient,
    config: PipelineConfig,
) -> ExtractedInvoice:
    """Extract the structured invoice object from invoice text."""

    extraction_prompt = build_extraction_prompt(invoice_text)
    extracted_payload = gemini.generate_json(
        extraction_prompt,
        model=config.extraction_model,
    )
    return _invoice_from_payload(extracted_payload)


def extract_invoice_from_pdf(
    pdf_bytes: bytes,
    *,
    gemini: GeminiPDFClient,
    config: PipelineConfig,
) -> ExtractedInvoice:
    """Extract the structured invoice object directly from PDF bytes using Gemini."""

    extraction_prompt = build_pdf_extraction_prompt()
    extracted_payload = gemini.generate_json_from_pdf(
        extraction_prompt,
        pdf_bytes=pdf_bytes,
        model=config.extraction_model,
    )
    return _invoice_from_payload(extracted_payload)


def code_extracted_invoice(
    invoice: ExtractedInvoice,
    *,
    gemini: GeminiClient,
    store: CodingHistoryStore,
    config: PipelineConfig,
) -> list[CodingPrediction]:
    """Retrieve evidence, resolve IC, and predict coding dimensions for an invoice."""

    predictions, _ = _code_extracted_invoice_with_evidence(
        invoice,
        gemini=gemini,
        store=store,
        config=config,
    )
    return predictions


def _code_extracted_invoice_with_evidence(
    invoice: ExtractedInvoice,
    *,
    gemini: GeminiClient,
    store: CodingHistoryStore,
    config: PipelineConfig,
) -> tuple[list[CodingPrediction], dict[str, list[HistoricalExample]]]:
    """Retrieve evidence, resolve IC, and predict coding dimensions for an invoice."""

    if not invoice.lines:
        return [], {}

    vendor_summary = store.fetch_vendor_summary(invoice.vendor, limit=50)
    ic_resolution = resolve_ic(
        invoice.vendor,
        store.fetch_vendor_ic_mappings(invoice.vendor),
        store.fetch_historical_ic_usage(invoice.vendor),
    )

    searches: list[SimilarLineSearch] = []
    for line in invoice.lines:
        vendor_only = is_generic_line_description(line.description)
        searches.append(
            SimilarLineSearch(
                line_id=line.line_id,
                vendor=invoice.vendor,
                line_description=line.description,
                embedding_content=build_embedding_content(
                    invoice.vendor,
                    line.description,
                    vendor_only=vendor_only,
                ),
                vendor_only=vendor_only,
            )
        )
    examples_by_line_id = _search_similar_lines_batch(store, searches, top_k=20)

    predictions: list[CodingPrediction] = []
    prediction_records: list[PredictionRecord] = []
    for line in invoice.lines:
        examples = examples_by_line_id.get(line.line_id, [])

        prediction_prompt = build_prediction_prompt(
            invoice,
            line,
            examples,
            vendor_summary,
            ic_resolution.ic,
        )
        prediction_payload = gemini.generate_json(
            prediction_prompt,
            model=config.prediction_model,
        )
        prediction = parse_coding_prediction(prediction_payload, resolved_ic=ic_resolution.ic)

        prediction_records.append(
            PredictionRecord(
                invoice=invoice,
                line_id=line.line_id,
                prediction=prediction,
                vector_example_row_ids=[example.historical_row_id for example in examples],
                prompt_version=config.prompt_version,
                extraction_model=config.extraction_model,
                prediction_model=config.prediction_model,
            )
        )
        predictions.append(prediction)

    _save_predictions(store, prediction_records)
    return predictions, examples_by_line_id


def _search_similar_lines_batch(
    store: CodingHistoryStore,
    searches: list[SimilarLineSearch],
    *,
    top_k: int,
) -> dict[str, list[HistoricalExample]]:
    if not searches:
        return {}
    search_batch = getattr(store, "search_similar_lines_batch", None)
    if callable(search_batch):
        return search_batch(searches, top_k=top_k)
    return {
        search.line_id: store.search_similar_lines(
            line_id=search.line_id,
            vendor=search.vendor,
            line_description=search.line_description,
            embedding_content=search.embedding_content,
            vendor_only=search.vendor_only,
            top_k=top_k,
        )
        for search in searches
    }


def _save_predictions(store: CodingHistoryStore, prediction_records: list[PredictionRecord]) -> None:
    if not prediction_records:
        return
    save_batch = getattr(store, "save_predictions", None)
    if callable(save_batch):
        save_batch(prediction_records)
        return
    for prediction_record in prediction_records:
        store.save_prediction(prediction_record)


def _invoice_from_payload(payload: dict) -> ExtractedInvoice:
    """Parse an extracted invoice payload using only standard library types."""

    from datetime import date
    from decimal import Decimal

    from poc_ankrag.models import InvoiceLine

    lines = [
        InvoiceLine(
            line_id=str(line["line_id"]),
            description=str(line.get("description", "")),
            amount=Decimal(str(line["amount"])),
            quantity=Decimal(str(line["quantity"])) if line.get("quantity") is not None else None,
            tax_amount=Decimal(str(line["tax_amount"])) if line.get("tax_amount") is not None else None,
        )
        for line in payload.get("lines", [])
    ]
    invoice_date = date.fromisoformat(payload["invoice_date"]) if payload.get("invoice_date") else None
    return ExtractedInvoice(
        vendor=str(payload["vendor"]),
        invoice_number=str(payload["invoice_number"]),
        invoice_date=invoice_date,
        currency=str(payload.get("currency", "")),
        lines=lines,
    )


def prediction_to_json(prediction: CodingPrediction) -> str:
    """Serialize only the strict dimensions object for downstream systems."""

    return json.dumps(prediction.to_dimensions(), ensure_ascii=False, sort_keys=True)
