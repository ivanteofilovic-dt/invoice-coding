import unittest
from datetime import date
from decimal import Decimal

from poc_ankrag.config import PipelineConfig
from poc_ankrag.models import ExtractedInvoice, HistoricalExample, InvoiceLine
from poc_ankrag.pipeline import code_extracted_invoice, parse_coding_prediction, run_invoice_pdf_coding
from poc_ankrag.prompts import build_extraction_prompt, build_pdf_extraction_prompt, build_prediction_prompt


class PromptsAndPipelineTests(unittest.TestCase):
    def test_extraction_prompt_preserves_swedish_and_json_contract(self):
        prompt = build_extraction_prompt("Faktura för mobilabonnemang")

        self.assertIn("Preserve Swedish text exactly", prompt)
        self.assertIn('"vendor": "string"', prompt)
        self.assertIn("Faktura för mobilabonnemang", prompt)

    def test_pdf_extraction_prompt_uses_attached_invoice(self):
        prompt = build_pdf_extraction_prompt()

        self.assertIn("attached PDF invoice", prompt)
        self.assertIn('"invoice_number": "string"', prompt)

    def test_prediction_prompt_includes_ic_and_required_dimensions(self):
        invoice = ExtractedInvoice(
            vendor="Telia Sverige AB",
            invoice_number="INV-1",
            invoice_date=date(2026, 5, 6),
            currency="SEK",
            lines=[
                InvoiceLine(
                    line_id="line-001",
                    description="Mobilabonnemang mars",
                    amount=Decimal("1200.00"),
                )
            ],
        )

        prompt = build_prediction_prompt(
            invoice,
            invoice.lines[0],
            historical_examples=[],
            vendor_summary=[],
            resolved_ic="IC123",
        )

        self.assertIn("must be copied exactly: IC123", prompt)
        self.assertIn('"ACCOUNT": "string"', prompt)
        self.assertIn('"confidence": "number between 0 and 1"', prompt)
        self.assertIn("Mobilabonnemang mars", prompt)

    def test_parse_coding_prediction_requires_rule_resolved_ic(self):
        with self.assertRaises(ValueError):
            parse_coding_prediction(
                {
                    "ACCOUNT": "61000",
                    "DEPARTMENT": "1S1234",
                    "PRODUCT": "1S00000",
                    "IC": "WRONG",
                    "PROJECT": "000000",
                    "SYSTEM": "000000",
                    "RESERVE": "1S0000000000",
                },
                resolved_ic="IC123",
            )

    def test_code_extracted_invoice_reuses_vendor_context_and_batches_io(self):
        invoice = ExtractedInvoice(
            vendor="Telia Sverige AB",
            invoice_number="INV-1",
            invoice_date=date(2026, 5, 6),
            currency="SEK",
            lines=[
                InvoiceLine(line_id="line-001", description="Mobilabonnemang", amount=Decimal("100")),
                InvoiceLine(line_id="line-002", description="Router", amount=Decimal("200")),
            ],
        )
        gemini = _FakeGemini()
        store = _FakeStore()
        config = PipelineConfig(project_id="project", dataset_id="dataset", raw_gl_table="raw")

        predictions = code_extracted_invoice(invoice, gemini=gemini, store=store, config=config)

        self.assertEqual(len(predictions), 2)
        self.assertEqual(gemini.call_count, 2)
        self.assertEqual(store.vendor_summary_calls, 1)
        self.assertEqual(store.ic_mapping_calls, 1)
        self.assertEqual(store.historical_ic_calls, 1)
        self.assertEqual(store.batch_search_line_count, 2)
        self.assertEqual(store.saved_prediction_count, 2)

    def test_run_invoice_pdf_coding_returns_historical_evidence(self):
        gemini = _FakeGemini()
        store = _FakeStore()
        config = PipelineConfig(project_id="project", dataset_id="dataset", raw_gl_table="raw")

        result = run_invoice_pdf_coding(b"%PDF", gemini=gemini, store=store, config=config)

        self.assertEqual(len(result.predictions), 2)
        self.assertEqual(set(result.historical_examples_by_line_id), {"line-001", "line-002"})
        self.assertEqual(
            result.historical_examples_by_line_id["line-001"][0].gl_line_description,
            "Historical Mobilabonnemang",
        )


class _FakeGemini:
    def __init__(self) -> None:
        self.call_count = 0

    def generate_json(self, prompt: str, *, model: str) -> dict:
        self.call_count += 1
        return {
            "ACCOUNT": "61000",
            "DEPARTMENT": "1S1234",
            "PRODUCT": "1S00000",
            "IC": "",
            "PROJECT": "000000",
            "SYSTEM": "000000",
            "RESERVE": "1S0000000000",
            "confidence": 0.9,
        }

    def generate_json_from_pdf(
        self,
        prompt: str,
        *,
        pdf_bytes: bytes,
        model: str,
        mime_type: str = "application/pdf",
    ) -> dict:
        return {
            "vendor": "Telia Sverige AB",
            "invoice_number": "INV-1",
            "invoice_date": "2026-05-06",
            "currency": "SEK",
            "lines": [
                {"line_id": "line-001", "description": "Mobilabonnemang", "amount": "100"},
                {"line_id": "line-002", "description": "Router", "amount": "200"},
            ],
        }


class _FakeStore:
    def __init__(self) -> None:
        self.vendor_summary_calls = 0
        self.ic_mapping_calls = 0
        self.historical_ic_calls = 0
        self.batch_search_line_count = 0
        self.saved_prediction_count = 0

    def search_similar_lines_batch(self, searches, *, top_k: int = 20):
        self.batch_search_line_count = len(searches)
        return {
            search.line_id: [
                HistoricalExample(
                    historical_row_id=f"hist-{search.line_id}",
                    supplier_customer_name=search.vendor,
                    gl_line_description=f"Historical {search.line_description}",
                    hfm_descriptions="Historical coding",
                    account="61000",
                    department="1S1234",
                    product="1S00000",
                    ic="",
                    project="000000",
                    system="000000",
                    reserve="1S0000000000",
                    amount=Decimal("100"),
                    posting_date=date(2026, 4, 1),
                    distance=0.05,
                )
            ]
            for search in searches
        }

    def fetch_vendor_summary(self, vendor: str, *, limit: int = 50):
        self.vendor_summary_calls += 1
        return []

    def fetch_vendor_ic_mappings(self, vendor: str):
        self.ic_mapping_calls += 1
        return []

    def fetch_historical_ic_usage(self, vendor: str):
        self.historical_ic_calls += 1
        return []

    def save_predictions(self, predictions):
        self.saved_prediction_count = len(predictions)


if __name__ == "__main__":
    unittest.main()
