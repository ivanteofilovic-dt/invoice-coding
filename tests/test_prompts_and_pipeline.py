import unittest
from datetime import date
from decimal import Decimal

from poc_ankrag.models import ExtractedInvoice, InvoiceLine
from poc_ankrag.pipeline import parse_coding_prediction
from poc_ankrag.prompts import build_extraction_prompt, build_prediction_prompt


class PromptsAndPipelineTests(unittest.TestCase):
    def test_extraction_prompt_preserves_swedish_and_json_contract(self):
        prompt = build_extraction_prompt("Faktura för mobilabonnemang")

        self.assertIn("Preserve Swedish text exactly", prompt)
        self.assertIn('"vendor": "string"', prompt)
        self.assertIn("Faktura för mobilabonnemang", prompt)

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


if __name__ == "__main__":
    unittest.main()
