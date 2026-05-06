import unittest
from datetime import date

from poc_ankrag.ic_resolver import HistoricalICUsage, VendorICMapping, resolve_ic


class ICResolverTests(unittest.TestCase):
    def test_explicit_mapping_takes_precedence_over_history(self):
        resolution = resolve_ic(
            "Telenor Sverige AB",
            [VendorICMapping(vendor="Telenor Sverige AB", ic="IC123", source="master_data")],
            [
                HistoricalICUsage(
                    vendor="Telenor Sverige AB",
                    ic="IC999",
                    usage_count=10,
                    usage_share=1.0,
                )
            ],
            as_of=date(2026, 5, 6),
        )

        self.assertEqual(resolution.ic, "IC123")
        self.assertEqual(resolution.source, "mapping:master_data")
        self.assertEqual(resolution.confidence, 1.0)

    def test_historical_majority_is_used_when_no_mapping_exists(self):
        resolution = resolve_ic(
            "Telenor Sverige AB",
            [],
            [HistoricalICUsage(vendor="Telenor Sverige AB", ic="IC999", usage_count=10, usage_share=0.9)],
            as_of=date(2026, 5, 6),
        )

        self.assertEqual(resolution.ic, "IC999")
        self.assertEqual(resolution.source, "historical_majority")

    def test_default_is_used_when_majority_is_below_threshold(self):
        resolution = resolve_ic(
            "Telenor Sverige AB",
            [],
            [HistoricalICUsage(vendor="Telenor Sverige AB", ic="IC999", usage_count=10, usage_share=0.5)],
            as_of=date(2026, 5, 6),
            default_ic="",
        )

        self.assertEqual(resolution.ic, "")
        self.assertEqual(resolution.source, "default_non_ic")


if __name__ == "__main__":
    unittest.main()
