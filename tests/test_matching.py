import unittest

from poc_ankrag.matching import build_embedding_content, is_generic_line_description, normalize_vendor


class MatchingTests(unittest.TestCase):
    def test_generic_invoice_reference_uses_vendor_only_matching(self):
        self.assertTrue(is_generic_line_description("Faktura 123456789"))
        self.assertEqual(
            build_embedding_content("Telia Sverige AB", "Faktura 123456789"),
            "Telia Sverige AB |  | ",
        )

    def test_meaningful_description_is_kept_in_embedding_content(self):
        self.assertFalse(is_generic_line_description("Mobilabonnemang mars"))
        self.assertEqual(
            build_embedding_content("Telia Sverige AB", "Mobilabonnemang mars"),
            "Telia Sverige AB | Mobilabonnemang mars | Mobilabonnemang mars",
        )

    def test_vendor_normalization_is_case_and_whitespace_insensitive(self):
        self.assertEqual(normalize_vendor("  TELIA   Sverige AB "), "telia sverige ab")


if __name__ == "__main__":
    unittest.main()
