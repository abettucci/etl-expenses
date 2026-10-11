import unittest

from merchant_mapping_reviews import (
    candidate_id,
    is_auto_applicable,
    merchant_fingerprint,
    parse_human_correction,
    suggestion_from_structured_payload,
    suggest_from_existing_mappings,
)


class MerchantMappingReviewsTest(unittest.TestCase):
    def setUp(self):
        self.rules = [
            {
                "flow": "bank_payments",
                "match_value": "UDEMY",
                "comercio_raw": "UDEMY ONLINE COURSE",
                "comercio_depurado": "Udemy",
                "categoria": "Educación",
                "subcategoria": "Cursos Online",
                "activo": True,
                "review_status": "confirmed",
            },
            {
                "flow": "mp_data",
                "match_value": "UBER",
                "comercio_depurado": "Uber",
                "categoria": "Transporte",
                "subcategoria": "Ride",
                "activo": True,
                "review_status": "confirmed",
            },
        ]

    def test_fingerprint_removes_processors_and_ids(self):
        self.assertEqual(merchant_fingerprint("MERPAGO* UDEMY ONLINE 123456"), "UDEMY")

    def test_candidate_id_is_stable_per_flow_and_normalized_name(self):
        self.assertEqual(
            candidate_id("bank_payments", "UDEMY ONLINE"),
            candidate_id("bank_payments", "udemy-online"),
        )
        self.assertNotEqual(
            candidate_id("bank_payments", "UDEMY ONLINE"),
            candidate_id("mp_data", "UDEMY ONLINE"),
        )

    def test_existing_mapping_can_auto_apply_only_with_unique_high_score(self):
        suggestion, margin = suggest_from_existing_mappings(
            "MERPAGO*UDEMY ONLINE COURSE 99881", "mp_data", self.rules,
        )
        self.assertIsNotNone(suggestion)
        self.assertEqual(suggestion.comercio_depurado, "Udemy")
        self.assertTrue(is_auto_applicable(suggestion, margin))

    def test_short_generic_merchant_does_not_match(self):
        suggestion, _margin = suggest_from_existing_mappings("NAN", "bank_payments", self.rules)
        self.assertIsNone(suggestion)

    def test_llm_payload_must_use_existing_taxonomy(self):
        taxonomy = [("Educación", "Cursos Online")]
        accepted = suggestion_from_structured_payload({
            "comercio_depurado": "Udemy",
            "categoria": "Educación",
            "subcategoria": "Cursos Online",
        }, "UDEMY COURSE", taxonomy)
        rejected = suggestion_from_structured_payload({
            "comercio_depurado": "Udemy",
            "categoria": "Inventada",
            "subcategoria": "Nueva",
        }, "UDEMY COURSE", taxonomy)
        self.assertIsNotNone(accepted)
        self.assertIsNone(rejected)

    def test_subcategory_is_optional_when_category_exists(self):
        accepted = suggestion_from_structured_payload({
            "comercio_depurado": "Udemy",
            "categoria": "Educación",
            "subcategoria": "",
        }, "UDEMY COURSE", [("Educación", "Cursos Online")])
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted.subcategoria, "")

    def test_human_correction_accepts_explicit_fields(self):
        parsed = parse_human_correction(
            "nombre: Udemy; categoría: Educación; subcategoría: Cursos Online"
        )
        self.assertEqual(parsed, {
            "comercio_depurado": "Udemy",
            "categoria": "Educación",
            "subcategoria": "Cursos Online",
        })

    def test_human_correction_can_clear_subcategory(self):
        parsed = parse_human_correction("subcategoría: ninguna")
        self.assertEqual(parsed, {"subcategoria": ""})


if __name__ == "__main__":
    unittest.main()
