import unittest

from fx_utils import parse_bcra_records


class ParseBcraRecordsTests(unittest.TestCase):
    def test_keeps_valid_decimal_rates(self):
        rows = parse_bcra_records({"results": [{"detalle": [
            {"fecha": "2026-09-24", "valor": 1500.25},
            {"fecha": "2026-09-25", "valor": "1.501,50"},
            {"fecha": "2026-09-26", "valor": 0},
        ]}]})
        self.assertEqual(["1500.25", "1501.50"], [row["tipo_cambio_ars"] for row in rows])


if __name__ == "__main__":
    unittest.main()
