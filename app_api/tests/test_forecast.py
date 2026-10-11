from datetime import date
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from forecast import assert_read_only_query, build_monthly_forecast, monthly_expenses_query


class ForecastTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            {"month_start": "2026-04-01", "merchant": "Notion", "category": "Software", "amount": 10000},
            {"month_start": "2026-05-01", "merchant": "Notion", "category": "Software", "amount": 10000},
            {"month_start": "2026-06-01", "merchant": "Notion", "category": "Software", "amount": 10000},
            {"month_start": "2026-07-01", "merchant": "Notion", "category": "Software", "amount": 10000},
            {"month_start": "2026-08-01", "merchant": "Notion", "category": "Software", "amount": 10000},
            {"month_start": "2026-09-01", "merchant": "Notion", "category": "Software", "amount": 10000},
            {"month_start": "2026-09-01", "merchant": "Coto", "category": "Comida", "amount": 40000},
            {"month_start": "2026-10-01", "merchant": "Notion", "category": "Software", "amount": 10000},
        ]

    def test_identifies_recurrent_expenses_and_monthly_projection(self):
        result = build_monthly_forecast(self.rows, as_of=date(2026, 10, 10))
        notion = next(item for item in result["recurring"] if item["merchant"] == "Notion")
        self.assertEqual(notion["months_observed"], 6)
        self.assertEqual(notion["expected_monthly"], 10000.0)
        self.assertEqual(result["forecast"]["current_month_spent"], 10000.0)
        self.assertGreaterEqual(result["forecast"]["projected_month_end"], result["forecast"]["monthly_total"])

    def test_forecast_keeps_missing_months_as_zeroes(self):
        result = build_monthly_forecast([], as_of=date(2026, 10, 10))
        self.assertEqual(len(result["monthly_history"]), 6)
        self.assertEqual(result["forecast"]["monthly_total"], 0.0)
        self.assertEqual(result["recurring"], [])

    def test_query_is_fixed_read_only_and_parameterized(self):
        query = monthly_expenses_query(
            project_id="hazel-pillar-400222",
            dataset="PRD",
            gastos_view="gastos_totales",
            mapping_table="dim_comercio_mapping",
            has_monto_ars=True,
            use_mapping=True,
        )
        self.assertTrue(query.lstrip().startswith("WITH"))
        self.assertIn("@start_date", query)
        self.assertIn("@end_date", query)
        self.assertNotIn("UPDATE", query.upper())
        assert_read_only_query(query)

    def test_write_statement_is_rejected(self):
        with self.assertRaises(ValueError):
            assert_read_only_query("UPDATE any_table SET amount = 0")
