import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from variation_alerts import completed_comparison_periods, exceeds_variation_threshold


class VariationAlertRulesTest(unittest.TestCase):
    def test_weekly_uses_two_completed_calendar_weeks(self):
        periods = completed_comparison_periods("weekly", date(2026, 1, 5))
        self.assertEqual(periods.current_start, date(2025, 12, 29))
        self.assertEqual(periods.current_end, date(2026, 1, 4))
        self.assertEqual(periods.previous_start, date(2025, 12, 22))

    def test_monthly_handles_year_boundary(self):
        periods = completed_comparison_periods("monthly", date(2026, 1, 3))
        self.assertEqual(periods.current_start, date(2025, 12, 1))
        self.assertEqual(periods.current_end, date(2025, 12, 31))
        self.assertEqual(periods.previous_start, date(2025, 11, 1))

    def test_threshold_is_percentage_or_absolute_increase(self):
        self.assertTrue(exceeds_variation_threshold(10000, 11000))
        self.assertTrue(exceeds_variation_threshold(100000, 105000))
        self.assertFalse(exceeds_variation_threshold(100000, 104999))
        self.assertFalse(exceeds_variation_threshold(0, 5000))
        self.assertFalse(exceeds_variation_threshold(10000, 9000))


if __name__ == "__main__":
    unittest.main()
