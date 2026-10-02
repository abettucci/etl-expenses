"""Tests for the strict, SQL-free /variacion command parser."""

from datetime import date
from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).parent))
from variation_commands import VariationCommandError, parse_variation_command


class VariationCommandTest(unittest.TestCase):
    def test_parses_two_single_days(self):
        command = parse_variation_command("/variacion 2026-10-01 2026-09-30")
        self.assertEqual(command.period_kind, "Día")
        self.assertEqual(command.periods.current_start, date(2026, 10, 1))
        self.assertEqual(command.periods.current_end, date(2026, 10, 1))
        self.assertEqual(command.periods.previous_start, date(2026, 9, 30))

    def test_parses_custom_ranges_of_equal_size(self):
        command = parse_variation_command(
            "/variacion 2026-10-01..2026-10-07 2026-09-24..2026-09-30"
        )
        self.assertEqual(command.period_kind, "Rango personalizado")
        self.assertEqual(command.periods.current_end, date(2026, 10, 7))
        self.assertEqual(command.periods.previous_start, date(2026, 9, 24))

    def test_week_uses_monday_to_sunday_for_any_date_in_the_week(self):
        command = parse_variation_command("/variacion semana 2026-09-23 2026-09-16")
        self.assertEqual(command.period_kind, "Semana")
        self.assertEqual(command.periods.current_start, date(2026, 9, 21))
        self.assertEqual(command.periods.current_end, date(2026, 9, 27))
        self.assertEqual(command.periods.previous_start, date(2026, 9, 14))

    def test_parses_complete_calendar_months(self):
        command = parse_variation_command("/variacion mes 2026-09 2026-08")
        self.assertEqual(command.period_kind, "Mes")
        self.assertEqual(command.periods.current_start, date(2026, 9, 1))
        self.assertEqual(command.periods.current_end, date(2026, 9, 30))
        self.assertEqual(command.periods.previous_end, date(2026, 8, 31))

    def test_rejects_ambiguous_or_uneven_ranges(self):
        with self.assertRaises(VariationCommandError):
            parse_variation_command("/variacion 2026-10-01..2026-10-07 2026-09-30..2026-09-30")
        with self.assertRaises(VariationCommandError):
            parse_variation_command("/variacion mes septiembre agosto")


if __name__ == "__main__":
    unittest.main()
