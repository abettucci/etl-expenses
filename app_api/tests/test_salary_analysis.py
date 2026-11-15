import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import salary_analysis as sa  # noqa: E402

INFLATION = {'2026-01': 2.0, '2026-02': 2.0, '2026-03': 3.0, '2026-04': 3.0, '2026-05': 2.0, '2026-06': 2.0,
             '2026-07': 2.0, '2026-08': 2.0, '2026-09': 2.0, '2026-10': 2.0}
SLIPS = [
    {'period': '2026-01', 'gross': 1000.0, 'net': 800.0},
    {'period': '2026-02', 'gross': 1000.0, 'net': 800.0},   # no change
    {'period': '2026-03', 'gross': 1100.0, 'net': 880.0},   # +10% against inflation of 3%
    {'period': '2026-04', 'gross': 1100.0, 'net': 880.0},
]


class StepTests(unittest.TestCase):
    def setUp(self):
        self.result = sa.analyze(SLIPS, INFLATION, year=2026)

    def test_one_step_per_consecutive_pair(self):
        self.assertEqual([s['to'] for s in self.result['steps']], ['2026-02', '2026-03', '2026-04'])

    def test_a_raise_is_counted_with_its_month_and_the_real_change(self):
        march = self.result['steps'][1]
        self.assertEqual(march['gross_change_pct'], 10.0)
        self.assertEqual(march['inflation_pct'], 3.0)
        self.assertEqual(march['real_change_pct'], 6.8)   # 1.10 / 1.03 - 1
        self.assertTrue(march['beat_inflation'])
        self.assertTrue(march['raise'])

    def test_no_change_is_not_a_raise(self):
        february = self.result['steps'][0]
        self.assertFalse(february['raise'])
        self.assertEqual(february['gross_change_pct'], 0.0)
        self.assertEqual(february['beat_inflation'], False)

    def test_raises_list_and_largest(self):
        self.assertEqual([r['to'] for r in self.result['raises']], ['2026-03'])
        self.assertEqual(self.result['largest_raise']['to'], '2026-03')


class YearSummaryTests(unittest.TestCase):
    def test_growth_and_inflation_of_the_year(self):
        year = sa.analyze(SLIPS, INFLATION, year=2026)['year']
        self.assertEqual(year['first_period'], '2026-01')
        self.assertEqual(year['last_period'], '2026-04')
        self.assertEqual(year['gross_growth_pct'], 10.0)
        # Inflation after January up to April: 2%, 3%, 3%, 2% compounded.
        self.assertAlmostEqual(year['inflation_pct'], round((1.02 * 1.03 * 1.03 - 1) * 100, 1), places=1)
        self.assertEqual(year['raises_count'], 1)
        self.assertEqual(year['raise_months'], ['2026-03'])
        self.assertEqual(year['months_beating_inflation'], 1)

    def test_a_single_payslip_gives_no_growth_figure(self):
        result = sa.analyze(SLIPS[:1], INFLATION, year=2026)
        self.assertIsNone(result['all_time']['gross_growth_pct'])
        self.assertIsNone(result['year']['gross_growth_pct'])

    def test_all_time_summary_matches_the_year_when_there_is_one_year(self):
        result = sa.analyze(SLIPS, INFLATION)
        self.assertEqual(result['all_time']['gross_growth_pct'], 10.0)
        self.assertIsNone(result['year'])


class GapTests(unittest.TestCase):
    def test_a_gap_compounds_every_month_in_between(self):
        slips = [{'period': '2026-01', 'gross': 1000.0, 'net': 800.0}, {'period': '2026-04', 'gross': 1100.0, 'net': 880.0}]
        step = sa.analyze(slips, INFLATION)['steps'][0]
        self.assertEqual(step['months'], 3)
        self.assertAlmostEqual(step['inflation_pct'], round((1.02 * 1.03 * 1.03 - 1) * 100, 1), places=1)

    def test_missing_inflation_gives_no_comparison_instead_of_a_wrong_one(self):
        slips = [{'period': '2026-01', 'gross': 1000.0, 'net': 800.0}, {'period': '2026-03', 'gross': 1100.0, 'net': 880.0}]
        step = sa.analyze(slips, {'2026-02': 2.0})['steps'][0]
        self.assertIsNone(step['inflation_pct'])
        self.assertIsNone(step['real_change_pct'])
        self.assertIsNone(step['beat_inflation'])

    def test_december_rolls_over_to_january(self):
        self.assertEqual(sa._next_month('2025-12'), '2026-01')

    def test_empty_history(self):
        result = sa.analyze([], INFLATION, year=2026)
        self.assertEqual(result['count'], 0)
        self.assertIsNone(result['all_time'])
        self.assertIsNone(result['year'])


if __name__ == '__main__':
    unittest.main()
