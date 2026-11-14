import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from insights import build_insights  # noqa: E402


def row(month, merchant, category, amount):
    return {'month_start': month, 'merchant': merchant, 'category': category, 'amount': amount}


# Four closed months (Jul-Oct 2026) plus a partial November.
ROWS = [
    row(date(2026, 7, 1), 'Carrefour', 'Supermercado', 100000),
    row(date(2026, 7, 1), 'Edenor', 'Servicios', 20000),
    row(date(2026, 8, 1), 'Carrefour', 'Supermercado', 100000),
    row(date(2026, 8, 1), 'Edenor', 'Servicios', 20000),
    row(date(2026, 9, 1), 'Carrefour', 'Supermercado', 100000),
    row(date(2026, 9, 1), 'Edenor', 'Servicios', 20000),
    row(date(2026, 10, 1), 'Carrefour', 'Supermercado', 100000),
    row(date(2026, 10, 1), 'Edenor', 'Servicios', 20000),
    row(date(2026, 10, 1), 'Cabify', 'Transporte', 10000),
    row(date(2026, 11, 1), 'Carrefour', 'Supermercado', 60000),
]


class BuildInsightsTests(unittest.TestCase):
    def setUp(self):
        self.result = build_insights(ROWS, as_of=date(2026, 11, 15))

    def test_monthly_series_marks_current_month_as_partial(self):
        months = self.result['months']
        self.assertEqual([m['month'] for m in months], ['2026-07', '2026-08', '2026-09', '2026-10', '2026-11'])
        self.assertEqual(months[3]['total'], 130000)
        self.assertTrue(months[-1]['partial'])
        self.assertFalse(months[0]['partial'])

    def test_latest_month_compares_with_previous_and_baseline(self):
        latest = self.result['latest']
        self.assertEqual(latest['month'], '2026-10')
        self.assertEqual(latest['change_abs'], 10000)
        self.assertEqual(latest['change_pct'], 8.3)
        self.assertEqual(latest['baseline_total'], 120000)

    def test_new_merchant_is_detected_and_unchanged_ones_are_not(self):
        names = [m['name'] for m in self.result['new_merchants']]
        self.assertEqual(names, ['Cabify'])
        self.assertEqual([m['name'] for m in self.result['merchants_up']], ['Cabify'])
        self.assertEqual(self.result['merchants_down'], [])

    def test_categories_are_ranked_by_current_spending_with_share(self):
        first = self.result['categories'][0]
        self.assertEqual(first['name'], 'Supermercado')
        self.assertEqual(first['change_abs'], 0)
        self.assertAlmostEqual(first['share'], 100000 / 130000, places=3)

    def test_current_month_projection_uses_elapsed_days(self):
        pace = self.result['current_month']
        self.assertEqual(pace['spent'], 60000)
        self.assertEqual(pace['days_elapsed'], 15)
        self.assertEqual(pace['projected'], 120000)
        self.assertEqual(pace['vs_latest_closed_pct'], -7.7)

    def test_trend_covers_the_closed_months_for_top_categories(self):
        self.assertEqual(self.result['trend_months'], ['2026-07', '2026-08', '2026-09', '2026-10'])
        self.assertEqual(self.result['category_trend']['Transporte'], [0, 0, 0, 10000])

    def test_spike_in_a_category_over_its_baseline_is_flagged(self):
        rows = [r for r in ROWS if r['month_start'] != date(2026, 10, 1)]
        rows += [row(date(2026, 10, 1), 'Carrefour', 'Supermercado', 100000),
                 row(date(2026, 10, 1), 'Edenor', 'Servicios', 60000)]
        result = build_insights(rows, as_of=date(2026, 11, 15))
        spikes = [i for i in result['insights'] if i['kind'] == 'category_spike']
        self.assertEqual([i['name'] for i in spikes], ['Servicios'])
        self.assertEqual(spikes[0]['change_pct'], 200.0)

    def test_needs_two_closed_months_for_variation(self):
        result = build_insights(ROWS[:2], as_of=date(2026, 8, 15))
        self.assertFalse(result['has_history'])
        self.assertEqual(result['insights'], [])
        self.assertIsNone(result['latest'])

    def test_ignores_refunds_nan_and_unparseable_months(self):
        rows = ROWS + [row(date(2026, 10, 1), 'X', 'Y', -5000), row(date(2026, 10, 1), 'X', 'Y', float('nan')),
                       row('basura', 'X', 'Y', 999), row(None, 'X', 'Y', 999)]
        result = build_insights(rows, as_of=date(2026, 11, 15))
        self.assertEqual(result['latest']['total'], 130000)

    def test_accepts_iso_strings_and_blank_labels(self):
        rows = [row('2026-09-01', '', ' ', 100), row('2026-10-01', None, None, 300)]
        result = build_insights(rows, as_of=date(2026, 11, 15))
        self.assertEqual(result['categories'][0]['name'], 'Sin clasificar')
        self.assertEqual(result['latest']['change_pct'], 200.0)


if __name__ == '__main__':
    unittest.main()
