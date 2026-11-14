import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import period_comparisons as pc  # noqa: E402

AS_OF = date(2026, 11, 15)


def row(month, merchant, category, amount, method='Visa'):
    return {'month_start': month, 'merchant': merchant, 'category': category, 'amount': amount, 'method': method}


# Six closed months (May-Oct 2026) and a partial November.
ROWS = []
for m in range(5, 11):
    ROWS.append(row(date(2026, m, 1), 'Carrefour', 'Supermercado', 100))
    ROWS.append(row(date(2026, m, 1), 'Edenor', 'Servicios', 50))
ROWS.append(row(date(2026, 10, 1), 'Cabify', 'Transporte', 30, method='Mercado Pago'))
ROWS.append(row(date(2026, 11, 1), 'Carrefour', 'Supermercado', 60))
ROWS.append(row(date(2026, 11, 1), 'Cabify', 'Transporte', 20, method='Mercado Pago'))


class ComparePeriodsTests(unittest.TestCase):
    def setUp(self):
        self.result = pc.compare_periods(ROWS, as_of=AS_OF)
        self.cats = {i['name']: i for i in self.result['categories']['items']}

    def test_windows_are_the_previous_closed_months(self):
        self.assertEqual(self.result['windows']['current'], '2026-11')
        self.assertEqual(self.result['windows']['avg_3m'], ['2026-10', '2026-09', '2026-08'])
        self.assertEqual(self.result['windows']['avg_6m'][-1], '2026-05')

    def test_nominal_comparison_against_three_and_six_month_averages(self):
        sup = self.cats['Supermercado']
        self.assertEqual(sup['current'], 60)
        self.assertEqual(sup['avg_3m'], 100)
        self.assertEqual(sup['avg_6m'], 100)
        self.assertEqual(sup['vs_avg_3m_abs'], -40)
        self.assertEqual(sup['vs_avg_3m_pct'], -40.0)

    def test_share_is_computed_against_each_window_total(self):
        sup = self.cats['Supermercado']
        self.assertEqual(sup['share_current'], 75.0)   # 60 of 80
        self.assertEqual(sup['share_avg_3m'], 62.5)    # 100 of 160 (Oct 180, Sep 150, Aug 150, averaged)
        self.assertEqual(sup['share_avg_6m'], 64.5)  # 100 of 155 (930 over six months)
        self.assertEqual(sup['share_vs_avg_3m_pp'], 12.5)

    def test_category_missing_in_a_window_counts_as_zero(self):
        transport = self.cats['Transporte']
        self.assertEqual(transport['current'], 20)
        self.assertEqual(transport['avg_3m'], 10)  # 30 in Oct only, over 3 months
        self.assertEqual(transport['avg_6m'], 5)

    def test_merchant_dimension_is_computed_too(self):
        merchants = {i['name']: i for i in self.result['merchants']['items']}
        self.assertEqual(merchants['Carrefour']['current'], 60)
        self.assertEqual(merchants['Edenor']['current'], 0)
        self.assertEqual(merchants['Edenor']['avg_3m'], 50)

    def test_totals_and_projection_for_the_partial_month(self):
        self.assertEqual(self.result['categories']['totals']['current'], 80)
        self.assertEqual(self.result['categories']['totals']['avg_3m'], 160)
        self.assertEqual(self.result['days_elapsed'], 15)
        self.assertEqual(self.result['projected_total'], 160.0)  # 80 in 15 days over 30 days

    def test_items_are_ranked_by_current_spending(self):
        names = [i['name'] for i in self.result['categories']['items']]
        self.assertEqual(names, ['Supermercado', 'Transporte', 'Servicios'])


class EvolutionTests(unittest.TestCase):
    def test_monthly_totals_shares_and_top_items(self):
        result = pc.evolution(ROWS, as_of=AS_OF, months=6)
        months = result['months']
        self.assertEqual([m['month'] for m in months], ['2026-06', '2026-07', '2026-08', '2026-09', '2026-10', '2026-11'])
        october = next(m for m in months if m['month'] == '2026-10')
        self.assertEqual(october['total'], 180)
        self.assertEqual(october['top_categories'][0], {'name': 'Supermercado', 'amount': 100, 'share': 55.6})
        self.assertEqual(october['top_merchants'][0]['name'], 'Carrefour')
        self.assertTrue(months[-1]['partial'])
        self.assertFalse(months[0]['partial'])

    def test_share_of_window_adds_up_to_one_hundred(self):
        months = pc.evolution(ROWS, as_of=AS_OF, months=6)['months']
        self.assertAlmostEqual(sum(m['share_of_window'] for m in months), 100.0, delta=0.5)


class PaymentShareTests(unittest.TestCase):
    def test_share_by_method_over_the_window(self):
        result = pc.payment_share(ROWS, as_of=AS_OF, months=6)
        methods = {m['name']: m for m in result['methods']}
        # Visa: Carrefour and Edenor May-Oct (600 + 300) plus Carrefour in November (60) = 960.
        # Mercado Pago: Cabify in October (30) and November (20) = 50. Total 1010.
        self.assertEqual(methods['Visa']['amount'], 960)
        self.assertEqual(methods['Mercado Pago'], {'name': 'Mercado Pago', 'amount': 50.0, 'share': 5.0})
        self.assertEqual(result['window_total'], 1010)

    def test_blank_method_is_labelled_not_dropped(self):
        rows = [row(date(2026, 10, 1), 'X', 'Y', 40, method='  ')]
        result = pc.payment_share(rows, as_of=AS_OF)
        self.assertEqual(result['methods'][0]['name'], 'Sin dato')

    def test_months_outside_the_window_are_ignored(self):
        rows = [row(date(2025, 1, 1), 'X', 'Y', 999, method='Old')]
        self.assertEqual(pc.payment_share(rows, as_of=AS_OF)['methods'], [])


class RobustnessTests(unittest.TestCase):
    def test_bad_values_are_ignored_and_no_division_by_zero(self):
        rows = [row('basura', 'X', 'Y', 10), row(date(2026, 10, 1), 'X', 'Y', float('nan')), row(None, 'X', 'Y', 5)]
        result = pc.compare_periods(rows, as_of=AS_OF)
        self.assertEqual(result['categories']['items'], [])
        self.assertEqual(result['projected_total'], 0.0)

    def test_empty_history_returns_empty_lists(self):
        result = pc.compare_periods([], as_of=AS_OF)
        self.assertEqual(result['merchants']['items'], [])
        self.assertEqual(pc.evolution([], as_of=AS_OF)['window_total'], 0.0)


if __name__ == '__main__':
    unittest.main()
