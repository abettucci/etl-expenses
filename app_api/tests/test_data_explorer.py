import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import data_explorer as dx  # noqa: E402

PROJECT, DATASET, VIEW = 'hazel-pillar-400222', 'PRD', 'gastos_totales'


class SourceSummaryTests(unittest.TestCase):
    ROWS = [
        {'source': 'carrefour_data', 'currency': 'ARS', 'rows_n': 1780, 'total': 6816957.0, 'invalid_dates': 1780,
         'first_date': None, 'last_date': None, 'last_ingested_at': datetime(2026, 11, 5, 12, 43)},
        {'source': 'bank_payments', 'currency': 'ARS', 'rows_n': 405, 'total': 13883276.0, 'invalid_dates': 0,
         'first_date': datetime(2025, 4, 26), 'last_date': datetime(2026, 11, 13), 'last_ingested_at': datetime(2026, 11, 14, 13)},
        {'source': 'bank_payments', 'currency': 'USD', 'rows_n': 176, 'total': 6613.0, 'invalid_dates': 0,
         'first_date': datetime(2025, 5, 2), 'last_date': datetime(2026, 11, 13), 'last_ingested_at': datetime(2026, 11, 14, 13)},
        {'source': 'supermarket_receipts', 'currency': '(sin divisa)', 'rows_n': 28, 'total': 20810.0, 'invalid_dates': 0,
         'first_date': datetime(2026, 9, 17), 'last_date': datetime(2026, 9, 17), 'last_ingested_at': None},
    ]

    def setUp(self):
        self.result = dx.summarize_sources(self.ROWS)
        self.by = {s['source']: s for s in self.result['sources']}

    def test_groups_currencies_under_each_source_and_totals_rows(self):
        payments = self.by['bank_payments']
        self.assertEqual(payments['rows'], 581)
        self.assertEqual({t['currency']: t['amount'] for t in payments['totals']}, {'ARS': 13883276.0, 'USD': 6613.0})
        self.assertEqual(payments['first_date'], '2025-04-26T00:00:00')
        self.assertEqual(payments['last_date'], '2026-11-13T00:00:00')
        self.assertEqual(self.result['total_rows'], 405 + 176 + 1780 + 28)

    def test_flags_invalid_dates_and_missing_currency(self):
        self.assertEqual(self.by['carrefour_data']['flags'], [{'kind': 'invalid_dates', 'count': 1780}])
        self.assertEqual(self.by['supermarket_receipts']['flags'], [{'kind': 'no_currency', 'count': 28}])
        self.assertEqual(self.by['bank_payments']['flags'], [])

    def test_known_sources_get_a_readable_label(self):
        self.assertEqual(self.by['mp_data' if 'mp_data' in self.by else 'bank_payments']['label'], 'Tarjetas y débitos (Gmail)')
        self.assertEqual(self.by['carrefour_data']['label'], 'Carrefour (tickets)')


class TransactionQueryTests(unittest.TestCase):
    def build(self, **raw):
        return dx.transactions_query(PROJECT, DATASET, VIEW, dx.parse_transaction_params(raw))

    def test_defaults_are_bounded_and_sorted_newest_first(self):
        sql, params = self.build()
        self.assertIn('ORDER BY fecha DESC NULLS LAST, transaction_key', sql)
        self.assertEqual(dict((n, v) for n, _, v in params), {'limit': 50, 'offset': 0})
        self.assertIn('COUNT(*) OVER() AS _total', sql)
        self.assertIn('CAST(nro_tarjeta AS STRING) AS nro_tarjeta', sql)

    def test_user_values_travel_as_parameters_never_in_the_sql(self):
        sql, params = self.build(source='mp_data', q='carrefour', category='Supermercado', date_from='2026-01-01',
                                 date_to='2026-03-31', min_amount='1000,5', max_amount='9000')
        for secret in ('mp_data', 'carrefour', 'Supermercado', '2026-01-01', '1000'):
            self.assertNotIn(secret, sql)
        values = {n: v for n, _, v in params}
        self.assertEqual(values['source'], 'mp_data')
        self.assertEqual(values['q'], '%carrefour%')
        self.assertEqual(values['date_from'], date(2026, 1, 1))
        self.assertEqual(values['min_amount'], 1000.5)

    def test_like_wildcards_in_the_search_are_escaped(self):
        _, params = self.build(q='50%_off')
        self.assertEqual(dict((n, v) for n, _, v in params)['q'], '%50\\%\\_off%')

    def test_injection_attempts_are_rejected(self):
        for bad in ({'source': "x'; DROP TABLE a;--"}, {'category': 'a`b'}, {'q': 'a;b'}, {'sort': 'fecha; DROP'},
                    {'dir': 'sideways'}, {'quality': 'everything'}):
            with self.assertRaises(ValueError, msg=str(bad)):
                dx.parse_transaction_params(bad)

    def test_limit_and_offset_are_capped(self):
        for bad in ({'limit': '0'}, {'limit': '5001'}, {'limit': 'abc'}, {'offset': '-1'}, {'offset': '100001'}):
            with self.assertRaises(ValueError, msg=str(bad)):
                dx.parse_transaction_params(bad)
        self.assertEqual(dx.parse_transaction_params({'limit': '5000'})['limit'], 5000)

    def test_only_allowlisted_sort_columns_reach_the_sql(self):
        sql, _ = self.build(sort='monto_total', dir='asc')
        self.assertIn('ORDER BY monto_total ASC NULLS LAST', sql)
        with self.assertRaises(ValueError):
            dx.parse_transaction_params({'sort': 'nro_tarjeta'})

    def test_quality_filter_adds_a_fixed_predicate_and_no_parameter(self):
        sql, params = self.build(quality='invalid_dates')
        self.assertIn('NOT (fecha BETWEEN', sql)
        self.assertEqual([n for n, _, _ in params], ['limit', 'offset'])

    def test_bad_dates_and_numbers_are_reported_in_spanish(self):
        with self.assertRaisesRegex(ValueError, 'AAAA-MM-DD'):
            dx.parse_transaction_params({'date_from': '15/01/2026'})
        with self.assertRaisesRegex(ValueError, 'número'):
            dx.parse_transaction_params({'min_amount': 'mucho'})

    def test_identifiers_are_validated(self):
        with self.assertRaises(ValueError):
            dx.transactions_query('bad project', DATASET, VIEW, dx.parse_transaction_params({}))
        with self.assertRaises(ValueError):
            dx.transactions_query(PROJECT, 'PRD`; --', VIEW, dx.parse_transaction_params({}))


class CatalogAndPreviewTests(unittest.TestCase):
    OBJECTS = [
        {'name': 'gastos_totales', 'type': 'VIEW', 'rows_n': 0, 'size_bytes': 0, 'modified': datetime(2026, 9, 27)},
        {'name': 'mp_data', 'type': 'BASE TABLE', 'rows_n': 2498, 'size_bytes': 9000, 'modified': datetime(2026, 9, 23)},
        {'name': 'bank_payments', 'type': 'BASE TABLE', 'rows_n': 581, 'size_bytes': 100, 'modified': None},
    ]
    COLUMNS = [
        {'table_name': 'mp_data', 'column_name': 'SOURCE_ID', 'data_type': 'STRING', 'ordinal_position': 1},
        {'table_name': 'bank_payments', 'column_name': 'NRO_TARJETA', 'data_type': 'FLOAT64', 'ordinal_position': 1},
        {'table_name': 'gastos_totales', 'column_name': 'fecha', 'data_type': 'TIMESTAMP', 'ordinal_position': 1},
    ]

    def setUp(self):
        self.catalog = dx.build_catalog(self.OBJECTS, self.COLUMNS)

    def test_tables_come_first_and_views_have_no_row_count(self):
        self.assertEqual([c['name'] for c in self.catalog], ['bank_payments', 'mp_data', 'gastos_totales'])
        view = self.catalog[-1]
        self.assertEqual((view['type'], view['rows']), ('VIEW', None))
        self.assertEqual(self.catalog[1]['rows'], 2498)
        self.assertEqual(self.catalog[1]['columns'][0]['name'], 'SOURCE_ID')
        self.assertIn('description', self.catalog[1]['columns'][0])
        self.assertIn('description', self.catalog[1])

    def test_preview_only_accepts_objects_and_columns_from_the_catalog(self):
        ok = dx.parse_preview_params({'name': 'mp_data', 'sort': 'SOURCE_ID', 'dir': 'desc', 'limit': '25'}, self.catalog)
        sql, params = dx.preview_query(PROJECT, DATASET, ok)
        self.assertEqual(sql.strip(), 'SELECT * FROM `hazel-pillar-400222.PRD.mp_data` ORDER BY `SOURCE_ID` DESC NULLS LAST LIMIT @limit OFFSET @offset')
        self.assertEqual({n: v for n, _, v in params}, {'limit': 25, 'offset': 0})
        for bad in ({'name': 'secrets'}, {'name': 'mp_data', 'sort': 'nope'}, {'name': 'mp_data` --'}, {}):
            with self.assertRaises(ValueError, msg=str(bad)):
                dx.parse_preview_params(bad, self.catalog)

    def test_catalog_queries_validate_project_and_dataset(self):
        queries = dx.catalog_queries(PROJECT, DATASET)
        self.assertIn('INFORMATION_SCHEMA.TABLES', queries['objects'])
        with self.assertRaises(ValueError):
            dx.catalog_queries('x; drop', DATASET)

    def test_files_queries_are_read_only_selects(self):
        for sql in dx.files_queries(PROJECT, DATASET).values():
            self.assertTrue(sql.lstrip().upper().startswith('SELECT'))
            self.assertNotRegex(sql.upper(), r'\b(INSERT|UPDATE|DELETE|DROP|MERGE|CREATE)\b')


class OutputTests(unittest.TestCase):
    def test_card_and_account_numbers_keep_only_the_last_four_digits(self):
        rows = [{'NRO_TARJETA': 4509953566234321.0, 'cbu_destino': '0170099220000012345678', 'COMERCIO': 'Shell', 'nro_cuenta': None}]
        shaped = dx.shape_rows(rows, ['NRO_TARJETA', 'cbu_destino', 'COMERCIO', 'nro_cuenta'])[0]
        self.assertEqual(shaped['NRO_TARJETA'], '••••4321')
        self.assertEqual(shaped['cbu_destino'], '••••5678')
        self.assertEqual(shaped['COMERCIO'], 'Shell')
        self.assertIsNone(shaped['nro_cuenta'])

    def test_values_are_json_safe(self):
        rows = [{'a': datetime(2026, 11, 3, 10, 5), 'b': date(2026, 11, 3), 'c': Decimal('12.50'), 'd': float('nan'), 'e': b'\x01\xff'}]
        shaped = dx.shape_rows(rows, ['a', 'b', 'c', 'd', 'e'])[0]
        self.assertEqual(shaped, {'a': '2026-11-03T10:05:00', 'b': '2026-11-03', 'c': 12.5, 'd': None, 'e': '01ff'})


if __name__ == '__main__':
    unittest.main()
