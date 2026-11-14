import sys
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fx  # noqa: E402

PAYLOAD = [
    {'casa': 'blue', 'fecha': '2026-10-01', 'compra': 1500, 'venta': 1520},
    {'casa': 'oficial', 'fecha': '2026-10-01', 'compra': 1400, 'venta': 1450},
    {'casa': 'oficial', 'fecha': '2026-10-02', 'compra': 1410, 'venta': 1460},
    {'casa': 'oficial', 'fecha': '2026-10-05', 'compra': 1420, 'venta': 1470},  # Monday, after a weekend gap
    {'casa': 'oficial', 'fecha': 'basura', 'compra': 1, 'venta': 1},
    {'casa': 'oficial', 'fecha': '2026-10-06', 'compra': 1, 'venta': 0},
]
SERIES = fx.parse_official(PAYLOAD)


class ParseOfficialTests(unittest.TestCase):
    def test_keeps_only_the_official_house_with_valid_selling_prices(self):
        self.assertEqual(SERIES, [(date(2026, 10, 1), 1450.0), (date(2026, 10, 2), 1460.0), (date(2026, 10, 5), 1470.0)])

    def test_bad_payloads_are_rejected(self):
        for bad in (None, {'x': 1}, [{'casa': 'blue', 'fecha': '2026-10-01', 'venta': 1}]):
            with self.assertRaises(fx.FxError, msg=str(bad)):
                fx.parse_official(bad)


class RateOnTests(unittest.TestCase):
    def test_uses_the_latest_quote_on_or_before_the_day(self):
        self.assertEqual(fx.rate_on(date(2026, 10, 2), SERIES), (1460.0, date(2026, 10, 2)))
        self.assertEqual(fx.rate_on(date(2026, 10, 4), SERIES), (1460.0, date(2026, 10, 2)))  # weekend uses Friday
        self.assertEqual(fx.rate_on(date(2026, 12, 1), SERIES), (1470.0, date(2026, 10, 5)))

    def test_dates_before_the_first_quote_use_the_first_quote(self):
        self.assertEqual(fx.rate_on(date(2026, 9, 1), SERIES), (1450.0, date(2026, 10, 1)))


class ConvertRowsTests(unittest.TestCase):
    def test_usd_rows_are_converted_with_their_day_and_ars_rows_are_kept(self):
        rows = [
            {'expense_date': date(2026, 10, 2), 'divisa': 'USD', 'amount': 10.0, 'merchant': 'Apple'},
            {'expense_date': date(2026, 10, 4), 'divisa': 'USD', 'amount': 5.0, 'merchant': 'Spotify'},
            {'expense_date': date(2026, 10, 3), 'divisa': 'ARS', 'amount': 1000.0, 'merchant': 'Coto'},
        ]
        result = fx.convert_rows(rows, SERIES)
        amounts = {r['merchant']: r['amount_ars'] for r in result['rows']}
        self.assertEqual(amounts, {'Apple': 14600.0, 'Spotify': 7300.0, 'Coto': 1000.0})
        self.assertEqual(result['usd_converted_ars'], 21900.0)
        self.assertEqual(result['quotes_used'], {'first': '2026-10-02', 'last': '2026-10-02'})
        self.assertEqual(result['skipped'], 0)

    def test_other_currencies_are_not_guessed_and_are_counted(self):
        rows = [{'expense_date': date(2026, 10, 2), 'divisa': 'EUR', 'amount': 10.0}, {'expense_date': date(2026, 10, 2), 'divisa': None, 'amount': 3.0}]
        result = fx.convert_rows(rows, SERIES)
        self.assertEqual(result['rows'], [])
        self.assertEqual(result['skipped'], 2)

    def test_string_and_datetime_days_are_accepted(self):
        rows = [{'expense_date': '2026-10-02', 'divisa': 'USD', 'amount': 1.0},
                {'expense_date': datetime(2026, 10, 2, 9, 30), 'divisa': 'USD', 'amount': 1.0}]
        self.assertEqual(sum(r['amount_ars'] for r in fx.convert_rows(rows, SERIES)['rows']), 2920.0)

    def test_source_is_named_so_the_figure_can_be_explained(self):
        result = fx.convert_rows([], SERIES)
        self.assertEqual(result['source']['name'], 'Argentina Datos')
        self.assertEqual(result['source']['field'], 'venta')


class CacheTests(unittest.TestCase):
    def setUp(self):
        fx._cache.update({'loaded_at': 0.0, 'series': None})

    def test_fetch_is_cached_for_hours(self):
        calls = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return __import__('json').dumps(PAYLOAD).encode('utf-8')

        def fake_open(url, timeout):
            calls.append(url)
            return Response()

        with patch.object(fx.urllib.request, 'urlopen', side_effect=fake_open):
            first = fx.official_series(now=1000.0)
            second = fx.official_series(now=1000.0 + 60)
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], fx.SOURCE_URL)

    def test_source_failure_without_cache_raises_a_named_error(self):
        with patch.object(fx.urllib.request, 'urlopen', side_effect=OSError('down')):
            with self.assertRaises(fx.FxError):
                fx.official_series(now=5.0)


if __name__ == '__main__':
    unittest.main()
