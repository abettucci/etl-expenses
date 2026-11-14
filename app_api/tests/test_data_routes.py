import importlib.util
import json
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

PATHS = ['/v1/finance/data/sources', '/v1/finance/data/files', '/v1/finance/data/catalog',
         '/v1/finance/data/transactions', '/v1/finance/data/preview']


class FakeConfig:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class FakeBigQuery:
    QueryJobConfig = FakeConfig

    @staticmethod
    def ScalarQueryParameter(name, kind, value):
        return (name, kind, value)


class FakeClient:
    """Returns queued results in order; an Exception in the queue is raised."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        outcome = self.results.pop(0) if self.results else []
        if isinstance(outcome, Exception):
            raise outcome
        return SimpleNamespace(result=lambda: outcome)


class DataRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.dict(os.environ, {'APP_TABLE': 'synthetic-test', 'MP_SECRET_ARN': ''}), patch('boto3.resource'):
            spec = importlib.util.spec_from_file_location('data_api_test', Path(__file__).resolve().parents[1] / 'lambda_function.py')
            cls.api = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.api)

    def setUp(self):
        self.table = Mock()
        for item in [patch.object(self.api, 'table', self.table), patch.object(self.api, 'ensure_profile', return_value={}),
                     patch.object(self.api, 'FORECAST_ENABLED', True), patch.object(self.api, 'FORECAST_OWNER_SUB', 'owner'),
                     patch.object(self.api, 'FORECAST_GCP_PROJECT_ID', 'hazel-pillar-400222'),
                     patch.object(self.api, 'FORECAST_BQ_DATASET', 'PRD')]:
            item.start()
            self.addCleanup(item.stop)

    def get(self, path, results=(), user='owner', params=None):
        client = FakeClient(results)
        event = {'rawPath': path, 'requestContext': {'http': {'method': 'GET'}, 'authorizer': {'jwt': {'claims': {'sub': user} if user else {}}}},
                 'queryStringParameters': params, 'headers': {}, 'body': json.dumps({})}
        with patch.object(self.api, '_forecast_bigquery_client', return_value=(client, FakeBigQuery)) as factory:
            result = self.api.lambda_handler(event, None)
        self.factory, self.client = factory, client
        return result['statusCode'], json.loads(result['body'])

    def test_session_and_owner_are_required_before_bigquery_is_touched(self):
        for path in PATHS:
            for user, code in [(None, 401), ('other', 403)]:
                status, _ = self.get(path, user=user)
                self.assertEqual(status, code, f'{path} as {user}')
                self.factory.assert_not_called()
                self.assertEqual(self.client.calls, [])

    def test_transactions_mask_card_numbers_and_report_the_total(self):
        row = {'fecha': None, 'comercio': 'Shell', 'monto_total': 1500.5, 'divisa': 'ARS', 'extraido_de': 'bank_payments',
               'nro_tarjeta': '4509953566234321', 'transaction_key': 'k1', '_total': 7}
        status, body = self.get('/v1/finance/data/transactions', [[row]], params={'source': 'bank_payments', 'limit': '10'})
        self.assertEqual(status, 200)
        self.assertEqual(body['rows'][0]['nro_tarjeta'], '••••4321')
        self.assertEqual((body['total'], body['limit'], body['offset']), (7, 10, 0))
        sql, config = self.client.calls[0]
        self.assertNotIn('bank_payments', sql)
        self.assertEqual(config.kwargs['maximum_bytes_billed'], self.api.DATA_MAX_BYTES)
        self.assertIn(('source', 'STRING', 'bank_payments'), config.kwargs['query_parameters'])

    def test_empty_first_page_reports_zero_total_and_later_pages_unknown(self):
        self.assertEqual(self.get('/v1/finance/data/transactions', [[]])[1]['total'], 0)
        self.assertIsNone(self.get('/v1/finance/data/transactions', [[]], params={'offset': '50'})[1]['total'])

    def test_invalid_filters_return_400_without_querying(self):
        status, body = self.get('/v1/finance/data/transactions', params={'sort': 'nro_tarjeta; DROP'})
        self.assertEqual(status, 400)
        self.assertEqual(self.client.calls, [])
        self.assertIn('sort', body['error'])

    def test_bigquery_failures_are_503_and_do_not_leak_details(self):
        status, body = self.get('/v1/finance/data/transactions', [RuntimeError('secret internal detail')])
        self.assertEqual(status, 503)
        self.assertNotIn('secret internal detail', json.dumps(body))

    def test_preview_rejects_tables_that_are_not_in_the_catalog(self):
        objects = [{'name': 'mp_data', 'type': 'BASE TABLE', 'rows_n': 10, 'size_bytes': 1, 'modified': None}]
        columns = [{'table_name': 'mp_data', 'column_name': 'SOURCE_ID', 'data_type': 'STRING', 'ordinal_position': 1}]
        status, body = self.get('/v1/finance/data/preview', [objects, columns], params={'name': 'secretos'})
        self.assertEqual(status, 400)
        self.assertIn('no existe', body['error'])
        self.assertEqual(len(self.client.calls), 2)  # only the catalog lookups, never the requested table

    def test_preview_returns_rows_with_sensitive_columns_masked(self):
        objects = [{'name': 'bank_transfers', 'type': 'BASE TABLE', 'rows_n': 16, 'size_bytes': 1, 'modified': None}]
        columns = [{'table_name': 'bank_transfers', 'column_name': c, 'data_type': 'STRING', 'ordinal_position': i}
                   for i, c in enumerate(['destinatario', 'cbu_destino'])]
        rows = [{'destinatario': 'Juan', 'cbu_destino': '0170099220000012345678'}]
        status, body = self.get('/v1/finance/data/preview', [objects, columns, rows], params={'name': 'bank_transfers'})
        self.assertEqual(status, 200)
        self.assertEqual(body['rows'], [{'destinatario': 'Juan', 'cbu_destino': '••••5678'}])
        self.assertEqual(body['rows_total'], 16)

    def test_sources_include_flags_and_categories(self):
        sources = [{'source': 'carrefour_data', 'currency': 'ARS', 'rows_n': 3, 'total': 10.0, 'invalid_dates': 3,
                    'first_date': None, 'last_date': None, 'last_ingested_at': None}]
        status, body = self.get('/v1/finance/data/sources', [sources, [{'category': 'Supermercado', 'rows_n': 3}]])
        self.assertEqual(status, 200)
        self.assertEqual(body['sources'][0]['flags'], [{'kind': 'invalid_dates', 'count': 3}])
        self.assertEqual(body['categories'], [{'name': 'Supermercado', 'rows': 3}])

    def test_files_screen_survives_a_missing_table(self):
        summary = [{'n': 115, 'first_at': None, 'last_at': None}]
        status, body = self.get('/v1/finance/data/files', [summary, RuntimeError('table not found'), [{'ref': 'a.jpg', 'detail': 'Coto'}], []])
        self.assertEqual(status, 200)
        self.assertEqual(body['errors'], ['batches'])
        self.assertEqual(body['batches']['summary']['n'], 115)
        self.assertEqual(body['receipts'][0]['ref'], 'a.jpg')


if __name__ == '__main__':
    unittest.main()
