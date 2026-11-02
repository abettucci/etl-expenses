from datetime import date
from pathlib import Path
import importlib.util
import json
import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance_balance import build_balance, save_income, validate_income
from forecast import build_monthly_forecast


def fixture():
    return build_monthly_forecast([
        {'month_start': '2026-08-01', 'merchant': 'Synthetic A', 'amount': 80},
        {'month_start': '2026-09-01', 'merchant': 'Synthetic A', 'amount': 120},
        {'month_start': '2026-10-01', 'merchant': 'Synthetic A', 'amount': 30},
    ], as_of=date(2026, 10, 10), lookback_months=3)


class BalanceTests(unittest.TestCase):
    def test_actuals_not_projections_and_deficit(self):
        result = build_balance(fixture(), [{'month': '2026-08', 'amount': '100.00'}, {'month': '2026-09', 'amount': '100.00'}])
        august, september, current = result['months'][-3:]
        self.assertEqual((august['savings'], august['savings_rate']), (20, 20))
        self.assertEqual((september['savings'], september['savings_rate']), (-20, -20))
        self.assertTrue(current['partial'])
        self.assertEqual(current['expenses'], 30)
        self.assertIsNone(current['income'])
        self.assertIsNone(current['savings'])

    def test_missing_expenses_are_not_assumed_zero(self):
        row = build_balance(fixture(), [{'month': '2026-07', 'amount': '100'}])['months'][0]
        self.assertIsNone(row['expenses'])
        self.assertIsNone(row['savings'])

    def test_zero_income_deficit_and_undefined_rate(self):
        row = build_balance(fixture(), [{'month': '2026-09', 'amount': '0'}])['months'][-2]
        self.assertEqual(row['savings'], -120)
        self.assertIsNone(row['savings_rate'])

    def test_validation_before_writes(self):
        table = Mock()
        common = dict(user_id='owner', enabled=True, owner_sub='owner', month='2026-09', today=date(2026, 10, 10), updated_at='synthetic')
        for amount in ['NaN', 'Infinity', '-1', '1e3', '12.345', '1,00', True, 20, '9' * 40]:
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                save_income(table, payload={'amount': amount, 'currency': 'ARS'}, **common)
        with self.assertRaises(ValueError):
            save_income(table, payload={'amount': '100', 'currency': 'ARS', 'user_id': 'victim'}, **common)
        table.put_item.assert_not_called()

    def test_owner_guard_and_disabled_precede_storage(self):
        table = Mock()
        for user, enabled, owner in [('other', True, 'owner'), ('owner', False, 'owner'), ('owner', True, '')]:
            with self.assertRaises(PermissionError):
                save_income(table, user_id=user, enabled=enabled, owner_sub=owner, month='2026-09', payload={'amount': '100', 'currency': 'ARS'}, today=date(2026, 10, 1), updated_at='synthetic')
        table.put_item.assert_not_called()

    def test_total_is_replaced_at_identity_scoped_key(self):
        table = Mock()
        for amount in ['100', '120.50']:
            save_income(table, user_id='owner', enabled=True, owner_sub='owner', month='2026-09', payload={'amount': amount, 'currency': 'ARS'}, today=date(2026, 10, 1), updated_at='synthetic')
        item = table.put_item.call_args.kwargs['Item']
        self.assertEqual((item['pk'], item['sk'], item['amount']), ('USER#owner', 'FINANCE_INCOME#2026-09', '120.50'))

    def test_future_invalid_month_and_currency(self):
        for month in ['2026-13', '2026-1', '2026-09#PROFILE', '2026-11']:
            with self.assertRaises(ValueError):
                validate_income(month, {'amount': '100', 'currency': 'ARS'}, today=date(2026, 10, 1))
        with self.assertRaises(ValueError):
            validate_income('2026-09', {'amount': '100', 'currency': 'USD'}, today=date(2026, 10, 1))


class ApiBalanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.dict(os.environ, {'APP_TABLE': 'synthetic-test', 'MP_SECRET_ARN': ''}), patch('boto3.resource'):
            spec = importlib.util.spec_from_file_location('balance_api_test', Path(__file__).resolve().parents[1] / 'lambda_function.py')
            cls.api = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.api)

    def setUp(self):
        self.table = Mock()
        self.patches = [patch.object(self.api, 'table', self.table), patch.object(self.api, 'ensure_profile', return_value={}),
                        patch.object(self.api, 'FORECAST_ENABLED', True), patch.object(self.api, 'FORECAST_OWNER_SUB', 'owner')]
        for item in self.patches:
            item.start(); self.addCleanup(item.stop)

    def event(self, method, path, body=None, user='owner'):
        return {'rawPath': path, 'requestContext': {'http': {'method': method}, 'authorizer': {'jwt': {'claims': {'sub': user} if user else {}}}}, 'body': json.dumps(body or {})}

    def test_income_route_requires_session_and_owner(self):
        payload = {'amount': '100', 'currency': 'ARS'}
        for user, code in [(None, 401), ('other', 403)]:
            result = self.api.lambda_handler(self.event('PUT', '/v1/finance/income/2026-09', payload, user), None)
            self.assertEqual(result['statusCode'], code)
        self.table.put_item.assert_not_called()

    def test_put_and_get_routes_and_scoped_query(self):
        result = self.api.lambda_handler(self.event('PUT', '/v1/finance/income/2026-09', {'amount': '100', 'currency': 'ARS'}), None)
        self.assertEqual(result['statusCode'], 200)
        self.table.query.return_value = {'Items': [{'sk': 'FINANCE_INCOME#2026-09', 'amount': '100'}]}
        with patch.object(self.api, 'finance_forecast', return_value=fixture()):
            result = self.api.lambda_handler(self.event('GET', '/v1/finance/balance'), None)
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(json.loads(result['body'])['months'][-2]['savings'], -20)
        expression = self.table.query.call_args.kwargs['KeyConditionExpression'].get_expression()
        self.assertEqual(expression['values'][0].get_expression()['values'][1], 'USER#owner')

    def test_balance_denies_warehouse_access_for_other_user(self):
        with patch.object(self.api, '_forecast_bigquery_client') as client:
            result = self.api.lambda_handler(self.event('GET', '/v1/finance/balance', user='other'), None)
        self.assertEqual(result['statusCode'], 403)
        client.assert_not_called(); self.table.query.assert_not_called()

    def test_storage_error_does_not_expose_sensitive_details(self):
        self.table.put_item.side_effect = RuntimeError('financial-details-do-not-expose')
        with patch('builtins.print') as log:
            result = self.api.lambda_handler(self.event('PUT', '/v1/finance/income/2026-09', {'amount': '100', 'currency': 'ARS'}), None)
        self.assertEqual(result['statusCode'], 503)
        self.assertNotIn('financial-details', result['body'])
        self.assertNotIn('financial-details', str(log.call_args_list))

    def test_image_includes_balance_module(self):
        dockerfile = (Path(__file__).resolve().parents[1] / 'app_api.dockerfile').read_text()
        self.assertIn('finance_balance.py', dockerfile)
