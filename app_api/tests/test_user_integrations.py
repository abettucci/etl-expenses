import base64
import hashlib
import json
import time
import unittest
from datetime import date
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import user_integrations as ui
from test_finance_balance import ApiBalanceTests


class UserIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.table = Mock()
        self.user = 'synthetic-user-a'
        self.config = ({'client_id': 'synthetic-client', 'client_secret': 'synthetic-secret'}, 'https://example.invalid/callback')

    def test_gmail_state_binds_user_and_uses_pkce(self):
        with patch.object(ui, '_config', return_value=self.config):
            result = ui.start_gmail(self.table, self.user)
        query = parse_qs(urlparse(result['authorization_url']).query)
        item = self.table.put_item.call_args.kwargs['Item']
        self.assertEqual(item['user_id'], self.user)
        self.assertEqual(item['pk'], 'GMAIL_OAUTH#' + hashlib.sha256(query['state'][0].encode()).hexdigest())
        self.assertNotIn(query['state'][0], str(item))
        self.assertEqual(query['code_challenge_method'], ['S256'])
        expected = base64.urlsafe_b64encode(hashlib.sha256(item['verifier'].encode()).digest()).decode().rstrip('=')
        self.assertEqual(query['code_challenge'], [expected])

    def test_callback_rejects_expired_state_without_exchange(self):
        from botocore.exceptions import ClientError
        self.table.delete_item.side_effect = ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'DeleteItem')
        with patch.object(ui, '_config', return_value=self.config), patch.object(ui.requests, 'post') as exchange:
            with self.assertRaises(ui.IntegrationError):
                ui.finish_gmail(self.table, self.user, 'A' * 43, 'code')
        exchange.assert_not_called()

    def test_callback_saves_only_refresh_token_in_secret_and_owner_partition(self):
        self.table.delete_item.return_value = {'Attributes': {'user_id': self.user, 'verifier': 'V' * 64}}
        self.table.get_item.return_value = {}
        response = Mock(status_code=200); response.json.return_value = {'refresh_token': 'synthetic-refresh', 'access_token': 'synthetic-access', 'scope': ui.GMAIL_SCOPE}
        profile = Mock(status_code=200); profile.json.return_value = {'emailAddress': 'test@example.invalid'}
        with patch.object(ui, '_config', return_value=self.config), patch.object(ui.requests, 'post', return_value=response), patch.object(ui.requests, 'get', return_value=profile), patch.object(ui, '_new_secret', return_value='arn:synthetic') as store:
            ui.finish_gmail(self.table, self.user, 'A' * 43, 'code')
        item = self.table.put_item.call_args.kwargs['Item']
        self.assertEqual(item['pk'], 'USER#' + self.user)
        self.assertEqual(item['secret_arn'], 'arn:synthetic')
        self.assertNotIn('synthetic-refresh', str(item))
        self.assertEqual(store.call_args.args[1], {'refresh_token': 'synthetic-refresh'})
        self.assertIn('#ttl > :now', self.table.delete_item.call_args.kwargs['ConditionExpression'])
        self.assertEqual(self.table.delete_item.call_args.kwargs['ExpressionAttributeValues'][':user'], self.user)

    def test_callback_cannot_claim_a_state_belonging_to_another_user(self):
        self.table.delete_item.return_value = {'Attributes': {'user_id': 'other-user', 'verifier': 'V' * 64}}
        with patch.object(ui, '_config', return_value=self.config), patch.object(ui.requests, 'post') as exchange:
            with self.assertRaises(ui.IntegrationError):
                ui.finish_gmail(self.table, self.user, 'A' * 43, 'code')
        exchange.assert_not_called()

    def test_gmail_sync_writes_only_authenticated_user_partition(self):
        self.table.get_item.return_value = {'Item': {'secret_arn': 'arn:synthetic', 'account_hash': 'a' * 64}}
        html = '<p>Monto</p><p>$ 100,00</p><p>Fecha</p><p>10/11/2026</p><p>Comercio</p><p>Ejemplo</p>'
        encoded = base64.urlsafe_b64encode(html.encode()).decode()
        listing = Mock(status_code=200); listing.json.return_value = {'messages': [{'id': 'msg1'}]}
        detail = Mock(status_code=200); detail.json.return_value = {'id': 'msg1', 'payload': {'headers': [{'name': 'From', 'value': 'mensajesyavisos@mails.santander.com.ar'}, {'name': 'Subject', 'value': 'Pagaste'}], 'mimeType': 'text/html', 'body': {'data': encoded}}}
        with patch.object(ui, '_gmail_access_token', return_value='synthetic-token'), patch.object(ui.requests, 'get', side_effect=[listing, detail]):
            self.assertEqual(ui.sync_gmail(self.table, self.user)['imported'], 1)
        item = self.table.put_item.call_args.kwargs['Item']
        self.assertEqual(item['pk'], 'USER#' + self.user)
        self.assertEqual(item['sk'], 'GMAIL_EXPENSE#' + 'a' * 64 + '#msg1')
        self.assertNotIn(html, json.dumps(item))
        self.assertEqual(self.table.update_item.call_args.kwargs['ExpressionAttributeValues'][':arn'], 'arn:synthetic')

    def test_gmail_expenses_are_scoped_to_current_account_and_user(self):
        self.table.get_item.return_value = {'Item': {'secret_arn': 'arn:synthetic', 'account_hash': 'b' * 64}}
        self.table.query.return_value = {'Items': []}
        self.assertEqual(ui.gmail_expenses(self.table, self.user), [])
        condition = self.table.query.call_args.kwargs['KeyConditionExpression']
        self.assertIn('b' * 64, str(condition._values[1]._values))
        self.assertNotIn('a' * 64, str(condition._values[1]._values))

    def test_binance_rejects_trading_keys_before_secret_storage(self):
        restrictions = {'enableReading': True, 'enableWithdrawals': False, 'enableSpotAndMarginTrading': True}
        with patch.object(ui, '_binance_get', return_value=restrictions), patch.object(ui, '_new_secret') as store:
            with self.assertRaises(ui.IntegrationError):
                ui.connect_binance(self.table, self.user, {'api_key': 'A' * 32, 'api_secret': 'B' * 32})
        store.assert_not_called(); self.table.put_item.assert_not_called()

    def test_binance_success_stores_secret_reference_only_under_owner(self):
        restrictions = {'enableReading': True, 'enableWithdrawals': False, 'enableSpotAndMarginTrading': False}
        self.table.get_item.return_value = {}
        with patch.object(ui, '_binance_get', return_value=restrictions), patch.object(ui, '_new_secret', return_value='arn:synthetic'):
            result = ui.connect_binance(self.table, self.user, {'api_key': 'A' * 32, 'api_secret': 'B' * 32})
        self.assertEqual(result, {'connected': True})
        item = self.table.put_item.call_args.kwargs['Item']
        self.assertEqual(item['pk'], 'USER#' + self.user)
        self.assertNotIn('A' * 32, str(item)); self.assertNotIn('B' * 32, str(item))

    def test_binance_movements_read_only_and_owner_scoped(self):
        self.table.get_item.return_value = {'Item': {'secret_arn': 'arn:alice'}}
        restrictions = {'enableReading': True, 'enableWithdrawals': False, 'enableSpotAndMarginTrading': False}
        rows = [{'coin': 'BTC', 'amount': '0.1000', 'insertTime': 1700000000000, 'address': 'private-address'}]
        with patch.object(ui, '_read_secret', return_value={'api_key': 'key', 'api_secret': 'secret'}), patch.object(ui, '_binance_get', side_effect=[restrictions, rows]):
            result = ui.binance_movements(self.table, self.user, 'deposit', 0)
        self.assertEqual(self.table.get_item.call_args.kwargs['Key']['pk'], 'USER#' + self.user)
        self.assertEqual(result['items'][0]['amount'], '0.1000')
        self.assertNotIn('private-address', str(result))

    def test_trade_query_validates_pair_and_discards_extra_provider_fields(self):
        self.table.get_item.return_value = {'Item': {'secret_arn': 'arn:alice'}}
        restrictions = {'enableReading': True, 'enableWithdrawals': False, 'enableSpotAndMarginTrading': False}
        rows = [{'qty': '0.01', 'price': '50000', 'time': 1700000000000, 'isBuyer': True, 'private': 'discard-me'}]
        with patch.object(ui, '_read_secret', return_value={'api_key': 'key', 'api_secret': 'secret'}), patch.object(ui, '_binance_get', side_effect=[restrictions, rows]):
            result = ui.binance_trades(self.table, self.user, 'BTCUSDT')
        self.assertEqual(result['items'][0]['side'], 'buy')
        self.assertNotIn('discard-me', str(result))
        with self.assertRaises(ui.IntegrationError):
            ui.binance_trades(self.table, self.user, '../bad')

    def test_disconnect_uses_only_caller_connection(self):
        self.table.get_item.return_value = {'Item': {'secret_arn': 'arn:alice'}}
        with patch.object(ui, '_delete_secret') as delete:
            ui.disconnect(self.table, self.user, 'binance')
        self.assertEqual(self.table.delete_item.call_args.kwargs['Key'], {'pk': 'USER#' + self.user, 'sk': 'CONNECTION#binance'})
        delete.assert_called_once_with('arn:alice')


class UserIntegrationRouteTests(ApiBalanceTests):
    def setUp(self):
        super().setUp()
        self.connections = Mock()
        connection_patch = patch.object(self.api, 'connections_table', self.connections)
        connection_patch.start()
        self.addCleanup(connection_patch.stop)

    def test_route_uses_jwt_actor_and_requires_authentication(self):
        with patch.object(self.api.user_integrations, 'status', return_value={'gmail': {}, 'binance': {}}) as status:
            result = self.api.lambda_handler(self.event('GET', '/v1/integrations/status', user='alice'), None)
            self.assertEqual(result['statusCode'], 200)
            self.assertIs(status.call_args.args[0], self.connections)
            self.assertEqual(status.call_args.args[1], 'alice')
            result = self.api.lambda_handler(self.event('GET', '/v1/integrations/status', user=None), None)
            self.assertEqual(result['statusCode'], 401)
            self.assertEqual(status.call_count, 1)

    def test_oauth_callback_never_links_without_authenticated_confirmation(self):
        event = {'rawPath': '/v1/integrations/gmail/callback', 'requestContext': {'http': {'method': 'GET'}}, 'queryStringParameters': {'state': 'A' * 43, 'code': 'synthetic-code'}}
        with patch.object(self.api.user_integrations, 'finish_gmail') as finish:
            result = self.api.lambda_handler(event, None)
        self.assertEqual(result['statusCode'], 303)
        self.assertIn('#gmail=confirm', result['headers']['location'])
        finish.assert_not_called()

    def test_gmail_balance_reads_authenticated_partition_without_legacy_bigquery(self):
        self.connections.get_item.return_value = {'Item': {'secret_arn': 'arn:alice', 'account_hash': 'a' * 64, 'last_sync_at': 1}}
        self.connections.query.return_value = {'Items': []}
        rows = [{'date': '2026-11-10', 'amount': '120.00', 'currency': 'ARS', 'merchant': 'Synthetic'}]
        with patch.object(self.api.user_integrations, 'gmail_expenses', return_value=rows) as expenses, patch.object(self.api, 'finance_forecast') as legacy:
            result = self.api.finance_balance({'user_id': 'alice'})
        self.assertEqual(expenses.call_args.args[1], 'alice')
        self.assertIs(expenses.call_args.args[0], self.connections)
        self.table.query.assert_not_called()
        legacy.assert_not_called()
        self.assertEqual(result['expense_source'], 'gmail_santander')
        self.assertIn('no se calcula ahorro', result['notice'])

    def test_gmail_balance_never_treats_partial_coverage_as_total_savings(self):
        from finance_balance import build_gmail_balance
        rows = [{'date': '2026-11-01', 'amount': '200.00', 'currency': 'ARS'}, {'date': '2026-11-02', 'amount': '99', 'currency': 'USD'}]
        report = build_gmail_balance(rows, [{'month': '2026-11', 'amount': '1000'}], today=date(2026, 11, 10))
        november = report['months'][-1]
        self.assertEqual(november['expenses'], 200)
        self.assertIsNone(november['savings'])
        self.assertTrue(november['partial'])
