import hashlib
import json
import os
import time
import unittest
from unittest.mock import Mock, patch
from test_voice_expense_flow import load_lambda_module


class FinanceLinkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agent = load_lambda_module()

    def setUp(self):
        self.code = 'A' * 48
        self.event = {'headers': {'x-telegram-bot-api-secret-token': 'secret'}}
        self.message = {'chat': {'id': 123, 'type': 'private'}, 'from': {'id': 123}}
        self.table = Mock()
        self.table.get_item.return_value = {'Item': {'user_id': 'owner', 'ttl': int(time.time()) + 60}}
        self.dynamo = Mock()
        self.dynamo.Table.return_value = self.table
        self.env = patch.dict(os.environ, {'FINANCE_LINK_TABLE': 'test-table', 'WEB_FINANCE_OWNER_SUB': ''})
        self.env.start(); self.addCleanup(self.env.stop)
        self.auth = patch.object(self.agent, '_telegram_webhook_is_authorized', return_value=True)
        self.auth.start(); self.addCleanup(self.auth.stop)
        self.resource = patch.object(self.agent.boto3, 'resource', return_value=self.dynamo)
        self.resource.start(); self.addCleanup(self.resource.stop)

    def test_claim_atomically_consumes_unexpired_challenge_and_locks_owner(self):
        reply = self.agent._claim_finance_link(self.event, self.message, self.code)
        self.assertIn('Historial vinculado', reply)
        key = self.table.get_item.call_args.kwargs['Key']['pk']
        self.assertEqual(key, 'FINANCE_LINK#' + hashlib.sha256(self.code.encode()).hexdigest())
        ops = self.dynamo.meta.client.transact_write_items.call_args.kwargs['TransactItems']
        self.assertIn('attribute_not_exists(pk)', ops[0]['Put']['ConditionExpression'])
        self.assertIn('#ttl > :now', ops[1]['Delete']['ConditionExpression'])

    def test_transaction_sends_native_python_values_not_pre_typed_attributes(self):
        # boto3's resource client serializes values itself; pre-typed values such
        # as {'S': ...} are nested as maps and DynamoDB rejects the transaction.
        self.agent._claim_finance_link(self.event, self.message, self.code)
        ops = self.dynamo.meta.client.transact_write_items.call_args.kwargs['TransactItems']
        self.assertEqual(ops[0]['Put']['Item']['user_id'], 'owner')
        self.assertEqual(ops[0]['Put']['ExpressionAttributeValues'], {':owner': 'owner'})
        self.assertEqual(ops[1]['Delete']['Key']['sk'], 'META')
        self.assertIsInstance(ops[1]['Delete']['ExpressionAttributeValues'][':now'], int)

    def test_failed_transaction_is_reported_and_logs_only_the_error_type(self):
        self.dynamo.meta.client.transact_write_items.side_effect = RuntimeError('detail with owner id')
        with patch('builtins.print') as log:
            reply = self.agent._claim_finance_link(self.event, self.message, self.code)
        self.assertIn('No se pudo completar', reply)
        logged = ' '.join(str(call) for call in log.call_args_list)
        self.assertIn('finance_link_claim_failed: RuntimeError', logged)
        self.assertNotIn('owner id', logged)

    def test_unauthorized_or_expired_code_never_grants_access(self):
        for msg in [{'chat': {'id': 123, 'type': 'group'}, 'from': {'id': 123}},
                    {'chat': {'id': 123, 'type': 'private'}, 'from': {'id': 456}}]:
            self.agent._claim_finance_link(self.event, msg, self.code)
        self.table.get_item.assert_not_called()
        self.table.get_item.return_value = {'Item': {'user_id': 'owner', 'ttl': int(time.time()) - 1}}
        self.assertIn('venció', self.agent._claim_finance_link(self.event, self.message, self.code))
        self.dynamo.meta.client.transact_write_items.assert_not_called()

    def test_invalid_webhook_and_existing_owner_cannot_claim(self):
        with patch.object(self.agent, '_telegram_webhook_is_authorized', return_value=False):
            self.agent._claim_finance_link(self.event, self.message, self.code)
        with patch.dict(os.environ, {'WEB_FINANCE_OWNER_SUB': 'different-owner'}):
            self.agent._claim_finance_link(self.event, self.message, self.code)
        self.table.get_item.assert_not_called()

    def test_bot_intercepts_code_before_message_logging_or_bigquery(self):
        payload = {'message': {**self.message, 'text': '/conectar_historial ' + self.code}}
        with patch.object(self.agent, '_claim_finance_link', return_value='ok'), patch.object(self.agent, 'send_telegram_message'), patch.object(self.agent, 'get_bigquery_client') as bq, patch('builtins.print') as log:
            self.agent.lambda_handler({**self.event, 'body': json.dumps(payload)}, None)
        bq.assert_not_called()
        self.assertNotIn(self.code, str(log.call_args_list))
