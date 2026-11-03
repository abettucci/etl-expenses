import time
import json
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from test_finance_balance import ApiBalanceTests


class TelegramOnboardingTests(ApiBalanceTests):
    def setUp(self):
        super().setUp()
        self.api.dynamodb.meta.client.transact_write_items = Mock()

    def link(self):
        return {'pk': 'IDENTITY_LINK#SYNTHETIC', 'sk': 'META', 'user_id': 'owner', 'ttl': int(time.time()) + 900}

    def test_creation_uses_authenticated_actor_and_does_not_audit_code(self):
        with patch.object(self.api, 'put_audit') as audit:
            result = self.api.create_identity_link('owner')
        self.assertEqual(len(result['code']), 48)
        self.assertEqual(result['expires_in_minutes'], 15)
        item = self.table.put_item.call_args.kwargs['Item']
        self.assertEqual(item['user_id'], 'owner')
        self.assertNotIn(result['code'], str(audit.call_args))

    def test_api_cannot_choose_a_different_actor(self):
        result = self.api.lambda_handler(self.event('POST', '/v1/identity-links', {'user_id': 'someone-else'}), None)
        self.assertEqual(result['statusCode'], 201)
        stored = [call.kwargs['Item'] for call in self.table.put_item.call_args_list if call.kwargs['Item']['pk'].startswith('IDENTITY_LINK#')]
        self.assertEqual(stored[0]['user_id'], 'owner')

    def test_public_chat_never_claims_and_private_errors_are_explained(self):
        for chat_type in ['group', 'private']:
            event = {'headers': {'x-telegram-bot-api-secret-token': 'synthetic-test-secret'}, 'body': json.dumps({'message': {'text': '/vincular SYNTHETIC', 'from': {'id': 123}, 'chat': {'id': 123, 'type': chat_type}}})}
            with patch.object(self.api, 'group_bot_config', return_value={'webhook_secret': 'synthetic-test-secret'}), patch.object(self.api, 'claim_telegram_identity', side_effect=self.api.ApiError(400, 'El código venció')) as claim, patch.object(self.api, '_group_bot_reply') as reply:
                result = self.api.handle_group_bot_webhook(event)
            self.assertEqual(result['statusCode'], 200)
            if chat_type == 'group':
                claim.assert_not_called(); self.assertIn('privado', reply.call_args.args[1])
            else:
                claim.assert_called_once(); self.assertEqual(reply.call_args.args[1], 'El código venció')

    def test_no_legacy_groups_can_link_atomically(self):
        self.table.query.return_value = {'Items': []}
        with patch.object(self.api, 'get_item', side_effect=[self.link(), None]), patch.object(self.api, 'put_audit'):
            result = self.api.claim_telegram_identity('SYNTHETIC', 'telegram-synthetic')
        self.assertEqual(result, {'linked_groups': 0, 'user_id': 'owner'})
        operations = self.api.dynamodb.meta.client.transact_write_items.call_args.kwargs['TransactItems']
        self.assertEqual(len(operations), 2)
        self.assertIn('attribute_not_exists(user_id)', operations[0]['Put']['ConditionExpression'])
        self.assertIn('#ttl > :now', operations[1]['Delete']['ConditionExpression'])
        self.table.put_item.assert_not_called()

    def test_expired_code_is_rejected_even_if_dynamo_ttl_has_not_deleted_it(self):
        expired = {**self.link(), 'ttl': int(time.time()) - 1}
        with patch.object(self.api, 'get_item', return_value=expired):
            with self.assertRaises(self.api.ApiError) as raised:
                self.api.claim_telegram_identity('SYNTHETIC', 'telegram-synthetic')
        self.assertEqual(raised.exception.status, 400)
        self.table.query.assert_not_called()
        self.api.dynamodb.meta.client.transact_write_items.assert_not_called()

    def test_cannot_rebind_telegram_to_another_web_account(self):
        with patch.object(self.api, 'get_item', side_effect=[self.link(), {'user_id': 'someone-else'}]):
            with self.assertRaises(self.api.ApiError) as raised:
                self.api.claim_telegram_identity('SYNTHETIC', 'telegram-synthetic')
        self.assertEqual(raised.exception.status, 409)
        self.api.dynamodb.meta.client.transact_write_items.assert_not_called()

    def test_code_consumed_in_parallel_fails_without_partial_write(self):
        self.table.query.return_value = {'Items': []}
        self.api.dynamodb.meta.client.transact_write_items.side_effect = ClientError({'Error': {'Code': 'TransactionCanceledException'}}, 'TransactWriteItems')
        with patch.object(self.api, 'get_item', side_effect=[self.link(), None]), patch.object(self.api, 'put_audit') as audit:
            with self.assertRaises(self.api.ApiError) as raised:
                self.api.claim_telegram_identity('SYNTHETIC', 'telegram-synthetic')
        self.assertEqual(raised.exception.status, 409)
        audit.assert_not_called()
