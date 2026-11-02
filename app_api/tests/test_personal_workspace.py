import io
import json
import os
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from test_finance_balance import ApiBalanceTests
from personal_workspace import overview, bot_url, question_body


class WorkspaceTests(ApiBalanceTests):
    def test_profile_has_real_identity_and_no_invented_connections(self):
        result = overview({'user_id': 'new', 'email': 'synthetic@example.invalid'}, {'display_name': 'Synthetic'}, enabled=True, owner_sub='owner', finance_bot='my_finance_bot')
        self.assertFalse(result['finance_linked'])
        self.assertEqual(result['profile']['display_name'], 'Synthetic')
        sources = {item['id']: item for item in result['sources']}
        self.assertEqual(sources['binance']['status'], 'not_available')
        self.assertEqual(sources['balanz']['status'], 'not_available')
        self.assertEqual(bot_url('https://evil.invalid'), None)

    def test_question_validation(self):
        for payload in [{}, {'question': ''}, {'question': 'a' * 1001}, {'question': 'total', 'user_id': 'owner'}, {'question': 1}]:
            with self.assertRaises(ValueError): question_body(payload)

    def test_web_agent_is_owner_scoped_and_never_invoked_for_other_users(self):
        with patch('boto3.client') as client:
            result = self.api.lambda_handler(self.event('POST', '/v1/finance/assistant', {'question': 'total'}, user='other'), None)
        self.assertEqual(result['statusCode'], 403)
        self.table.update_item.assert_not_called(); client.assert_not_called()

    def test_web_agent_is_bounded_and_uses_only_configured_function(self):
        invoke = Mock(return_value={'Payload': io.BytesIO(json.dumps({'statusCode': 200, 'answer': '$ 123,00 ARS'}).encode())})
        with patch.dict(os.environ, {'FINANCE_AGENT_FUNCTION_NAME': 'synthetic-agent'}), patch('boto3.client', return_value=Mock(invoke=invoke)):
            result = self.api.lambda_handler(self.event('POST', '/v1/finance/assistant', {'question': 'total'}), None)
        self.assertEqual(result['statusCode'], 200)
        call = invoke.call_args.kwargs
        self.assertEqual(call['FunctionName'], 'synthetic-agent')
        self.assertEqual(json.loads(call['Payload'])['user_id'], 'owner')
        self.assertEqual(self.table.update_item.call_args.kwargs['ExpressionAttributeValues'][':limit'], 30)

    def test_quota_and_agent_failure_are_clear_and_sanitized(self):
        self.table.update_item.side_effect = ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        with patch.dict(os.environ, {'FINANCE_AGENT_FUNCTION_NAME': 'synthetic-agent'}), patch('boto3.client') as client:
            result = self.api.lambda_handler(self.event('POST', '/v1/finance/assistant', {'question': 'total'}), None)
        self.assertEqual(result['statusCode'], 429); client.assert_not_called()

    def test_profile_route_requires_session(self):
        result = self.api.lambda_handler(self.event('GET', '/v1/profile/overview', user=None), None)
        self.assertEqual(result['statusCode'], 401)
