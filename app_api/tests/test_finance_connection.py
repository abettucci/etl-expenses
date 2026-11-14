import hashlib
import json
from unittest.mock import patch
from test_finance_balance import ApiBalanceTests


class FinanceConnectionTests(ApiBalanceTests):
    def test_link_uses_authenticated_actor_and_stores_only_hash(self):
        with patch.object(self.api, 'FORECAST_OWNER_SUB', ''):
            self.table.get_item.return_value = {}
            result = self.api.lambda_handler(self.event('POST', '/v1/finance/connection', {}, user='owner'), None)
        self.assertEqual(result['statusCode'], 201)
        code = json.loads(result['body'])['code']
        item = self.table.put_item.call_args.kwargs['Item']
        self.assertEqual(item['user_id'], 'owner')
        self.assertEqual(item['pk'], 'FINANCE_LINK#' + hashlib.sha256(code.encode()).hexdigest())
        self.assertNotIn(code, str(item))

    def test_actor_cannot_choose_another_owner_or_overwrite_existing_one(self):
        with patch.object(self.api, 'FORECAST_OWNER_SUB', ''):
            self.table.get_item.return_value = {}
            result = self.api.lambda_handler(self.event('POST', '/v1/finance/connection', {'user_id': 'victim'}), None)
            self.assertEqual(result['statusCode'], 400)
            self.table.put_item.assert_not_called()
            self.table.get_item.return_value = {'Item': {'user_id': 'victim'}}
            result = self.api.lambda_handler(self.event('POST', '/v1/finance/connection'), None)
            self.assertEqual(result['statusCode'], 409)
            self.table.put_item.assert_not_called()

    def test_only_verified_owner_gets_access(self):
        with patch.object(self.api, 'FORECAST_OWNER_SUB', ''):
            self.table.get_item.return_value = {'Item': {'user_id': 'owner'}}
            self.assertTrue(self.api.profile_overview({'user_id': 'owner'})['finance_linked'])
            self.assertFalse(self.api.profile_overview({'user_id': 'other'})['finance_linked'])
            self.assertTrue(self.table.get_item.call_args.kwargs['ConsistentRead'])
