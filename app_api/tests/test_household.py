import unittest
from unittest.mock import patch
from test_finance_balance import ApiBalanceTests


class HouseholdTests(ApiBalanceTests):
    def test_only_owner_can_change_group_kind(self):
        with patch.object(self.api, 'assert_member', return_value={'role': 'member'}):
            with self.assertRaises(self.api.ApiError) as exc:
                self.api.update_group_kind('group', 'member', {'kind': 'household'})
        self.assertEqual(exc.exception.status, 403)
        self.table.update_item.assert_not_called()
        with patch.object(self.api, 'assert_member', return_value={'role': 'owner'}), patch.object(self.api, 'put_audit'):
            self.table.update_item.return_value = {'Attributes': {'kind': 'household'}}
            self.assertEqual(self.api.update_group_kind('group', 'owner', {'kind': 'household'})['kind'], 'household')
            with self.assertRaises(self.api.ApiError):
                self.api.update_group_kind('group', 'owner', {'kind': 'other'})

    def test_list_reads_every_dynamo_page_after_membership_check(self):
        self.table.query.side_effect = [{'Items': [{'expense_id': 'one'}], 'LastEvaluatedKey': {'pk': 'x'}}, {'Items': [{'expense_id': 'two'}]}]
        with patch.object(self.api, 'assert_member') as member:
            result = self.api.list_group_expenses('group', 'owner')
        member.assert_called_once_with('group', 'owner')
        self.assertEqual([row['expense_id'] for row in result], ['one', 'two'])
        self.assertEqual(self.table.query.call_args.kwargs['ExclusiveStartKey'], {'pk': 'x'})

    def test_expense_preserves_date_and_payer_and_rejects_invalid_date(self):
        payload = {'description': 'supermercado', 'amount': '100.00', 'participants': ['owner', 'partner'], 'paid_by': 'partner', 'expense_date': '2026-10-01'}
        with patch.object(self.api, 'assert_member'), patch.object(self.api, '_expense_member_ids', return_value={'owner', 'partner'}), patch.object(self.api, '_active_member_ids', return_value={'owner', 'partner'}), patch.object(self.api, 'put_audit'):
            expense = self.api.create_expense('group', 'owner', payload)
            self.assertEqual(expense['expense_date'], '2026-10-01')
            self.assertEqual(expense['payers'][0]['user_id'], 'partner')
            self.assertEqual(expense['allocations']['owner'], '50.00')
            self.table.put_item.reset_mock()
            with self.assertRaises(self.api.ApiError):
                self.api.create_expense('group', 'owner', {**payload, 'expense_date': '2026-02-30'})
            self.table.put_item.assert_not_called()
