from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain import allocations_from_items, group_balances, normalize_payers, parse_amount_text, replace_ledger_user_id


class GroupLedgerTests(unittest.TestCase):
    def test_multiple_payers_and_payment_keep_exact_balances(self):
        payers = normalize_payers("100", [{"user_id": "ana", "amount": "60"}, {"user_id": "beto", "amount": "40"}])
        balances = group_balances(
            [{"payers": payers, "allocations": {"ana": "50.00", "beto": "50.00"}}],
            [{"from_user_id": "beto", "to_user_id": "ana", "amount": "10"}],
        )
        self.assertEqual(balances, {"ana": Decimal("0.00"), "beto": Decimal("0.00")})

    def test_payer_contributions_must_equal_total(self):
        with self.assertRaisesRegex(ValueError, "sumar"):
            normalize_payers("100", [{"user_id": "ana", "amount": "99"}])

    def test_legacy_expense_remains_in_balance_calculation(self):
        balances = group_balances([{"paid_by": "ana", "amount": "21", "shares": {"ana": "10.50", "beto": "10.50"}}])
        self.assertEqual(balances, {"ana": Decimal("10.50"), "beto": Decimal("-10.50")})

    def test_itemized_allocations_sum_each_item_share(self):
        allocations = allocations_from_items([
            {"amount": "30", "participants": ["ana", "beto"]},
            {"amount": "20", "shared_by": ["ana"]},
        ])
        self.assertEqual(allocations, {"ana": Decimal("35.00"), "beto": Decimal("15.00")})

    def test_itemized_allocation_rejects_item_without_participants(self):
        with self.assertRaisesRegex(ValueError, "Cada ítem"):
            allocations_from_items([{"amount": "10"}])

    def test_identity_claim_rewrites_ledger_ids_and_allocation_keys(self):
        rewritten = replace_ledger_user_id({
            "paid_by": "external:telegram:1",
            "allocations": {"external:telegram:1": "20.00", "user-1": "5.00"},
            "payers": [{"user_id": "external:telegram:1", "amount": "25.00"}],
            "items": [{"shared_by": ["external:telegram:1", "user-2"]}],
        }, "external:telegram:1", "user-1")
        self.assertEqual(rewritten["paid_by"], "user-1")
        self.assertEqual(rewritten["allocations"], {"user-1": "25.00"})
        self.assertEqual(rewritten["payers"][0]["user_id"], "user-1")
        self.assertEqual(rewritten["items"][0]["shared_by"], ["user-1", "user-2"])

    def test_parse_argentine_currency_amounts(self):
        self.assertEqual(parse_amount_text("12.500,50"), Decimal("12500.50"))
        self.assertEqual(parse_amount_text("12.500"), Decimal("12500.00"))
        self.assertEqual(parse_amount_text("12500,50"), Decimal("12500.50"))
