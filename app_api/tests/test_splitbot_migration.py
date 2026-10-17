from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from splitbot_migration import transform
from scripts.export_splitbot import empty_snapshot, normalize_item


class SplitBotMigrationTests(unittest.TestCase):
    def test_transform_creates_pending_members_and_exact_ledger_rows(self):
        snapshot = {
            "groups": [{"id": "77", "title": "Asado"}],
            "members": [
                {"id": "m1", "group_id": "77", "telegram_user_id": 1, "display_name": "Ana"},
                {"id": "m2", "group_id": "77", "telegram_user_id": 2, "display_name": "Beto"},
            ],
            "expenses": [{"id": "e1", "group_id": "77", "description": "Cena", "total_amount": 100, "created_by": 1}],
            "splits": [{"expense_id": "e1", "telegram_user_id": 1, "amount": 50}, {"expense_id": "e1", "telegram_user_id": 2, "amount": 50}],
            "gift_lists": [{"id": "g1", "name": "Cumple"}],
            "gift_items": [{"id": "i1", "gift_list_id": "g1", "description": "Libro"}],
            "contributions": [{"id": "c1", "gift_list_id": "g1", "gift_item_id": "i1", "amount": 500}],
        }
        items = transform(snapshot, "2026-10-13T00:00:00+00:00")
        expense = next(item for item in items if item.get("expense_id") == "legacy-e1")
        pending = [item for item in items if item.get("claim_state") == "pending"]
        contribution = next(item for item in items if item.get("entity") == "recorded_gift_contribution")
        self.assertEqual(expense["amount"], "100.00")
        self.assertEqual(sum(map(float, expense["allocations"].values())), 100.0)
        self.assertEqual(len(pending), 2)
        self.assertEqual(contribution["payment_status"], "RECORDED_LEGACY")

    def test_transform_is_deterministic_for_the_same_snapshot(self):
        snapshot = {"groups": [{"id": "1", "title": "Grupo"}]}
        self.assertEqual(transform(snapshot, "t"), transform(snapshot, "t"))

    def test_paid_split_without_known_creditor_is_preserved_for_review(self):
        snapshot = {
            "groups": [{"id": "77"}],
            "members": [{"group_id": "77", "telegram_user_id": 1}],
            "expenses": [{"id": "e1", "group_id": "77", "total_amount": 100, "created_by": 1,
                          "payers": [{"user_id": 1}, {"user_id": 2}]}],
            "splits": [{"expense_id": "e1", "telegram_user_id": 1, "amount": 50, "is_paid": True}],
        }
        items = transform(snapshot, "t")
        review = next(item for item in items if item.get("entity") == "legacy_settlement_review")
        self.assertEqual(review["amount"], "50.00")
        self.assertEqual(review["reason"], "Creditor could not be determined from legacy SplitBot data")
        self.assertFalse(any(item.get("entity") == "settlement_payment" for item in items))

    def test_legacy_export_normalizes_only_known_entities(self):
        snapshot = empty_snapshot()
        self.assertIn("gift_list_links", snapshot)
        normalize_item({"PK": "GROUP#77", "SK": "METADATA", "ChatID": 77, "Title": "Asado"}, snapshot)
        normalize_item({"PK": "GROUP#77", "SK": "EXPENSE#e1", "ID": "e1", "ChatID": 77, "TotalAmount": "25.5"}, snapshot)
        normalize_item({"PK": "UNKNOWN", "SK": "UNKNOWN"}, snapshot)
        self.assertEqual(snapshot["groups"], [{"id": "77", "title": "Asado", "created_at": None}])
        self.assertEqual(snapshot["expenses"][0]["id"], "e1")
