from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from domain import simplify_debts, split_expense


class DomainTests(unittest.TestCase):
    def test_equal_split_preserves_total_with_rounding(self):
        result = split_expense("100", ["zoe", "ana", "beto"], "equal")
        self.assertEqual(sum(result.values()), Decimal("100.00"))
        self.assertEqual(result, {"ana": Decimal("33.33"), "beto": Decimal("33.33"), "zoe": Decimal("33.34")})


    def test_amount_split_requires_exact_total(self):
        with self.assertRaisesRegex(ValueError, "sumar"):
            split_expense("10", ["a", "b"], "amount", {"a": "4", "b": "5"})


    def test_simplify_debts_nets_group_balances(self):
        payments = simplify_debts({"ana": "20", "beto": "-5", "zoe": "-15"})
        self.assertEqual(payments, [{"from_user_id": "zoe", "to_user_id": "ana", "amount": "15.00"}, {"from_user_id": "beto", "to_user_id": "ana", "amount": "5.00"}])
