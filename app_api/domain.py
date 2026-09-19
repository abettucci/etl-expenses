"""Pure business rules for the Divi application API.

Keeping money maths outside the Lambda handler makes the most sensitive rules
deterministic and straightforward to test.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable


CENT = Decimal("0.01")


def money(value: object) -> Decimal:
    amount = Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)
    if amount < 0:
        raise ValueError("El monto no puede ser negativo")
    return amount


def split_expense(amount: object, participants: list[str], method: str, values: dict[str, object] | None = None) -> dict[str, Decimal]:
    """Return exact shares; the last deterministic recipient gets rounding cents."""
    total = money(amount)
    if not participants or len(set(participants)) != len(participants):
        raise ValueError("Los participantes deben ser únicos")
    values = values or {}
    ordered = sorted(participants)

    if method == "equal":
        base = (total / len(ordered)).quantize(CENT, rounding=ROUND_HALF_UP)
        result = {user_id: base for user_id in ordered}
        result[ordered[-1]] += total - sum(result.values())
        return result
    if method == "shares":
        weights = {user_id: Decimal(str(values.get(user_id, 0))) for user_id in ordered}
        if any(weight <= 0 for weight in weights.values()) or sum(weights.values()) <= 0:
            raise ValueError("Las partes deben ser positivas")
        return _proportional(total, weights)
    if method == "amount":
        result = {user_id: money(values.get(user_id, 0)) for user_id in ordered}
        if sum(result.values()) != total:
            raise ValueError("Los montos deben sumar el total")
        return result
    if method == "percentage":
        weights = {user_id: Decimal(str(values.get(user_id, 0))) for user_id in ordered}
        if sum(weights.values()) != Decimal("100") or any(weight < 0 for weight in weights.values()):
            raise ValueError("Los porcentajes deben sumar 100")
        return _proportional(total, weights)
    raise ValueError("Método de división inválido")


def _proportional(total: Decimal, weights: dict[str, Decimal]) -> dict[str, Decimal]:
    divisor = sum(weights.values())
    result = {user_id: (total * weight / divisor).quantize(CENT, rounding=ROUND_HALF_UP) for user_id, weight in weights.items()}
    result[sorted(result)[-1]] += total - sum(result.values())
    return result


def simplify_debts(balances: dict[str, object]) -> list[dict[str, object]]:
    """Turn positive/negative balances into the minimal greedy settlement set."""
    normalized = {user: Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP) for user, value in balances.items()}
    creditors = [[user, value] for user, value in normalized.items() if value > 0]
    debtors = [[user, -value] for user, value in normalized.items() if value < 0]
    creditors.sort(key=lambda row: row[1], reverse=True)
    debtors.sort(key=lambda row: row[1], reverse=True)
    settlements: list[dict[str, object]] = []
    while creditors and debtors:
        creditor, due = creditors[0]
        debtor, owed = debtors[0]
        paid = min(due, owed)
        settlements.append({"from_user_id": debtor, "to_user_id": creditor, "amount": str(paid)})
        creditors[0][1] -= paid
        debtors[0][1] -= paid
        if creditors[0][1] == 0:
            creditors.pop(0)
        if debtors[0][1] == 0:
            debtors.pop(0)
    return settlements
