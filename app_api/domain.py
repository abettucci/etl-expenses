"""Pure business rules for the Divi application API.

Keeping money maths outside the Lambda handler makes the most sensitive rules
deterministic and straightforward to test.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import re
from typing import Iterable


CENT = Decimal("0.01")


def money(value: object) -> Decimal:
    amount = Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)
    if amount < 0:
        raise ValueError("El monto no puede ser negativo")
    return amount


def parse_amount_text(value: object) -> Decimal:
    """Parse common ARS input forms such as 12500,50 or 12.500,50."""
    text = str(value).strip().replace("$", "").replace(" ", "")
    text = re.sub(r"(?i)ARS", "", text)
    if not text:
        raise ValueError("El monto está vacío")
    if "," in text and "." in text:
        whole, fraction = text.rsplit(",", 1)
        if not re.fullmatch(r"\d{1,3}(?:\.\d{3})*", whole) or not fraction.isdigit():
            raise ValueError("Formato de monto inválido")
        text = whole.replace(".", "") + "." + fraction
    elif "," in text:
        if text.count(",") != 1:
            raise ValueError("Formato de monto inválido")
        text = text.replace(",", ".")
    elif text.count(".") > 1:
        if not re.fullmatch(r"\d{1,3}(?:\.\d{3})+", text):
            raise ValueError("Formato de monto inválido")
        text = text.replace(".", "")
    elif re.fullmatch(r"\d{1,3}\.\d{3}", text):
        text = text.replace(".", "")
    return money(text)


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


def normalize_payers(total: object, payers: list[dict[str, object]]) -> list[dict[str, str]]:
    """Validate payer contributions and return a deterministic money-safe form.

    The legacy SplitBot model allowed more than one payer.  Keeping this rule in
    the pure domain module prevents a Lambda, a migration and a future bot from
    implementing subtly different arithmetic.
    """
    expected = money(total)
    if not isinstance(payers, list) or not payers:
        raise ValueError("Debe indicarse al menos un pagador")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for payer in payers:
        if not isinstance(payer, dict):
            raise ValueError("El pagador es inválido")
        user_id = str(payer.get("user_id", "")).strip()
        if not user_id or user_id in seen:
            raise ValueError("Los pagadores deben ser únicos")
        seen.add(user_id)
        amount = money(payer.get("amount", 0))
        if amount <= 0:
            raise ValueError("Los aportes de pagadores deben ser positivos")
        result.append({"user_id": user_id, "amount": str(amount)})
    if sum((Decimal(row["amount"]) for row in result), Decimal("0")) != expected:
        raise ValueError("Los aportes de pagadores deben sumar el total")
    return sorted(result, key=lambda row: row["user_id"])


def expense_allocations(
    amount: object,
    participants: list[str],
    method: str,
    values: dict[str, object] | None = None,
) -> dict[str, Decimal]:
    """Alias with a ledger-oriented name used by the group expense API."""
    if method == "exact":
        method = "amount"
    return split_expense(amount, participants, method, values)


def allocations_from_items(items: list[dict[str, object]]) -> dict[str, Decimal]:
    """Split each item equally among its selected people and total per person."""
    allocations: dict[str, Decimal] = {}
    if not items:
        raise ValueError("Debe agregarse al menos un ítem")
    for item in items:
        people = item.get("participants") or item.get("shared_by") or []
        if not isinstance(people, list) or not people:
            raise ValueError("Cada ítem debe tener participantes")
        amount = money(item.get("amount", 0))
        shares = split_expense(amount, [str(person) for person in people], "equal")
        for user_id, share in shares.items():
            allocations[user_id] = allocations.get(user_id, Decimal("0")) + share
    return {user_id: amount.quantize(CENT, rounding=ROUND_HALF_UP) for user_id, amount in allocations.items()}


def replace_ledger_user_id(item: dict[str, object], old_user_id: str, new_user_id: str) -> dict[str, object]:
    """Replace one participant ID in known ledger fields, including map keys."""
    updated = dict(item)
    for field in ("paid_by", "created_by", "legacy_created_by", "from_user_id", "to_user_id", "debtor_user_id"):
        if updated.get(field) == old_user_id:
            updated[field] = new_user_id
    for field in ("payers", "items"):
        rows = updated.get(field)
        if isinstance(rows, list):
            copied = []
            for row in rows:
                if not isinstance(row, dict):
                    copied.append(row)
                    continue
                new_row = dict(row)
                for key in ("user_id", "paid_by", "contributor_user_id"):
                    if new_row.get(key) == old_user_id:
                        new_row[key] = new_user_id
                for key in ("shared_by", "participants"):
                    if isinstance(new_row.get(key), list):
                        new_row[key] = [new_user_id if user_id == old_user_id else user_id for user_id in new_row[key]]
                copied.append(new_row)
            updated[field] = copied
    for field in ("allocations", "shares"):
        mapping = updated.get(field)
        if isinstance(mapping, dict) and old_user_id in mapping:
            copied = dict(mapping)
            value = copied.pop(old_user_id)
            if new_user_id in copied:
                try:
                    copied[new_user_id] = str(Decimal(str(copied[new_user_id])) + Decimal(str(value)))
                except Exception as exc:
                    raise ValueError("No se pudo consolidar el saldo del miembro vinculado") from exc
            else:
                copied[new_user_id] = value
            updated[field] = copied
    return updated


def group_balances(
    expenses: Iterable[dict[str, object]],
    payments: Iterable[dict[str, object]] = (),
) -> dict[str, Decimal]:
    """Calculate balances from append-only expenses and settlement payments.

    Old Divi expenses (``paid_by``/``shares``) remain readable while new
    expenses use ``payers``/``allocations``.  This is deliberate compatibility,
    not a silent rewrite of already-recorded records.
    """
    balances: dict[str, Decimal] = {}

    def add(user_id: str, value: Decimal) -> None:
        balances[user_id] = balances.get(user_id, Decimal("0")) + value

    for expense in expenses:
        if expense.get("deleted_at"):
            continue
        payers = expense.get("payers")
        allocations = expense.get("allocations")
        if isinstance(payers, list) and isinstance(allocations, dict):
            for payer in payers:
                if isinstance(payer, dict):
                    add(str(payer["user_id"]), Decimal(str(payer["amount"])))
            for user_id, share in allocations.items():
                add(str(user_id), -Decimal(str(share)))
            continue
        # Compatibility with records written by the original Divi API.
        paid_by = expense.get("paid_by")
        if paid_by:
            add(str(paid_by), Decimal(str(expense.get("amount", "0"))))
        for user_id, share in (expense.get("shares") or {}).items():
            add(str(user_id), -Decimal(str(share)))

    for payment in payments:
        if payment.get("voided_at"):
            continue
        amount = money(payment.get("amount", 0))
        debtor = str(payment.get("from_user_id", ""))
        creditor = str(payment.get("to_user_id", ""))
        if debtor and creditor and debtor != creditor:
            add(debtor, amount)
            add(creditor, -amount)
    return {user_id: value.quantize(CENT, rounding=ROUND_HALF_UP) for user_id, value in balances.items()}
