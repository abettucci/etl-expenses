"""Idempotent, offline-friendly transformation of legacy SplitBot exports.

This module intentionally has no AWS client.  It converts a sanitized JSON
snapshot into Divi single-table items, so validation can run locally before a
privileged writer ever touches the production application table.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


CENT = Decimal("0.01")
SOURCE = "splitbot"


def legacy_key(kind: str, value: object) -> str:
    return f"{SOURCE}:{kind}:{value}"


def as_money(value: object) -> str:
    return str(Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP))


def external_member_id(telegram_id: object) -> str:
    return f"external:telegram:{telegram_id}"


def transform(snapshot: dict[str, Any], migrated_at: str) -> list[dict[str, Any]]:
    """Map documented SplitBot export records to additive Divi entities.

    Input uses neutral arrays (groups, members, expenses, splits, payments,
    gift_lists, gift_items, contributions, reminders).  Missing optional
    arrays are valid. Every output record carries ``legacy_source_key`` for
    replay-safe writes and auditability.
    """
    items: list[dict[str, Any]] = []
    members_by_group: dict[str, dict[str, str]] = {}

    for group in snapshot.get("groups", []):
        group_id = f"legacy-{group['id']}"
        source_key = legacy_key("group", group["id"])
        items.append({
            "pk": f"GROUP#{group_id}", "sk": "META", "entity": "group",
            "group_id": group_id, "name": group.get("title") or "Grupo migrado",
            "currency": group.get("currency") or "ARS", "legacy_source_key": source_key,
            "migration_state": "migrated", "created_at": group.get("created_at", migrated_at),
            "migrated_at": migrated_at,
        })
        items.append({
            "pk": f"TELEGRAM_GROUP#{group['id']}", "sk": "META", "entity": "telegram_group_link",
            "group_id": group_id, "legacy_chat_id": str(group["id"]), "legacy_source_key": source_key,
            "migrated_at": migrated_at,
        })

    for member in snapshot.get("members", []):
        legacy_group = str(member["group_id"])
        group_id = f"legacy-{legacy_group}"
        external_id = external_member_id(member["telegram_user_id"])
        members_by_group.setdefault(legacy_group, {})[str(member["telegram_user_id"])] = external_id
        source_key = legacy_key("member", member.get("id", f"{legacy_group}:{member['telegram_user_id']}"))
        items.extend([
            {
                "pk": f"GROUP#{group_id}", "sk": f"MEMBER#{external_id}", "entity": "member",
                "group_id": group_id, "user_id": external_id, "external_identity": f"telegram:{member['telegram_user_id']}",
                "display_name": member.get("display_name") or "Miembro pendiente", "role": "member",
                "active": False, "claim_state": "pending", "legacy_source_key": source_key,
                "created_at": member.get("joined_at", migrated_at), "migrated_at": migrated_at,
            },
            {
                "pk": f"EXTERNAL#TELEGRAM#{member['telegram_user_id']}", "sk": f"GROUP#{group_id}",
                "entity": "external_membership", "group_id": group_id, "member_key": external_id,
                "legacy_source_key": source_key, "migrated_at": migrated_at,
            },
        ])

    splits_by_expense: dict[str, list[dict[str, Any]]] = {}
    for split in snapshot.get("splits", []):
        splits_by_expense.setdefault(str(split["expense_id"]), []).append(split)

    for expense in snapshot.get("expenses", []):
        legacy_group = str(expense["group_id"])
        group_id = f"legacy-{legacy_group}"
        expense_id = f"legacy-{expense['id']}"
        legacy_members = members_by_group.get(legacy_group, {})
        payer_id = str(expense.get("created_by", ""))
        payer = legacy_members.get(payer_id, external_member_id(payer_id))
        total = as_money(expense.get("total_amount", 0))
        allocations: dict[str, str] = {}
        for split in splits_by_expense.get(str(expense["id"]), []):
            user_id = legacy_members.get(str(split["telegram_user_id"]), external_member_id(split["telegram_user_id"]))
            allocations[user_id] = as_money(split.get("amount", 0))
        raw_payers = expense.get("payers") or [{"user_id": payer_id, "amount": total}]
        migrated_payers = [
            {"user_id": legacy_members.get(str(row.get("user_id")), external_member_id(row.get("user_id"))), "amount": as_money(row.get("amount", 0))}
            for row in raw_payers
        ]
        legacy_items = []
        for raw_item in expense.get("items", []):
            legacy_items.append({
                "description": raw_item.get("description", ""), "amount": as_money(raw_item.get("amount", 0)),
                "paid_by": legacy_members.get(str(raw_item.get("paid_by")), external_member_id(raw_item.get("paid_by"))),
                "shared_by": [legacy_members.get(str(user_id), external_member_id(user_id)) for user_id in raw_item.get("shared_by", [])],
            })
        mapped_method = expense.get("split_type") or "equal"
        if mapped_method not in {"equal", "exact", "shares", "amount", "percentage", "items"}:
            mapped_method = "exact"
        migrated_expense = {
            "pk": f"GROUP#{group_id}", "sk": f"EXPENSE#{expense_id}", "entity": "expense",
            "group_id": group_id, "expense_id": expense_id, "description": expense.get("description") or "Gasto migrado",
            "amount": total, "currency": expense.get("currency") or "ARS", "payers": migrated_payers,
            "allocations": allocations, "split_method": mapped_method, "items": legacy_items,
            "category": expense.get("category") or "Otro", "notes": expense.get("notes", ""),
            "legacy_source_key": legacy_key("expense", expense["id"]), "legacy_created_by": payer,
            "created_at": expense.get("created_at", migrated_at), "migrated_at": migrated_at,
        }
        if expense.get("is_deleted"):
            migrated_expense["deleted_at"] = expense.get("deleted_at") or migrated_at
        items.append(migrated_expense)

    # SplitBot records paid debt directly on each expense split. Preserve the
    # settlement effect as an explicit historical payment where the creditor
    # can be determined, while retaining paid-state evidence otherwise.
    expenses_by_id = {str(expense["id"]): expense for expense in snapshot.get("expenses", [])}
    for split in snapshot.get("splits", []):
        if not split.get("is_paid"):
            continue
        expense = expenses_by_id.get(str(split["expense_id"]))
        if not expense:
            continue
        debt_user_raw = str(split.get("telegram_user_id"))
        payer_rows = expense.get("payers") or [{"user_id": expense.get("created_by")}]
        creditor_raw = str(split.get("owed_to") or (payer_rows[0].get("user_id") if len(payer_rows) == 1 else ""))
        if not creditor_raw or creditor_raw == debt_user_raw:
            group_id = f"legacy-{expense['group_id']}"
            items.append({
                "pk": f"GROUP#{group_id}", "sk": f"LEGACY_PAYMENT_REVIEW#{split['expense_id']}:{debt_user_raw}",
                "entity": "legacy_settlement_review", "group_id": group_id,
                "expense_id": f"legacy-{split['expense_id']}",
                "debtor_user_id": members_by_group.get(str(expense["group_id"]), {}).get(debt_user_raw, external_member_id(debt_user_raw)),
                "amount": as_money(split.get("amount", 0)), "currency": expense.get("currency") or "ARS",
                "reason": "Creditor could not be determined from legacy SplitBot data",
                "legacy_source_key": legacy_key("paid_split_review", f"{split['expense_id']}:{debt_user_raw}"),
                "created_at": split.get("paid_at") or migrated_at, "migrated_at": migrated_at,
            })
            continue
        group_id = f"legacy-{expense['group_id']}"
        debt_user = members_by_group.get(str(expense["group_id"]), {}).get(debt_user_raw, external_member_id(debt_user_raw))
        creditor = members_by_group.get(str(expense["group_id"]), {}).get(creditor_raw, external_member_id(creditor_raw))
        payment_id = f"legacy-paid-{split['expense_id']}-{debt_user_raw}"
        items.append({
            "pk": f"GROUP#{group_id}", "sk": f"SETTLEMENT#{payment_id}", "entity": "settlement_payment",
            "payment_id": payment_id, "group_id": group_id, "from_user_id": debt_user, "to_user_id": creditor,
            "amount": as_money(split.get("amount", 0)), "currency": expense.get("currency") or "ARS",
            "payment_status": "RECORDED_LEGACY", "legacy_source_key": legacy_key("paid_split", f"{split['expense_id']}:{debt_user_raw}"),
            "created_at": split.get("paid_at") or migrated_at, "migrated_at": migrated_at,
        })

    for payment in snapshot.get("payments", []):
        group_id = f"legacy-{payment['group_id']}"
        members = members_by_group.get(str(payment["group_id"]), {})
        items.append({
            "pk": f"GROUP#{group_id}", "sk": f"SETTLEMENT#{payment['id']}", "entity": "settlement_payment",
            "payment_id": f"legacy-{payment['id']}", "group_id": group_id,
            "from_user_id": members.get(str(payment["from_telegram_user_id"]), external_member_id(payment["from_telegram_user_id"])),
            "to_user_id": members.get(str(payment["to_telegram_user_id"]), external_member_id(payment["to_telegram_user_id"])),
            "amount": as_money(payment["amount"]), "currency": payment.get("currency") or "ARS",
            "legacy_source_key": legacy_key("payment", payment["id"]), "created_at": payment.get("paid_at", migrated_at), "migrated_at": migrated_at,
        })

    # Generic SplitBot gift lists remain distinct from birthday wishlists.
    for gift_list in snapshot.get("gift_lists", []):
        list_id = f"legacy-{gift_list['id']}"
        items.append({
            "pk": f"GIFT_LIST#{list_id}", "sk": "META", "entity": "shared_gift_list", "gift_list_id": list_id,
            "name": gift_list.get("name") or "Lista migrada", "share_code": gift_list.get("share_code", ""),
            "legacy_source_key": legacy_key("gift_list", gift_list["id"]), "created_at": gift_list.get("created_at", migrated_at), "migrated_at": migrated_at,
        })
    for gift in snapshot.get("gift_items", []):
        list_id = f"legacy-{gift['gift_list_id']}"
        items.append({
            "pk": f"GIFT_LIST#{list_id}", "sk": f"ITEM#{gift['id']}", "entity": "shared_gift_item", "gift_item_id": f"legacy-{gift['id']}",
            "gift_list_id": list_id, "title": gift.get("description") or "Regalo", "target_amount": as_money(gift.get("target_amount", 0)),
            "legacy_source_key": legacy_key("gift_item", gift["id"]), "created_at": gift.get("created_at", migrated_at), "migrated_at": migrated_at,
        })
    for contribution in snapshot.get("contributions", []):
        list_id = f"legacy-{contribution['gift_list_id']}"
        items.append({
            "pk": f"GIFT_LIST#{list_id}", "sk": f"CONTRIBUTION#{contribution['id']}", "entity": "recorded_gift_contribution",
            "gift_list_id": list_id, "gift_item_id": f"legacy-{contribution['gift_item_id']}", "amount": as_money(contribution["amount"]),
            "contributor_user_id": external_member_id(contribution.get("contributor_id", "unknown")),
            "contributor_name": contribution.get("contributor_name", "Miembro pendiente"),
            "payment_status": "RECORDED_LEGACY", "legacy_source_key": legacy_key("contribution", contribution["id"]),
            "created_at": contribution.get("created_at", migrated_at), "migrated_at": migrated_at,
        })
    for link in snapshot.get("gift_list_links", []):
        list_id, group_id = f"legacy-{link['gift_list_id']}", f"legacy-{link['group_id']}"
        items.append({
            "pk": f"GROUP#{group_id}", "sk": f"GIFT_LIST#{list_id}", "entity": "shared_gift_list_link",
            "group_id": group_id, "gift_list_id": list_id, "share_code": link.get("share_code", ""),
            "name": link.get("name", "Lista migrada"), "legacy_source_key": legacy_key("gift_list_link", f"{link['group_id']}:{link['gift_list_id']}"),
            "created_at": link.get("created_at", migrated_at), "migrated_at": migrated_at,
        })
    for reminder in snapshot.get("reminders", []):
        user_id = external_member_id(reminder.get("telegram_user_id", "unknown"))
        reminder_id = f"legacy-{reminder['id']}"
        items.append({
            "pk": f"USER#{user_id}", "sk": f"REMINDER#{reminder_id}", "entity": "reminder",
            "reminder_id": reminder_id, "user_id": user_id, "description": reminder.get("description", "Recordatorio migrado"),
            "amount": as_money(reminder.get("amount", 0)), "payee_name": reminder.get("payee_name", ""),
            "reminder_type": reminder.get("reminder_type", "payment"), "total_installments": reminder.get("total_installments", 1),
            "current_installment": reminder.get("current_installment", 1), "frequency": reminder.get("frequency", "once"),
            "recurrence_rule": reminder.get("recurrence_rule", ""), "is_active": reminder.get("is_active", False),
            "is_recurring": reminder.get("is_recurring", False), "next_reminder_date": reminder.get("next_reminder_date", ""),
            "start_date": reminder.get("start_date", ""), "end_date": reminder.get("end_date", ""),
            "legacy_source_key": legacy_key("reminder", reminder["id"]), "created_at": reminder.get("created_at", migrated_at), "migrated_at": migrated_at,
        })
    return items
