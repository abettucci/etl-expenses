#!/usr/bin/env python3
"""Read-only exporter for the legacy SplitBot DynamoDB single table.

The output is deliberately a neutral snapshot consumed by migrate_splitbot.py.
This script never mutates the source table and should write its output outside
Git because it can contain personal names and financial records.
"""
from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


def json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported value: {type(value).__name__}")


def empty_snapshot() -> dict[str, list[dict[str, Any]]]:
    return {key: [] for key in ("groups", "members", "expenses", "splits", "payments", "gift_lists", "gift_list_links", "gift_items", "contributions", "reminders")}


def normalize_item(item: dict[str, Any], output: dict[str, list[dict[str, Any]]]) -> None:
    pk, sk = str(item.get("PK", "")), str(item.get("SK", ""))
    if pk.startswith("GROUP#") and sk == "METADATA":
        output["groups"].append({"id": str(item.get("chat_id") or item.get("ChatID") or pk.removeprefix("GROUP#")), "title": item.get("title") or item.get("Title"), "created_at": item.get("created_at") or item.get("CreatedAt")})
    elif pk.startswith("GROUP#") and sk.startswith("MEMBER#"):
        output["members"].append({"id": item.get("id"), "group_id": str(item.get("chat_id") or pk.removeprefix("GROUP#")), "telegram_user_id": item.get("user_id"), "display_name": item.get("display_name"), "joined_at": item.get("joined_at")})
    elif pk.startswith("GROUP#") and sk.startswith("GIFT_LIST#"):
        output["gift_list_links"].append({"group_id": str(item.get("chat_id") or pk.removeprefix("GROUP#")), "gift_list_id": item.get("gift_list_id") or sk.removeprefix("GIFT_LIST#"), "share_code": item.get("share_code"), "name": item.get("gift_list_name"), "created_at": item.get("linked_at")})
    elif pk.startswith("GROUP#") and sk.startswith("EXPENSE#"):
        output["expenses"].append({"id": item.get("id") or sk.removeprefix("EXPENSE#"), "group_id": str(item.get("chat_id") or pk.removeprefix("GROUP#")), "description": item.get("description"), "total_amount": item.get("total_amount"), "currency": item.get("currency"), "created_by": item.get("created_by"), "split_type": item.get("split_type"), "category": item.get("category"), "notes": item.get("notes"), "payers": item.get("payers", []), "items": item.get("items", []), "is_deleted": item.get("is_deleted", False), "deleted_at": item.get("deleted_at"), "created_at": item.get("created_at")})
    elif pk.startswith("EXPENSE#") and sk.startswith("SPLIT#"):
        output["splits"].append({"expense_id": item.get("expense_id") or pk.removeprefix("EXPENSE#"), "telegram_user_id": item.get("user_id"), "amount": item.get("amount"), "is_paid": item.get("is_paid", False), "paid_at": item.get("paid_at"), "owed_to": item.get("owed_to")})
    elif pk.startswith("GIFT_LIST#") and sk == "METADATA":
        output["gift_lists"].append({"id": item.get("id") or pk.removeprefix("GIFT_LIST#"), "name": item.get("name"), "share_code": item.get("share_code"), "created_by": item.get("created_by"), "created_by_name": item.get("created_by_name"), "created_at": item.get("created_at")})
    elif pk.startswith("GIFT_LIST#") and sk.startswith("GIFT#"):
        output["gift_items"].append({"id": item.get("id") or sk.removeprefix("GIFT#"), "gift_list_id": item.get("gift_list_id") or pk.removeprefix("GIFT_LIST#"), "description": item.get("description"), "target_amount": item.get("target_amount"), "created_by": item.get("created_by"), "created_at": item.get("created_at")})
    elif pk.startswith("GIFT_LIST#") and sk.startswith("CONTRIBUTION#"):
        output["contributions"].append({"id": item.get("id") or sk.rsplit("#", 1)[-1], "gift_list_id": item.get("gift_list_id") or pk.removeprefix("GIFT_LIST#"), "gift_item_id": item.get("gift_item_id"), "amount": item.get("amount"), "contributor_id": item.get("contributor_id"), "contributor_name": item.get("contributor_name"), "created_at": item.get("created_at")})
    elif pk.startswith("USER#") and sk.startswith("REMINDER#"):
        output["reminders"].append({"id": item.get("id") or sk.removeprefix("REMINDER#"), "telegram_user_id": item.get("user_id") or pk.removeprefix("USER#"), "description": item.get("description"), "amount": item.get("amount"), "payee_name": item.get("payee_name"), "reminder_type": item.get("reminder_type"), "total_installments": item.get("total_installments"), "current_installment": item.get("current_installment"), "frequency": item.get("frequency"), "recurrence_rule": item.get("recurrence_rule"), "is_active": item.get("is_active", True), "is_recurring": item.get("is_recurring", False), "next_reminder_date": item.get("next_reminder_date"), "start_date": item.get("start_date"), "end_date": item.get("end_date"), "created_at": item.get("created_at")})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--region", default=None)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    import boto3
    table = boto3.resource("dynamodb", region_name=args.region).Table(args.source_table)
    snapshot = empty_snapshot()
    kwargs: dict[str, Any] = {}
    while True:
        page = table.scan(**kwargs)
        for item in page.get("Items", []):
            normalize_item(item, snapshot)
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    args.output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    print(json.dumps({"output": str(args.output), **{key: len(value) for key, value in snapshot.items()}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
