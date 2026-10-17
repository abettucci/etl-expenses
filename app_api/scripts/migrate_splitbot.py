#!/usr/bin/env python3
"""Validate or apply a sanitized SplitBot export to the Divi application table.

Default mode is dry-run.  ``--apply`` is intentionally explicit and requires
``APP_TABLE``; no source system is ever mutated by this command.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from splitbot_migration import transform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path, help="Sanitized SplitBot export JSON")
    parser.add_argument("--apply", action="store_true", help="Write transformed items to APP_TABLE")
    args = parser.parse_args()
    snapshot = json.loads(args.input.read_text(encoding="utf-8"))
    items = transform(snapshot, datetime.now(timezone.utc).isoformat())
    summary = {"items": len(items), "groups": len(snapshot.get("groups", [])), "mode": "apply" if args.apply else "dry_run"}
    if not args.apply:
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    table_name = os.environ.get("APP_TABLE", "")
    if not table_name:
        raise SystemExit("APP_TABLE es obligatorio con --apply")
    import boto3
    table = boto3.resource("dynamodb").Table(table_name)
    # Source keys make replays deterministic: do not overwrite an already
    # migrated entity. BatchWriter cannot express conditional writes, so use
    # individual puts with an explicit condition.
    skipped = 0
    from botocore.exceptions import ClientError
    for item in items:
        try:
            table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            skipped += 1
    summary["skipped_existing"] = skipped
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
