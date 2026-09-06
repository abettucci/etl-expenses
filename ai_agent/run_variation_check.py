#!/usr/bin/env python3
"""Manually trigger the expense-variation check (weekly + monthly) for testing.

This reuses run_expense_variation_alert() exactly as EventBridge calls it in
production (action=alert_variations_weekly / alert_variations_monthly) — same
BigQuery query, same configurable thresholds (alert_variation_settings), same
DynamoDB idempotency (expense_variation_alerts) and same Telegram/SNS delivery.
Running this script twice for the same period is safe: alerts already marked
as delivered in DynamoDB are skipped, not resent.

Required environment variables (same ones the Lambda has via Terraform):
  TELEGRAM_BOT_TOKEN, TELEGRAM_ALERT_CHAT_ID, GCP_PROJECT_ID,
  and valid AWS credentials (Secrets Manager: gcp_sa_api_credentials,
  DynamoDB: expense_variation_alerts). ALERT_SNS_TOPIC_ARN is optional.

Usage:
  python3 run_variation_check.py            # both weekly and monthly
  python3 run_variation_check.py --kind weekly
  python3 run_variation_check.py --kind monthly
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import lambda_function as lf  # noqa: E402  (needs sys.path set up first)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kind", choices=["weekly", "monthly", "both"], default="both")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    kinds = ["weekly", "monthly"] if args.kind == "both" else [args.kind]
    for kind in kinds:
        print(f"--- {kind} ---")
        result = lf.run_expense_variation_alert(kind, {}, None)
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
