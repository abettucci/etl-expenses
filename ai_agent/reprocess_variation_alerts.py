"""Invoke the explicit historical expense-variation reprocess Lambda action.

It defaults to a BigQuery-backed dry run. Pass --send only after reviewing
the JSON response: that creates idempotent Telegram/SNS deliveries.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import boto3


COMPARISONS = ("WEEK_VS_WEEK", "WEEK_VS_2_WEEKS", "WEEK_VS_1_MONTH")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reprocesa alertas históricas de gastos por semana (dry-run por defecto)."
    )
    parser.add_argument("--from", dest="reprocess_from", required=True, help="Inicio inclusive (YYYY-MM-DD).")
    parser.add_argument("--to", dest="reprocess_to", required=True, help="Fin inclusive (YYYY-MM-DD).")
    parser.add_argument(
        "--comparison",
        choices=COMPARISONS,
        action="append",
        help="Comparación a incluir; se puede repetir. Por defecto se ejecutan las tres.",
    )
    parser.add_argument("--send", action="store_true", help="Envía los resultados elegibles por Telegram/SNS.")
    parser.add_argument("--limit", type=int, default=50, help="Máximo de candidatos que devuelve BigQuery (1-500).")
    parser.add_argument(
        "--function-name",
        default=os.getenv("AI_AGENT_LAMBDA_NAME", "ai_agent"),
        help="Nombre de la Lambda (por defecto: ai_agent).",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", "us-east-2"),
        help="Región AWS (por defecto: AWS_REGION o us-east-2).",
    )
    parser.add_argument("--profile", help="Perfil de AWS CLI opcional.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = {
        "action": "alert_variations_reprocess",
        "from": args.reprocess_from,
        "to": args.reprocess_to,
        "comparisons": args.comparison or list(COMPARISONS),
        "send": args.send,
        "limit": args.limit,
    }
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    client = session.client("lambda", region_name=args.region)
    response = client.invoke(
        FunctionName=args.function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    raw_response = response["Payload"].read().decode("utf-8")
    try:
        result = json.loads(raw_response)
    except json.JSONDecodeError:
        print(raw_response)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if response.get("FunctionError") or result.get("statusCode", 200) >= 400 else 0


if __name__ == "__main__":
    sys.exit(main())
