"""Sincroniza USD/ARS oficial vendedor del BCRA y completa gastos en ARS."""

from __future__ import annotations

import json
import os
from datetime import date, timedelta

import boto3
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from fx_utils import FX_SOURCE, parse_bcra_records

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_PROD = os.environ.get("BQ_DATASET_PROD", "PRD")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
FX_TABLE = os.environ.get("FX_TABLE", "fx_usd_ars_bcra")
SECRET_ID = os.environ.get("GCP_SERVICE_ACCOUNT_SECRET_ID", "gcp_sa_api_credentials")
BCRA_URL = "https://api.bcra.gob.ar/estadisticas/v4.0/Monetarias/4"

# Cada fecha usa la última cotización publicada anterior o igual a la compra.
EXPENSE_TABLES = (
    ("bank_payments", "MONTO", "DIVISA", "SAFE.PARSE_DATE('%d/%m/%Y', CAST(target.`FECHA_PAGO` AS STRING))"),
    ("bank_transfers", "IMPORTE", "DIVISA", "COALESCE(SAFE_CAST(SUBSTR(CAST(target.`DATE` AS STRING), 1, 10) AS DATE), SAFE.PARSE_DATE('%d/%m/%Y', CAST(target.`DATE` AS STRING)))"),
    ("supermarket_receipts", "total_amount", "currency", "COALESCE(SAFE_CAST(SUBSTR(CAST(target.`transaction_date` AS STRING), 1, 10) AS DATE), SAFE.PARSE_DATE('%d/%m/%Y', CAST(target.`transaction_date` AS STRING)))"),
)


def get_bigquery_client() -> bigquery.Client:
    secret = boto3.client("secretsmanager").get_secret_value(SecretId=SECRET_ID)
    info = json.loads(secret["SecretString"])
    credentials = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID, location=BQ_LOCATION)


def fetch_bcra_rates(start: date, end: date) -> list[dict]:
    response = requests.get(BCRA_URL, params={"desde": start.isoformat(), "hasta": end.isoformat()}, timeout=20)
    response.raise_for_status()
    rows = parse_bcra_records(response.json())
    if not rows:
        raise RuntimeError("El BCRA respondió sin cotizaciones válidas para el período solicitado")
    return rows


def ensure_fx_table(client: bigquery.Client) -> str:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{FX_TABLE}"
    table = bigquery.Table(table_id, schema=[
        bigquery.SchemaField("fecha_cotizacion", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("tipo_cambio_ars", "NUMERIC", mode="REQUIRED"),
        bigquery.SchemaField("fuente", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("serie", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("actualizado_en", "TIMESTAMP", mode="REQUIRED"),
    ])
    table.time_partitioning = bigquery.TimePartitioning(field="fecha_cotizacion")
    client.create_table(table, exists_ok=True)
    return table_id


def upsert_rates(client: bigquery.Client, rows: list[dict]) -> int:
    table_id = ensure_fx_table(client)
    staging_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}._stg_{FX_TABLE}"
    try:
        client.load_table_from_json(rows, staging_id, job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("fecha_cotizacion", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("tipo_cambio_ars", "NUMERIC", mode="REQUIRED"),
                bigquery.SchemaField("fuente", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("serie", "STRING", mode="REQUIRED"),
            ],
        )).result()
        client.query(f"""
          MERGE `{table_id}` target USING `{staging_id}` source
          ON target.fecha_cotizacion = source.fecha_cotizacion
          WHEN MATCHED THEN UPDATE SET tipo_cambio_ars = source.tipo_cambio_ars,
            fuente = source.fuente, serie = source.serie, actualizado_en = CURRENT_TIMESTAMP()
          WHEN NOT MATCHED THEN INSERT (fecha_cotizacion, tipo_cambio_ars, fuente, serie, actualizado_en)
          VALUES (source.fecha_cotizacion, source.tipo_cambio_ars, source.fuente, source.serie, CURRENT_TIMESTAMP())
        """).result()
    finally:
        client.delete_table(staging_id, not_found_ok=True)
    return len(rows)


def _table_columns(client: bigquery.Client, table_id: str) -> set[str]:
    return {field.name.lower() for field in client.get_table(table_id).schema}


def ensure_conversion_columns(client: bigquery.Client, table_id: str) -> None:
    existing = _table_columns(client, table_id)
    for column, type_name in {
        "tipo_cambio_ars": "NUMERIC", "fecha_tipo_cambio": "DATE",
        "monto_ars": "NUMERIC", "fuente_tipo_cambio": "STRING",
    }.items():
        if column not in existing:
            client.query(f"ALTER TABLE `{table_id}` ADD COLUMN `{column}` {type_name}").result()


def backfill_expenses(client: bigquery.Client) -> dict:
    fx_table_id = ensure_fx_table(client)
    result = {"updated_tables": [], "skipped_tables": []}
    for table_name, amount_column, currency_column, date_expression in EXPENSE_TABLES:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_PROD}.{table_name}"
        try:
            columns = _table_columns(client, table_id)
        except Exception as exc:
            print(f"fx_backfill_skipped table={table_name} reason=missing_table error={exc}")
            result["skipped_tables"].append(table_name)
            continue
        if not {amount_column.lower(), currency_column.lower()}.issubset(columns):
            print(f"fx_backfill_skipped table={table_name} reason=missing_columns")
            result["skipped_tables"].append(table_name)
            continue
        ensure_conversion_columns(client, table_id)
        amount = f"SAFE_CAST(target.`{amount_column}` AS NUMERIC)"
        currency = f"UPPER(TRIM(CAST(target.`{currency_column}` AS STRING)))"
        latest_rate = f"(SELECT tipo_cambio_ars FROM `{fx_table_id}` fx WHERE fx.fecha_cotizacion <= {date_expression} ORDER BY fx.fecha_cotizacion DESC LIMIT 1)"
        latest_date = f"(SELECT fecha_cotizacion FROM `{fx_table_id}` fx WHERE fx.fecha_cotizacion <= {date_expression} ORDER BY fx.fecha_cotizacion DESC LIMIT 1)"
        job = client.query(f"""
          UPDATE `{table_id}` AS target SET
            tipo_cambio_ars = IF({currency} IN ('USD', 'U$S', 'US$'), {latest_rate}, NULL),
            fecha_tipo_cambio = IF({currency} IN ('USD', 'U$S', 'US$'), {latest_date}, NULL),
            monto_ars = IF({currency} IN ('USD', 'U$S', 'US$'), {amount} * {latest_rate}, {amount}),
            fuente_tipo_cambio = IF({currency} IN ('USD', 'U$S', 'US$'), '{FX_SOURCE}', 'ORIGINAL_ARS')
          WHERE {amount} IS NOT NULL
            AND ({currency} NOT IN ('USD', 'U$S', 'US$') OR {date_expression} IS NOT NULL)
            AND monto_ars IS NULL
        """)
        job.result()
        updated = job.num_dml_affected_rows or 0
        print(f"fx_backfill_completed table={table_name} updated={updated}")
        result["updated_tables"].append({"table": table_name, "updated": updated})
    return result


def lambda_handler(event, context):
    event = event or {}
    action = event.get("action", "sync_rates")
    today = date.today()
    start = date.fromisoformat(event.get("from", (today - timedelta(days=int(event.get("lookback_days", 7)))).isoformat()))
    end = date.fromisoformat(event.get("to", today.isoformat()))
    if start > end:
        raise ValueError("`from` no puede ser posterior a `to`")
    client = get_bigquery_client()
    rates_synced = upsert_rates(client, fetch_bcra_rates(start, end))
    response = {"action": action, "rates_synced": rates_synced, "from": start.isoformat(), "to": end.isoformat()}
    if action in ("backfill_expenses", "sync_and_backfill"):
        response["backfill"] = backfill_expenses(client)
    elif action != "sync_rates":
        raise ValueError("action debe ser `sync_rates`, `sync_and_backfill` o `backfill_expenses`")
    print(f"fx_sync_completed {json.dumps(response, ensure_ascii=False)}")
    return response
