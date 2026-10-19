"""Authenticated API for Divi's collaborative expenses and birthday gifts.

It deliberately uses a transactional DynamoDB store rather than BigQuery: the
ETL warehouse remains analytical and never becomes a source of multi-tenant
application permissions.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import boto3
import requests
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from domain import allocations_from_items, expense_allocations, group_balances, money, normalize_payers, parse_amount_text, replace_ledger_user_id, simplify_debts
from forecast import build_monthly_forecast, monthly_expenses_query

TABLE_NAME = os.environ["APP_TABLE"]
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "http://localhost:5173")
MP_WEBHOOK_BASE_URL = os.environ.get("MP_WEBHOOK_BASE_URL", "")
FORECAST_ENABLED = os.environ.get("FINANCE_FORECAST_ENABLED", "false").lower() == "true"
FORECAST_OWNER_SUB = os.environ.get("FINANCE_FORECAST_OWNER_SUB", "").strip()
FORECAST_GCP_SECRET_ID = os.environ.get("FINANCE_FORECAST_GCP_SECRET_ID", "gcp_sa_api_credentials").strip()
FORECAST_GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip()
FORECAST_BQ_DATASET = os.environ.get("BQ_DATASET_PROD", "PRD").strip()
FORECAST_BQ_LOCATION = os.environ.get("BQ_LOCATION", "US").strip()
FORECAST_GASTOS_VIEW = os.environ.get("FINANCE_FORECAST_GASTOS_VIEW", "gastos_totales").strip()
FORECAST_MAPPING_TABLE = os.environ.get("FINANCE_FORECAST_MAPPING_TABLE", "dim_comercio_mapping").strip()

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def payment_secrets() -> dict[str, str]:
    """Load provider credentials once; no token is put in Lambda environment."""
    secret_arn = os.environ.get("MP_SECRET_ARN", "")
    if secret_arn:
        raw = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)["SecretString"]
        return json.loads(raw)
    # Local unit tests and explicit developer environments may use these values.
    return {"access_token": os.environ.get("MP_ACCESS_TOKEN", ""), "webhook_secret": os.environ.get("MP_WEBHOOK_SECRET", ""), "premium_plan_id": os.environ.get("MP_PREMIUM_PLAN_ID", ""), "oauth_client_id": os.environ.get("MP_OAUTH_CLIENT_ID", ""), "oauth_client_secret": os.environ.get("MP_OAUTH_CLIENT_SECRET", ""), "oauth_redirect_uri": os.environ.get("MP_OAUTH_REDIRECT_URI", "")}


MP = payment_secrets()
MP_ACCESS_TOKEN = MP.get("access_token", "")
MP_WEBHOOK_SECRET = MP.get("webhook_secret", "")
MP_PREMIUM_PLAN_ID = MP.get("premium_plan_id", "")
MP_OAUTH_CLIENT_ID = MP.get("oauth_client_id", "")
MP_OAUTH_CLIENT_SECRET = MP.get("oauth_client_secret", "")
MP_OAUTH_REDIRECT_URI = MP.get("oauth_redirect_uri", "")


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        self.status, self.message = status, message


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: json_decimal(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_decimal(item) for item in value]
    return value


def response(status: int, body: Any) -> dict[str, Any]:
    return {"statusCode": status, "headers": {"content-type": "application/json", "access-control-allow-origin": FRONTEND_ORIGIN, "access-control-allow-headers": "authorization,content-type", "access-control-allow-methods": "GET,POST,PATCH,PUT,DELETE,OPTIONS"}, "body": json.dumps(json_decimal(body), ensure_ascii=False)}


def parse_body(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ApiError(400, "JSON inválido") from exc
    if not isinstance(parsed, dict):
        raise ApiError(400, "El cuerpo debe ser un objeto JSON")
    return parsed


def principal(event: dict[str, Any]) -> dict[str, str]:
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    user_id = claims.get("sub")
    if not user_id:
        raise ApiError(401, "Sesión requerida")
    return {"user_id": user_id, "email": claims.get("email", ""), "name": claims.get("name") or claims.get("cognito:username") or "Usuario"}


def get_item(pk: str, sk: str, required: bool = True) -> dict[str, Any] | None:
    item = table.get_item(Key={"pk": pk, "sk": sk}).get("Item")
    if required and not item:
        raise ApiError(404, "No encontrado")
    return item


def assert_member(group_id: str, user_id: str) -> dict[str, Any]:
    member = get_item(f"GROUP#{group_id}", f"MEMBER#{user_id}")
    if not member.get("active", True):
        raise ApiError(403, "No pertenecés a este grupo")
    return member


def put_audit(user_id: str, action: str, target: str) -> None:
    table.put_item(Item={"pk": f"USER#{user_id}", "sk": f"AUDIT#{now()}#{uuid.uuid4().hex[:8]}", "action": action, "target": target, "created_at": now(), "ttl": int(time.time()) + 31536000})


def ensure_profile(identity: dict[str, str]) -> dict[str, Any]:
    profile = get_item(f"USER#{identity['user_id']}", "PROFILE", required=False)
    if profile:
        return profile
    profile = {"pk": f"USER#{identity['user_id']}", "sk": "PROFILE", "user_id": identity["user_id"], "email": identity["email"], "display_name": identity["name"], "created_at": now()}
    try:
        table.put_item(Item=profile, ConditionExpression="attribute_not_exists(pk)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
    return get_item(f"USER#{identity['user_id']}", "PROFILE")


def _forecast_lookback_months() -> int:
    try:
        months = int(os.environ.get("FINANCE_FORECAST_LOOKBACK_MONTHS", "6"))
    except ValueError as exc:
        raise ApiError(503, "La configuración del panorama financiero es inválida") from exc
    if not 3 <= months <= 12:
        raise ApiError(503, "La configuración del panorama financiero es inválida")
    return months


def _forecast_bigquery_client():
    """Create a read-only reporting client only after the endpoint is requested."""
    if not FORECAST_GCP_SECRET_ID or not FORECAST_GCP_PROJECT_ID:
        raise ApiError(503, "El panorama financiero todavía no está conectado")
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        secret = boto3.client("secretsmanager").get_secret_value(
            SecretId=FORECAST_GCP_SECRET_ID
        )["SecretString"]
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(secret),
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(
            project=FORECAST_GCP_PROJECT_ID,
            credentials=credentials,
            location=FORECAST_BQ_LOCATION,
        ), bigquery
    except ApiError:
        raise
    except Exception as exc:
        print(f"forecast_bigquery_client_failed: {type(exc).__name__}")
        raise ApiError(503, "El panorama financiero todavía no está disponible") from exc


def finance_forecast(identity: dict[str, str]) -> dict[str, Any]:
    """Return an aggregate cash-flow forecast for the explicitly linked owner.

    Divi has no general ETL-to-Cognito identity mapping yet. Until it does,
    serving warehouse history to arbitrary authenticated accounts would be an
    access-control bug. The route is therefore opt-in and constrained to one
    configured Cognito subject.
    """
    if not FORECAST_ENABLED or not FORECAST_OWNER_SUB:
        raise ApiError(503, "El panorama financiero todavía no está conectado")
    if not hmac.compare_digest(identity["user_id"], FORECAST_OWNER_SUB):
        raise ApiError(403, "Tu cuenta todavía no tiene una fuente financiera vinculada")

    client, bigquery = _forecast_bigquery_client()
    lookback_months = _forecast_lookback_months()
    today = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires")).date()
    history_start = date(today.year, today.month, 1)
    for _ in range(lookback_months):
        history_start = date(
            history_start.year - (history_start.month == 1),
            12 if history_start.month == 1 else history_start.month - 1,
            1,
        )

    try:
        gastos_schema = {
            field.name.lower()
            for field in client.get_table(
                f"{FORECAST_GCP_PROJECT_ID}.{FORECAST_BQ_DATASET}.{FORECAST_GASTOS_VIEW}"
            ).schema
        }
        required_columns = {"fecha", "comercio", "categoria", "monto_total", "divisa", "extraido_de"}
        if not required_columns.issubset(gastos_schema):
            raise ValueError("gastos view lacks forecast columns")
        try:
            mapping_schema = {
                field.name.lower()
                for field in client.get_table(
                    f"{FORECAST_GCP_PROJECT_ID}.{FORECAST_BQ_DATASET}.{FORECAST_MAPPING_TABLE}"
                ).schema
            }
        except Exception as exc:
            print(f"forecast_mapping_schema_unavailable: {type(exc).__name__}")
            mapping_schema = set()
        use_mapping = {"flow", "comercio_raw", "comercio_depurado", "categoria", "activo"}.issubset(mapping_schema)
        query = monthly_expenses_query(
            project_id=FORECAST_GCP_PROJECT_ID,
            dataset=FORECAST_BQ_DATASET,
            gastos_view=FORECAST_GASTOS_VIEW,
            mapping_table=FORECAST_MAPPING_TABLE,
            has_monto_ars="monto_ars" in gastos_schema,
            use_mapping=use_mapping,
        )
        config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", history_start),
            bigquery.ScalarQueryParameter("end_date", "DATE", today),
        ], use_query_cache=True)
        rows = [dict(row.items()) for row in client.query(query, job_config=config).result()]
    except ApiError:
        raise
    except Exception as exc:
        print(f"forecast_query_failed: {type(exc).__name__}")
        raise ApiError(503, "No pudimos calcular el panorama financiero ahora") from exc

    result = build_monthly_forecast(rows, as_of=today, lookback_months=lookback_months)
    result["mode"] = "live"
    result["data_source"] = "etl_aggregated_read_only"
    put_audit(identity["user_id"], "finance.forecast.read", result["period"]["forecast_month"])
    return result


def list_groups(user_id: str) -> list[dict[str, Any]]:
    memberships = table.query(KeyConditionExpression=Key("pk").eq(f"USER#{user_id}") & Key("sk").begins_with("GROUP#")).get("Items", [])
    groups = []
    for membership in memberships:
        group = get_item(f"GROUP#{membership['group_id']}", "META", required=False)
        if group:
            groups.append({**group, "role": membership["role"]})
    return groups


def create_group(user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name", "")).strip()
    if not 2 <= len(name) <= 80:
        raise ApiError(400, "El nombre del grupo debe tener entre 2 y 80 caracteres")
    group_id = uuid.uuid4().hex
    created = now()
    group = {"pk": f"GROUP#{group_id}", "sk": "META", "group_id": group_id, "name": name, "currency": "ARS", "owner_user_id": user_id, "created_at": created}
    membership = {"pk": f"GROUP#{group_id}", "sk": f"MEMBER#{user_id}", "group_id": group_id, "user_id": user_id, "role": "owner", "active": True, "created_at": created}
    reverse = {"pk": f"USER#{user_id}", "sk": f"GROUP#{group_id}", "group_id": group_id, "role": "owner", "created_at": created}
    dynamodb.meta.client.transact_write_items(TransactItems=[{"Put": {"TableName": TABLE_NAME, "Item": marshal(group)}}, {"Put": {"TableName": TABLE_NAME, "Item": marshal(membership)}}, {"Put": {"TableName": TABLE_NAME, "Item": marshal(reverse)}}])
    put_audit(user_id, "group.created", group_id)
    return group


def create_invite(group_id: str, user_id: str) -> dict[str, Any]:
    assert_member(group_id, user_id)
    token = secrets.token_urlsafe(24)
    invite = {"pk": f"INVITE#{token}", "sk": "META", "token": token, "group_id": group_id, "created_by": user_id, "created_at": now(), "ttl": int(time.time()) + 7 * 86400}
    table.put_item(Item=invite)
    put_audit(user_id, "invite.created", group_id)
    return {"token": token, "expires_in_days": 7}


def accept_invite(token: str, user_id: str) -> dict[str, Any]:
    invite = get_item(f"INVITE#{token}", "META")
    group_id = invite["group_id"]
    created = now()
    member = {"pk": f"GROUP#{group_id}", "sk": f"MEMBER#{user_id}", "group_id": group_id, "user_id": user_id, "role": "member", "active": True, "created_at": created}
    reverse = {"pk": f"USER#{user_id}", "sk": f"GROUP#{group_id}", "group_id": group_id, "role": "member", "created_at": created}
    try:
        dynamodb.meta.client.transact_write_items(TransactItems=[{"Put": {"TableName": TABLE_NAME, "Item": marshal(member)}}, {"Put": {"TableName": TABLE_NAME, "Item": marshal(reverse)}}, {"Delete": {"TableName": TABLE_NAME, "Key": marshal({"pk": f"INVITE#{token}", "sk": "META"}), "ConditionExpression": "attribute_exists(pk)"}}])
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "TransactionCanceledException":
            raise ApiError(409, "La invitación ya fue usada o venció") from exc
        raise
    put_audit(user_id, "invite.accepted", group_id)
    return get_item(f"GROUP#{group_id}", "META")


def create_identity_link(user_id: str) -> dict[str, Any]:
    """Create a short-lived code that a pending Telegram member can claim."""
    code = secrets.token_urlsafe(8).upper().replace("-", "").replace("_", "")[:10]
    item = {"pk": f"IDENTITY_LINK#{code}", "sk": "META", "code": code, "user_id": user_id, "created_at": now(), "ttl": int(time.time()) + 15 * 60}
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
    put_audit(user_id, "identity_link.created", code)
    return {"code": code, "expires_in_minutes": 15, "instruction": "Enviá /vincular " + code + " al bot Divi Groups desde tu cuenta de Telegram."}


def claim_telegram_identity(code: str, telegram_user_id: str, display_name: str = "") -> dict[str, Any]:
    """Promote pending legacy Telegram memberships to the authenticated user."""
    link = get_item(f"IDENTITY_LINK#{code.strip().upper()}", "META", required=False)
    if not link:
        raise ApiError(400, "El código venció o ya fue utilizado")
    identity_pk = f"EXTERNAL#TELEGRAM#{telegram_user_id}"
    links = table.query(KeyConditionExpression=Key("pk").eq(identity_pk) & Key("sk").begins_with("GROUP#")).get("Items", [])
    if not links:
        raise ApiError(404, "No encontramos grupos pendientes para esta cuenta de Telegram")
    user_id = link["user_id"]
    for external in links:
        group_id, old_member_key = external["group_id"], external["member_key"]
        old = get_item(f"GROUP#{group_id}", f"MEMBER#{old_member_key}", required=False)
        if not old:
            continue
        existing = get_item(f"GROUP#{group_id}", f"MEMBER#{user_id}", required=False) or {}
        member = {**old, **existing, "pk": f"GROUP#{group_id}", "sk": f"MEMBER#{user_id}", "user_id": user_id, "active": True, "claim_state": "claimed", "claimed_at": now(), "display_name": display_name or existing.get("display_name") or old.get("display_name", "Usuario")}
        reverse = {"pk": f"USER#{user_id}", "sk": f"GROUP#{group_id}", "group_id": group_id, "role": existing.get("role", old.get("role", "member")), "created_at": existing.get("created_at", old.get("created_at", now())), "claimed_at": now()}
        ledger_rows = []
        query_args: dict[str, Any] = {"KeyConditionExpression": Key("pk").eq(f"GROUP#{group_id}"), "ConsistentRead": True}
        while True:
            page = table.query(**query_args)
            ledger_rows.extend(page.get("Items", []))
            if "LastEvaluatedKey" not in page:
                break
            query_args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        for row in ledger_rows:
            if row.get("entity") not in {"expense", "settlement_payment", "legacy_settlement_review"}:
                continue
            try:
                updated = replace_ledger_user_id(row, old_member_key, user_id)
            except ValueError as exc:
                raise ApiError(409, str(exc)) from exc
            if updated != row:
                table.put_item(Item=updated)
        reminder_args: dict[str, Any] = {"KeyConditionExpression": Key("pk").eq(f"USER#{old_member_key}"), "ConsistentRead": True}
        while True:
            page = table.query(**reminder_args)
            for reminder in page.get("Items", []):
                if reminder.get("entity") != "reminder":
                    continue
                table.put_item(Item={**reminder, "pk": f"USER#{user_id}", "user_id": user_id, "claimed_at": now()})
                table.delete_item(Key={"pk": reminder["pk"], "sk": reminder["sk"]})
            if "LastEvaluatedKey" not in page:
                break
            reminder_args["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        dynamodb.meta.client.transact_write_items(TransactItems=[
            {"Put": {"TableName": TABLE_NAME, "Item": marshal(member)}},
            {"Put": {"TableName": TABLE_NAME, "Item": marshal(reverse)}},
            {"Delete": {"TableName": TABLE_NAME, "Key": marshal({"pk": f"GROUP#{group_id}", "sk": f"MEMBER#{old_member_key}"})}},
            {"Delete": {"TableName": TABLE_NAME, "Key": marshal({"pk": identity_pk, "sk": external["sk"]})}},
        ])
    table.put_item(Item={"pk": identity_pk, "sk": "META", "entity": "telegram_identity", "user_id": user_id, "linked_at": now()})
    table.delete_item(Key={"pk": link["pk"], "sk": "META"})
    put_audit(user_id, "identity_link.claimed", f"telegram:{telegram_user_id}")
    return {"linked_groups": len(links), "user_id": user_id}


def list_members(group_id: str) -> list[dict[str, Any]]:
    return table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("MEMBER#")).get("Items", [])


def list_group_members(group_id: str, user_id: str) -> list[dict[str, Any]]:
    assert_member(group_id, user_id)
    return [
        {
            "user_id": str(member["user_id"]),
            "display_name": str(member.get("display_name", "Integrante")),
            "role": str(member.get("role", "member")),
            "active": bool(member.get("active", True)),
            "claim_state": str(member.get("claim_state", "claimed")),
        }
        for member in list_members(group_id)
    ]


def _active_member_ids(group_id: str) -> set[str]:
    return {str(member["user_id"]) for member in list_members(group_id) if member.get("active", True)}


def _expense_member_ids(group_id: str) -> set[str]:
    """Include legacy members who have not claimed their Divi identity yet."""
    return {
        str(member["user_id"])
        for member in list_members(group_id)
        if member.get("active", True) or member.get("claim_state") == "pending"
    }


def list_group_expenses(group_id: str, user_id: str) -> list[dict[str, Any]]:
    assert_member(group_id, user_id)
    return table.query(
        KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("EXPENSE#"),
        ScanIndexForward=False,
    ).get("Items", [])


def _expense_item(group_id: str, expense_id: str) -> dict[str, Any]:
    direct = get_item(f"GROUP#{group_id}", f"EXPENSE#{expense_id}", required=False)
    if direct:
        return direct
    # Compatibility for early Divi records whose sort key included a timestamp.
    rows = table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("EXPENSE#")).get("Items", [])
    for row in rows:
        if row.get("expense_id") == expense_id:
            return row
    raise ApiError(404, "Gasto no encontrado")


def _expense_ledger(group_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    expenses = table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("EXPENSE#")).get("Items", [])
    payments = table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("SETTLEMENT#")).get("Items", [])
    return expenses, payments


def create_expense(group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    description = str(payload.get("description", "")).strip()
    split_method = str(payload.get("split_method", "equal"))
    participants = payload.get("participants") or []
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    if split_method == "items":
        try:
            allocations = allocations_from_items(items)
        except (ValueError, ArithmeticError) as exc:
            raise ApiError(400, str(exc)) from exc
        participants = list(allocations)
    if not description or not isinstance(participants, list):
        raise ApiError(400, "Descripción y participantes son requeridos")
    participants = [str(value) for value in participants]
    eligible_participants = _expense_member_ids(group_id)
    active_ids = _active_member_ids(group_id)
    if not set(participants).issubset(eligible_participants):
        raise ApiError(400, "Todos los participantes deben pertenecer al grupo")
    try:
        total = money(payload.get("amount"))
        if split_method == "items":
            if sum(allocations.values()) != total:
                raise ValueError("Los montos de los ítems deben sumar el total del gasto")
        else:
            allocations = expense_allocations(total, participants, split_method, payload.get("split_values"))
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, str(exc)) from exc
    raw_payers = payload.get("payers") or [{"user_id": payload.get("paid_by") or user_id, "amount": str(total)}]
    try:
        payers = normalize_payers(total, raw_payers)
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, str(exc)) from exc
    if not {row["user_id"] for row in payers}.issubset(active_ids):
        raise ApiError(400, "Los pagadores deben pertenecer al grupo")
    expense_id = uuid.uuid4().hex
    expense = {
        "pk": f"GROUP#{group_id}", "sk": f"EXPENSE#{expense_id}", "entity": "expense", "expense_id": expense_id,
        "group_id": group_id, "description": description, "amount": str(total), "currency": "ARS", "payers": payers,
        "allocations": {key: str(value) for key, value in allocations.items()}, "split_method": split_method,
        "items": items, "category": payload.get("category", "Otro"),
        "created_by": user_id, "created_at": now(),
    }
    table.put_item(Item=expense)
    put_audit(user_id, "expense.created", expense_id)
    return expense


def update_expense(group_id: str, expense_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    expense = _expense_item(group_id, expense_id)
    if expense.get("deleted_at"):
        raise ApiError(409, "El gasto fue eliminado")
    merged = {
        "description": payload.get("description", expense.get("description")),
        "amount": payload.get("amount", expense.get("amount")),
        "participants": payload.get("participants", list((expense.get("allocations") or expense.get("shares") or {}).keys())),
        "split_method": payload.get("split_method", expense.get("split_method", "equal")),
        "split_values": payload.get("split_values", expense.get("allocations") or expense.get("shares") or {}),
        "payers": payload.get("payers", expense.get("payers") or [{"user_id": expense.get("paid_by"), "amount": expense.get("amount")}]),
        "items": payload.get("items", expense.get("items", [])), "category": payload.get("category", expense.get("category", "Otro")),
    }
    replacement = create_expense(group_id, user_id, merged)
    expense["deleted_at"], expense["deleted_by"], expense["replaced_by"] = now(), user_id, replacement["expense_id"]
    table.put_item(Item=expense)
    put_audit(user_id, "expense.updated", expense_id)
    return replacement


def record_settlement_payment(group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    debtor, creditor = str(payload.get("from_user_id", "")), str(payload.get("to_user_id", ""))
    if debtor == creditor or not {debtor, creditor}.issubset(_active_member_ids(group_id)):
        raise ApiError(400, "Los participantes del pago son inválidos")
    try:
        amount = money(payload.get("amount"))
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, str(exc)) from exc
    if amount <= 0:
        raise ApiError(400, "El pago debe ser positivo")
    payment_id = uuid.uuid4().hex
    payment = {"pk": f"GROUP#{group_id}", "sk": f"SETTLEMENT#{payment_id}", "entity": "settlement_payment", "payment_id": payment_id, "group_id": group_id, "from_user_id": debtor, "to_user_id": creditor, "amount": str(amount), "currency": "ARS", "created_by": user_id, "created_at": now()}
    table.put_item(Item=payment)
    put_audit(user_id, "settlement.recorded", payment_id)
    return payment


def group_settlements(group_id: str, user_id: str) -> dict[str, Any]:
    assert_member(group_id, user_id)
    expenses, payments = _expense_ledger(group_id)
    balances = group_balances(expenses, payments)
    for member in list_members(group_id):
        balances.setdefault(str(member["user_id"]), Decimal("0"))
    return {"balances": {key: str(value.quantize(Decimal('0.01'))) for key, value in balances.items()}, "settlements": simplify_debts(balances)}


def create_shared_gift_list(group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    name = str(payload.get("name", "")).strip()
    if not 2 <= len(name) <= 100:
        raise ApiError(400, "El nombre de la lista debe tener entre 2 y 100 caracteres")
    list_id, share_code = uuid.uuid4().hex, secrets.token_urlsafe(6).upper()[:8]
    gift_list = {"pk": f"GIFT_LIST#{list_id}", "sk": "META", "entity": "shared_gift_list", "gift_list_id": list_id, "name": name, "share_code": share_code, "created_by": user_id, "created_at": now()}
    group_link = {"pk": f"GROUP#{group_id}", "sk": f"GIFT_LIST#{list_id}", "entity": "shared_gift_list_link", "group_id": group_id, "gift_list_id": list_id, "share_code": share_code, "name": name, "created_at": now()}
    table.put_item(Item=gift_list)
    table.put_item(Item=group_link)
    table.put_item(Item={"pk": f"GIFT_LIST_CODE#{share_code}", "sk": "META", "gift_list_id": list_id, "created_at": now()}, ConditionExpression="attribute_not_exists(pk)")
    return gift_list


def link_shared_gift_list(group_id: str, user_id: str, share_code: str) -> dict[str, Any]:
    assert_member(group_id, user_id)
    code = share_code.strip().upper()
    code_item = get_item(f"GIFT_LIST_CODE#{code}", "META", required=False)
    if not code_item:
        raise ApiError(404, "No encontramos esa lista compartida")
    gift_list = get_item(f"GIFT_LIST#{code_item['gift_list_id']}", "META")
    link = {"pk": f"GROUP#{group_id}", "sk": f"GIFT_LIST#{gift_list['gift_list_id']}", "entity": "shared_gift_list_link", "group_id": group_id, "gift_list_id": gift_list["gift_list_id"], "share_code": code, "name": gift_list["name"], "linked_by": user_id, "created_at": now()}
    table.put_item(Item=link)
    return gift_list


def list_shared_gift_lists(group_id: str, user_id: str) -> list[dict[str, Any]]:
    assert_member(group_id, user_id)
    return table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("GIFT_LIST#")).get("Items", [])


def _shared_gift_list_for_group(group_id: str, list_id: str, user_id: str) -> dict[str, Any]:
    assert_member(group_id, user_id)
    if not get_item(f"GROUP#{group_id}", f"GIFT_LIST#{list_id}", required=False):
        raise ApiError(403, "La lista no pertenece a este grupo")
    return get_item(f"GIFT_LIST#{list_id}", "META")


def add_shared_gift_item(group_id: str, list_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    _shared_gift_list_for_group(group_id, list_id, user_id)
    title = str(payload.get("title", "")).strip()
    if not 2 <= len(title) <= 140:
        raise ApiError(400, "El regalo debe tener entre 2 y 140 caracteres")
    try:
        target = money(payload.get("target_amount", 0))
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, str(exc)) from exc
    item_id = uuid.uuid4().hex
    item = {"pk": f"GIFT_LIST#{list_id}", "sk": f"ITEM#{item_id}", "entity": "shared_gift_item", "gift_item_id": item_id, "gift_list_id": list_id, "title": title, "target_amount": str(target), "created_by": user_id, "created_at": now()}
    table.put_item(Item=item)
    return item


def record_shared_gift_contribution(group_id: str, list_id: str, item_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    _shared_gift_list_for_group(group_id, list_id, user_id)
    get_item(f"GIFT_LIST#{list_id}", f"ITEM#{item_id}")
    try:
        amount = money(payload.get("amount"))
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, str(exc)) from exc
    if amount <= 0:
        raise ApiError(400, "El aporte debe ser positivo")
    contribution_id = uuid.uuid4().hex
    contribution = {"pk": f"GIFT_LIST#{list_id}", "sk": f"CONTRIBUTION#{item_id}#{contribution_id}", "entity": "recorded_gift_contribution", "gift_list_id": list_id, "gift_item_id": item_id, "contribution_id": contribution_id, "contributor_user_id": user_id, "amount": str(amount), "currency": "ARS", "payment_status": "RECORDED", "created_at": now()}
    table.put_item(Item=contribution)
    return contribution


def create_event(group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    celebrant_id = str(payload.get("celebrant_user_id", ""))
    assert_member(group_id, celebrant_id)
    birthday = str(payload.get("birthday_date", ""))
    try:
        date.fromisoformat(birthday)
    except ValueError as exc:
        raise ApiError(400, "La fecha de cumpleaños es inválida") from exc
    event_id = f"{celebrant_id}#{birthday[:4]}"
    event = {"pk": f"EVENT#{event_id}", "sk": "META", "event_id": event_id, "celebrant_user_id": celebrant_id, "birthday_date": birthday, "created_at": now()}
    try:
        table.put_item(Item=event, ConditionExpression="attribute_not_exists(pk)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        event = get_item(f"EVENT#{event_id}", "META")
    # Multiple groups can attach to the same birthday occurrence.  The wishlist
    # and its reservation lock therefore stay genuinely global to the event.
    table.put_item(Item={"pk": f"GROUP#{group_id}", "sk": f"EVENT#{event_id}", "event_id": event_id, "group_id": group_id, "celebrant_user_id": celebrant_id, "created_at": now()})
    return event


def event_for_group(event_id: str, group_id: str) -> dict[str, Any]:
    event = get_item(f"EVENT#{event_id}", "META")
    if not get_item(f"GROUP#{group_id}", f"EVENT#{event_id}", required=False):
        raise ApiError(403, "El evento no pertenece a este grupo")
    return event


def create_wishlist_item(event_id: str, group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    event = event_for_group(event_id, group_id)
    if event["celebrant_user_id"] != user_id:
        raise ApiError(403, "Solo el cumpleañero puede editar su wishlist")
    title = str(payload.get("title", "")).strip()
    if not 2 <= len(title) <= 140:
        raise ApiError(400, "El regalo debe tener entre 2 y 140 caracteres")
    item_id = uuid.uuid4().hex
    item = {"pk": f"EVENT#{event_id}", "sk": f"WISH#{item_id}", "item_id": item_id, "event_id": event_id, "title": title, "url": str(payload.get("url", ""))[:1000], "target_amount": str(payload.get("target_amount", "0")), "created_at": now()}
    table.put_item(Item=item)
    return item


def list_wishlist(event_id: str, group_id: str, user_id: str) -> list[dict[str, Any]]:
    event_for_group(event_id, group_id)
    assert_member(group_id, user_id)
    is_celebrant = get_item(f"EVENT#{event_id}", "META")["celebrant_user_id"] == user_id
    items = table.query(KeyConditionExpression=Key("pk").eq(f"EVENT#{event_id}") & Key("sk").begins_with("WISH#")).get("Items", [])
    result = []
    for item in items:
        reservation = get_item(f"RESERVATION#{event_id}", f"ITEM#{item['item_id']}", required=False)
        visible = {key: value for key, value in item.items() if key not in {"pk", "sk"}}
        if not is_celebrant:
            visible["available"] = not bool(reservation)
        result.append(visible)
    return result


def reserve_item(event_id: str, item_id: str, group_id: str, user_id: str) -> dict[str, Any]:
    assert_member(group_id, user_id)
    event_for_group(event_id, group_id)
    get_item(f"EVENT#{event_id}", f"WISH#{item_id}")
    reservation = {"pk": f"RESERVATION#{event_id}", "sk": f"ITEM#{item_id}", "event_id": event_id, "item_id": item_id, "group_id": group_id, "reserved_by": user_id, "reserved_at": now()}
    try:
        table.put_item(Item=reservation, ConditionExpression="attribute_not_exists(pk)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise ApiError(409, "Este regalo ya fue reservado por otro grupo") from exc
        raise
    put_audit(user_id, "gift.reserved", f"{event_id}/{item_id}")
    return {"item_id": item_id, "available": False}


def release_item(event_id: str, item_id: str, group_id: str, user_id: str) -> None:
    assert_member(group_id, user_id)
    try:
        table.delete_item(Key={"pk": f"RESERVATION#{event_id}", "sk": f"ITEM#{item_id}"}, ConditionExpression="group_id = :group", ExpressionAttributeValues={":group": group_id})
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise ApiError(403, "Solo el grupo que reservó puede liberarlo") from exc
        raise
    put_audit(user_id, "gift.released", f"{event_id}/{item_id}")


def require_organizer_token(user_id: str) -> dict[str, Any]:
    connection = get_item(f"USER#{user_id}", "MP_OAUTH", required=False)
    if not connection or not connection.get("access_token"):
        raise ApiError(409, "El organizador debe vincular Mercado Pago antes de crear un pool")
    return connection


def create_pool(event_id: str, item_id: str, group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    reservation = get_item(f"RESERVATION#{event_id}", f"ITEM#{item_id}")
    if reservation["group_id"] != group_id:
        raise ApiError(403, "El grupo debe reservar el regalo antes de crear el pool")
    require_organizer_token(user_id)
    target = Decimal(str(payload.get("target_amount", "0")))
    if target <= 0:
        raise ApiError(400, "La meta debe ser mayor a cero")
    pool_id = uuid.uuid4().hex
    pool = {"pk": f"POOL#{pool_id}", "sk": "META", "pool_id": pool_id, "event_id": event_id, "item_id": item_id, "group_id": group_id, "organizer_user_id": user_id, "target_amount": target, "collected_amount": Decimal("0"), "status": "OPEN", "closes_at": str(payload.get("closes_at", "")), "created_at": now()}
    table.put_item(Item=pool)
    return pool


def premium_checkout(user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    card_token = str(payload.get("card_token_id", ""))
    if not MP_ACCESS_TOKEN or not MP_PREMIUM_PLAN_ID:
        raise ApiError(503, "Premium no está configurado todavía")
    if not card_token:
        raise ApiError(400, "Se requiere el token de tarjeta de Mercado Pago")
    external_reference = f"premium#{user_id}#{uuid.uuid4().hex}"
    body = {"preapproval_plan_id": MP_PREMIUM_PLAN_ID, "reason": "Divi Premium", "external_reference": external_reference, "card_token_id": card_token, "status": "authorized", "back_url": f"{FRONTEND_ORIGIN}/wallet"}
    result = mp_request("POST", "/preapproval", MP_ACCESS_TOKEN, body)
    subscription = {"pk": f"USER#{user_id}", "sk": "SUBSCRIPTION", "user_id": user_id, "external_reference": external_reference, "mp_preapproval_id": result.get("id"), "status": result.get("status", "pending"), "plan": "premium", "amount": "3000", "currency": "ARS", "updated_at": now()}
    table.put_item(Item=subscription)
    return {"status": subscription["status"], "subscription_id": subscription["mp_preapproval_id"]}


def pool_checkout(pool_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    pool = get_item(f"POOL#{pool_id}", "META")
    if pool["status"] != "OPEN":
        raise ApiError(409, "El pool no acepta más aportes")
    assert_member(pool["group_id"], user_id)
    amount = Decimal(str(payload.get("amount", "0")))
    if amount <= 0:
        raise ApiError(400, "El aporte debe ser mayor a cero")
    connection = require_organizer_token(pool["organizer_user_id"])
    contribution_id = uuid.uuid4().hex
    reference = f"contribution#{contribution_id}"
    contribution = {"pk": f"POOL#{pool_id}", "sk": f"CONTRIBUTION#{contribution_id}", "contribution_id": contribution_id, "pool_id": pool_id, "user_id": user_id, "amount": str(amount), "status": "PENDING", "external_reference": reference, "created_at": now()}
    preference = {"items": [{"id": contribution_id, "title": "Aporte regalo Split", "quantity": 1, "currency_id": "ARS", "unit_price": float(amount)}], "external_reference": reference, "notification_url": f"{MP_WEBHOOK_BASE_URL}/v1/webhooks/mercadopago/payments", "back_urls": {"success": f"{FRONTEND_ORIGIN}/pools/{pool_id}?payment=success", "failure": f"{FRONTEND_ORIGIN}/pools/{pool_id}?payment=failure"}, "auto_return": "approved"}
    result = mp_request("POST", "/checkout/preferences", connection["access_token"], preference)
    contribution["mp_preference_id"] = result.get("id")
    table.put_item(Item=contribution)
    return {"checkout_url": result.get("init_point"), "contribution_id": contribution_id}


def mp_request(method: str, path: str, token: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    result = requests.request(method, f"https://api.mercadopago.com{path}", headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json", "X-Idempotency-Key": uuid.uuid4().hex}, json=body, timeout=12)
    if result.status_code >= 400:
        raise ApiError(502, "Mercado Pago rechazó la operación")
    return result.json()


def verify_mp_signature(event: dict[str, Any]) -> None:
    if not MP_WEBHOOK_SECRET:
        raise ApiError(503, "Webhook de pagos no configurado")
    headers = {key.lower(): value for key, value in event.get("headers", {}).items()}
    signature = headers.get("x-signature", "")
    request_id = headers.get("x-request-id", "")
    params = event.get("queryStringParameters") or {}
    data_id = str(params.get("data.id") or params.get("id") or "")
    values = dict(part.split("=", 1) for part in signature.split(",") if "=" in part)
    manifest = f"id:{data_id};request-id:{request_id};ts:{values.get('ts', '')};"
    expected = hmac.new(MP_WEBHOOK_SECRET.encode(), manifest.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, values.get("v1", "")):
        raise ApiError(401, "Firma de Mercado Pago inválida")


def handle_payment_webhook(event: dict[str, Any], kind: str) -> dict[str, Any]:
    verify_mp_signature(event)
    params = event.get("queryStringParameters") or {}
    payment_id = str(params.get("data.id") or params.get("id") or "")
    if not payment_id:
        return {"accepted": True}
    event_key = {"pk": "MP_EVENT", "sk": f"{kind}#{payment_id}", "received_at": now(), "ttl": int(time.time()) + 31536000}
    try:
        table.put_item(Item=event_key, ConditionExpression="attribute_not_exists(pk)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"accepted": True, "duplicate": True}
        raise
    detail = mp_request("GET", "/v1/payments/" + payment_id if kind == "payment" else "/preapproval/" + payment_id, MP_ACCESS_TOKEN)
    reference = detail.get("external_reference", "")
    if reference.startswith("contribution#"):
        contribution_id = reference.split("#", 1)[1]
        rows = table.scan(FilterExpression=Attr("contribution_id").eq(contribution_id)).get("Items", [])
        if rows:
            contribution = rows[0]
            contribution["status"] = detail.get("status", "pending").upper()
            contribution["mp_payment_id"] = payment_id
            contribution["updated_at"] = now()
            table.put_item(Item=contribution)
            if contribution["status"] == "APPROVED":
                update_pool_total(contribution)
    elif reference.startswith("premium#"):
        user_id = reference.split("#", 2)[1]
        subscription = get_item(f"USER#{user_id}", "SUBSCRIPTION", required=False)
        if subscription:
            subscription["status"] = detail.get("status", "pending")
            subscription["updated_at"] = now()
            table.put_item(Item=subscription)
    return {"accepted": True}


def update_pool_total(contribution: dict[str, Any]) -> None:
    # Idempotency is guaranteed by the MP_EVENT write above.
    result = table.update_item(Key={"pk": f"POOL#{contribution['pool_id']}", "sk": "META"}, UpdateExpression="ADD collected_amount :amount SET updated_at = :now", ExpressionAttributeValues={":amount": Decimal(contribution["amount"]), ":now": now()}, ReturnValues="ALL_NEW")
    pool = result["Attributes"]
    if Decimal(pool["collected_amount"]) >= Decimal(pool["target_amount"]):
        table.update_item(Key={"pk": f"POOL#{contribution['pool_id']}", "sk": "META"}, UpdateExpression="SET #status = :closed", ExpressionAttributeNames={"#status": "status"}, ExpressionAttributeValues={":closed": "GOAL_REACHED"})


def oauth_start(user_id: str) -> dict[str, str]:
    if not MP_OAUTH_CLIENT_ID or not MP_OAUTH_REDIRECT_URI:
        raise ApiError(503, "La vinculación de Mercado Pago no está configurada")
    state = secrets.token_urlsafe(24)
    table.put_item(Item={"pk": f"OAUTH_STATE#{state}", "sk": "META", "user_id": user_id, "ttl": int(time.time()) + 600})
    return {"authorization_url": f"https://auth.mercadopago.com/authorization?client_id={MP_OAUTH_CLIENT_ID}&response_type=code&platform_id=mp&redirect_uri={MP_OAUTH_REDIRECT_URI}&state={state}"}


def oauth_callback(event: dict[str, Any]) -> dict[str, Any]:
    """Exchange one short-lived authorization code and retain only its token."""
    params = event.get("queryStringParameters") or {}
    code, state = params.get("code", ""), params.get("state", "")
    if not code or not state or not MP_OAUTH_CLIENT_SECRET:
        raise ApiError(400, "La autorización de Mercado Pago no es válida")
    state_item = get_item(f"OAUTH_STATE#{state}", "META", required=False)
    if not state_item:
        raise ApiError(400, "La autorización venció o ya fue usada")
    result = requests.post("https://api.mercadopago.com/oauth/token", data={"client_secret": MP_OAUTH_CLIENT_SECRET, "client_id": MP_OAUTH_CLIENT_ID, "grant_type": "authorization_code", "code": code, "redirect_uri": MP_OAUTH_REDIRECT_URI}, timeout=12)
    if result.status_code >= 400:
        raise ApiError(502, "Mercado Pago no pudo autorizar la cuenta")
    authorized = result.json()
    user_id = state_item["user_id"]
    dynamodb.meta.client.transact_write_items(TransactItems=[
        {"Put": {"TableName": TABLE_NAME, "Item": marshal({"pk": f"USER#{user_id}", "sk": "MP_OAUTH", "access_token": authorized["access_token"], "refresh_token": authorized.get("refresh_token", ""), "public_key": authorized.get("public_key", ""), "user_id": authorized.get("user_id", ""), "updated_at": now()})}},
        {"Delete": {"TableName": TABLE_NAME, "Key": marshal({"pk": f"OAUTH_STATE#{state}", "sk": "META"}), "ConditionExpression": "attribute_exists(pk)"}}
    ])
    return {"statusCode": 302, "headers": {"location": f"{FRONTEND_ORIGIN}/settings?mercadopago=connected", "cache-control": "no-store"}, "body": ""}


def marshal(item: dict[str, Any]) -> dict[str, Any]:
    return boto3.dynamodb.types.TypeSerializer().serialize(item)["M"]


def group_bot_config() -> dict[str, str]:
    secret_arn = os.environ.get("DIVI_GROUP_BOT_SECRET_ARN", "").strip()
    if secret_arn:
        raw = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)["SecretString"]
        value = json.loads(raw)
        return {"token": value.get("telegram_bot_token", ""), "webhook_secret": value.get("telegram_webhook_secret", "")}
    return {"token": os.environ.get("DIVI_GROUP_BOT_TOKEN", ""), "webhook_secret": os.environ.get("DIVI_GROUP_BOT_WEBHOOK_SECRET", "")}


def _group_bot_reply(chat_id: int, text: str) -> None:
    config = group_bot_config()
    if not config["token"]:
        print("group_bot_reply_skipped: bot token unavailable")
        return
    requests.post(f"https://api.telegram.org/bot{config['token']}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=8).raise_for_status()


def _group_bot_user(telegram_user_id: str) -> str | None:
    identity = get_item(f"EXTERNAL#TELEGRAM#{telegram_user_id}", "META", required=False)
    return str(identity["user_id"]) if identity and identity.get("user_id") else None


def _group_bot_chat_group(chat_id: object) -> dict[str, Any] | None:
    link = get_item(f"TELEGRAM_GROUP#{chat_id}", "META", required=False)
    if not link or not link.get("group_id"):
        return None
    return get_item(f"GROUP#{link['group_id']}", "META", required=False)


def _format_group_bot_settlements(group_id: str, user_id: str) -> str:
    result = group_settlements(group_id, user_id)
    members = {str(row["user_id"]): row.get("display_name", "Integrante") for row in list_members(group_id)}
    settlements = result["settlements"]
    if not settlements:
        return "✅ El grupo está saldado."
    lines = ["💸 Saldos sugeridos:"]
    for row in settlements[:10]:
        debtor = members.get(str(row["from_user_id"]), "Integrante")
        creditor = members.get(str(row["to_user_id"]), "Integrante")
        lines.append(f"• {debtor} → {creditor}: ${row['amount']}")
    return "\n".join(lines)


def _format_recent_group_expenses(group_id: str, user_id: str) -> str:
    rows = [row for row in list_group_expenses(group_id, user_id) if not row.get("deleted_at")]
    if not rows:
        return "Todavía no hay gastos en este grupo."
    rows.sort(key=lambda row: str(row.get("created_at", "")), reverse=True)
    lines = ["🧾 Últimos gastos:"]
    for row in rows[:5]:
        lines.append(f"• {row.get('description', 'Gasto')}: ${row.get('amount', '0')} {row.get('currency', 'ARS')}")
    return "\n".join(lines)


def _parse_group_amount(raw: str) -> Decimal:
    try:
        amount = parse_amount_text(raw)
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, "Monto inválido. Ejemplo: /gasto 12500,50 Cena") from exc
    if amount <= 0:
        raise ApiError(400, "El monto debe ser mayor a cero")
    return amount


def handle_group_bot_webhook(event: dict[str, Any]) -> dict[str, Any]:
    config = group_bot_config()
    expected = config.get("webhook_secret", "")
    received = (event.get("headers") or {}).get("x-telegram-bot-api-secret-token") or (event.get("headers") or {}).get("X-Telegram-Bot-Api-Secret-Token")
    if not expected or not received or not hmac.compare_digest(expected, received):
        raise ApiError(401, "Webhook no autorizado")
    update = parse_body(event)
    message = update.get("message") or {}
    text = str(message.get("text") or "").strip()
    sender = message.get("from") or {}
    chat = message.get("chat") or {}
    telegram_id, chat_id = str(sender.get("id", "")), chat.get("id")
    if not telegram_id or not chat_id or not text:
        return response(200, {"ok": True})
    command, *arguments = text.split()
    command = command.split("@", 1)[0].lower()
    if command in {"/start", "/help", "/ayuda"}:
        reply = "👋 Divi Groups\n\n/vincular CODIGO: vincular tu cuenta\n/grupos: listar tus grupos\nEn un grupo: /saldo, /gastos y /gasto MONTO DESCRIPCION."
    elif command == "/vincular" and arguments:
        result = claim_telegram_identity(arguments[0], telegram_id, str(sender.get("first_name") or ""))
        reply = f"✅ Cuenta vinculada. Reclamaste {result['linked_groups']} grupo(s) en Divi."
    elif command == "/grupos":
        user_id = _group_bot_user(telegram_id)
        if not user_id:
            reply = "Vinculá primero tu cuenta desde un chat privado: /vincular CODIGO."
        else:
            rows = table.query(KeyConditionExpression=Key("pk").eq(f"USER#{user_id}") & Key("sk").begins_with("GROUP#")).get("Items", [])
            names = [get_item(f"GROUP#{row['group_id']}", "META", required=False) for row in rows]
            group_names = [str(group.get("name", "Grupo")) for group in names if group]
            reply = "👥 Tus grupos:\n" + "\n".join(f"• {name}" for name in group_names) if group_names else "No tenés grupos vinculados todavía."
    elif command in {"/saldo", "/gastos"}:
        user_id = _group_bot_user(telegram_id)
        group = _group_bot_chat_group(chat_id)
        if not user_id:
            reply = "Vinculá tu cuenta desde un chat privado con /vincular CODIGO."
        elif not group:
            reply = "Este chat no está vinculado a un grupo de Divi."
        else:
            try:
                reply = _format_group_bot_settlements(group["group_id"], user_id) if command == "/saldo" else _format_recent_group_expenses(group["group_id"], user_id)
            except ApiError as exc:
                reply = exc.message
    elif command == "/gasto":
        user_id = _group_bot_user(telegram_id)
        group = _group_bot_chat_group(chat_id)
        detail = text.partition(" ")[2].strip().split(maxsplit=1)
        if not user_id:
            reply = "Vinculá tu cuenta desde un chat privado con /vincular CODIGO."
        elif not group:
            reply = "Este chat no está vinculado a un grupo de Divi."
        elif len(detail) != 2:
            reply = "Formato: /gasto MONTO DESCRIPCION. Ejemplo: /gasto 12500,50 Cena."
        else:
            try:
                amount = _parse_group_amount(detail[0])
                participants = sorted(_expense_member_ids(group["group_id"]))
                create_expense(group["group_id"], user_id, {
                    "amount": str(amount), "description": detail[1], "participants": participants,
                    "split_method": "equal", "paid_by": user_id,
                })
                reply = f"✅ Gasto agregado: {detail[1]} · ${amount}. Dividido entre {len(participants)} integrantes."
            except ApiError as exc:
                reply = exc.message
    else:
        reply = "Comando no reconocido. Usá /ayuda para ver las opciones disponibles."
    _group_bot_reply(int(chat_id), reply)
    return response(200, {"ok": True})


def route(event: dict[str, Any]) -> dict[str, Any]:
    method = event.get("requestContext", {}).get("http", {}).get("method", "").upper()
    path = event.get("rawPath", "")
    if method == "OPTIONS":
        return response(204, {})
    if path == "/v1/webhooks/mercadopago/payments" and method == "POST":
        return response(200, handle_payment_webhook(event, "payment"))
    if path == "/v1/webhooks/mercadopago/subscriptions" and method == "POST":
        return response(200, handle_payment_webhook(event, "subscription"))
    if path == "/v1/mercadopago/oauth/callback" and method == "GET":
        return oauth_callback(event)
    if path == "/v1/group-bot/telegram" and method == "POST":
        return handle_group_bot_webhook(event)
    identity = principal(event)
    ensure_profile(identity)
    payload = parse_body(event) if method in {"POST", "PATCH", "PUT"} else {}
    segments = [part for part in path.split("/") if part]
    if path == "/v1/me":
        if method == "GET": return response(200, ensure_profile(identity))
        if method == "PATCH":
            profile = ensure_profile(identity); profile["display_name"] = str(payload.get("display_name", profile["display_name"]))[:80]; table.put_item(Item=profile); return response(200, profile)
    if path == "/v1/finance/forecast" and method == "GET":
        return response(200, finance_forecast(identity))
    if path == "/v1/identity-links" and method == "POST":
        return response(201, create_identity_link(identity["user_id"]))
    if path == "/v1/groups":
        if method == "GET": return response(200, list_groups(identity["user_id"]))
        if method == "POST": return response(201, create_group(identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "invites" and method == "POST":
        return response(201, create_invite(segments[2], identity["user_id"]))
    if len(segments) == 4 and segments[:2] == ["v1", "invitations"] and segments[3] == "accept" and method == "POST":
        return response(200, accept_invite(segments[2], identity["user_id"]))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "expenses":
        if method == "GET":
            return response(200, list_group_expenses(segments[2], identity["user_id"]))
        if method == "POST": return response(201, create_expense(segments[2], identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "members" and method == "GET":
        return response(200, list_group_members(segments[2], identity["user_id"]))
    if len(segments) == 5 and segments[:2] == ["v1", "groups"] and segments[3] == "expenses" and method == "PATCH":
        return response(200, update_expense(segments[2], segments[4], identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "settlements" and method == "GET":
        return response(200, group_settlements(segments[2], identity["user_id"]))
    if len(segments) == 5 and segments[:2] == ["v1", "groups"] and segments[3] == "settlements" and segments[4] == "payments" and method == "POST":
        return response(201, record_settlement_payment(segments[2], identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "gift-lists":
        if method == "GET": return response(200, list_shared_gift_lists(segments[2], identity["user_id"]))
        if method == "POST": return response(201, create_shared_gift_list(segments[2], identity["user_id"], payload))
    if len(segments) == 5 and segments[:2] == ["v1", "groups"] and segments[3] == "gift-lists" and segments[4] == "link" and method == "POST":
        return response(200, link_shared_gift_list(segments[2], identity["user_id"], str(payload.get("share_code", ""))))
    if len(segments) == 6 and segments[:2] == ["v1", "groups"] and segments[3] == "gift-lists" and segments[5] == "items" and method == "POST":
        return response(201, add_shared_gift_item(segments[2], segments[4], identity["user_id"], payload))
    if len(segments) == 8 and segments[:2] == ["v1", "groups"] and segments[3] == "gift-lists" and segments[5] == "items" and segments[7] == "contributions" and method == "POST":
        return response(201, record_shared_gift_contribution(segments[2], segments[4], segments[6], identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "events" and method == "POST":
        return response(201, create_event(segments[2], identity["user_id"], payload))
    if len(segments) >= 5 and segments[:2] == ["v1", "groups"] and segments[3] == "events":
        group_id, event_id = segments[2], segments[4]
        if len(segments) == 6 and segments[5] == "wishlist":
            if method == "GET": return response(200, list_wishlist(event_id, group_id, identity["user_id"]))
            if method == "POST": return response(201, create_wishlist_item(event_id, group_id, identity["user_id"], payload))
        if len(segments) == 8 and segments[5] == "wishlist" and segments[7] == "reserve":
            if method == "POST": return response(200, reserve_item(event_id, segments[6], group_id, identity["user_id"]))
            if method == "DELETE": release_item(event_id, segments[6], group_id, identity["user_id"]); return response(204, {})
        if len(segments) == 8 and segments[5] == "wishlist" and segments[7] == "pool" and method == "POST":
            return response(201, create_pool(event_id, segments[6], group_id, identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "pools"] and segments[3] == "checkout" and method == "POST":
        return response(200, pool_checkout(segments[2], identity["user_id"], payload))
    if path == "/v1/premium/checkout" and method == "POST": return response(200, premium_checkout(identity["user_id"], payload))
    if path == "/v1/mercadopago/oauth/start" and method == "POST": return response(200, oauth_start(identity["user_id"]))
    raise ApiError(404, "Ruta no encontrada")


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    try:
        return route(event)
    except ApiError as exc:
        return response(exc.status, {"error": exc.message})
    except Exception as exc:  # Avoid leaking provider or infrastructure detail.
        print(f"Unhandled error: {exc!r}")
        return response(500, {"error": "Ocurrió un error inesperado"})
