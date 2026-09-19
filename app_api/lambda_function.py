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

import boto3
import requests
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from domain import simplify_debts, split_expense

TABLE_NAME = os.environ["APP_TABLE"]
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "http://localhost:5173")
MP_WEBHOOK_BASE_URL = os.environ.get("MP_WEBHOOK_BASE_URL", "")

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


def list_members(group_id: str) -> list[dict[str, Any]]:
    return table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("MEMBER#")).get("Items", [])


def create_expense(group_id: str, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    assert_member(group_id, user_id)
    description = str(payload.get("description", "")).strip()
    participants = payload.get("participants") or []
    if not description or not isinstance(participants, list):
        raise ApiError(400, "Descripción y participantes son requeridos")
    active_ids = {member["user_id"] for member in list_members(group_id)}
    if not set(participants).issubset(active_ids):
        raise ApiError(400, "Todos los participantes deben pertenecer al grupo")
    try:
        shares = split_expense(payload.get("amount"), participants, payload.get("split_method", "equal"), payload.get("split_values"))
    except (ValueError, ArithmeticError) as exc:
        raise ApiError(400, str(exc)) from exc
    paid_by = payload.get("paid_by") or user_id
    if paid_by not in active_ids:
        raise ApiError(400, "El pagador debe pertenecer al grupo")
    expense_id = uuid.uuid4().hex
    expense = {"pk": f"GROUP#{group_id}", "sk": f"EXPENSE#{now()}#{expense_id}", "entity": "expense", "expense_id": expense_id, "group_id": group_id, "description": description, "amount": str(sum(shares.values())), "paid_by": paid_by, "shares": {key: str(value) for key, value in shares.items()}, "split_method": payload.get("split_method", "equal"), "category": payload.get("category", "Otro"), "created_by": user_id, "created_at": now()}
    table.put_item(Item=expense)
    put_audit(user_id, "expense.created", expense_id)
    return expense


def group_settlements(group_id: str, user_id: str) -> dict[str, Any]:
    assert_member(group_id, user_id)
    expenses = table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{group_id}") & Key("sk").begins_with("EXPENSE#")).get("Items", [])
    balances = {member["user_id"]: Decimal("0") for member in list_members(group_id)}
    for expense in expenses:
        balances[expense["paid_by"]] += Decimal(expense["amount"])
        for participant, share in expense["shares"].items():
            balances[participant] -= Decimal(share)
    return {"balances": {key: str(value.quantize(Decimal('0.01'))) for key, value in balances.items()}, "settlements": simplify_debts(balances)}


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
    preference = {"items": [{"id": contribution_id, "title": "Aporte regalo Divi", "quantity": 1, "currency_id": "ARS", "unit_price": float(amount)}], "external_reference": reference, "notification_url": f"{MP_WEBHOOK_BASE_URL}/v1/webhooks/mercadopago/payments", "back_urls": {"success": f"{FRONTEND_ORIGIN}/pools/{pool_id}?payment=success", "failure": f"{FRONTEND_ORIGIN}/pools/{pool_id}?payment=failure"}, "auto_return": "approved"}
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
    identity = principal(event)
    ensure_profile(identity)
    payload = parse_body(event) if method in {"POST", "PATCH", "PUT"} else {}
    segments = [part for part in path.split("/") if part]
    if path == "/v1/me":
        if method == "GET": return response(200, ensure_profile(identity))
        if method == "PATCH":
            profile = ensure_profile(identity); profile["display_name"] = str(payload.get("display_name", profile["display_name"]))[:80]; table.put_item(Item=profile); return response(200, profile)
    if path == "/v1/groups":
        if method == "GET": return response(200, list_groups(identity["user_id"]))
        if method == "POST": return response(201, create_group(identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "invites" and method == "POST":
        return response(201, create_invite(segments[2], identity["user_id"]))
    if len(segments) == 4 and segments[:2] == ["v1", "invitations"] and segments[3] == "accept" and method == "POST":
        return response(200, accept_invite(segments[2], identity["user_id"]))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "expenses":
        if method == "GET":
            assert_member(segments[2], identity["user_id"])
            rows = table.query(KeyConditionExpression=Key("pk").eq(f"GROUP#{segments[2]}") & Key("sk").begins_with("EXPENSE#"), ScanIndexForward=False).get("Items", [])
            return response(200, rows)
        if method == "POST": return response(201, create_expense(segments[2], identity["user_id"], payload))
    if len(segments) == 4 and segments[:2] == ["v1", "groups"] and segments[3] == "settlements" and method == "GET":
        return response(200, group_settlements(segments[2], identity["user_id"]))
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
