"""Per-user Gmail and Binance connections; credentials stay in Secrets Manager."""
from __future__ import annotations

import base64
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
import re
import secrets
import time
from urllib.parse import urlencode
from typing import Annotated, Literal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from botocore.config import Config
import requests
from pydantic import BaseModel, ConfigDict, Field, SecretStr, StringConstraints, ValidationError, field_validator

from integration_parsers import parse_santander_payment, validate_binance_read_only

GOOGLE_AUTH = 'https://accounts.google.com/o/oauth2/v2/auth'
GOOGLE_TOKEN = 'https://oauth2.googleapis.com/token'
GMAIL_API = 'https://gmail.googleapis.com/gmail/v1/users/me/messages'
GMAIL_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly'
BINANCE_URL = 'https://api.binance.com'


class IntegrationError(Exception):
    def __init__(self, status, message):
        self.status, self.message = status, message


class BinanceCredentials(BaseModel):
    model_config = ConfigDict(extra='forbid')
    api_key: SecretStr
    api_secret: SecretStr

    @field_validator('api_key', 'api_secret')
    @classmethod
    def validate_credential(cls, value):
        if not re.fullmatch(r'[A-Za-z0-9]{16,128}', value.get_secret_value()):
            raise ValueError('invalid credential')
        return value


class GmailCallback(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    state: Annotated[str, StringConstraints(pattern=r'^[A-Za-z0-9_-]{30,100}$')]
    code: Annotated[str, StringConstraints(min_length=1, max_length=2048)]


class BinanceMovementRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    kind: Literal['deposit', 'withdrawal']
    offset: int = Field(ge=0, le=1_000_000)


class BinanceTradeRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    symbol: Annotated[str, StringConstraints(pattern=r'^[A-Z0-9]{5,24}$')]


def _connection(table, user_id, provider):
    item = table.get_item(Key={'pk': f'USER#{user_id}', 'sk': f'CONNECTION#{provider}'}, ConsistentRead=True).get('Item')
    return item if isinstance(item, dict) else {}


def _secret_client():
    return boto3.client('secretsmanager')


def _new_secret(provider, data):
    name = f'divi/user-connections/{provider}/{secrets.token_hex(24)}'
    return _secret_client().create_secret(Name=name, SecretString=json.dumps(data), Description='Split user connection')['ARN']


def _delete_secret(arn):
    if arn:
        try:
            _secret_client().delete_secret(SecretId=arn, RecoveryWindowInDays=7)
        except ClientError as exc:
            error = exc.response.get('Error', {})
            if error.get('Code') == 'InvalidRequestException' and 'marked for deletion' in error.get('Message', '').lower():
                return
            if error.get('Code') != 'ResourceNotFoundException':
                raise


def _read_secret(arn):
    return json.loads(_secret_client().get_secret_value(SecretId=arn)['SecretString'])


def _config():
    arn = os.environ.get('GMAIL_OAUTH_SECRET_ARN', '')
    callback = os.environ.get('GMAIL_OAUTH_CALLBACK_URL', '')
    if not arn or not callback:
        raise IntegrationError(503, 'La conexión de Gmail todavía no está configurada.')
    config = _read_secret(arn)
    if not config.get('client_id') or not config.get('client_secret'):
        raise IntegrationError(503, 'La conexión de Gmail todavía no está configurada.')
    return config, callback


def _safe_json(response):
    try:
        data = response.json()
    except ValueError:
        raise IntegrationError(502, 'El proveedor devolvió una respuesta inválida.') from None
    if response.status_code >= 400 or not isinstance(data, dict):
        raise IntegrationError(502, 'No se pudo conectar con el proveedor. Probá nuevamente.')
    return data


def start_gmail(table, user_id):
    config, callback = _config()
    state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(48)
    digest = hashlib.sha256(state.encode()).hexdigest()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    table.put_item(Item={'pk': f'GMAIL_OAUTH#{digest}', 'sk': 'STATE', 'user_id': user_id,
                         'verifier': verifier, 'ttl': int(time.time()) + 600}, ConditionExpression='attribute_not_exists(pk)')
    params = {'client_id': config['client_id'], 'redirect_uri': callback, 'response_type': 'code',
              'scope': GMAIL_SCOPE, 'state': state, 'code_challenge': challenge,
              'code_challenge_method': 'S256', 'access_type': 'offline', 'prompt': 'consent'}
    return {'authorization_url': GOOGLE_AUTH + '?' + urlencode(params)}


def finish_gmail(table, user_id, state, code):
    try:
        request = GmailCallback.model_validate({'state': state, 'code': code})
    except ValidationError:
        raise IntegrationError(400, 'La autorización de Gmail venció o no es válida.') from None
    state, code = request.state, request.code
    config, callback = _config()
    digest = hashlib.sha256(state.encode()).hexdigest()
    try:
        result = table.delete_item(Key={'pk': f'GMAIL_OAUTH#{digest}', 'sk': 'STATE'},
            ConditionExpression='attribute_exists(pk) AND #ttl > :now AND user_id = :user', ExpressionAttributeNames={'#ttl': 'ttl'},
            ExpressionAttributeValues={':now': int(time.time()), ':user': user_id}, ReturnValues='ALL_OLD')
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
            raise IntegrationError(400, 'La autorización de Gmail venció o ya se usó.') from None
        raise
    item = result.get('Attributes') or {}
    if item.get('user_id') != user_id or not item.get('verifier'):
        raise IntegrationError(400, 'La autorización de Gmail no es válida.')
    token = _safe_json(requests.post(GOOGLE_TOKEN, data={'client_id': config['client_id'], 'client_secret': config['client_secret'],
        'redirect_uri': callback, 'code': code, 'code_verifier': item['verifier'], 'grant_type': 'authorization_code'}, timeout=10))
    if not isinstance(token.get('refresh_token'), str) or not token['refresh_token']:
        raise IntegrationError(400, 'Google no devolvió acceso permanente; volvé a conectar Gmail.')
    if GMAIL_SCOPE not in str(token.get('scope', '')).split():
        raise IntegrationError(400, 'No se autorizó el acceso de lectura a Gmail.')
    if not isinstance(token.get('access_token'), str) or not token['access_token']:
        raise IntegrationError(400, 'Google no devolvió acceso a Gmail.')
    profile = _safe_json(requests.get('https://gmail.googleapis.com/gmail/v1/users/me/profile',
                                      headers={'Authorization': f"Bearer {token['access_token']}"}, timeout=8))
    address = profile.get('emailAddress')
    if not isinstance(address, str) or not re.fullmatch(r'[^\s@]{1,128}@[^\s@]{1,128}', address):
        raise IntegrationError(502, 'No pudimos identificar la cuenta de Gmail autorizada.')
    account_hash = hashlib.sha256(address.strip().lower().encode()).hexdigest()
    previous = _connection(table, user_id, 'gmail')
    arn = _new_secret('gmail', {'refresh_token': token['refresh_token']})
    try:
        table.put_item(Item={'pk': f'USER#{user_id}', 'sk': 'CONNECTION#gmail', 'secret_arn': arn, 'account_hash': account_hash, 'status': 'connected', 'connected_at': int(time.time())})
    except Exception:
        _delete_secret(arn)
        raise
    if previous.get('secret_arn'):
        try:
            _delete_secret(previous['secret_arn'])
        except ClientError:
            pass
    return {'status': 'connected'}


def status(table, user_id):
    result = {}
    for provider in ('gmail', 'binance'):
        item = _connection(table, user_id, provider)
        result[provider] = {'connected': bool(item.get('secret_arn')), 'last_sync_at': int(item['last_sync_at']) if item.get('last_sync_at') is not None else None,
                            'has_more': bool(item.get('next_page_token')) if provider == 'gmail' else False}
    return result


def disconnect(table, user_id, provider):
    if provider not in ('gmail', 'binance'):
        raise IntegrationError(400, 'Conexión inválida.')
    item = _connection(table, user_id, provider)
    if provider == 'gmail' and item.get('secret_arn'):
        credential = _read_secret(item['secret_arn'])
        result = requests.post('https://oauth2.googleapis.com/revoke', data={'token': credential['refresh_token']}, timeout=8)
        if result.status_code >= 300 and result.status_code != 400:
            raise IntegrationError(502, 'No se pudo revocar Gmail. Probá nuevamente.')
    if item.get('secret_arn'):
        _delete_secret(item['secret_arn'])
        table.delete_item(Key={'pk': f'USER#{user_id}', 'sk': f'CONNECTION#{provider}'},
                          ConditionExpression='secret_arn = :arn', ExpressionAttributeValues={':arn': item['secret_arn']})
    return {'connected': False}


def _gmail_access_token(secret_arn):
    config, _ = _config()
    secret = _read_secret(secret_arn)
    result = requests.post(GOOGLE_TOKEN, data={'client_id': config['client_id'], 'client_secret': config['client_secret'],
        'refresh_token': secret['refresh_token'], 'grant_type': 'refresh_token'}, timeout=6)
    if result.status_code in (400, 401):
        raise IntegrationError(409, 'La autorización de Gmail venció. Desconectá y volvé a conectar la cuenta.')
    data = _safe_json(result)
    if not isinstance(data.get('access_token'), str):
        raise IntegrationError(502, 'No se pudo renovar la conexión de Gmail.')
    return data['access_token']


def sync_gmail(table, user_id):
    connection = _connection(table, user_id, 'gmail')
    if not connection.get('secret_arn') or not re.fullmatch(r'[a-f0-9]{64}', str(connection.get('account_hash', ''))):
        raise IntegrationError(400, 'Conectá Gmail primero.')
    token = _gmail_access_token(connection['secret_arn'])
    headers = {'Authorization': f'Bearer {token}'}
    params = {'q': 'from:mensajesyavisos@mails.santander.com.ar {subject:Pagaste subject:"Aviso de débito automático"} newer_than:90d', 'maxResults': 3}
    if connection.get('next_page_token'):
        params['pageToken'] = connection['next_page_token']
    listing = _safe_json(requests.get(GMAIL_API, headers=headers, params=params, timeout=3))
    imported = 0
    for row in listing.get('messages', []):
        msg_id = row.get('id') if isinstance(row, dict) else None
        if not isinstance(msg_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', msg_id):
            continue
        data = _safe_json(requests.get(f'{GMAIL_API}/{msg_id}', headers=headers, params={'format': 'full'}, timeout=3))
        expense = parse_santander_payment(data)
        if expense and expense['source_id'] == msg_id:
            table.put_item(Item={'pk': f'USER#{user_id}', 'sk': f"GMAIL_EXPENSE#{connection['account_hash']}#{msg_id}", **expense})
            imported += 1
    next_page = listing.get('nextPageToken', '')
    if not isinstance(next_page, str) or len(next_page) > 1024:
        raise IntegrationError(502, 'Gmail devolvió una página inválida.')
    if next_page and next_page == connection.get('next_page_token'):
        raise IntegrationError(502, 'Gmail no avanzó a la siguiente página. Reintentá.')
    table.update_item(Key={'pk': f'USER#{user_id}', 'sk': 'CONNECTION#gmail'},
        UpdateExpression='SET last_sync_at = :now, next_page_token = :page',
        ConditionExpression='secret_arn = :arn AND (attribute_not_exists(next_page_token) OR next_page_token = :previous)',
        ExpressionAttributeValues={':now': int(time.time()), ':page': next_page, ':arn': connection['secret_arn'], ':previous': connection.get('next_page_token', '')})
    return {'imported': imported, 'has_more': bool(next_page)}


def gmail_expenses(table, user_id):
    connection = _connection(table, user_id, 'gmail')
    account_hash = connection.get('account_hash', '')
    if not connection.get('secret_arn') or not re.fullmatch(r'[a-f0-9]{64}', str(account_hash)):
        return []
    args = {'KeyConditionExpression': Key('pk').eq(f'USER#{user_id}') & Key('sk').begins_with(f'GMAIL_EXPENSE#{account_hash}#')}
    rows = []
    while True:
        page = table.query(**args)
        rows.extend({key: row[key] for key in ('source_id', 'date', 'merchant', 'amount', 'currency') if key in row} for row in page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            break
        args['ExclusiveStartKey'] = page['LastEvaluatedKey']
    return sorted(rows, key=lambda row: row.get('date', ''), reverse=True)


def _binance_get(key, secret, path, params=None):
    if path not in ('/sapi/v1/account/apiRestrictions', '/api/v3/account', '/api/v3/myTrades', '/sapi/v1/capital/deposit/hisrec', '/sapi/v1/capital/withdraw/history'):
        raise IntegrationError(400, 'Consulta de Binance inválida.')
    bridge = os.environ.get('BINANCE_REGIONAL_FUNCTION_ARN', '')
    if bridge:
        arn = re.fullmatch(r'arn:aws:lambda:([a-z0-9-]+):\d{12}:function:[A-Za-z0-9_-]+', bridge)
        if not arn:
            raise IntegrationError(503, 'La conexión regional de Binance no está configurada correctamente.')
        result = boto3.client('lambda', region_name=arn.group(1), config=Config(connect_timeout=2, read_timeout=8, retries={'max_attempts': 0})).invoke(
            FunctionName=bridge, InvocationType='RequestResponse',
            Payload=json.dumps({'path': path, 'params': params or {}, 'signed': True, 'api_key': key, 'api_secret': secret}).encode())
        if result.get('FunctionError'):
            raise IntegrationError(502, 'No se pudo consultar Binance.')
        raw = result['Payload'].read(1_000_001)
        if len(raw) > 1_000_000:
            raise IntegrationError(502, 'La respuesta de Binance es demasiado grande.')
        payload = json.loads(raw)
        if not isinstance(payload, dict) or int(payload.get('status', 502)) >= 400 or not isinstance(payload.get('payload'), (dict, list)):
            raise IntegrationError(400, 'Binance rechazó la clave, sus permisos o la ubicación de acceso.')
        return payload['payload']
    import hmac
    query = urlencode({**(params or {}), 'timestamp': int(time.time() * 1000), 'recvWindow': 5000})
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    response = requests.get(f'{BINANCE_URL}{path}?{query}&signature={signature}', headers={'X-MBX-APIKEY': key}, timeout=8)
    data = response.json()
    if response.status_code >= 400 or not isinstance(data, (dict, list)):
        raise IntegrationError(502, 'No se pudo consultar Binance.')
    return data


def connect_binance(table, user_id, payload):
    try:
        credentials = BinanceCredentials.model_validate(payload)
    except ValidationError:
        raise IntegrationError(400, 'Ingresá la API Key y el Secret de Binance.')
    key, secret = credentials.api_key.get_secret_value(), credentials.api_secret.get_secret_value()
    try:
        validate_binance_read_only(_binance_get(key, secret, '/sapi/v1/account/apiRestrictions'))
    except ValueError as exc:
        raise IntegrationError(400, str(exc)) from None
    previous = _connection(table, user_id, 'binance')
    arn = _new_secret('binance', {'api_key': key, 'api_secret': secret})
    try:
        table.put_item(Item={'pk': f'USER#{user_id}', 'sk': 'CONNECTION#binance', 'secret_arn': arn, 'status': 'connected', 'connected_at': int(time.time())})
    except Exception:
        _delete_secret(arn)
        raise
    if previous.get('secret_arn'):
        try:
            _delete_secret(previous['secret_arn'])
        except ClientError:
            pass
    return {'connected': True}


def binance_balances(table, user_id):
    connection = _connection(table, user_id, 'binance')
    if not connection.get('secret_arn'):
        raise IntegrationError(400, 'Conectá Binance primero.')
    credential = _read_secret(connection['secret_arn'])
    try:
        validate_binance_read_only(_binance_get(credential['api_key'], credential['api_secret'], '/sapi/v1/account/apiRestrictions'))
    except ValueError as exc:
        raise IntegrationError(403, str(exc)) from None
    account = _binance_get(credential['api_key'], credential['api_secret'], '/api/v3/account')
    balances = []
    for row in account.get('balances', []):
        if not isinstance(row, dict) or not re.fullmatch(r'[A-Z0-9]{1,20}', str(row.get('asset', ''))):
            continue
        try:
            free, locked = Decimal(str(row.get('free', '0'))), Decimal(str(row.get('locked', '0')))
            if not free.is_finite() or not locked.is_finite() or free < 0 or locked < 0:
                continue
            if free + locked:
                balances.append({'asset': row['asset'], 'free': str(free), 'locked': str(locked)})
        except InvalidOperation:
            continue
    return {'balances': balances}


def binance_movements(table, user_id, kind, offset):
    paths = {'deposit': '/sapi/v1/capital/deposit/hisrec', 'withdrawal': '/sapi/v1/capital/withdraw/history'}
    try:
        request = BinanceMovementRequest.model_validate({'kind': kind, 'offset': offset})
    except ValidationError:
        raise IntegrationError(400, 'Solicitud de movimientos inválida.') from None
    kind, offset = request.kind, request.offset
    connection = _connection(table, user_id, 'binance')
    if not connection.get('secret_arn'):
        raise IntegrationError(400, 'Conectá Binance primero.')
    credential = _read_secret(connection['secret_arn'])
    try:
        validate_binance_read_only(_binance_get(credential['api_key'], credential['api_secret'], '/sapi/v1/account/apiRestrictions'))
    except ValueError as exc:
        raise IntegrationError(403, str(exc)) from None
    page_size = 500
    rows = _binance_get(credential['api_key'], credential['api_secret'], paths[kind], {'limit': page_size, 'offset': offset})
    if not isinstance(rows, list):
        raise IntegrationError(502, 'Binance devolvió movimientos inválidos.')
    items = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = str(row.get('coin', ''))
        amount = str(row.get('amount', ''))
        if not re.fullmatch(r'[A-Z0-9]{1,20}', asset) or not re.fullmatch(r'\d{1,30}(?:\.\d{1,18})?', amount):
            continue
        date_value = row.get('insertTime') if kind == 'deposit' else row.get('applyTime')
        status_value = row.get('status')
        items.append({'asset': asset, 'amount': amount, 'date': str(date_value or ''), 'kind': kind,
                      'status': int(status_value) if isinstance(status_value, int) else None})
    return {'items': items, 'next_offset': offset + len(rows) if len(rows) == page_size else None}


def binance_trades(table, user_id, symbol):
    try:
        request = BinanceTradeRequest.model_validate({'symbol': symbol})
    except ValidationError:
        raise IntegrationError(400, 'Ingresá un par válido, por ejemplo BTCUSDT.') from None
    symbol = request.symbol
    connection = _connection(table, user_id, 'binance')
    if not connection.get('secret_arn'):
        raise IntegrationError(400, 'Conectá Binance primero.')
    credential = _read_secret(connection['secret_arn'])
    try:
        validate_binance_read_only(_binance_get(credential['api_key'], credential['api_secret'], '/sapi/v1/account/apiRestrictions'))
    except ValueError as exc:
        raise IntegrationError(403, str(exc)) from None
    rows = _binance_get(credential['api_key'], credential['api_secret'], '/api/v3/myTrades', {'symbol': symbol, 'limit': 1000})
    if not isinstance(rows, list):
        raise IntegrationError(502, 'Binance devolvió operaciones inválidas.')
    items = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        quantity, price = str(row.get('qty', '')), str(row.get('price', ''))
        if not all(re.fullmatch(r'\d{1,30}(?:\.\d{1,18})?', value) for value in (quantity, price)):
            continue
        timestamp = row.get('time')
        if not isinstance(timestamp, int) or not isinstance(row.get('isBuyer'), bool):
            continue
        items.append({'quantity': quantity, 'price': price, 'time': timestamp, 'side': 'buy' if row['isBuyer'] else 'sell'})
    return {'symbol': symbol, 'items': items, 'limited': len(rows) == 1000}
