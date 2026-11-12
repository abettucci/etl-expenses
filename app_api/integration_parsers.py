"""Small, deterministic parsers for user-owned financial connections.

The caller must supply the authenticated user partition separately. These
helpers never log or retain provider credentials or full email contents.
"""
from __future__ import annotations

import base64
from datetime import datetime
from decimal import Decimal, InvalidOperation
from email.utils import parseaddr
from html.parser import HTMLParser
import re
from typing import Any


_SANTANDER_SENDER = 'mensajesyavisos@mails.santander.com.ar'
_UNSAFE_BINANCE_PERMISSIONS = (
    'enableSpotAndMarginTrading', 'enableWithdrawals', 'enableInternalTransfer',
    'permitsUniversalTransfer', 'enableMargin', 'enableFutures',
    'enableVanillaOptions', 'enablePortfolioMarginTrading', 'enablePortfolioMarginProTrading',
)


def validate_binance_read_only(restrictions: Any) -> None:
    """Require affirmative read access and reject trading or withdrawal rights."""
    if not isinstance(restrictions, dict) or restrictions.get('enableReading') is not True:
        raise ValueError('La API Key debe permitir lectura.')
    if any(restrictions.get(key) is not False for key in ('enableWithdrawals', 'enableSpotAndMarginTrading')):
        raise ValueError('No se pudieron verificar los permisos de retiros y trading.')
    if any(restrictions.get(key, False) is not False for key in _UNSAFE_BINANCE_PERMISSIONS):
        raise ValueError('La API Key debe ser de solo lectura, sin trading ni retiros.')


class _Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {'script', 'style'}:
            self.hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {'script', 'style'} and self.hidden_depth:
            self.hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.hidden_depth:
            return
        value = ' '.join(data.split())
        if value:
            self.parts.append(value)


def _html_part(payload: dict, depth: int = 0) -> str:
    if depth > 10 or not isinstance(payload, dict):
        return ''
    if payload.get('mimeType') == 'text/html':
        value = (payload.get('body') or {}).get('data', '')
        if isinstance(value, str) and len(value) <= 2_000_000:
            try:
                return base64.urlsafe_b64decode(value + '=' * (-len(value) % 4)).decode('utf-8', errors='replace')
            except (ValueError, TypeError):
                return ''
    for part in payload.get('parts') or []:
        result = _html_part(part, depth + 1)
        if result:
            return result
    return ''


def parse_santander_payment(message: Any) -> dict[str, str] | None:
    """Extract a Santander card payment, or skip an unsupported email."""
    if not isinstance(message, dict):
        return None
    message_id = message.get('id')
    payload = message.get('payload')
    if not isinstance(message_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', message_id) or not isinstance(payload, dict):
        return None
    headers = {str(item.get('name', '')).lower(): str(item.get('value', '')) for item in payload.get('headers', []) if isinstance(item, dict)}
    sender = parseaddr(headers.get('from', ''))[1].lower()
    subject = headers.get('subject', '')
    if sender != _SANTANDER_SENDER or not any(text in subject for text in ('Pagaste', 'Aviso de débito automático')):
        return None
    parser = _Text()
    parser.feed(_html_part(payload))
    values = parser.parts
    def field(label: str) -> str:
        try:
            return values[values.index(label) + 1]
        except (ValueError, IndexError):
            return ''
    raw_amount, raw_date, merchant = field('Monto'), field('Fecha'), field('Comercio')
    match = re.fullmatch(r'\s*(U\$S|USD|US\$|ARS\$|AR\$|\$)\s*((?:\d{1,3}(?:\.\d{3}){1,3}|\d{1,12})(?:,\d{1,2})?)\s*', raw_amount, re.I)
    if not match or not merchant or len(merchant) > 300:
        return None
    try:
        amount = Decimal(match.group(2).replace('.', '').replace(',', '.'))
        date = datetime.strptime(raw_date, '%d/%m/%Y').date().isoformat()
        if not amount.is_finite() or amount <= 0:
            return None
        normalized_amount = format(amount.quantize(Decimal('.01')), '.2f')
    except (ValueError, InvalidOperation):
        return None
    return {'source_id': message_id, 'date': date, 'merchant': merchant,
            'amount': normalized_amount,
            'currency': 'USD' if match.group(1).upper() in {'U$S', 'USD', 'US$'} else 'ARS'}
