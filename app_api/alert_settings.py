"""Owner-level settings for variation alerts: thresholds and delivery channels.

Thresholds live in the same BigQuery row the Telegram bot reads
(alert_variation_settings, id='default'). Channel switches live in the
application table as one owner item, which the bot Lambda also reads.
Everything here is pure: the caller performs the reads and writes.
"""
from __future__ import annotations

import re
from typing import Any

PREFS_PK = 'NOTIFICATIONS#PRIMARY'
PREFS_SK = 'PREFS'
SETTINGS_TABLE = 'alert_variation_settings'
CHANNELS = ('telegram', 'whatsapp')
PERCENT_RANGE = (0.5, 100.0)
ARS_RANGE = (1.0, 1_000_000_000.0)
_ALLOWED_KEYS = {'percent_threshold', 'absolute_threshold', 'telegram_enabled', 'whatsapp_enabled'}


def _parse_argentine(text: str) -> float:
    """Accept "1000", "1000,5", "1.000,50" and "1.000" (thousands dot) the way people type them in Argentina."""
    cleaned = text.strip().replace(' ', '')
    if ',' in cleaned:
        cleaned = cleaned.replace('.', '').replace(',', '.')
    elif re.fullmatch(r'\d{1,3}(\.\d{3})+', cleaned):
        cleaned = cleaned.replace('.', '')
    return float(cleaned)


def _number(value: Any, name: str, low: float, high: float) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError(f'"{name}" debe ser un número')
    try:
        number = _parse_argentine(value) if isinstance(value, str) else float(value)
    except ValueError:
        raise ValueError(f'"{name}" debe ser un número') from None
    if number != number or not low <= number <= high:
        raise ValueError(f'"{name}" debe estar entre {low:g} y {high:g}')
    return round(number, 2)


def _flag(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f'"{name}" debe ser verdadero o falso')
    return value


def validate_update(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError('Enviá los datos como un objeto JSON')
    unknown = set(payload) - _ALLOWED_KEYS
    if unknown:
        raise ValueError(f'Campos no permitidos: {", ".join(sorted(unknown))}')
    if not {'percent_threshold', 'absolute_threshold'} <= set(payload):
        raise ValueError('Indicá el porcentaje y el monto mínimo')
    if not {'telegram_enabled', 'whatsapp_enabled'} <= set(payload):
        raise ValueError('Indicá si Telegram y WhatsApp están activos')
    return {
        'percent_threshold': _number(payload['percent_threshold'], 'percent_threshold', *PERCENT_RANGE),
        'absolute_threshold': _number(payload['absolute_threshold'], 'absolute_threshold', *ARS_RANGE),
        'channels': {
            'telegram': _flag(payload['telegram_enabled'], 'telegram_enabled'),
            'whatsapp': _flag(payload['whatsapp_enabled'], 'whatsapp_enabled'),
        },
    }


def channels_from_item(item: dict[str, Any] | None) -> dict[str, bool]:
    """Missing switches default to on, which matches the behaviour before this setting existed."""
    item = item or {}
    return {
        'telegram': item.get('telegram_enabled') is not False,
        'whatsapp': item.get('whatsapp_enabled') is not False,
    }


def thresholds_from_row(row: dict[str, Any] | None, defaults: tuple[float, float]) -> dict[str, float]:
    if not row or row.get('percent_threshold') is None or row.get('absolute_threshold') is None:
        return {'percent_threshold': defaults[0], 'absolute_threshold': defaults[1], 'source': 'default'}
    return {'percent_threshold': float(row['percent_threshold']), 'absolute_threshold': float(row['absolute_threshold']),
            'source': 'stored'}


def read_thresholds_sql(project: str, dataset: str) -> str:
    return f"SELECT percent_threshold, absolute_threshold, updated_at, updated_by FROM `{project}.{dataset}.{SETTINGS_TABLE}` WHERE id = 'default' LIMIT 1"


def upsert_thresholds_sql(project: str, dataset: str) -> str:
    return f"""
    MERGE `{project}.{dataset}.{SETTINGS_TABLE}` T
    USING (SELECT 'default' AS id) S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET percent_threshold = @percent, absolute_threshold = @absolute,
                 updated_at = CURRENT_TIMESTAMP(), updated_by = @updated_by
    WHEN NOT MATCHED THEN
      INSERT (id, percent_threshold, absolute_threshold, updated_at, updated_by)
      VALUES ('default', @percent, @absolute, CURRENT_TIMESTAMP(), @updated_by)
    """
