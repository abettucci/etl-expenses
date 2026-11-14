"""Official USD to ARS rate from Argentina Datos.

Source: https://api.argentinadatos.com/v1/cotizaciones/dolares (public, no key).
The official rate is the house "oficial". We convert with its selling price
("venta"), the price a person pays to buy dollars.

Each expense uses the latest quote published on or before its date, so weekends
and holidays use the last business day's rate. Quotes are cached in memory for
a few hours; the request carries no personal data.
"""
from __future__ import annotations

import bisect
import json
import time
import urllib.request
from datetime import date, datetime
from typing import Any

SOURCE_URL = 'https://api.argentinadatos.com/v1/cotizaciones/dolares'
OFFICIAL_HOUSE = 'oficial'
CACHE_SECONDS = 6 * 60 * 60
TIMEOUT_SECONDS = 8

_cache: dict[str, Any] = {'loaded_at': 0.0, 'series': None}


class FxError(RuntimeError):
    pass


def parse_official(payload: Any) -> list[tuple[date, float]]:
    """Official quotes as (date, venta), sorted by date, skipping invalid rows."""
    if not isinstance(payload, list):
        raise FxError('respuesta inesperada')
    series: dict[date, float] = {}
    for row in payload:
        if not isinstance(row, dict) or row.get('casa') != OFFICIAL_HOUSE:
            continue
        try:
            day = date.fromisoformat(str(row['fecha'])[:10])
            venta = float(row['venta'])
        except (KeyError, TypeError, ValueError):
            continue
        if venta > 0:
            series[day] = venta
    if not series:
        raise FxError('sin cotizaciones oficiales')
    return sorted(series.items())


def rate_on(day: date, series: list[tuple[date, float]]) -> tuple[float, date]:
    """Latest official selling rate on or before ``day``. Falls back to the first quote for earlier dates."""
    dates = [d for d, _ in series]
    index = bisect.bisect_right(dates, day) - 1
    if index < 0:
        index = 0
    quote_day, venta = series[index]
    return venta, quote_day


def official_series(now: float | None = None) -> list[tuple[date, float]]:
    """Cached series. Raises FxError when the source cannot be read and nothing is cached."""
    current = time.time() if now is None else now
    if _cache['series'] is not None and current - _cache['loaded_at'] < CACHE_SECONDS:
        return _cache['series']
    try:
        request = urllib.request.Request(SOURCE_URL, headers={'User-Agent': 'finance-app/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode('utf-8'))
        series = parse_official(payload)
    except (OSError, ValueError, FxError) as exc:
        if _cache['series'] is not None:
            return _cache['series']
        raise FxError(type(exc).__name__) from exc
    _cache.update({'loaded_at': current, 'series': series})
    return series


def convert_rows(rows: list[dict[str, Any]], series: list[tuple[date, float]], *,
                 day_key: str = 'expense_date', currency_key: str = 'divisa', amount_key: str = 'amount') -> dict[str, Any]:
    """Return rows with an ARS amount. ARS rows are kept as they are; USD rows use the rate of their day.

    Rows in any other currency are not converted and are counted under ``skipped``.
    """
    out: list[dict[str, Any]] = []
    used: list[date] = []
    converted_usd = 0.0
    skipped = 0
    for row in rows:
        currency = str(row.get(currency_key) or '').upper()
        amount = float(row.get(amount_key) or 0)
        day = row.get(day_key)
        if isinstance(day, datetime):
            day = day.date()
        if isinstance(day, str):
            day = date.fromisoformat(day[:10])
        if currency == 'ARS':
            out.append({**row, 'amount_ars': amount})
        elif currency == 'USD' and isinstance(day, date):
            venta, quote_day = rate_on(day, series)
            used.append(quote_day)
            converted_usd += amount * venta
            out.append({**row, 'amount_ars': amount * venta, 'fx_rate': venta})
        else:
            skipped += 1
    return {
        'rows': out,
        'skipped': skipped,
        'usd_converted_ars': round(converted_usd, 2),
        'quotes_used': {'first': min(used).isoformat(), 'last': max(used).isoformat()} if used else None,
        'source': {'name': 'Argentina Datos', 'house': OFFICIAL_HOUSE, 'field': 'venta', 'url': SOURCE_URL},
    }
