"""Monthly inflation (IPC) from Argentina Datos.

Source: https://api.argentinadatos.com/v1/finanzas/indices/inflacion (public, no key).
Each row is one month with its published percentage. The series is cached in
memory for six hours; the request carries no personal data.
"""
from __future__ import annotations

import json
import time
import urllib.request
from typing import Any

SOURCE_URL = 'https://api.argentinadatos.com/v1/finanzas/indices/inflacion'
CACHE_SECONDS = 6 * 60 * 60
TIMEOUT_SECONDS = 8

_cache: dict[str, Any] = {'loaded_at': 0.0, 'series': None}


class InflationError(RuntimeError):
    pass


def parse_monthly(payload: Any) -> dict[str, float]:
    """Monthly percentages keyed by 'YYYY-MM'. Invalid rows are skipped."""
    if not isinstance(payload, list):
        raise InflationError('respuesta inesperada')
    series: dict[str, float] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        try:
            period = str(row['fecha'])[:7]
            value = float(row['valor'])
        except (KeyError, TypeError, ValueError):
            continue
        if len(period) == 7 and period[4] == '-':
            series[period] = value
    if not series:
        raise InflationError('sin datos de inflación')
    return series


def monthly_series(now: float | None = None) -> dict[str, float]:
    current = time.time() if now is None else now
    if _cache['series'] is not None and current - _cache['loaded_at'] < CACHE_SECONDS:
        return _cache['series']
    try:
        request = urllib.request.Request(SOURCE_URL, headers={'User-Agent': 'finance-app/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode('utf-8'))
        series = parse_monthly(payload)
    except (OSError, ValueError, InflationError) as exc:
        if _cache['series'] is not None:
            return _cache['series']
        raise InflationError(type(exc).__name__) from exc
    _cache.update({'loaded_at': current, 'series': series})
    return series
