"""Period comparisons, monthly evolution and payment-method share.

Pure functions: the caller reads BigQuery and passes rows in. Expense rows have
``month_start``, ``merchant``, ``category`` and ``amount`` (ARS). Payment rows
have ``month_start``, ``method`` and ``amount`` (ARS).

Comparisons use the current month as spent so far, against the average monthly
spend of the previous 3 and 6 closed months. The current month is partial, so
its figures are shown next to a projection and are not judged as final.
"""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime
from typing import Any

TOP_ITEMS = 15
TOP_PER_MONTH = 3
EVOLUTION_MONTHS = 12


def _month(value: Any) -> date | None:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value[:10])
        except ValueError:
            return None
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    return None


def _amount(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if number == number and number > 0 else 0.0


def _label(value: Any, fallback: str) -> str:
    text = str(value or '').strip()
    return text or fallback


def _pct(part: float, base: float) -> float | None:
    return round(part / base * 100, 1) if base else None


def _shift(month: date, months: int) -> date:
    year, index = divmod(month.year * 12 + month.month - 1 - months, 12)
    return date(year, index + 1, 1)


def _window(current: date, count: int) -> list[date]:
    return [_shift(current, back) for back in range(1, count + 1)]


def _aggregate(rows: list[dict[str, Any]], key: str, fallback: str) -> dict[date, dict[str, float]]:
    table: dict[date, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        month = _month(row.get('month_start'))
        amount = _amount(row.get('amount'))
        if month is None or amount == 0:
            continue
        table[month][_label(row.get(key), fallback)] += amount
    return table


def _compare_dimension(table: dict[date, dict[str, float]], current: date, closed3: list[date], closed6: list[date]) -> dict[str, Any]:
    names = set(table.get(current, {}))
    for month in closed6:
        names |= set(table.get(month, {}))
    now_total = sum(table.get(current, {}).values())
    avg3_total = sum(sum(table.get(m, {}).values()) for m in closed3) / 3 if closed3 else 0.0
    avg6_total = sum(sum(table.get(m, {}).values()) for m in closed6) / 6 if closed6 else 0.0

    items = []
    for name in names:
        now = table.get(current, {}).get(name, 0.0)
        avg3 = sum(table.get(m, {}).get(name, 0.0) for m in closed3) / 3 if closed3 else 0.0
        avg6 = sum(table.get(m, {}).get(name, 0.0) for m in closed6) / 6 if closed6 else 0.0
        share_now = _pct(now, now_total)
        share_avg3 = _pct(avg3, avg3_total)
        share_avg6 = _pct(avg6, avg6_total)
        items.append({
            'name': name,
            'current': round(now, 2), 'avg_3m': round(avg3, 2), 'avg_6m': round(avg6, 2),
            'vs_avg_3m_abs': round(now - avg3, 2), 'vs_avg_3m_pct': _pct(now - avg3, avg3),
            'vs_avg_6m_abs': round(now - avg6, 2), 'vs_avg_6m_pct': _pct(now - avg6, avg6),
            'share_current': share_now, 'share_avg_3m': share_avg3, 'share_avg_6m': share_avg6,
            'share_vs_avg_3m_pp': None if share_now is None or share_avg3 is None else round(share_now - share_avg3, 1),
        })
    items.sort(key=lambda item: (item['current'], item['avg_6m']), reverse=True)
    return {'items': items[:TOP_ITEMS], 'totals': {
        'current': round(now_total, 2), 'avg_3m': round(avg3_total, 2), 'avg_6m': round(avg6_total, 2)}}


def compare_periods(rows: list[dict[str, Any]], *, as_of: date) -> dict[str, Any]:
    current = date(as_of.year, as_of.month, 1)
    closed = _window(current, 6)
    days = calendar.monthrange(as_of.year, as_of.month)[1]
    categories = _aggregate(rows, 'category', 'Sin categoría')
    merchants = _aggregate(rows, 'merchant', 'Sin comercio')
    closed3, closed6 = closed[:3], closed
    total_now = sum(categories.get(current, {}).values())
    return {
        'as_of': as_of.isoformat(),
        'partial_month': current.strftime('%Y-%m'),
        'days_elapsed': as_of.day,
        'days_in_month': days,
        'projected_total': round(total_now / max(as_of.day, 1) * days, 2) if total_now else 0.0,
        'windows': {'current': current.strftime('%Y-%m'),
                    'avg_3m': [m.strftime('%Y-%m') for m in closed3], 'avg_6m': [m.strftime('%Y-%m') for m in closed6]},
        'categories': _compare_dimension(categories, current, closed3, closed6),
        'merchants': _compare_dimension(merchants, current, closed3, closed6),
    }


def evolution(rows: list[dict[str, Any]], *, as_of: date, months: int = EVOLUTION_MONTHS) -> dict[str, Any]:
    current = date(as_of.year, as_of.month, 1)
    series_months = sorted(_window(current, months - 1) + [current]) if months > 1 else [current]
    categories = _aggregate(rows, 'category', 'Sin categoría')
    merchants = _aggregate(rows, 'merchant', 'Sin comercio')
    totals = {m: sum(categories.get(m, {}).values()) for m in series_months}
    series = []
    for month in series_months:
        total = totals[month]
        top_c = sorted(categories.get(month, {}).items(), key=lambda kv: kv[1], reverse=True)[:TOP_PER_MONTH]
        top_m = sorted(merchants.get(month, {}).items(), key=lambda kv: kv[1], reverse=True)[:TOP_PER_MONTH]
        series.append({
            'month': month.strftime('%Y-%m'), 'partial': month == current,
            'total': round(total, 2),
            'share_of_window': _pct(total, sum(totals.values())),
            'top_categories': [{'name': n, 'amount': round(a, 2), 'share': _pct(a, total)} for n, a in top_c],
            'top_merchants': [{'name': n, 'amount': round(a, 2), 'share': _pct(a, total)} for n, a in top_m],
        })
    return {'months': series, 'window_total': round(sum(totals.values()), 2)}


def payment_share(rows: list[dict[str, Any]], *, as_of: date, months: int = 6) -> dict[str, Any]:
    """Share of spending by payment method over the last closed months plus the current month."""
    current = date(as_of.year, as_of.month, 1)
    window = set(_window(current, months)) | {current}
    by_method: dict[str, float] = defaultdict(float)
    by_month: dict[date, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        month = _month(row.get('month_start'))
        amount = _amount(row.get('amount'))
        if month is None or month not in window or amount == 0:
            continue
        method = _label(row.get('method'), 'Sin dato')
        by_method[method] += amount
        by_month[month][method] += amount
    total = sum(by_method.values())
    ranking = sorted(by_method.items(), key=lambda kv: kv[1], reverse=True)
    return {
        'window_total': round(total, 2),
        'methods': [{'name': n, 'amount': round(a, 2), 'share': _pct(a, total)} for n, a in ranking],
        'by_month': [{'month': m.strftime('%Y-%m'), 'partial': m == current,
                      'methods': [{'name': n, 'amount': round(a, 2), 'share': _pct(a, sum(by_month[m].values()))}
                                  for n, a in sorted(by_month[m].items(), key=lambda kv: kv[1], reverse=True)]}
                     for m in sorted(by_month)],
    }
