"""Spending analysis built from the monthly expense rows the warehouse already serves.

Pure functions only: the caller reads BigQuery and passes rows in. Rows are
dicts with ``month_start`` (date/datetime/ISO string), ``merchant``,
``category`` and ``amount`` in ARS.
"""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime
from typing import Any

TOP_CATEGORIES = 8
TOP_ITEMS = 5
BASELINE_MONTHS = 3
SPIKE_PCT = 30.0  # a category this far above its baseline is flagged
MATERIAL_SHARE = 0.03  # ignore movements below 3% of the month's spending


def _month_key(value: Any) -> date | None:
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
    return number if number == number and number > 0 else 0.0  # drop NaN and refunds


def _pct(change: float, base: float) -> float | None:
    return round(change / base * 100, 1) if base > 0 else None


def _label(value: str) -> str:
    return (value or '').strip() or 'Sin clasificar'


def _movement(name: str, current: float, previous: float, baseline: float, total: float) -> dict[str, Any]:
    change = current - previous
    return {
        'name': name,
        'current': round(current, 2),
        'previous': round(previous, 2),
        'change_abs': round(change, 2),
        'change_pct': _pct(change, previous),
        'share': round(current / total, 4) if total > 0 else 0.0,
        'baseline': round(baseline, 2),
        'vs_baseline_pct': _pct(current - baseline, baseline),
    }


def build_insights(rows: list[dict[str, Any]], *, as_of: date) -> dict[str, Any]:
    current_month = date(as_of.year, as_of.month, 1)
    by_month: dict[date, float] = defaultdict(float)
    by_category: dict[date, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    by_merchant: dict[date, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        month = _month_key(row.get('month_start'))
        amount = _amount(row.get('amount'))
        if month is None or amount == 0:
            continue
        by_month[month] += amount
        by_category[month][_label(str(row.get('category') or ''))] += amount
        by_merchant[month][_label(str(row.get('merchant') or ''))] += amount

    closed = sorted(month for month in by_month if month < current_month)[-6:]
    series = [{'month': m.strftime('%Y-%m'), 'total': round(by_month[m], 2), 'partial': False} for m in closed]
    spent_now = by_month.get(current_month, 0.0)
    if spent_now:
        series.append({'month': current_month.strftime('%Y-%m'), 'total': round(spent_now, 2), 'partial': True})

    result: dict[str, Any] = {
        'currency': 'ARS',
        'as_of': as_of.isoformat(),
        'months': series,
        'has_history': len(closed) >= 2,
        'current_month': _current_month(as_of, spent_now, by_month[closed[-1]] if closed else 0.0),
    }
    if len(closed) < 2:
        result.update({'latest': None, 'categories': [], 'merchants_up': [], 'merchants_down': [],
                       'new_merchants': [], 'category_trend': {}, 'insights': []})
        return result

    latest, previous = closed[-1], closed[-2]
    baseline_months = closed[-1 - BASELINE_MONTHS:-1]
    total_latest, total_previous = by_month[latest], by_month[previous]
    baseline_total = sum(by_month[m] for m in baseline_months) / len(baseline_months)
    result['latest'] = {
        'month': latest.strftime('%Y-%m'), 'total': round(total_latest, 2),
        'previous_month': previous.strftime('%Y-%m'), 'previous_total': round(total_previous, 2),
        'change_abs': round(total_latest - total_previous, 2), 'change_pct': _pct(total_latest - total_previous, total_previous),
        'baseline_total': round(baseline_total, 2), 'vs_baseline_pct': _pct(total_latest - baseline_total, baseline_total),
    }

    def baseline_for(table: dict[date, dict[str, float]], name: str) -> float:
        return sum(table[m].get(name, 0.0) for m in baseline_months) / len(baseline_months)

    names = set(by_category[latest]) | set(by_category[previous])
    categories = [_movement(n, by_category[latest].get(n, 0.0), by_category[previous].get(n, 0.0),
                            baseline_for(by_category, n), total_latest) for n in names]
    categories.sort(key=lambda item: item['current'], reverse=True)
    result['categories'] = categories[:TOP_CATEGORIES]

    names = set(by_merchant[latest]) | set(by_merchant[previous])
    merchants = [_movement(n, by_merchant[latest].get(n, 0.0), by_merchant[previous].get(n, 0.0),
                           baseline_for(by_merchant, n), total_latest) for n in names]
    result['merchants_up'] = sorted((m for m in merchants if m['change_abs'] > 0),
                                    key=lambda m: m['change_abs'], reverse=True)[:TOP_ITEMS]
    result['merchants_down'] = sorted((m for m in merchants if m['change_abs'] < 0),
                                      key=lambda m: m['change_abs'])[:TOP_ITEMS]
    seen_before = set().union(*(set(by_merchant[m]) for m in closed[:-1]))
    result['new_merchants'] = sorted((m for m in merchants if m['current'] > 0 and m['name'] not in seen_before),
                                     key=lambda m: m['current'], reverse=True)[:TOP_ITEMS]

    window_totals = {name: sum(by_category[m].get(name, 0.0) for m in closed)
                     for name in {n for m in closed for n in by_category[m]}}
    top_names = sorted(window_totals, key=window_totals.get, reverse=True)[:TOP_ITEMS]
    result['category_trend'] = {name: [round(by_category[m].get(name, 0.0), 2) for m in closed] for name in top_names}
    result['trend_months'] = [m.strftime('%Y-%m') for m in closed]
    result['insights'] = _insights(result, total_latest)
    return result


def _current_month(as_of: date, spent: float, reference_total: float) -> dict[str, Any]:
    days_in_month = calendar.monthrange(as_of.year, as_of.month)[1]
    elapsed = max(as_of.day, 1)
    projected = round(spent / elapsed * days_in_month, 2) if spent else 0.0
    return {
        'month': as_of.strftime('%Y-%m'), 'spent': round(spent, 2), 'days_elapsed': elapsed,
        'days_in_month': days_in_month, 'projected': projected,
        'vs_latest_closed_pct': _pct(projected - reference_total, reference_total) if reference_total else None,
    }


def _insights(result: dict[str, Any], total_latest: float) -> list[dict[str, Any]]:
    """Short, ranked findings. Each has a kind, a severity and the numbers behind it."""
    items: list[dict[str, Any]] = []
    latest = result['latest']
    if latest['change_pct'] is not None and abs(latest['change_pct']) >= 5:
        items.append({'kind': 'total', 'severity': 'up' if latest['change_abs'] > 0 else 'down',
                      'month': latest['month'], 'change_abs': latest['change_abs'], 'change_pct': latest['change_pct']})
    for category in result['categories']:
        material = category['current'] >= total_latest * MATERIAL_SHARE
        if material and category['vs_baseline_pct'] is not None and category['vs_baseline_pct'] >= SPIKE_PCT:
            items.append({'kind': 'category_spike', 'severity': 'up', 'name': category['name'],
                          'current': category['current'], 'baseline': category['baseline'],
                          'change_pct': category['vs_baseline_pct']})
    if result['merchants_up']:
        top = result['merchants_up'][0]
        if abs(top['change_abs']) >= total_latest * MATERIAL_SHARE:
            items.append({'kind': 'merchant_up', 'severity': 'up', 'name': top['name'],
                          'change_abs': top['change_abs'], 'change_pct': top['change_pct']})
    if result['new_merchants']:
        top = result['new_merchants'][0]
        if top['current'] >= total_latest * MATERIAL_SHARE:
            items.append({'kind': 'new_merchant', 'severity': 'info', 'name': top['name'], 'current': top['current']})
    pace = result['current_month']
    if pace['vs_latest_closed_pct'] is not None and pace['days_elapsed'] >= 5 and abs(pace['vs_latest_closed_pct']) >= 10:
        items.append({'kind': 'pace', 'severity': 'up' if pace['vs_latest_closed_pct'] > 0 else 'down',
                      'projected': pace['projected'], 'change_pct': pace['vs_latest_closed_pct']})
    return items
