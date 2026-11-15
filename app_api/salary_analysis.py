"""Salary history from payslips, compared with monthly inflation.

Pure functions. Inputs are payslip summaries (period "YYYY-MM", gross, net) and
the monthly inflation series as {"YYYY-MM": percent}. Percentages are returned
as numbers rounded to one decimal.

Changes are measured between consecutive payslips. When months are missing in
between, the change covers the whole span and the inflation of every month in
the span is compounded. A raise is a gross change above RAISE_THRESHOLD_PCT.
"""
from __future__ import annotations

from typing import Any

RAISE_THRESHOLD_PCT = 0.5


def _months_between(earlier: str, later: str) -> int:
    y1, m1 = int(earlier[:4]), int(earlier[5:7])
    y2, m2 = int(later[:4]), int(later[5:7])
    return (y2 - y1) * 12 + (m2 - m1)


def _next_month(period: str) -> str:
    year, month = int(period[:4]), int(period[5:7])
    year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return f'{year:04d}-{month:02d}'


def _pct(part: float, base: float) -> float | None:
    return round((part / base - 1) * 100, 1) if base else None


def inflation_between(start_exclusive: str, end_inclusive: str, inflation: dict[str, float]) -> float | None:
    """Compounded inflation for the months after start up to and including end, in percent.

    Returns None when any month of the span is missing from the series, so a gap never
    shows a misleading comparison.
    """
    factor = 1.0
    period = _next_month(start_exclusive)
    while True:
        if period not in inflation:
            return None
        factor *= 1 + inflation[period] / 100
        if period == end_inclusive:
            break
        period = _next_month(period)
    return round((factor - 1) * 100, 1)


def analyze(payslips: list[dict[str, Any]], inflation: dict[str, float], *, year: int | None = None) -> dict[str, Any]:
    slips = sorted(
        ({'period': p['period'], 'gross': float(p['gross']), 'net': float(p['net'])} for p in payslips if p.get('period')),
        key=lambda s: s['period'])
    steps: list[dict[str, Any]] = []
    for previous, current in zip(slips, slips[1:]):
        inflation_pct = inflation_between(previous['period'], current['period'], inflation)
        gross_pct = _pct(current['gross'], previous['gross'])
        net_pct = _pct(current['net'], previous['net'])
        real_pct = None
        if gross_pct is not None and inflation_pct is not None:
            real_pct = round(((1 + gross_pct / 100) / (1 + inflation_pct / 100) - 1) * 100, 1)
        steps.append({
            'from': previous['period'], 'to': current['period'], 'months': _months_between(previous['period'], current['period']),
            'gross': current['gross'], 'net': current['net'],
            'gross_change_pct': gross_pct, 'net_change_pct': net_pct,
            'inflation_pct': inflation_pct, 'real_change_pct': real_pct,
            'beat_inflation': None if real_pct is None else real_pct > 0,
            'raise': gross_pct is not None and gross_pct > RAISE_THRESHOLD_PCT,
        })

    raises = [s for s in steps if s['raise']]
    largest = max(raises, key=lambda s: s['gross_change_pct']) if raises else None

    def summary(window: list[dict[str, Any]], first: dict[str, Any], last: dict[str, Any]) -> dict[str, Any]:
        # One payslip is not a trend: growth is only reported between two different payslips.
        if first is last:
            return {'first_period': first['period'], 'last_period': last['period'], 'first_gross': first['gross'],
                    'last_gross': last['gross'], 'first_net': first['net'], 'last_net': last['net'],
                    'gross_growth_pct': None, 'inflation_pct': None, 'real_growth_pct': None,
                    'raises_count': 0, 'largest_raise': None, 'raise_months': [], 'months_beating_inflation': 0,
                    'months_compared': 0}
        growth = _pct(last['gross'], first['gross'])
        inflation_pct = inflation_between(first['period'], last['period'], inflation)
        real = None
        if growth is not None and inflation_pct is not None:
            real = round(((1 + growth / 100) / (1 + inflation_pct / 100) - 1) * 100, 1)
        window_raises = [s for s in window if s['raise']]
        return {
            'first_period': first['period'], 'last_period': last['period'],
            'first_gross': first['gross'], 'last_gross': last['gross'],
            'first_net': first['net'], 'last_net': last['net'],
            'gross_growth_pct': growth, 'inflation_pct': inflation_pct, 'real_growth_pct': real,
            'raises_count': len(window_raises),
            'largest_raise': max(window_raises, key=lambda s: s['gross_change_pct']) if window_raises else None,
            'raise_months': [s['to'] for s in window_raises],
            'months_beating_inflation': sum(1 for s in window if s['beat_inflation']),
            'months_compared': len(window),
        }

    result: dict[str, Any] = {
        'count': len(slips),
        'steps': steps,
        'raises': raises,
        'largest_raise': largest,
        'all_time': summary(steps, slips[0], slips[-1]) if slips else None,
        'year': None,
        'raise_threshold_pct': RAISE_THRESHOLD_PCT,
    }
    if year is not None and slips:
        in_year = [s for s in slips if s['period'][:4] == str(year)]
        if in_year:
            year_steps = [s for s in steps if s['to'][:4] == str(year)]
            result['year'] = {'year': year, **summary(year_steps, in_year[0], in_year[-1])}
    return result
