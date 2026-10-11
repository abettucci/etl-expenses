"""Read-only monthly spending forecast primitives for the Divi application API.

The module contains no AWS or BigQuery side effects.  It accepts already
aggregated monthly rows and produces a deterministic forecast, which makes the
business logic easy to unit-test independently of cloud credentials.
"""
from __future__ import annotations

import calendar
import re
from collections import defaultdict
from datetime import date
from statistics import mean
from typing import Any, Iterable


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROJECT_IDENTIFIER = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")
_WRITE_SQL = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|EXPORT)\b", re.IGNORECASE)


def _safe_identifier(value: str, name: str, *, project: bool = False) -> str:
    if not (pattern := _PROJECT_IDENTIFIER if project else _IDENTIFIER).fullmatch(value):
        raise ValueError(f"invalid {name} identifier")
    return value


def assert_read_only_query(query: str) -> None:
    """Prevent accidental expansion from this fixed reporting query to writes."""
    normalized = query.strip()
    if not normalized.upper().startswith(("SELECT", "WITH")) or _WRITE_SQL.search(normalized):
        raise ValueError("forecast queries must be predefined read-only SELECT/WITH statements")


def monthly_expenses_query(
    *,
    project_id: str,
    dataset: str,
    gastos_view: str,
    mapping_table: str,
    has_monto_ars: bool,
    use_mapping: bool,
) -> str:
    """Build the only query used by the forecast endpoint.

    Identifiers are configuration values validated before interpolation. Runtime
    values are supplied separately as BigQuery query parameters.
    """
    project_id = _safe_identifier(project_id, "project", project=True)
    dataset = _safe_identifier(dataset, "dataset")
    gastos_view = _safe_identifier(gastos_view, "gastos view")
    mapping_table = _safe_identifier(mapping_table, "mapping table")
    gastos_fqn = f"`{project_id}.{dataset}.{gastos_view}`"
    mapping_fqn = f"`{project_id}.{dataset}.{mapping_table}`"

    expense_date = """
      COALESCE(
        SAFE_CAST(NULLIF(TRIM(CAST(g.fecha AS STRING)), '') AS DATE),
        DATE(SAFE_CAST(NULLIF(TRIM(CAST(g.fecha AS STRING)), '') AS TIMESTAMP)),
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(CAST(g.fecha AS STRING)), ''))
      )
    """
    amount = (
        "COALESCE(SAFE_CAST(g.monto_ars AS FLOAT64), "
        "IF(UPPER(TRIM(CAST(g.divisa AS STRING))) = 'ARS', SAFE_CAST(g.monto_total AS FLOAT64), NULL))"
        if has_monto_ars
        else "IF(UPPER(TRIM(CAST(g.divisa AS STRING))) = 'ARS', SAFE_CAST(g.monto_total AS FLOAT64), NULL)"
    )
    merchant = (
        "COALESCE(NULLIF(TRIM(m.comercio_depurado), ''), NULLIF(TRIM(CAST(g.comercio AS STRING)), ''), 'Sin comercio')"
        if use_mapping
        else "COALESCE(NULLIF(TRIM(CAST(g.comercio AS STRING)), ''), 'Sin comercio')"
    )
    category = (
        "COALESCE(NULLIF(TRIM(m.categoria), ''), NULLIF(TRIM(CAST(g.categoria AS STRING)), ''), 'Sin categorizar')"
        if use_mapping
        else "COALESCE(NULLIF(TRIM(CAST(g.categoria AS STRING)), ''), 'Sin categorizar')"
    )
    mapping_join = f"""
      LEFT JOIN {mapping_fqn} AS m
        ON UPPER(TRIM(CAST(g.comercio AS STRING))) = UPPER(TRIM(m.comercio_raw))
       AND g.extraido_de = m.flow
       AND IFNULL(m.activo, TRUE) = TRUE
    """ if use_mapping else ""

    query = f"""
    WITH normalized_expenses AS (
      SELECT
        {expense_date} AS expense_date,
        {merchant} AS merchant,
        {category} AS category,
        {amount} AS amount
      FROM {gastos_fqn} AS g
      {mapping_join}
    )
    SELECT
      DATE_TRUNC(expense_date, MONTH) AS month_start,
      merchant,
      category,
      SUM(amount) AS amount
    FROM normalized_expenses
    WHERE expense_date BETWEEN @start_date AND @end_date
      AND amount > 0
    GROUP BY month_start, merchant, category
    ORDER BY month_start ASC, amount DESC
    """
    assert_read_only_query(query)
    return query


def _parse_month(value: Any) -> date:
    if isinstance(value, date):
        return value.replace(day=1)
    return date.fromisoformat(str(value)[:10]).replace(day=1)


def _months_between(start: date, end: date) -> list[date]:
    current = start.replace(day=1)
    last = end.replace(day=1)
    months: list[date] = []
    while current <= last:
        months.append(current)
        current = date(current.year + (current.month == 12), 1 if current.month == 12 else current.month + 1, 1)
    return months


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _subtract_months(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 - months
    return date(index // 12, index % 12 + 1, 1)


def forecast_window(as_of: date, lookback_months: int = 6) -> tuple[date, date, date]:
    """Return complete historical months plus the current-month reporting date."""
    if not 3 <= lookback_months <= 12:
        raise ValueError("lookback_months must be between 3 and 12")
    current_month = _month_start(as_of)
    history_end = _subtract_months(current_month, 1)
    history_start = _subtract_months(current_month, lookback_months)
    return history_start, history_end, as_of


def build_monthly_forecast(
    rows: Iterable[dict[str, Any]],
    *,
    as_of: date,
    lookback_months: int = 6,
) -> dict[str, Any]:
    """Create a cash-flow estimate from grouped, positive expense rows.

    A merchant is considered recurrent after appearing in at least two complete
    months and in at least 40% of the observed history. The forecast remains an
    estimate: no data is written and the caller can label confidence clearly.
    """
    history_start, history_end, _ = forecast_window(as_of, lookback_months)
    history_months = _months_between(history_start, history_end)
    current_month = _month_start(as_of)

    monthly_totals: dict[date, float] = defaultdict(float)
    category_months: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    merchant_months: dict[tuple[str, str], dict[date, float]] = defaultdict(lambda: defaultdict(float))
    current_month_spent = 0.0

    for row in rows:
        try:
            month = _parse_month(row.get("month_start"))
            amount = float(row.get("amount") or 0)
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        merchant = str(row.get("merchant") or "Sin comercio").strip() or "Sin comercio"
        category = str(row.get("category") or "Sin categorizar").strip() or "Sin categorizar"
        if history_start <= month <= history_end:
            monthly_totals[month] += amount
            category_months[category][month] += amount
            merchant_months[(merchant, category)][month] += amount
        elif month == current_month:
            current_month_spent += amount

    complete_totals = [monthly_totals[month] for month in history_months]
    forecast_total = round(mean(complete_totals), 2) if complete_totals else 0.0

    recurring: list[dict[str, Any]] = []
    recurring_total = 0.0
    required_observations = max(2, (len(history_months) * 2 + 4) // 5)  # ceil(40%) without floats
    for (merchant, category), amounts_by_month in merchant_months.items():
        observations = sum(1 for value in amounts_by_month.values() if value > 0)
        if observations < required_observations:
            continue
        expected = sum(amounts_by_month.values()) / len(history_months)
        typical_charge = sum(amounts_by_month.values()) / observations
        recurring_total += expected
        recurring.append({
            "merchant": merchant,
            "category": category,
            "expected_monthly": round(expected, 2),
            "typical_charge": round(typical_charge, 2),
            "months_observed": observations,
            "observation_rate": round(observations / len(history_months), 2),
        })
    recurring.sort(key=lambda item: item["expected_monthly"], reverse=True)

    category_forecast = []
    for category, amounts_by_month in category_months.items():
        average = sum(amounts_by_month.values()) / len(history_months)
        category_forecast.append({
            "category": category,
            "expected_monthly": round(average, 2),
            "share": round(average / forecast_total, 4) if forecast_total else 0.0,
        })
    category_forecast.sort(key=lambda item: item["expected_monthly"], reverse=True)

    days_in_month = calendar.monthrange(as_of.year, as_of.month)[1]
    elapsed_days = max(as_of.day, 1)
    current_run_rate = current_month_spent / elapsed_days * days_in_month
    projected_month_end = round(max(forecast_total, current_run_rate), 2)
    prior_total = monthly_totals[history_months[-1]] if history_months else 0.0
    trend = round((forecast_total - prior_total) / prior_total, 4) if prior_total else None
    confidence = "alta" if len(history_months) >= 6 else "media" if len(history_months) >= 3 else "baja"

    return {
        "currency": "ARS",
        "as_of": as_of.isoformat(),
        "period": {
            "history_start": history_start.isoformat(),
            "history_end": history_end.isoformat(),
            "forecast_month": current_month.isoformat(),
            "months_observed": len(history_months),
        },
        "forecast": {
            "monthly_total": forecast_total,
            "recurring_total": round(recurring_total, 2),
            "flexible_total": round(max(forecast_total - recurring_total, 0), 2),
            "current_month_spent": round(current_month_spent, 2),
            "projected_month_end": projected_month_end,
            "trend_vs_last_month": trend,
            "confidence": confidence,
        },
        "monthly_history": [
            {"month": month.isoformat(), "amount": round(monthly_totals[month], 2)}
            for month in history_months
        ],
        "categories": category_forecast[:8],
        "recurring": recurring[:8],
    }
