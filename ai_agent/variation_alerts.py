"""Deterministic business rules for expense-variation alerts."""

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class ComparisonPeriods:
    current_start: date
    current_end: date
    previous_start: date
    previous_end: date


def completed_comparison_periods(kind: str, today: date) -> ComparisonPeriods:
    """Return the two last *complete* calendar periods before ``today``."""
    if kind == "weekly":
        current_end = today - timedelta(days=today.weekday() + 1)
        current_start = current_end - timedelta(days=6)
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=6)
    elif kind == "biweekly":
        # Dos ventanas consecutivas de 14 días, ambas cerradas. La ventana
        # actual siempre termina el domingo anterior para no comparar días
        # incompletos con días cerrados.
        current_end = today - timedelta(days=today.weekday() + 1)
        current_start = current_end - timedelta(days=13)
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=13)
    elif kind == "monthly":
        current_end = today.replace(day=1) - timedelta(days=1)
        current_start = current_end.replace(day=1)
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end.replace(day=1)
    else:
        raise ValueError("Unsupported comparison period")
    return ComparisonPeriods(current_start, current_end, previous_start, previous_end)


def exceeds_variation_threshold(
    previous_amount: float,
    current_amount: float,
    percent_threshold: float = 10.0,
    absolute_threshold: float = 5000.0,
) -> bool:
    """An increase qualifies when it meets either configured threshold."""
    increase = current_amount - previous_amount
    if previous_amount <= 0 or increase <= 0:
        return False
    return (increase / previous_amount * 100) >= percent_threshold or increase >= absolute_threshold
