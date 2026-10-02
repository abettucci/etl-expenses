"""Strict parsing for the Telegram expense-variation comparison command.

This module deliberately only turns a small, documented command grammar into
date ranges.  It does not build SQL, access BigQuery, or send Telegram
messages; those responsibilities stay in the Lambda boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from variation_alerts import ComparisonPeriods


class VariationCommandError(ValueError):
    """Raised when a /variacion request does not follow the documented syntax."""


@dataclass(frozen=True)
class VariationCommand:
    """A validated, human-readable comparison request."""

    periods: ComparisonPeriods
    period_kind: str


USAGE = (
    "Usá uno de estos formatos (primero el período actual y luego el anterior):\n"
    "• /variacion 2026-10-01 2026-09-30\n"
    "• /variacion 2026-10-01..2026-10-07 2026-09-24..2026-09-30\n"
    "• /variacion semana 2026-09-21 2026-09-14\n"
    "• /variacion mes 2026-09 2026-08"
)


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise VariationCommandError("las fechas deben usar AAAA-MM-DD") from exc


def _parse_month(value: str) -> tuple[date, date]:
    try:
        parsed = datetime.strptime(value, "%Y-%m").date()
    except ValueError as exc:
        raise VariationCommandError("los meses deben usar AAAA-MM") from exc
    start = parsed.replace(day=1)
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return start, next_month - timedelta(days=1)


def _parse_range(value: str) -> tuple[date, date]:
    parts = value.split("..")
    if len(parts) != 2 or not all(parts):
        raise VariationCommandError("cada rango debe usar AAAA-MM-DD..AAAA-MM-DD")
    start, end = (_parse_iso_date(part) for part in parts)
    if end < start:
        raise VariationCommandError("el final del rango no puede ser anterior al inicio")
    return start, end


def _build(
    current: tuple[date, date],
    previous: tuple[date, date],
    kind: str,
    *,
    require_equal_days: bool = True,
) -> VariationCommand:
    current_start, current_end = current
    previous_start, previous_end = previous
    if require_equal_days and (current_end - current_start) != (previous_end - previous_start):
        raise VariationCommandError("los dos períodos deben tener la misma cantidad de días")
    return VariationCommand(
        periods=ComparisonPeriods(current_start, current_end, previous_start, previous_end),
        period_kind=kind,
    )


def parse_variation_command(text: str) -> VariationCommand:
    """Parse the allowlisted /variacion grammar into comparable calendar ranges."""
    tokens = text.strip().split()
    if tokens and tokens[0].lower() == "/variacion":
        tokens = tokens[1:]
    if not tokens:
        raise VariationCommandError(USAGE)

    mode = tokens[0].lower()
    if mode in {"dia", "día"}:
        if len(tokens) != 3:
            raise VariationCommandError(USAGE)
        return _build(
            (_parse_iso_date(tokens[1]), _parse_iso_date(tokens[1])),
            (_parse_iso_date(tokens[2]), _parse_iso_date(tokens[2])),
            "Día",
        )
    if mode == "semana":
        if len(tokens) != 3:
            raise VariationCommandError(USAGE)
        current_day, previous_day = _parse_iso_date(tokens[1]), _parse_iso_date(tokens[2])
        current_start = current_day - timedelta(days=current_day.weekday())
        previous_start = previous_day - timedelta(days=previous_day.weekday())
        return _build(
            (current_start, current_start + timedelta(days=6)),
            (previous_start, previous_start + timedelta(days=6)),
            "Semana",
        )
    if mode == "mes":
        if len(tokens) != 3:
            raise VariationCommandError(USAGE)
        return _build(
            _parse_month(tokens[1]),
            _parse_month(tokens[2]),
            "Mes",
            require_equal_days=False,
        )
    if len(tokens) == 2 and ".." in tokens[0] and ".." in tokens[1]:
        return _build(_parse_range(tokens[0]), _parse_range(tokens[1]), "Rango personalizado")
    if len(tokens) == 2:
        current_day, previous_day = _parse_iso_date(tokens[0]), _parse_iso_date(tokens[1])
        return _build((current_day, current_day), (previous_day, previous_day), "Día")
    raise VariationCommandError(USAGE)
