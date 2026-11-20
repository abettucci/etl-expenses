"""Balanz statements: holdings (PDF "Resumen de cuenta"), movements and trades (XLSX).

Pure functions. Personal identifiers that appear in the statements (comitente,
cuotapartista, CUIT, address, names) are never returned. A file is accepted only
when its own totals reconcile:

- holdings: the value of each instrument section equals the section total printed
  in the summary, and the sections add up to the printed total of holdings
  (cash balances are reported as printed but not reconciled, because their
  dollar conversion is not in the statement);
- trades: gross equals quantity times price, within a small rounding tolerance.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

SECTIONS = ('Acciones', 'Bonos', 'Cedears', 'Corporativos', 'Fondos')
TRADE_TOLERANCE = 0.01  # relative tolerance for quantity times price against the gross amount


class BalanzError(ValueError):
    pass


def ar_number(text: str) -> float:
    """Argentine format: 1.234.567,89 with dots as thousands. A plain 0.00 is read as a decimal point."""
    cleaned = text.strip().replace('$', '').replace(' ', '')
    if ',' in cleaned:
        cleaned = cleaned.replace('.', '').replace(',', '.')
    elif re.fullmatch(r'-?\d{1,3}(\.\d{3})+', cleaned):
        cleaned = cleaned.replace('.', '')
    return float(cleaned)


def _digest(*parts: Any) -> str:
    return hashlib.sha256('|'.join(str(p) for p in parts).encode('utf-8')).hexdigest()[:24]


# ---------------------------------------------------------------- holdings (PDF text)

_ITEM = re.compile(
    r'^([A-Z][A-Z0-9.]*) (.+?) ([\d.]+,\d+|\d+) (-?\d+\.\d+) \$ ([\d.]+,\d+) \$ ([\d.]+(?:,\d+)?)$')
_SECTION_TOTAL = re.compile(r'^(Acciones|Bonos|Cedears|Corporativos|Fondos) \$ ([\d.]+(?:,\d+)?)$')
_TOTAL = re.compile(r'^Total \$ ([\d.]+(?:,\d+)?)$')
_DATE = re.compile(r'Fecha resumen (\d{2})/(\d{2})/(\d{4})')


def parse_holdings(text: str) -> dict[str, Any]:
    if not isinstance(text, str) or 'Posición consolidada' not in text:
        raise BalanzError('El PDF no parece un resumen de cuenta de Balanz')
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    date_match = _DATE.search(text)
    if not date_match:
        raise BalanzError('No encontré la fecha del resumen')
    as_of = f'{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}'

    printed_sections: dict[str, float] = {}
    printed_total = None
    for line in lines:
        section = _SECTION_TOTAL.match(line)
        if section and section.group(1) not in printed_sections:
            printed_sections[section.group(1)] = ar_number(section.group(2))
        total = _TOTAL.match(line)
        if total and printed_total is None:
            printed_total = ar_number(total.group(1))
    if printed_total is None or not printed_sections:
        raise BalanzError('No encontré los totales de instrumentos')

    positions: list[dict[str, Any]] = []
    current: str | None = None
    for line in lines:
        if line in SECTIONS:
            current = line
            continue
        if current is None or line.startswith('Especie'):
            continue
        match = _ITEM.match(line)
        if match:
            positions.append({
                'section': current, 'ticker': match.group(1), 'description': match.group(2).strip(),
                'quantity': ar_number(match.group(3)), 'price': ar_number(match.group(5)),
                'value': ar_number(match.group(6)),
            })

    checks: dict[str, bool] = {}
    for section, expected in printed_sections.items():
        total_value = round(sum(p['value'] for p in positions if p['section'] == section), 2)
        checks[section] = abs(total_value - expected) < 0.5
    holdings_total = round(sum(printed_sections.values()), 2)
    checks['sections_total'] = holdings_total <= printed_total + 0.5
    if not all(checks.values()):
        raise BalanzError('Las posiciones no cuadran con los totales del resumen; no lo guardé')
    return {
        'as_of': as_of,
        'positions': positions,
        'section_totals': printed_sections,
        'holdings_total': holdings_total,
        'printed_total': printed_total,
        'cash_and_dollars_note': 'El total incluye saldos en pesos y dólares que el resumen no convierte; no se reconcilian.',
        'checks': checks,
        'fingerprint': _digest('positions', as_of, printed_total),
    }


# ---------------------------------------------------------------- movements and trades (XLSX rows)

def _text(value: Any) -> str:
    return str(value or '').strip()


def _date(value: Any) -> str:
    text = _text(value)[:10]
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
        raise BalanzError('fecha inválida')
    return text


def _kind(description: str) -> str:
    lowered = description.lower()
    if lowered.startswith('dividendo'):
        return 'dividend'
    if lowered.startswith('renta') or 'amortiz' in lowered:
        return 'amortization'
    if 'interes' in lowered or 'interés' in lowered:
        return 'interest'
    return 'other'


def parse_movements(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Cash movements: dividends, amortizations, interest and the rest, with the currency as printed."""
    out = []
    for row in rows:
        description = _text(row.get('Descripcion'))
        if not description:
            continue
        try:
            amount = float(row.get('Importe') or 0)
            when = _date(row.get('Liquidacion') or row.get('Concertacion'))
        except (TypeError, ValueError, BalanzError):
            continue
        currency = _text(row.get('Moneda')) or 'Sin moneda'
        out.append({
            'id': _digest('mov', description, row.get('Ticker'), row.get('Concertacion'), row.get('Cantidad'), currency, amount),
            'description': description, 'ticker': _text(row.get('Ticker')), 'instrument_type': _text(row.get('Tipo de Instrumento')),
            'date': when, 'quantity': float(row.get('Cantidad') or 0), 'currency': currency, 'amount': round(amount, 2),
            'kind': _kind(description),
        })
    return out


def parse_trades(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Trade confirmations. A trade is kept only if its gross equals quantity times price."""
    kept, rejected = [], 0
    for row in rows:
        ticker = _text(row.get('Ticker'))
        side = _text(row.get('Tipo')).upper()
        if not ticker or side not in ('COMPRA', 'VENTA'):
            continue
        try:
            quantity = float(row.get('Cantidad') or 0)
            price = float(row.get('Precio') or 0)
            gross = float(row.get('Bruto') or 0)
            when, settle = _date(row.get('Concertacion')), _date(row.get('Liquidacion'))
        except (TypeError, ValueError, BalanzError):
            rejected += 1
            continue
        expected = quantity * price
        if abs(abs(gross) - expected) > max(1.0, expected * TRADE_TOLERANCE):
            rejected += 1
            continue
        kept.append({
            'id': _digest('trade', _text(row.get('Num Boleto')), ticker, side, when, quantity, price),
            'ticker': ticker, 'description': _text(row.get('Especie')), 'side': side,
            'trade_date': when, 'settle_date': settle, 'quantity': quantity, 'price': price,
            'gross': round(gross, 2), 'costs': round(float(row.get('Costos Mercado') or 0), 2),
            'fee': round(float(row.get('Arancel') or 0), 2), 'net': round(float(row.get('Neto') or 0), 2),
        })
    return {'trades': kept, 'rejected': rejected}


def income_by_month(movements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dividends, amortizations and interest per month and currency. Purchases and sales are not income."""
    totals: dict[tuple[str, str], float] = {}
    for m in movements:
        if m['kind'] == 'other':
            continue
        key = (m['date'][:7], m['currency'])
        totals[key] = totals.get(key, 0.0) + m['amount']
    return [{'month': month, 'currency': currency, 'amount': round(amount, 2)}
            for (month, currency), amount in sorted(totals.items())]


# ---------------------------------------------------------------- upload routing

def _pdf_text(raw: bytes) -> str:
    from io import BytesIO
    from pypdf import PdfReader
    reader = PdfReader(BytesIO(raw))
    if len(reader.pages) > 20:
        raise BalanzError('El PDF tiene más páginas de las esperadas para un resumen')
    return '\n'.join(page.extract_text() or '' for page in reader.pages)


def _sheet_rows(raw: bytes) -> list[dict[str, Any]]:
    from io import BytesIO
    import openpyxl
    workbook = openpyxl.load_workbook(BytesIO(raw), read_only=True, data_only=True)
    try:
        rows = list(workbook.worksheets[0].iter_rows(values_only=True))
    finally:
        workbook.close()
    if not rows:
        return []
    header = [str(h or '').strip() for h in rows[0]]
    return [dict(zip(header, r)) for r in rows[1:] if any(v is not None for v in r)]


def read_upload(raw: bytes) -> dict[str, Any]:
    """Detects which Balanz file this is and parses it. Returns {'kind', 'data'}.

    kind is 'holdings' (PDF resumen de cuenta), 'trades' (boletos.xlsx) or 'movements' (movimientos.xlsx).
    Anything else is refused.
    """
    if raw.startswith(b'%PDF'):
        return {'kind': 'holdings', 'data': parse_holdings(_pdf_text(raw))}
    if raw.startswith(b'PK'):
        rows = _sheet_rows(raw)
        headers = set(rows[0].keys()) if rows else set()
        if 'Num Boleto' in headers:
            return {'kind': 'trades', 'data': parse_trades(rows)}
        if 'Tipo de Instrumento' in headers and 'Importe' in headers:
            return {'kind': 'movements', 'data': parse_movements(rows)}
        raise BalanzError('Este Excel no es de boletos ni de movimientos de Balanz')
    raise BalanzError('El archivo no es un PDF ni un Excel de Balanz')
