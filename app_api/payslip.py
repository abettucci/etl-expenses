"""Parse Argentine payslips (recibos de sueldo) from their extracted text.

Pure functions. The caller extracts the text from the PDF and stores only the
result returned here. Personal identifiers (CUIL, legajo, account) are masked
before they leave this module: the full values never appear in the output.

A payslip is accepted only if its own totals reconcile: the sum of the income
lines equals the printed gross, the deductions equal the printed total, and
gross minus deductions equals the printed net.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

MONTHS = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
    'julio': 7, 'agosto': 8, 'septiembre': 9, 'setiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
}
MONEY = r'-?\d{1,3}(?:\.\d{3})*,\d{2}'
DEDUCTION_PREFIXES = ('aporte', 'impuesto', 'dto', 'descuento', 'retencion')
# Plain-language labels for the deductions the payslip prints.
DEDUCTION_LABELS = (
    ('sijp', 'Jubilación (SIJP)'),
    ('inssjp', 'PAMI (INSSJP)'),
    ('o.soc', 'Obra social (O.SOC.)'),
    ('impuesto', 'Impuesto a las ganancias'),
    ('gimnasio', 'Gimnasio'),
)


class PayslipError(ValueError):
    pass


def _money(text: str) -> float:
    return round(float(text.replace('.', '').replace(',', '.')), 2)


def _mask(value: str) -> str:
    digits = re.sub(r'\D', '', value)
    return '••••' + digits[-4:] if len(digits) >= 4 else '••••'


def _search(pattern: str, text: str, flags: int = re.I | re.M) -> str | None:
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else None


def _period(text: str) -> tuple[str, str] | None:
    raw = _search(r'Periodo\s+abonado\s+([A-Za-zÁÉÍÓÚáéíóúñÑ]+)\s+(\d{4})', text)
    year = re.search(r'Periodo\s+abonado\s+[A-Za-zÁÉÍÓÚáéíóúñÑ]+\s+(\d{4})', text, re.I | re.M)
    if not raw or not year:
        return None
    month = MONTHS.get(raw.lower())
    if not month:
        return None
    return f'{year.group(1)}-{month:02d}', f'{raw.capitalize()} {year.group(1)}'


def _pay_date(text: str) -> str | None:
    match = re.search(r'Fecha\s+y\s+lugar\s+de\s+pago\s+(\d{2})\.(\d{2})\.(\d{4})', text, re.I)
    if not match:
        return None
    return f'{match.group(3)}-{match.group(2)}-{match.group(1)}'


def _concept_lines(text: str) -> list[tuple[str, list[float]]]:
    """Lines of the concept table: the label, then the money and unit values printed on the line."""
    start = re.search(r'Concepto\s+Unidad', text, re.I)
    end = re.search(r'^\s*Totales', text, re.I | re.M)
    if not start or not end or end.start() < start.end():
        raise PayslipError('No encontré la tabla de conceptos')
    lines = []
    for raw in text[start.end():end.start()].splitlines():
        line = raw.strip()
        if not line or re.match(r'(Haberes|Con aporte|Sin aporte|Descuentos)$', line, re.I):
            continue
        values = re.findall(MONEY, line)
        if not values:
            continue
        label = re.split(MONEY, line, maxsplit=1)[0]
        label = re.sub(r'\s+\d+,\d{2}\s*$', '', label).strip()
        if not label:
            continue
        # A unit column prints a number with two decimals too (for example 30,00). It is
        # always the first value when there are two; the amount is the last one.
        lines.append((label, [_money(v) for v in values]))
    return lines


def _deduction_label(concept: str) -> str:
    lowered = concept.lower()
    for key, label in DEDUCTION_LABELS:
        if key in lowered:
            return label
    return concept.strip()


def parse_payslip(text: str) -> dict[str, Any]:
    if not isinstance(text, str) or 'concepto' not in text.lower():
        raise PayslipError('El PDF no parece un recibo de sueldo con tabla de conceptos')
    period = _period(text)
    if not period:
        raise PayslipError('No pude leer el período abonado')
    period_key, period_label = period

    haberes: list[dict[str, Any]] = []
    descuentos: list[dict[str, Any]] = []
    for label, values in _concept_lines(text):
        amount = values[-1]
        if label.lower().startswith(DEDUCTION_PREFIXES):
            descuentos.append({'concept': _deduction_label(label), 'amount': amount, 'raw': label})
        else:
            haberes.append({'concept': label, 'amount': amount})

    totals = re.search(rf'^\s*Totales\s+({MONEY})\s+({MONEY})', text, re.I | re.M)
    net = re.search(rf'Total\s+Neto\s+({MONEY})', text, re.I)
    if not totals or not net:
        raise PayslipError('No encontré los totales del recibo')
    printed_gross, printed_deductions, printed_net = _money(totals.group(1)), _money(totals.group(2)), _money(net.group(1))

    # The payslip prints one line per base (for example SIJP on salary and on the SAC bonus).
    # Show each concept once, with the sum.
    grouped: dict[str, float] = {}
    for item in descuentos:
        grouped[item['concept']] = round(grouped.get(item['concept'], 0.0) + item['amount'], 2)
    descuentos = [{'concept': concept, 'amount': amount} for concept, amount in grouped.items()]

    gross = round(sum(h['amount'] for h in haberes), 2)
    deductions = round(sum(d['amount'] for d in descuentos), 2)
    checks = {
        'gross': abs(gross - printed_gross) < 0.01,
        'deductions': abs(deductions - printed_deductions) < 0.01,
        'net': abs(gross - deductions - printed_net) < 0.01,
    }
    if not all(checks.values()):
        raise PayslipError('Los importes del recibo no cuadran; no lo guardé para no mostrar datos incorrectos')

    identifiers = {
        'cuil': _mask(_search(r'Cuil\s+(\d{2}-\d{8}-\d)', text) or ''),
        'legajo': _mask(_search(r'Legajo\s+(\d+)', text) or ''),
        'account': _mask(_search(r'N[°º]?\s*Cuenta:?\s*(\d+)', text) or ''),
    }
    deduction_pct = round(deductions / gross * 100, 2) if gross else None
    return {
        'period': period_key,
        'period_label': period_label,
        'pay_date': _pay_date(text),
        'employer': _search(r'^(.+?S\.R\.L\.)\s*$', text),
        'position': _search(r'Posici[oó]n\s*\n\s*(.+)', text),
        'level': _search(r'Nivel\s*\n\s*(.+)', text),
        'bank': _search(r'Banco:\s*(.+?)\s{2,}', text) or _search(r'Banco:\s*(.+)', text),
        'identifiers': identifiers,
        'haberes': haberes,
        'descuentos': descuentos,
        'gross': gross,
        'deductions': deductions,
        'net': printed_net,
        'deductions_pct_of_gross': deduction_pct,
        'checks': checks,
        'fingerprint': hashlib.sha256(f'{period_key}|{printed_gross}|{printed_net}'.encode()).hexdigest()[:16],
    }


def summarize(payslips: list[dict[str, Any]]) -> dict[str, Any]:
    """Averages and shares across payslips, ordered by period."""
    ordered = sorted(payslips, key=lambda p: p['period'])
    if not ordered:
        return {'count': 0, 'months': [], 'averages': None}
    by_label: dict[str, float] = {}
    for slip in ordered:
        for item in slip['descuentos']:
            by_label[item['concept']] = by_label.get(item['concept'], 0.0) + item['amount']
    count = len(ordered)
    gross_total = sum(s['gross'] for s in ordered)
    deductions_total = sum(s['deductions'] for s in ordered)
    return {
        'count': count,
        'months': [{'period': s['period'], 'period_label': s['period_label'], 'gross': s['gross'], 'deductions': s['deductions'],
                    'net': s['net'], 'deductions_pct_of_gross': s['deductions_pct_of_gross']} for s in ordered],
        'averages': {
            'gross': round(gross_total / count, 2), 'deductions': round(deductions_total / count, 2),
            'net': round(sum(s['net'] for s in ordered) / count, 2),
            'deductions_pct_of_gross': round(deductions_total / gross_total * 100, 2) if gross_total else None,
        },
        'deductions_by_concept': [
            {'concept': k, 'average': round(v / count, 2), 'pct_of_gross': round(v / gross_total * 100, 2) if gross_total else None}
            for k, v in sorted(by_label.items(), key=lambda kv: kv[1], reverse=True)],
    }
