"""Plain, truthful single-value answers; no cloud or model calls."""
from decimal import Decimal, InvalidOperation
import html
import re


def single_value_answer(columns, row):
    if len(columns) != 1:
        return None
    column = columns[0]
    value = row[column]
    if value is None or str(value).strip().upper() in {'', 'NULL', 'NONE', 'NAN'}:
        return 'No encontré un importe disponible para ese período y esas fuentes. Esto no equivale a gasto cero.'
    if isinstance(value, bool):
        return 'Sí' if value else 'No'
    name = column.upper()
    monetary = any(word in name for word in ('MONTO', 'AMOUNT', 'GASTO', 'PRECIO', 'ARS', 'USD'))
    count_or_rate = any(word in name for word in ('COUNT', 'CANTIDAD', 'REGISTROS', 'FILAS', 'PORCENTAJE', 'PERCENT', 'TASA'))
    if monetary and not count_or_rate:
        try:
            number = Decimal(str(value))
            if not number.is_finite():
                return 'No pude calcular un importe válido con esos datos. No equivale a gasto cero.'
            formatted = format(abs(number), ',.2f').replace(',', 'X').replace('.', ',').replace('X', '.')
            currency = ' ARS' if 'ARS' in column.upper() else ''
            return f'<b>{"−" if number < 0 else ""}$ {formatted}{currency}</b>'
        except (InvalidOperation, ValueError):
            pass
    return html.escape(str(value))
