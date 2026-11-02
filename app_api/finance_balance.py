"""Monthly actuals, never forecasts. No cloud calls or financial-data logging."""
from decimal import Decimal
import hmac
import re


def require_finance_owner(user_id, enabled, owner_sub):
    if not enabled or not owner_sub:
        raise PermissionError('El balance financiero todavía no está conectado')
    if not isinstance(user_id, str) or not hmac.compare_digest(user_id, owner_sub):
        raise PermissionError('Tu cuenta todavía no tiene una fuente financiera vinculada')


def validate_income(month, payload, *, today):
    if not isinstance(month, str) or not re.fullmatch(r'20\d{2}-(0[1-9]|1[0-2])', month):
        raise ValueError('Mes inválido; usá AAAA-MM')
    if month > today.strftime('%Y-%m'):
        raise ValueError('No se pueden registrar ingresos futuros como ingresos reales')
    if not isinstance(payload, dict) or set(payload) != {'amount', 'currency'} or payload['currency'] != 'ARS':
        raise ValueError('Completá únicamente amount y currency=ARS')
    raw = payload['amount']
    if not isinstance(raw, str) or not re.fullmatch(r'\d{1,12}(\.\d{1,2})?', raw):
        raise ValueError('El ingreso debe ser un importe positivo o cero, con hasta dos decimales')
    amount = Decimal(raw)
    if amount > Decimal('999999999999.99'):
        raise ValueError('Importe fuera de rango')
    return format(amount.quantize(Decimal('.01')), '.2f')


def save_income(table, *, user_id, enabled, owner_sub, month, payload, today, updated_at):
    require_finance_owner(user_id, enabled, owner_sub)
    amount = validate_income(month, payload, today=today)
    # Never accept owner, partition keys or record IDs from the request body.
    table.put_item(Item={'pk': f'USER#{user_id}', 'sk': f'FINANCE_INCOME#{month}',
                         'amount': amount, 'currency': 'ARS', 'updated_at': updated_at})
    return {'month': month, 'amount': amount, 'currency': 'ARS', 'source': 'manual_monthly_total'}


def build_balance(forecast, incomes):
    current = forecast['period']['forecast_month'][:7]
    amounts = {row['month']: Decimal(str(row['amount'])) for row in incomes}
    history = [(row['month'][:7], row['amount'], row.get('recorded', False))
               for row in forecast['monthly_history']]
    history.append((current, forecast['forecast']['current_month_spent'],
                    forecast['forecast'].get('current_month_recorded', False)))
    months = []
    for month, expense, recorded in history:
        income = amounts.get(month)
        spent = Decimal(str(expense)) if recorded else None
        saving = income - spent if income is not None and spent is not None else None
        rate = saving / income * 100 if saving is not None and income > 0 else None
        months.append({'month': month, 'income': float(income) if income is not None else None,
                       'expenses': float(spent) if spent is not None else None,
                       'savings': float(saving) if saving is not None else None,
                       'savings_rate': round(float(rate), 2) if rate is not None else None,
                       'partial': month == current})
    return {'currency': 'ARS', 'as_of': forecast['as_of'], 'mode': 'live',
            'income_source': 'manual_monthly_total', 'expense_source': 'etl_aggregated_read_only',
            'months': months}
