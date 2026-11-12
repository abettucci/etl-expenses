"""Safe account onboarding and bounded access to the existing finance agent."""
import re
from finance_balance import require_finance_owner


def bot_url(username):
    return f'https://t.me/{username}' if isinstance(username, str) and re.fullmatch(r'[A-Za-z0-9_]{5,32}', username) else None


def overview(identity, profile, *, enabled, owner_sub, finance_bot='', groups_bot=''):
    try:
        require_finance_owner(identity['user_id'], enabled, owner_sub)
        linked = True
    except PermissionError:
        linked = False
    return {'profile': {'display_name': profile.get('display_name', identity.get('name', 'Usuario')), 'email': identity.get('email', '')},
            'finance_linked': linked,
            'sources': [
                {'id': 'gmail', 'name': 'Gmail y bancos', 'status': 'setup_required', 'detail': 'El ETL procesa comprobantes y avisos de la cuenta configurada. La web todavía no conecta cuentas nuevas de Gmail ni bancos.'},
                {'id': 'mercadopago', 'name': 'Mercado Pago', 'status': 'setup_required', 'detail': 'Los reportes de gastos se configuran en el ETL. Conectar Mercado Pago para pagos de grupos no conecta tu historial financiero.'},
                {'id': 'telegram', 'name': 'Tickets, texto y voz', 'status': 'setup_required', 'detail': 'Se registran desde el bot financiero autorizado. Verificá el chat habilitado antes de enviar datos personales.'},
                {'id': 'balanz', 'name': 'Balanz', 'status': 'not_available', 'detail': 'Conector de inversiones todavía no implementado. No solicitamos claves ni mostramos saldos simulados.'},
                {'id': 'binance', 'name': 'Binance', 'status': 'not_available', 'detail': 'Conector de inversiones todavía no implementado. No ingreses API keys ni permisos de trading o retiro en esta web.'}],
            'telegram': {'finance_url': bot_url(finance_bot), 'groups_url': bot_url(groups_bot),
                         'personal_access': linked,
                         'detail': 'Son dos bots distintos: el financiero consulta tus gastos y alertas; el de grupos gestiona saldos compartidos. La vinculación de grupos no habilita el acceso a tu warehouse.'}}


def question_body(payload):
    if not isinstance(payload, dict) or set(payload) != {'question'}:
        raise ValueError('Enviá únicamente una pregunta.')
    question = payload['question']
    if not isinstance(question, str) or not 1 <= len(question.strip()) <= 1000:
        raise ValueError('Escribí una pregunta de hasta 1000 caracteres.')
    return question.strip()
