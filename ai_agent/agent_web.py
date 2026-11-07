"""IAM-invoked web adapter. Never accepts a Telegram event or executes mutations."""
from html.parser import HTMLParser
import hmac
import re

def looks_read_only(sql):
    return isinstance(sql, str) and bool(re.match(r'^\s*(SELECT|WITH)\b', sql, re.I)) and not re.search(r'\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|EXPORT|EXECUTE|EXTERNAL_QUERY)\b|\bML\.', sql, re.I) and ';' not in sql.strip().rstrip(';')


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
    def handle_data(self, data):
        self.parts.append(data)
    def handle_starttag(self, tag, attrs):
        if tag in {'br', 'p', 'li'}:
            self.parts.append('\n')


def run_web_question(event, *, enabled, owner_sub, generate, execute):
    if not enabled or not owner_sub:
        return {'statusCode': 503, 'answer': 'El asistente personal todavía no está conectado.'}
    user = event.get('user_id', '')
    if event.get('body') is not None or event.get('requestContext') is not None or not isinstance(user, str) or not hmac.compare_digest(user, owner_sub):
        return {'statusCode': 403, 'answer': 'No hay una fuente financiera vinculada a tu cuenta.'}
    question = event.get('question')
    if not isinstance(question, str) or not 1 <= len(question.strip()) <= 1000:
        return {'statusCode': 400, 'answer': 'Escribí una pregunta de hasta 1000 caracteres.'}
    try:
        sql = generate(question.strip())
        if not sql:
            return {'statusCode': 503, 'answer': 'No pude interpretar la pregunta. Intentá reformularla.'}
        if not looks_read_only(sql):
            return {'statusCode': 400, 'answer': 'El asistente web solo puede consultar tus gastos.'}
        text, _ = execute(sql)
        if text.startswith('❌'):
            return {'statusCode': 503, 'answer': 'No pude consultar los gastos ahora. Intentá nuevamente.'}
        parser = PlainText(); parser.feed(text)
        answer = ''.join(parser.parts).strip()
        if not answer:
            raise ValueError('empty')
        return {'statusCode': 200, 'answer': answer}
    except Exception:
        return {'statusCode': 503, 'answer': 'No pude consultar los gastos ahora. Intentá nuevamente.'}
