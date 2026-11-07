"""Plain, truthful single-value answers; no cloud or model calls."""
from decimal import Decimal, InvalidOperation
import html
import re
from html.parser import HTMLParser


def _telegram_length(text):
    return len(text.encode('utf-16-le')) // 2


class _HTMLMessageSplitter(HTMLParser):
    """Split generated HTML while keeping tags and entities intact."""

    def __init__(self, limit):
        super().__init__(convert_charrefs=False)
        self.limit = limit
        self.stack = []
        self.parts = []
        self.length = 0
        self.has_content = False
        self.messages = []

    def _flush(self):
        if self.has_content:
            closing = ''.join(f'</{tag}>' for tag, _ in reversed(self.stack))
            self.messages.append(''.join(self.parts) + closing)
        self.parts = [opening for _, opening in self.stack]
        self.length = sum(_telegram_length(part) for part in self.parts)
        self.has_content = False

    def _append(self, text, stack=None, content=False):
        next_stack = self.stack if stack is None else stack
        closing_length = sum(_telegram_length(f'</{tag}>') for tag, _ in next_stack)
        size = _telegram_length(text)
        if self.length + size + closing_length > self.limit:
            self._flush()
        if self.length + size + closing_length > self.limit:
            raise ValueError('HTML formatting exceeds Telegram message size')
        self.parts.append(text)
        self.length += size
        self.stack = next_stack
        self.has_content = self.has_content or content

    def handle_starttag(self, tag, attrs):
        opening = self.get_starttag_text()
        if tag in {'br', 'hr'}:
            self._append(opening, content=True)
        else:
            self._append(opening, self.stack + [(tag, opening)])

    def handle_startendtag(self, tag, attrs):
        self._append(self.get_starttag_text(), content=True)

    def handle_endtag(self, tag):
        if not self.stack or self.stack[-1][0] != tag:
            raise ValueError('Unbalanced Telegram HTML')
        self._append(f'</{tag}>', self.stack[:-1])

    def handle_data(self, data):
        for char in data:
            self._append(char, content=True)

    def handle_entityref(self, name):
        self._append(f'&{name};', content=True)

    def handle_charref(self, name):
        self._append(f'&#{name};', content=True)


def split_html_messages(text, limit=4000):
    """Return every character in Telegram-sized, independently valid messages."""
    if limit < 128:
        raise ValueError('Telegram chunk size is too small')
    if _telegram_length(text) <= limit:
        return [text]
    parser = _HTMLMessageSplitter(limit)
    parser.feed(text)
    parser.close()
    parser._flush()
    return parser.messages


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
