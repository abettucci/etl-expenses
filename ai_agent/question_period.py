"""Explicit calendar context for month names without an invented year."""
import re
import unicodedata
from datetime import date

MONTHS = ('enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre').split()


def named_month_period(question, today):
    text = ''.join(char for char in unicodedata.normalize('NFD', question.lower()) if unicodedata.category(char) != 'Mn')
    if re.search(r'\b(?:19|20)\d{2}\b', text):
        return None  # Respect explicit years and multi-period requests.
    found = [index + 1 for index, month in enumerate(MONTHS) if re.search(r'\b' + month + r'\b', text)]
    if len(found) != 1:
        return None
    month = found[0]
    year = today.year - (month > today.month)
    start = date(year, month, 1)
    end = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return start, end


def calendar_context(question, today):
    period = named_month_period(question, today)
    text = f'Fecha actual en Argentina: {today.isoformat()}.'
    if period:
        text += f' El mes sin año de esta pregunta corresponde exactamente a [{period[0].isoformat()}, {period[1].isoformat()}) (fin exclusivo). No inventar otro año.'
    return text


def dates_match_question(sql, question, today):
    period = named_month_period(question, today)
    if not period:
        return True
    allowed = {period[0].year, period[1].year}
    years = [int(year) for year in re.findall(r"['\"]((?:19|20)\d{2})-\d{2}-\d{2}['\"]", sql)]
    return all(year in allowed for year in years)
