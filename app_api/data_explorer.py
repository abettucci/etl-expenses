"""Read-only data exploration for the linked owner.

Builds the BigQuery statements and shapes the results for three screens: the
ingested sources, the table/view catalog, and a filterable grid of movements.
Everything here is pure: the caller runs the queries. User input never reaches
SQL text; it travels as query parameters, and identifiers come from allowlists.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

_IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,127}$')
_PROJECT = re.compile(r'^[a-z][a-z0-9-]{4,28}[a-z0-9]$')
_FREE_TEXT = re.compile(r"^[\w .,()\-/&*+#@%':]{0,80}$", re.UNICODE)
_CODE = re.compile(r'^[A-Za-z0-9_ ()\-]{1,60}$')

MAX_LIMIT = 200
MAX_EXPORT_LIMIT = 5000  # full downloads of the movements grid
DEFAULT_LIMIT = 50
MAX_OFFSET = 100_000
FACET_LIMIT = 100
SENSITIVE_COLUMNS = re.compile(r'(nro_tarjeta|numero_tarjeta|card_number|cbu|nro_cuenta|cuenta_origen)', re.I)

SOURCE_LABELS = {
    'bank_payments': 'Tarjetas y débitos (Gmail)',
    'bank_transfers': 'Transferencias bancarias (Gmail)',
    'mp_data': 'Mercado Pago (reportes)',
    'mp_transfer_data': 'Transferencias de Mercado Pago (Gmail)',
    'carrefour_data': 'Carrefour (tickets)',
    'supermarket_receipts': 'Tickets de supermercado (fotos)',
    'manual_expenses': 'Gastos manuales (Telegram)',
}

# Dates outside this window are almost always parsing mistakes (for example a
# two-digit year read as year 0026) and are flagged instead of hidden.
_VALID_DATE = "(fecha BETWEEN TIMESTAMP('2015-01-01') AND TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))"
QUALITY_FILTERS = {
    'invalid_dates': f'(fecha IS NULL OR NOT {_VALID_DATE})',
    'no_currency': "(divisa IS NULL OR TRIM(CAST(divisa AS STRING)) = '')",
    'no_category': "(categoria IS NULL OR TRIM(CAST(categoria AS STRING)) = '')",
}
TX_SORTABLE = {'fecha', 'monto_total', 'comercio', 'categoria', 'extraido_de'}
TX_COLUMNS = ['fecha', 'comercio', 'categoria', 'producto', 'monto_total', 'divisa', 'extraido_de',
              'metodo_pago', 'tarjeta', 'nro_tarjeta', 'cuotas', 'detalles', 'transaction_key']

Param = tuple[str, str, Any]


def _ident(value: str, name: str) -> str:
    if not isinstance(value, str) or not _IDENT.fullmatch(value):
        raise ValueError(f'invalid {name} identifier')
    return value


def _qualified(project: str, dataset: str, table: str) -> str:
    if not isinstance(project, str) or not _PROJECT.fullmatch(project):
        raise ValueError('invalid project identifier')
    return f'`{project}.{_ident(dataset, "dataset")}.{_ident(table, "table")}`'


# ---------------------------------------------------------------- sources

def sources_query(project: str, dataset: str, view: str) -> str:
    return f"""
    SELECT
      COALESCE(NULLIF(TRIM(CAST(extraido_de AS STRING)), ''), '(sin fuente)') AS source,
      COALESCE(NULLIF(TRIM(CAST(divisa AS STRING)), ''), '(sin divisa)') AS currency,
      COUNT(*) AS rows_n,
      SUM(SAFE_CAST(monto_total AS FLOAT64)) AS total,
      MIN(IF({_VALID_DATE}, fecha, NULL)) AS first_date,
      MAX(IF({_VALID_DATE}, fecha, NULL)) AS last_date,
      COUNTIF(fecha IS NULL OR NOT {_VALID_DATE}) AS invalid_dates,
      MAX(extraido_en) AS last_ingested_at
    FROM {_qualified(project, dataset, view)}
    GROUP BY source, currency
    ORDER BY source, currency
    """


def facets_query(project: str, dataset: str, view: str) -> str:
    return f"""
    SELECT COALESCE(NULLIF(TRIM(CAST(categoria AS STRING)), ''), '(sin categoría)') AS category, COUNT(*) AS rows_n
    FROM {_qualified(project, dataset, view)}
    GROUP BY category
    ORDER BY rows_n DESC
    LIMIT {FACET_LIMIT}
    """


def summarize_sources(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_source: dict[str, dict[str, Any]] = {}
    for row in rows:
        source = str(row.get('source') or '(sin fuente)')
        entry = by_source.setdefault(source, {
            'source': source, 'label': SOURCE_LABELS.get(source, source), 'rows': 0, 'totals': [],
            'first_date': None, 'last_date': None, 'last_ingested_at': None, 'invalid_dates': 0, 'flags': []})
        count = int(row.get('rows_n') or 0)
        entry['rows'] += count
        entry['invalid_dates'] += int(row.get('invalid_dates') or 0)
        currency = str(row.get('currency') or '(sin divisa)')
        entry['totals'].append({'currency': currency, 'amount': round(float(row.get('total') or 0), 2), 'rows': count})
        for key, pick in (('first_date', min), ('last_date', max), ('last_ingested_at', max)):
            value = jsonable(row.get(key))
            if value:
                entry[key] = value if entry[key] is None else pick(entry[key], value)
    sources = sorted(by_source.values(), key=lambda item: item['source'])
    for entry in sources:
        if entry['invalid_dates']:
            entry['flags'].append({'kind': 'invalid_dates', 'count': entry['invalid_dates']})
        missing = sum(t['rows'] for t in entry['totals'] if t['currency'] == '(sin divisa)')
        if missing:
            entry['flags'].append({'kind': 'no_currency', 'count': missing})
    return {'sources': sources, 'total_rows': sum(item['rows'] for item in sources)}


def files_queries(project: str, dataset: str) -> dict[str, str]:
    """Recent ingestion units per source. Each source keeps a different kind of reference."""
    batches = _qualified(project, dataset, 'archivos_ingestados')
    receipts = _qualified(project, dataset, 'supermarket_receipts')
    payments = _qualified(project, dataset, 'bank_payments')
    return {
        'batches_summary': f'SELECT COUNT(*) AS n, MIN(ins_dttm) AS first_at, MAX(ins_dttm) AS last_at FROM {batches}',
        'batches': f'SELECT id AS ref, ins_dttm AS ingested_at FROM {batches} ORDER BY ins_dttm DESC LIMIT 25',
        'receipts': f"""SELECT s3_key AS ref, ANY_VALUE(merchant_name) AS detail,
            MAX(SAFE_CAST(total_amount AS FLOAT64)) AS amount, MAX(processed_at) AS ingested_at, ANY_VALUE(extraction_method) AS method
            FROM {receipts} GROUP BY s3_key ORDER BY ingested_at DESC LIMIT 25""",
        'gmail': f"""SELECT MESSAGE_ID AS ref, ANY_VALUE(COMERCIO) AS detail,
            SUM(MONTO) AS amount, MAX(EXTRAIDO_EN) AS ingested_at
            FROM {payments} GROUP BY MESSAGE_ID ORDER BY ingested_at DESC LIMIT 25""",
    }


# ---------------------------------------------------------------- catalog

def catalog_queries(project: str, dataset: str) -> dict[str, str]:
    if not isinstance(project, str) or not _PROJECT.fullmatch(project):
        raise ValueError('invalid project identifier')
    base = f'{project}.{_ident(dataset, "dataset")}'
    return {
        'objects': f"""SELECT t.table_name AS name, t.table_type AS type, s.row_count AS rows_n,
            s.size_bytes AS size_bytes, TIMESTAMP_MILLIS(s.last_modified_time) AS modified
            FROM `{base}.INFORMATION_SCHEMA.TABLES` t
            LEFT JOIN `{base}.__TABLES__` s ON s.table_id = t.table_name
            ORDER BY t.table_type, t.table_name""",
        'columns': f"""SELECT table_name, column_name, data_type, ordinal_position
            FROM `{base}.INFORMATION_SCHEMA.COLUMNS` ORDER BY table_name, ordinal_position""",
    }


def build_catalog(object_rows: list[dict[str, Any]], column_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    columns: dict[str, list[dict[str, str]]] = {}
    for row in column_rows:
        columns.setdefault(str(row['table_name']), []).append(
            {'name': str(row['column_name']), 'type': str(row['data_type'])})
    catalog = []
    for row in object_rows:
        name = str(row['name'])
        is_view = str(row.get('type')) == 'VIEW'
        catalog.append({
            'name': name, 'type': 'VIEW' if is_view else 'TABLE',
            'rows': None if is_view or row.get('rows_n') is None else int(row['rows_n']),
            'size_bytes': None if is_view or row.get('size_bytes') is None else int(row['size_bytes']),
            'modified': jsonable(row.get('modified')), 'columns': columns.get(name, [])})
    return sorted(catalog, key=lambda item: (item['type'] != 'TABLE', item['name']))


# ---------------------------------------------------------------- params

def _int(params: dict[str, Any], key: str, default: int, low: int, high: int) -> int:
    raw = params.get(key)
    if raw in (None, ''):
        return default
    try:
        value = int(str(raw))
    except ValueError:
        raise ValueError(f'"{key}" debe ser un número entero') from None
    if not low <= value <= high:
        raise ValueError(f'"{key}" debe estar entre {low} y {high}')
    return value


def _float(params: dict[str, Any], key: str) -> float | None:
    raw = params.get(key)
    if raw in (None, ''):
        return None
    try:
        value = float(str(raw).replace(',', '.'))
    except ValueError:
        raise ValueError(f'"{key}" debe ser un número') from None
    if value != value or abs(value) > 1e15:
        raise ValueError(f'"{key}" no es válido')
    return value


def _date(params: dict[str, Any], key: str) -> date | None:
    raw = params.get(key)
    if raw in (None, ''):
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        raise ValueError(f'"{key}" debe tener formato AAAA-MM-DD') from None


def parse_transaction_params(params: dict[str, Any]) -> dict[str, Any]:
    params = params or {}
    parsed: dict[str, Any] = {}
    for key in ('source', 'currency', 'category'):
        value = str(params.get(key) or '').strip()
        if value and not _CODE.fullmatch(value):
            raise ValueError(f'"{key}" contiene caracteres no permitidos')
        parsed[key] = value
    text = str(params.get('q') or '').strip()
    if len(text) > 80 or (text and not _FREE_TEXT.fullmatch(text)):
        raise ValueError('"q" admite hasta 80 caracteres comunes')
    parsed['q'] = text
    quality = str(params.get('quality') or '').strip()
    if quality and quality not in QUALITY_FILTERS:
        raise ValueError('"quality" no es válido')
    parsed['quality'] = quality
    parsed['date_from'], parsed['date_to'] = _date(params, 'date_from'), _date(params, 'date_to')
    parsed['min_amount'], parsed['max_amount'] = _float(params, 'min_amount'), _float(params, 'max_amount')
    sort = str(params.get('sort') or 'fecha')
    if sort not in TX_SORTABLE:
        raise ValueError('"sort" no es válido')
    direction = str(params.get('dir') or 'desc').lower()
    if direction not in ('asc', 'desc'):
        raise ValueError('"dir" debe ser asc o desc')
    parsed['sort'], parsed['dir'] = sort, direction
    parsed['limit'] = _int(params, 'limit', DEFAULT_LIMIT, 1, MAX_EXPORT_LIMIT)
    parsed['offset'] = _int(params, 'offset', 0, 0, MAX_OFFSET)
    return parsed


def _like(value: str) -> str:
    return '%' + value.lower().replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'


def transactions_query(project: str, dataset: str, view: str, filters: dict[str, Any]) -> tuple[str, list[Param]]:
    where: list[str] = []
    params: list[Param] = []
    equality = {'source': 'extraido_de', 'currency': 'divisa', 'category': 'categoria'}
    for key, column in equality.items():
        if filters.get(key):
            where.append(f'CAST({column} AS STRING) = @{key}')
            params.append((key, 'STRING', filters[key]))
    if filters.get('q'):
        where.append('(' + ' OR '.join(f'LOWER(CAST({c} AS STRING)) LIKE @q' for c in
                                       ('comercio', 'producto', 'categoria', 'detalles')) + ')')
        params.append(('q', 'STRING', _like(filters['q'])))
    if filters.get('quality'):
        where.append(QUALITY_FILTERS[filters['quality']])
    if filters.get('date_from'):
        where.append('DATE(fecha) >= @date_from')
        params.append(('date_from', 'DATE', filters['date_from']))
    if filters.get('date_to'):
        where.append('DATE(fecha) <= @date_to')
        params.append(('date_to', 'DATE', filters['date_to']))
    if filters.get('min_amount') is not None:
        where.append('SAFE_CAST(monto_total AS FLOAT64) >= @min_amount')
        params.append(('min_amount', 'FLOAT64', filters['min_amount']))
    if filters.get('max_amount') is not None:
        where.append('SAFE_CAST(monto_total AS FLOAT64) <= @max_amount')
        params.append(('max_amount', 'FLOAT64', filters['max_amount']))
    params += [('limit', 'INT64', filters['limit']), ('offset', 'INT64', filters['offset'])]
    select = ', '.join('CAST(nro_tarjeta AS STRING) AS nro_tarjeta' if c == 'nro_tarjeta' else c for c in TX_COLUMNS)
    clause = ('WHERE ' + ' AND '.join(where)) if where else ''
    sql = f"""
    SELECT {select}, COUNT(*) OVER() AS _total
    FROM {_qualified(project, dataset, view)}
    {clause}
    ORDER BY {filters['sort']} {filters['dir'].upper()} NULLS LAST, transaction_key
    LIMIT @limit OFFSET @offset
    """
    return sql, params


def parse_preview_params(params: dict[str, Any], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    params = params or {}
    by_name = {item['name']: item for item in catalog}
    name = str(params.get('name') or '')
    if name not in by_name:
        raise ValueError('La tabla o vista no existe en este proyecto')
    columns = [column['name'] for column in by_name[name]['columns']]
    sort = str(params.get('sort') or '')
    if sort and sort not in columns:
        raise ValueError('"sort" no es una columna de la tabla')
    direction = str(params.get('dir') or 'asc').lower()
    if direction not in ('asc', 'desc'):
        raise ValueError('"dir" debe ser asc o desc')
    return {'name': name, 'columns': columns, 'sort': sort, 'dir': direction,
            'limit': _int(params, 'limit', DEFAULT_LIMIT, 1, MAX_LIMIT), 'offset': _int(params, 'offset', 0, 0, MAX_OFFSET)}


def preview_query(project: str, dataset: str, request: dict[str, Any]) -> tuple[str, list[Param]]:
    order = f"ORDER BY `{_ident(request['sort'], 'column')}` {request['dir'].upper()} NULLS LAST" if request['sort'] else ''
    sql = f"SELECT * FROM {_qualified(project, dataset, request['name'])} {order} LIMIT @limit OFFSET @offset"
    return sql, [('limit', 'INT64', request['limit']), ('offset', 'INT64', request['offset'])]


# ---------------------------------------------------------------- output

def jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def mask(value: Any) -> Any:
    """Keep only the last four digits of card, account and CBU numbers."""
    if value is None or value == '':
        return value
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = re.sub(r'\s', '', str(value))
    return '••••' + text[-4:] if len(text) > 4 else '••••'


def shape_rows(rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
    sensitive = {c for c in columns if SENSITIVE_COLUMNS.search(c)}
    return [{c: mask(row.get(c)) if c in sensitive else jsonable(row.get(c)) for c in columns} for row in rows]
