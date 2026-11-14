import { useEffect, useRef, useState, type FormEvent } from 'react'
import './financial-data.css'
import { ExportMenu } from './export-menu'
import { PayslipsTab } from './payslips'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Tab = 'sources' | 'files' | 'catalog' | 'transactions' | 'payslips'
type Flag = { kind: 'invalid_dates' | 'no_currency'; count: number }
type SourceEntry = { source: string; label: string; rows: number; totals: { currency: string; amount: number; rows: number }[]; first_date: string | null; last_date: string | null; last_ingested_at: string | null; invalid_dates: number; flags: Flag[] }
type SourcesReport = { sources: SourceEntry[]; total_rows: number; categories: { name: string; rows: number }[]; dataset: string; view: string }
type Column = { name: string; type: string }
type CatalogObject = { name: string; type: 'TABLE' | 'VIEW'; description: string; rows: number | null; size_bytes: number | null; modified: string | null; columns: (Column & { description: string })[] }
type Catalog = { project: string; dataset: string; objects: CatalogObject[] }
type Grid = { columns: string[]; rows: Record<string, unknown>[]; total?: number | null; limit: number; offset: number; sort?: string; dir?: string }
type TxResult = Grid & { columns: string[]; total: number | null }
type FilesReport = { batches: { summary: { n: number; first_at: string | null; last_at: string | null } | null; items: Record<string, unknown>[] }; receipts: Record<string, unknown>[]; gmail: Record<string, unknown>[]; errors: string[] }
type Preview = Omit<Grid, 'columns'> & { name: string; type: string; rows_total: number | null; columns: Column[] }

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 })
const cash = (value: number | null | undefined) => value === null || value === undefined ? '—' : money.format(value)
const dateOf = (value: unknown) => typeof value === 'string' && value ? value.slice(0, 10) : '—'
const dateTime = (value: unknown) => typeof value === 'string' && value ? value.replace('T', ' ').slice(0, 16) : '—'
const cell = (value: unknown) => value === null || value === undefined || value === '' ? '—' : typeof value === 'number' ? value.toLocaleString('es-AR', { maximumFractionDigits: 2 }) : String(value)
const bytes = (value: number | null) => value === null ? '—' : value > 1e9 ? `${(value / 1e9).toFixed(1)} GB` : value > 1e6 ? `${(value / 1e6).toFixed(1)} MB` : `${Math.round(value / 1e3)} KB`
const FLAG_TEXT: Record<Flag['kind'], string> = { invalid_dates: 'fechas inválidas o fuera de rango', no_currency: 'sin divisa' }

function ErrorBox({ message, onRetry }: { message: string; onRetry: () => void }) {
  return <div className="data-error" role="alert"><p>{message}</p><button className="outline" onClick={onRetry}>Reintentar</button></div>
}

function Pager({ grid, onPage }: { grid: Grid; onPage: (offset: number) => void }) {
  const total = grid.total ?? null
  const from = grid.rows.length ? grid.offset + 1 : 0
  const to = grid.offset + grid.rows.length
  return <div className="data-pager">
    <span>{total === null ? `Filas ${from}–${to}` : total === 0 ? 'Sin resultados' : `Filas ${from}–${to} de ${total.toLocaleString('es-AR')}`}</span>
    <div><button className="outline" disabled={grid.offset === 0} onClick={() => onPage(Math.max(0, grid.offset - grid.limit))}>Anterior</button>
      <button className="outline" disabled={total !== null ? to >= total : grid.rows.length < grid.limit} onClick={() => onPage(grid.offset + grid.limit)}>Siguiente</button></div>
  </div>
}

function GridTable({ columns, rows, empty }: { columns: string[]; rows: Record<string, unknown>[]; empty: string }) {
  if (!rows.length) return <p className="data-empty">{empty}</p>
  return <div className="data-table-wrap"><table className="data-table"><thead><tr>{columns.map(c => <th key={c} scope="col">{c}</th>)}</tr></thead>
    <tbody>{rows.map((row, index) => <tr key={index}>{columns.map(c => <td key={c}>{cell(row[c])}</td>)}</tr>)}</tbody></table></div>
}

function SourcesTab({ api }: { api: Api }) {
  const [report, setReport] = useState<SourcesReport | null>(null)
  const [error, setError] = useState('')
  const load = () => { setError(''); api('/v1/finance/data/sources').then(r => setReport(r as SourcesReport)).catch(e => setError(e instanceof Error ? e.message : 'No pudimos cargar las fuentes.')) }
  useEffect(load, [])  // eslint-disable-line react-hooks/exhaustive-deps
  if (error) return <ErrorBox message={error} onRetry={load} />
  if (!report) return <p className="data-empty">Leyendo tus fuentes…</p>
  return <div className="data-stack">
    <p className="data-note">{report.total_rows.toLocaleString('es-AR')} filas en {report.dataset}.{report.view}. Los montos se suman por divisa; nunca se mezclan pesos y dólares.</p>
    <div className="data-cards">{report.sources.map(s => <article className="data-card" key={s.source}>
      <span className="data-kicker">{s.source}</span><h3>{s.label}</h3>
      <dl><div><dt>Filas</dt><dd>{s.rows.toLocaleString('es-AR')}</dd></div>
        <div><dt>Desde</dt><dd>{dateOf(s.first_date)}</dd></div><div><dt>Hasta</dt><dd>{dateOf(s.last_date)}</dd></div>
        <div><dt>Última carga</dt><dd>{dateTime(s.last_ingested_at)}</dd></div></dl>
      <ul className="data-totals">{s.totals.map(t => <li key={t.currency}><span>{t.currency}</span><strong>{t.currency === 'ARS' ? cash(t.amount) : t.amount.toLocaleString('es-AR', { maximumFractionDigits: 2 })}</strong><small>{t.rows} filas</small></li>)}</ul>
      {s.flags.map(f => <p className="data-flag" key={f.kind}>⚠ {f.count.toLocaleString('es-AR')} {FLAG_TEXT[f.kind]}</p>)}
    </article>)}</div>
    <section className="data-card"><h3>Categorías en el historial</h3>
      <div className="data-chips">{report.categories.map(c => <span key={c.name}>{c.name} <small>{c.rows}</small></span>)}</div></section>
  </div>
}

function FilesTab({ api }: { api: Api }) {
  const [report, setReport] = useState<FilesReport | null>(null)
  const [error, setError] = useState('')
  const load = () => { setError(''); api('/v1/finance/data/files').then(r => setReport(r as FilesReport)).catch(e => setError(e instanceof Error ? e.message : 'No pudimos cargar los archivos.')) }
  useEffect(load, [])  // eslint-disable-line react-hooks/exhaustive-deps
  if (error) return <ErrorBox message={error} onRetry={load} />
  if (!report) return <p className="data-empty">Leyendo archivos…</p>
  const summary = report.batches.summary
  return <div className="data-stack">
    <section className="data-card"><h3>Lotes de ingesta</h3>
      <p className="data-note">{summary ? `${summary.n} lotes entre ${dateTime(summary.first_at)} y ${dateTime(summary.last_at)}.` : 'Sin datos de lotes.'}</p>
      <GridTable columns={['ref', 'ingested_at']} rows={report.batches.items} empty="Sin lotes recientes." /></section>
    <section className="data-card"><h3>Tickets de supermercado (fotos)</h3>
      <GridTable columns={['ref', 'detail', 'amount', 'method', 'ingested_at']} rows={report.receipts} empty="Sin tickets recientes." /></section>
    <section className="data-card"><h3>Gmail (pagos y transferencias)</h3>
      <GridTable columns={['ref', 'detail', 'amount', 'ingested_at']} rows={report.gmail} empty="Sin mensajes recientes." /></section>
    {report.errors.length > 0 && <p className="data-flag">⚠ No se pudo leer: {report.errors.join(', ')}.</p>}
  </div>
}

function CatalogTab({ api }: { api: Api }) {
  const [catalog, setCatalog] = useState<Catalog | null>(null)
  const [error, setError] = useState('')
  const [open, setOpen] = useState<CatalogObject | null>(null)
  const [preview, setPreview] = useState<Preview | null>(null)
  const [previewError, setPreviewError] = useState('')
  const [sort, setSort] = useState('')
  const [dir, setDir] = useState<'asc' | 'desc'>('asc')
  const [offset, setOffset] = useState(0)
  const panel = useRef<HTMLElement | null>(null)
  const load = () => { setError(''); api('/v1/finance/data/catalog').then(r => setCatalog(r as Catalog)).catch(e => setError(e instanceof Error ? e.message : 'No pudimos cargar el catálogo.')) }
  useEffect(load, [])  // eslint-disable-line react-hooks/exhaustive-deps

  async function show(object: CatalogObject, nextOffset = 0, nextSort = sort, nextDir = dir) {
    setOpen(object); setPreviewError(''); setOffset(nextOffset); setSort(nextSort); setDir(nextDir)
    requestAnimationFrame(() => panel.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }))
    const query = new URLSearchParams({ name: object.name, offset: String(nextOffset), limit: '50', dir: nextDir })
    if (nextSort) query.set('sort', nextSort)
    try { setPreview(await api(`/v1/finance/data/preview?${query}`) as Preview) }
    catch (e) { setPreview(null); setPreviewError(e instanceof Error ? e.message : 'No pudimos leer la tabla.') }
  }
  if (error) return <ErrorBox message={error} onRetry={load} />
  if (!catalog) return <p className="data-empty">Leyendo el catálogo…</p>
  return <div className="data-stack">
    <p className="data-note">Proyecto <code>{catalog.project}</code>, dataset <code>{catalog.dataset}</code>. Tocá una tabla o vista para ver su esquema y su descripción.</p>
    <div className="data-catalog">{catalog.objects.map(o => <button key={o.name} className={`data-object ${open?.name === o.name ? 'active' : ''}`} onClick={() => void show(o)}>
      <span className={`data-badge ${o.type.toLowerCase()}`}>{o.type === 'VIEW' ? 'vista' : 'tabla'}</span><strong>{o.name}</strong>
      <small>{o.description}</small>
      <small>{o.rows === null ? 'sin filas propias' : `${o.rows.toLocaleString('es-AR')} filas`} · {bytes(o.size_bytes)} · {dateTime(o.modified)}</small>
      <small className="data-cols">{o.columns.length} columnas</small></button>)}</div>
    {open && <section className="data-card" ref={panel}>
      <div className="data-section-head"><h3>{open.name} <span className={`data-badge ${open.type.toLowerCase()}`}>{open.type === 'VIEW' ? 'vista' : 'tabla'}</span></h3>
        <button className="text-link" onClick={() => setOpen(null)}>Cerrar</button></div>
      <p className="alert-note">{open.description}</p>
      <h4>Esquema</h4>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Campo</th><th>Tipo</th><th>Descripción</th></tr></thead>
        <tbody>{open.columns.map(c => <tr key={c.name}><td><code>{c.name}</code></td><td>{c.type}</td><td className="data-wrap">{c.description}</td></tr>)}</tbody></table></div>
      <h4>Vista previa</h4>
      {previewError && <ErrorBox message={previewError} onRetry={() => void show(open, offset)} />}
      {preview && !previewError && <>
        <p className="data-note">Primeras filas. Los números de tarjeta y CBU aparecen enmascarados.</p>
        <div className="data-table-wrap"><table className="data-table"><thead><tr>{preview.columns.map(c => <th key={c.name}>
          <button className="data-sort" onClick={() => void show(open, 0, c.name, sort === c.name && dir === 'asc' ? 'desc' : 'asc')}>{c.name}{sort === c.name ? (dir === 'asc' ? ' ▲' : ' ▼') : ''}</button></th>)}</tr></thead>
          <tbody>{preview.rows.map((row, i) => <tr key={i}>{preview.columns.map(c => <td key={c.name}>{cell(row[c.name])}</td>)}</tr>)}</tbody></table></div>
        <Pager grid={{ ...preview, total: preview.rows_total, columns: preview.columns.map(c => c.name) }} onPage={next => void show(open, next)} />
      </>}
    </section>}
  </div>
}

const SOURCE_OPTIONS = [['', 'Todas'], ['bank_payments', 'Tarjetas y débitos'], ['bank_transfers', 'Transferencias bancarias'], ['mp_data', 'Mercado Pago'],
  ['mp_transfer_data', 'Transferencias de MP'], ['carrefour_data', 'Carrefour'], ['supermarket_receipts', 'Tickets de supermercado']] as const
const QUALITY_OPTIONS = [['', 'Todos los movimientos'], ['invalid_dates', 'Solo fechas inválidas'], ['no_currency', 'Solo sin divisa'], ['no_category', 'Solo sin categoría']] as const
const EMPTY_FILTERS = { source: '', q: '', category: '', date_from: '', date_to: '', min_amount: '', max_amount: '', quality: '' }

// Columns and order of the movements download. Card and account numbers are
// already masked by the API, so the file carries no more than the screen does.
const MOVEMENT_EXPORT_COLUMNS = [
  { key: 'fecha', label: 'Fecha' }, { key: 'comercio', label: 'Comercio' }, { key: 'categoria', label: 'Categoría' },
  { key: 'producto', label: 'Producto' }, { key: 'monto_total', label: 'Monto' }, { key: 'divisa', label: 'Divisa' },
  { key: 'extraido_de', label: 'Fuente' }, { key: 'metodo_pago', label: 'Medio de pago' }, { key: 'tarjeta', label: 'Tarjeta' },
  { key: 'nro_tarjeta', label: 'Nro. tarjeta' }, { key: 'cuotas', label: 'Cuotas' }, { key: 'detalles', label: 'Detalles' },
]
const EXPORT_LIMIT = 5000

async function fetchAllMovements(api: Api, filters: typeof EMPTY_FILTERS, sort: { by: string; dir: 'asc' | 'desc' }): Promise<Record<string, unknown>[]> {
  const query = new URLSearchParams({ limit: String(EXPORT_LIMIT), offset: '0', sort: sort.by, dir: sort.dir })
  Object.entries(filters).forEach(([key, value]) => { if (value) query.set(key, value) })
  const result = await api(`/v1/finance/data/transactions?${query}`) as TxResult
  return result.rows
}

function TransactionsTab({ api, categories }: { api: Api; categories: string[] }) {
  const [filters, setFilters] = useState(EMPTY_FILTERS)
  const [applied, setApplied] = useState(EMPTY_FILTERS)
  const [grid, setGrid] = useState<TxResult | null>(null)
  const [error, setError] = useState('')
  const [sort, setSort] = useState({ by: 'fecha', dir: 'desc' as 'asc' | 'desc' })
  const [offset, setOffset] = useState(0)
  const LIMIT = 50

  async function run(nextFilters: typeof EMPTY_FILTERS, nextOffset: number, nextSort: typeof sort) {
    setError(''); setOffset(nextOffset)
    const query = new URLSearchParams({ limit: String(LIMIT), offset: String(nextOffset), sort: nextSort.by, dir: nextSort.dir })
    Object.entries(nextFilters).forEach(([key, value]) => { if (value) query.set(key, value) })
    try { setGrid(await api(`/v1/finance/data/transactions?${query}`) as TxResult) }
    catch (e) { setGrid(null); setError(e instanceof Error ? e.message : 'No pudimos cargar los movimientos.') }
  }
  useEffect(() => { void run(applied, 0, sort) }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  function submit(event: FormEvent) {
    event.preventDefault()
    setApplied(filters)
    void run(filters, 0, sort)
  }
  function reset() { setFilters(EMPTY_FILTERS); setApplied(EMPTY_FILTERS); void run(EMPTY_FILTERS, 0, sort) }
  function sortBy(by: string) {
    const next = { by, dir: (sort.by === by && sort.dir === 'desc' ? 'asc' : 'desc') as 'asc' | 'desc' }
    setSort(next); void run(applied, 0, next)
  }
  const field = (key: keyof typeof EMPTY_FILTERS, label: string, type = 'text', extra: Record<string, string> = {}) =>
    <label className="data-field"><span>{label}</span><input type={type} value={filters[key]} {...extra}
      onChange={e => setFilters({ ...filters, [key]: e.target.value })} /></label>

  return <div className="data-stack">
    <form className="data-filters" onSubmit={submit} aria-label="Filtros de movimientos">
      <label className="data-field"><span>Fuente</span><select value={filters.source} onChange={e => setFilters({ ...filters, source: e.target.value })}>
        {SOURCE_OPTIONS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}</select></label>
      {field('q', 'Buscar comercio o producto', 'search', { maxLength: '80', placeholder: 'ej. carrefour' })}
      <label className="data-field"><span>Categoría</span><select value={filters.category} onChange={e => setFilters({ ...filters, category: e.target.value })}>
        <option value="">Todas</option>{categories.map(c => <option key={c} value={c}>{c}</option>)}</select></label>
      {field('date_from', 'Desde', 'date')}{field('date_to', 'Hasta', 'date')}
      {field('min_amount', 'Monto mínimo', 'number', { step: '0.01' })}{field('max_amount', 'Monto máximo', 'number', { step: '0.01' })}
      <label className="data-field"><span>Calidad</span><select value={filters.quality} onChange={e => setFilters({ ...filters, quality: e.target.value })}>
        {QUALITY_OPTIONS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}</select></label>
      <div className="data-filter-actions"><button className="primary" type="submit">Aplicar</button><button className="outline" type="button" onClick={reset}>Limpiar</button></div>
    </form>
    {error && <ErrorBox message={error} onRetry={() => void run(applied, offset, sort)} />}
    {grid && !error && <section className="data-card">
      <div className="data-toolbar"><ExportMenu title="Movimientos" filename="movimientos" columns={MOVEMENT_EXPORT_COLUMNS}
        load={() => fetchAllMovements(api, applied, sort)} /></div>
      <Pager grid={grid} onPage={next => void run(applied, next, sort)} />
      <div className="data-table-wrap"><table className="data-table"><thead><tr>{grid.columns.map(c => <th key={c}>
        <button className="data-sort" onClick={() => sortBy(c)} disabled={c === 'nro_tarjeta'}>
          {c}{sort.by === c ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ''}</button></th>)}</tr></thead>
        <tbody>{grid.rows.map((row, i) => <tr key={i}>{grid.columns.map(c => <td key={c}>{c === 'fecha' ? dateTime(row[c]) : c === 'monto_total' ? cash(row[c] as number) : cell(row[c])}</td>)}</tr>)}</tbody></table></div>
      {grid.rows.length === 0 && <p className="data-empty">Ningún movimiento coincide con estos filtros.</p>}
      <Pager grid={grid} onPage={next => void run(applied, next, sort)} />
    </section>}
  </div>
}

export function FinancialData({ authenticated, api, onLogin, onConnections }: { authenticated: boolean; api: Api; onLogin: () => void; onConnections: () => void }) {
  const [tab, setTab] = useState<Tab>('sources')
  const [categories, setCategories] = useState<string[]>([])
  const [gate, setGate] = useState<'ok' | 'unlinked' | 'error'>('ok')
  const [gateMessage, setGateMessage] = useState('')

  useEffect(() => {
    if (!authenticated) return
    api('/v1/finance/data/sources').then(r => setCategories((r as SourcesReport).categories.map(c => c.name)))
      .catch(e => {
        const text = e instanceof Error ? e.message : ''
        setGateMessage(text || 'No pudimos cargar tus datos.')
        setGate(/vincul|conect|403|503/i.test(text) ? 'unlinked' : 'error')
      })
  }, [authenticated]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!authenticated) return <section className="workspace-card"><h1>Datos</h1><p>Ingresá para explorar tus datos de BigQuery.</p><button className="primary" onClick={onLogin}>Ingresar</button></section>
  if (gate === 'unlinked') return <section className="workspace-card"><h1>Datos</h1><p>{gateMessage}</p><p>Vinculá tu historial financiero desde Conexiones.</p><button className="primary" onClick={onConnections}>Ir a Conexiones</button></section>
  if (gate === 'error') return <section className="workspace-card"><h1>Datos</h1><p role="alert" className="balance-error">{gateMessage}</p></section>

  const tabs: [Tab, string][] = [['sources', 'Fuentes'], ['files', 'Archivos'], ['catalog', 'Tablas y vistas'], ['transactions', 'Movimientos'], ['payslips', 'Recibos de sueldo']]
  return <section className="data-page">
    <header className="data-heading"><span className="data-eyebrow">MIS FINANZAS</span><h1>Datos</h1>
      <p>Qué se ingiere, desde cuándo, cuánto trae cada fuente, y todos los movimientos para validar o explorar.</p></header>
    <nav className="data-tabs" role="tablist" aria-label="Secciones de datos">
      {tabs.map(([key, label]) => <button key={key} role="tab" aria-selected={tab === key} className={tab === key ? 'active' : ''} onClick={() => setTab(key)}>{label}</button>)}
    </nav>
    {tab === 'sources' && <SourcesTab api={api} />}
    {tab === 'files' && <FilesTab api={api} />}
    {tab === 'catalog' && <CatalogTab api={api} />}
    {tab === 'transactions' && <TransactionsTab api={api} categories={categories} />}
    {tab === 'payslips' && <PayslipsTab api={api} />}
  </section>
}
