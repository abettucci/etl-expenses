import { useEffect, useState } from 'react'
import { ExportMenu, type ExportColumn } from './export-menu'
import './phase1.css'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Item = {
  name: string; current: number; avg_3m: number; avg_6m: number
  vs_avg_3m_abs: number; vs_avg_3m_pct: number | null; vs_avg_6m_abs: number; vs_avg_6m_pct: number | null
  share_current: number | null; share_avg_3m: number | null; share_avg_6m: number | null; share_vs_avg_3m_pp: number | null
}
type Dimension = { items: Item[]; totals: { current: number; avg_3m: number; avg_6m: number } }
type Named = { name: string; amount: number; share: number | null }
type Report = {
  comparisons: { as_of: string; partial_month: string; days_elapsed: number; days_in_month: number; projected_total: number;
    windows: { current: string; avg_3m: string[]; avg_6m: string[] }; categories: Dimension; merchants: Dimension }
  evolution: { window_total: number; months: { month: string; partial: boolean; total: number; share_of_window: number | null;
    top_categories: Named[]; top_merchants: Named[] }[] }
  payment_methods: { window_total: number; methods: Named[] }
}

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 })
const cash = (v: number | null | undefined) => v === null || v === undefined ? '—' : money.format(v)
const pct = (v: number | null | undefined, signed = false) => v === null || v === undefined ? 'sin base' : `${signed && v > 0 ? '+' : ''}${v.toLocaleString('es-AR', { maximumFractionDigits: 1 })}%`
const pp = (v: number | null) => v === null ? '—' : `${v > 0 ? '+' : ''}${v.toLocaleString('es-AR', { maximumFractionDigits: 1 })} pp`
const monthLabel = (m: string) => new Date(`${m}-01T12:00:00`).toLocaleDateString('es-AR', { month: 'short', year: '2-digit' }).replace('.', '')

const TABLE_COLUMNS: ExportColumn[] = [
  { key: 'name', label: 'Nombre' }, { key: 'current', label: 'Mes actual (parcial)' }, { key: 'avg_3m', label: 'Promedio 3 meses' },
  { key: 'avg_6m', label: 'Promedio 6 meses' }, { key: 'vs_avg_3m_abs', label: 'Vs 3 meses ARS' }, { key: 'vs_avg_3m_pct', label: 'Vs 3 meses %' },
  { key: 'vs_avg_6m_pct', label: 'Vs 6 meses %' }, { key: 'share_current', label: 'Share actual %' },
  { key: 'share_avg_3m', label: 'Share 3 meses %' }, { key: 'share_avg_6m', label: 'Share 6 meses %' }, { key: 'share_vs_avg_3m_pp', label: 'Share vs 3 meses (pp)' },
]

type CompareKey = keyof Item
const NOMINAL_COLUMNS: { key: CompareKey; label: string }[] = [
  { key: 'name', label: 'Nombre' }, { key: 'current', label: 'Mes actual' }, { key: 'avg_3m', label: 'Prom. 3 meses' },
  { key: 'vs_avg_3m_abs', label: 'Vs 3 meses' }, { key: 'avg_6m', label: 'Prom. 6 meses' }, { key: 'vs_avg_6m_pct', label: 'Vs 6 meses' },
]
const SHARE_COLUMNS: { key: CompareKey; label: string }[] = [
  { key: 'name', label: 'Nombre' }, { key: 'share_current', label: 'Share actual' }, { key: 'share_avg_3m', label: 'Share 3 meses' },
  { key: 'share_vs_avg_3m_pp', label: 'Vs 3 meses' }, { key: 'share_avg_6m', label: 'Share 6 meses' },
]

// Numbers sort numerically, text alphabetically, and missing values always go last.
function compareValues(a: unknown, b: unknown, dir: 1 | -1): number {
  const missing = (v: unknown) => v === null || v === undefined
  if (missing(a) && missing(b)) return 0
  if (missing(a)) return 1
  if (missing(b)) return -1
  if (typeof a === 'number' && typeof b === 'number') return (a - b) * dir
  return String(a).localeCompare(String(b), 'es-AR') * dir
}

function ComparisonTable({ dimension, mode }: { dimension: Dimension; mode: 'nominal' | 'share' }) {
  const [sort, setSort] = useState<{ key: CompareKey; dir: 'asc' | 'desc' } | null>(null)
  if (!dimension.items.length) return <p className="alert-note">Sin movimientos para comparar.</p>
  const columns = mode === 'nominal' ? NOMINAL_COLUMNS : SHARE_COLUMNS
  const rows = sort ? [...dimension.items].sort((a, b) => compareValues(a[sort.key], b[sort.key], sort.dir === 'asc' ? 1 : -1)) : dimension.items
  function toggle(key: CompareKey) {
    setSort(prev => prev && prev.key === key ? { key, dir: prev.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'desc' })
  }
  function cell(item: Item, key: CompareKey) {
    switch (key) {
      case 'name': return item.name
      case 'current': case 'avg_3m': case 'avg_6m': return cash(item[key])
      case 'vs_avg_3m_abs': return <span className={item.vs_avg_3m_abs > 0 ? 'insight-delta up' : item.vs_avg_3m_abs < 0 ? 'insight-delta down' : ''}>{cash(item.vs_avg_3m_abs)} · {pct(item.vs_avg_3m_pct, true)}</span>
      case 'vs_avg_6m_pct': return pct(item.vs_avg_6m_pct, true)
      case 'share_current': case 'share_avg_3m': case 'share_avg_6m': return pct(item[key])
      case 'share_vs_avg_3m_pp': return pp(item.share_vs_avg_3m_pp)
      default: return ''
    }
  }
  return <div className="data-table-wrap"><table className="data-table">
    <thead><tr>{columns.map(c => <th key={c.key}>
      <button className="data-sort" onClick={() => toggle(c.key)} aria-label={`Ordenar por ${c.label}`}>
        {c.label}{sort?.key === c.key ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ' ↕'}</button></th>)}</tr></thead>
    <tbody>{rows.map(item => <tr key={item.name}>{columns.map(c => <td key={c.key}>{cell(item, c.key)}</td>)}</tr>)}</tbody></table></div>
}

const PALETTE = ['#a1ffd0', '#4fbf8f', '#ffb494', '#8fb8ff', '#e6d27a', '#c6a0ff', '#7fd1d1']
const OTHERS = '#6f9585'

type Segment = { name: string; value: number; color: string }
type Column = { month: string; partial: boolean; total: number; segments: Segment[] }

// Stacked columns: each month shows its top categories and the rest as "Otros".
// In share mode every column adds up to 100%, so the mix is what changes over time.
function EvolutionChart({ months, mode }: { months: Report['evolution']['months']; mode: 'nominal' | 'share' }) {
  const names: string[] = []
  months.forEach(m => m.top_categories.forEach(c => { if (!names.includes(c.name)) names.push(c.name) }))
  const colorOf = (name: string) => PALETTE[names.indexOf(name) % PALETTE.length]
  const columns: Column[] = months.map(m => {
    const shareTop = m.top_categories.reduce((acc, c) => acc + (c.share ?? 0), 0)
    const topSegments = m.top_categories.map(c => ({
      name: c.name, color: colorOf(c.name),
      value: mode === 'share' ? (c.share ?? 0) : m.total * (c.share ?? 0) / 100,
    }))
    const rest = mode === 'share' ? Math.max(100 - shareTop, 0) : Math.max(m.total - topSegments.reduce((a, s) => a + s.value, 0), 0)
    const segments = rest > 0.05 ? [...topSegments, { name: 'Otros', color: OTHERS, value: rest }] : topSegments
    return { month: m.month, partial: m.partial, total: m.total, segments }
  })
  const max = mode === 'share' ? 100 : Math.max(...columns.map(c => c.total), 1)
  const width = 760, height = 300, pad = 36, slot = (width - pad * 2) / columns.length, barW = slot * 0.62
  const plotH = height - 58
  const legend = [...names, 'Otros']
  return <div className="evo-chart">
    <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={mode === 'share' ? 'Mezcla de categorías por mes, en porcentaje' : 'Gasto mensual por categoría, en pesos'}>
      {[0.25, 0.5, 0.75, 1].map(f => <line key={f} x1={pad} x2={width - pad} y1={height - 26 - plotH * f} y2={height - 26 - plotH * f} className="evo-grid" />)}
      {columns.map((col, index) => {
        let y = height - 26
        const x = pad + index * slot + (slot - barW) / 2
        return <g key={col.month}>
          {col.segments.map(seg => {
            const h = Math.max(seg.value / max * plotH, 0)
            y -= h
            return <rect key={seg.name} x={x} y={y} width={barW} height={h} fill={seg.color} rx={2}>
              <title>{`${monthLabel(col.month)} · ${seg.name}: ${mode === 'share' ? `${seg.value.toFixed(1)}%` : cash(seg.value)}`}</title></rect>
          })}
          <text x={x + barW / 2} y={height - 8} textAnchor="middle" className="evo-label">{monthLabel(col.month)}{col.partial ? '*' : ''}</text>
          {mode === 'nominal' && <text x={x + barW / 2} y={y - 6} textAnchor="middle" className="evo-value">{cash(col.total)}</text>}
        </g>
      })}
    </svg>
    <div className="evo-legend">{legend.map(name => <span key={name}><i style={{ background: name === 'Otros' ? OTHERS : colorOf(name) }} />{name}</span>)}</div>
    <small className="alert-note">* Mes en curso, todavía incompleto.</small>
  </div>
}

function MethodBars({ methods }: { methods: Named[] }) {
  const top = methods.length ? Math.max(...methods.map(m => m.share ?? 0), 1) : 1
  return <div className="method-bars">{methods.map((m, i) => <div className="method-row" key={m.name}>
    <span className="method-name">{m.name}</span>
    <div className="method-track"><i style={{ width: `${((m.share ?? 0) / top) * 100}%`, background: PALETTE[i % PALETTE.length] }} /></div>
    <strong>{pct(m.share)}</strong><small>{cash(m.amount)}</small>
  </div>)}</div>
}

export function FinancialComparisons({ api }: { api: Api }) {
  const [report, setReport] = useState<Report | null>(null)
  const [error, setError] = useState('')
  const [dimension, setDimension] = useState<'categories' | 'merchants'>('categories')
  const [mode, setMode] = useState<'nominal' | 'share'>('nominal')
  const [evoMode, setEvoMode] = useState<'nominal' | 'share'>('nominal')

  const load = () => {
    setError('')
    api('/v1/finance/comparisons').then(r => setReport(r as Report)).catch(e => setError(e instanceof Error ? e.message : 'No pudimos cargar las comparaciones.'))
  }
  useEffect(load, [])  // eslint-disable-line react-hooks/exhaustive-deps

  if (error) return <section className="insight-card"><h2>Comparaciones</h2><p role="alert" className="balance-error">{error}</p><button className="outline" onClick={load}>Reintentar</button></section>
  if (!report) return <section className="insight-card"><h2>Comparaciones</h2><p className="insight-empty">Calculando…</p></section>

  const c = report.comparisons
  const active = c[dimension]
  const evo = report.evolution

  return <>
    <section className="insight-card">
      <div className="data-section-head"><h2>Comparaciones por {dimension === 'categories' ? 'categoría' : 'comercio'}</h2>
        <div className="data-filter-actions">
          <button className={`outline ${dimension === 'categories' ? 'active-chip' : ''}`} onClick={() => setDimension('categories')}>Categorías</button>
          <button className={`outline ${dimension === 'merchants' ? 'active-chip' : ''}`} onClick={() => setDimension('merchants')}>Comercios</button>
          <button className={`outline ${mode === 'nominal' ? 'active-chip' : ''}`} onClick={() => setMode('nominal')}>Pesos</button>
          <button className={`outline ${mode === 'share' ? 'active-chip' : ''}`} onClick={() => setMode('share')}>% share</button>
        </div></div>
      <p className="alert-note">Mes actual: {monthLabel(c.partial_month)}, día {c.days_elapsed} de {c.days_in_month}. Proyección del mes: {cash(c.projected_total)}. Los promedios son mensuales sobre los meses cerrados ({c.windows.avg_3m.length} y {c.windows.avg_6m.length}).</p>
      <ComparisonTable dimension={active} mode={mode} />
      <ExportMenu title={`Comparaciones ${dimension === 'categories' ? 'categorías' : 'comercios'}`} filename={`comparaciones-${dimension}-${c.partial_month}`}
        columns={TABLE_COLUMNS} rows={active.items as unknown as Record<string, unknown>[]} />
    </section>

    <section className="insight-card">
      <div className="data-section-head"><h2>Evolución mensual</h2>
        <div className="data-filter-actions">
          <button className={`outline ${evoMode === 'nominal' ? 'active-chip' : ''}`} onClick={() => setEvoMode('nominal')}>Pesos</button>
          <button className={`outline ${evoMode === 'share' ? 'active-chip' : ''}`} onClick={() => setEvoMode('share')}>% del período</button>
        </div></div>
      <p className="alert-note">{evoMode === 'share' ? 'Cada columna suma 100%: muestra cómo se reparte tu gasto entre categorías en cada mes.' : 'Gasto total de cada mes, separado por categoría principal.'}</p>
      <EvolutionChart months={evo.months} mode={evoMode} />
    </section>

    <section className="insight-card">
      <h2>Medios de pago (últimos 6 meses + mes actual)</h2>
      {report.payment_methods.methods.length === 0 ? <p className="alert-note">Sin gastos con medio de pago en el período.</p>
        : <MethodBars methods={report.payment_methods.methods} />}
      <ExportMenu title="Medios de pago" filename={`medios-de-pago-${c.partial_month}`}
        columns={[{ key: 'name', label: 'Medio de pago' }, { key: 'amount', label: 'Monto ARS' }, { key: 'share', label: 'Share %' }]}
        rows={report.payment_methods.methods as unknown as Record<string, unknown>[]} />
    </section>
  </>
}
