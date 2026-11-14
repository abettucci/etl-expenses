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

function ComparisonTable({ dimension, mode }: { dimension: Dimension; mode: 'nominal' | 'share' }) {
  if (!dimension.items.length) return <p className="alert-note">Sin movimientos para comparar.</p>
  return <div className="data-table-wrap"><table className="data-table">
    <thead><tr><th>Nombre</th>
      {mode === 'nominal' ? <><th>Mes actual</th><th>Prom. 3 meses</th><th>Vs 3 meses</th><th>Prom. 6 meses</th><th>Vs 6 meses</th></>
        : <><th>Share actual</th><th>Share 3 meses</th><th>Vs 3 meses</th><th>Share 6 meses</th></>}</tr></thead>
    <tbody>{dimension.items.map(item => <tr key={item.name}>
      <td>{item.name}</td>
      {mode === 'nominal' ? <>
        <td>{cash(item.current)}</td><td>{cash(item.avg_3m)}</td>
        <td><span className={item.vs_avg_3m_abs > 0 ? 'insight-delta up' : item.vs_avg_3m_abs < 0 ? 'insight-delta down' : ''}>{cash(item.vs_avg_3m_abs)} · {pct(item.vs_avg_3m_pct, true)}</span></td>
        <td>{cash(item.avg_6m)}</td><td>{pct(item.vs_avg_6m_pct, true)}</td></>
        : <>
          <td>{pct(item.share_current)}</td><td>{pct(item.share_avg_3m)}</td><td>{pp(item.share_vs_avg_3m_pp)}</td><td>{pct(item.share_avg_6m)}</td></>}
    </tr>)}</tbody></table></div>
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
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Mes</th><th>{evoMode === 'nominal' ? 'Total' : '% del período'}</th><th>Top categorías</th><th>Top comercios</th></tr></thead>
        <tbody>{evo.months.map(m => <tr key={m.month}>
          <td>{monthLabel(m.month)}{m.partial ? ' (parcial)' : ''}</td>
          <td>{evoMode === 'nominal' ? cash(m.total) : pct(m.share_of_window)}</td>
          <td>{m.top_categories.map(t => `${t.name} ${pct(t.share)}`).join(' · ') || '—'}</td>
          <td>{m.top_merchants.map(t => `${t.name} ${pct(t.share)}`).join(' · ') || '—'}</td></tr>)}</tbody></table></div>
    </section>

    <section className="insight-card">
      <h2>Medios de pago (últimos 6 meses + mes actual)</h2>
      {report.payment_methods.methods.length === 0 ? <p className="alert-note">Sin gastos con medio de pago en el período.</p> :
        <ul className="insight-list">{report.payment_methods.methods.map(m => <li key={m.name}>
          <span>{m.name}</span><strong>{cash(m.amount)} · {pct(m.share)}</strong></li>)}</ul>}
      <ExportMenu title="Medios de pago" filename={`medios-de-pago-${c.partial_month}`}
        columns={[{ key: 'name', label: 'Medio de pago' }, { key: 'amount', label: 'Monto ARS' }, { key: 'share', label: 'Share %' }]}
        rows={report.payment_methods.methods as unknown as Record<string, unknown>[]} />
    </section>
  </>
}
