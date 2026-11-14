import { useEffect, useState } from 'react'
import './financial-insights.css'
import { ExportMenu } from './export-menu'
import { FinancialComparisons, FxNote } from './financial-comparisons'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Movement = { name: string; current: number; previous: number; change_abs: number; change_pct: number | null; share: number; baseline: number; vs_baseline_pct: number | null }
type Insight = { kind: 'total' | 'category_spike' | 'merchant_up' | 'new_merchant' | 'pace'; severity: 'up' | 'down' | 'info'; name?: string; current?: number; baseline?: number; change_abs?: number; change_pct?: number | null; projected?: number; month?: string }
export type InsightsReport = {
  fx?: { usd_converted_ars: number; quotes_used: { first: string; last: string } | null; skipped_currencies: number; source: { name: string; house: string; field: string } }
  currency: string; as_of: string; has_history: boolean
  months: { month: string; total: number; partial: boolean }[]
  current_month: { month: string; spent: number; days_elapsed: number; days_in_month: number; projected: number; vs_latest_closed_pct: number | null }
  latest: null | { month: string; total: number; previous_month: string; previous_total: number; change_abs: number; change_pct: number | null; baseline_total: number; vs_baseline_pct: number | null }
  categories: Movement[]; merchants_up: Movement[]; merchants_down: Movement[]; new_merchants: Movement[]
  category_trend: Record<string, number[]>; trend_months?: string[]; insights: Insight[]
}

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 })
const cash = (value: number) => money.format(value)
const pct = (value: number | null, signed = true) => value === null ? 'sin base' : `${signed && value > 0 ? '+' : ''}${value.toLocaleString('es-AR', { maximumFractionDigits: 1 })}%`
const shortMonth = (month: string) => new Date(`${month}-01T12:00:00`).toLocaleDateString('es-AR', { month: 'short' }).replace('.', '')
const longMonth = (month: string) => new Date(`${month}-01T12:00:00`).toLocaleDateString('es-AR', { month: 'long', year: 'numeric' })

const CATEGORY_EXPORT_COLUMNS = [
  { key: 'name', label: 'Categoría' }, { key: 'current', label: 'Este mes' }, { key: 'previous', label: 'Mes anterior' },
  { key: 'change_abs', label: 'Variación ARS' }, { key: 'change_pct', label: 'Variación %' },
  { key: 'baseline', label: 'Promedio 3 meses' }, { key: 'vs_baseline_pct', label: 'Vs. promedio %' },
  { key: 'share', label: 'Peso (fracción)' },
]

export function isInsightsReport(value: unknown): value is InsightsReport {
  const report = value as InsightsReport
  return Boolean(report) && typeof report === 'object' && typeof report.has_history === 'boolean' && Array.isArray(report.months)
    && Array.isArray(report.categories) && Array.isArray(report.insights) && typeof report.current_month === 'object' && report.current_month !== null
}

export function describeInsight(item: Insight): string {
  const name = item.name ?? ''
  switch (item.kind) {
    case 'total': return `Gastaste ${cash(Math.abs(item.change_abs ?? 0))} ${(item.change_abs ?? 0) >= 0 ? 'más' : 'menos'} que el mes anterior (${pct(item.change_pct ?? null)}).`
    case 'category_spike': return `${name} está ${pct(item.change_pct ?? null, false)} arriba de tu promedio de los últimos meses: ${cash(item.current ?? 0)} contra ${cash(item.baseline ?? 0)}.`
    case 'merchant_up': return `${name} fue lo que más subió: ${(item.change_abs ?? 0) >= 0 ? '+' : '−'}${cash(Math.abs(item.change_abs ?? 0))}${item.change_pct === null || item.change_pct === undefined ? '' : ` (${pct(item.change_pct)})`}.`
    case 'new_merchant': return `Apareció un comercio nuevo: ${name}, con ${cash(item.current ?? 0)} este mes.`
    case 'pace': return `Al ritmo de este mes cerrarías en ${cash(item.projected ?? 0)}, ${pct(item.change_pct ?? null)} respecto del mes pasado.`
  }
}

function Delta({ amount, percent }: { amount: number; percent: number | null }) {
  if (amount === 0) return <span className="insight-delta flat">Sin cambios</span>
  return <span className={`insight-delta ${amount > 0 ? 'up' : 'down'}`}>{amount > 0 ? '▲' : '▼'} {cash(Math.abs(amount))}{percent === null ? '' : ` · ${pct(percent)}`}</span>
}

function MonthlyChart({ months }: { months: InsightsReport['months'] }) {
  const max = Math.max(...months.map(m => m.total), 1)
  const width = 560, height = 190, pad = 28, slot = (width - pad * 2) / months.length
  return <svg className="insight-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`Gasto mensual de los últimos ${months.length} meses`}>
    {months.map((m, index) => {
      const barHeight = Math.max((m.total / max) * (height - 56), 2)
      const x = pad + index * slot + slot * 0.18
      return <g key={m.month}>
        <rect x={x} y={height - 28 - barHeight} width={slot * 0.64} height={barHeight} rx={5} className={m.partial ? 'bar partial' : 'bar'} />
        <text x={x + slot * 0.32} y={height - 28 - barHeight - 6} textAnchor="middle" className="bar-value">{cash(m.total)}</text>
        <text x={x + slot * 0.32} y={height - 9} textAnchor="middle" className="bar-label">{shortMonth(m.month)}{m.partial ? '*' : ''}</text>
      </g>
    })}
  </svg>
}

function Sparkline({ values }: { values: number[] }) {
  const max = Math.max(...values, 1), width = 120, height = 34
  const points = values.map((v, i) => `${values.length === 1 ? width / 2 : (i / (values.length - 1)) * width},${height - 3 - (v / max) * (height - 8)}`).join(' ')
  return <svg className="insight-spark" viewBox={`0 0 ${width} ${height}`} aria-hidden="true"><polyline points={points} /></svg>
}

function MerchantList({ title, items, empty }: { title: string; items: Movement[]; empty: string }) {
  return <section className="insight-card"><h3>{title}</h3>
    {items.length === 0 ? <p className="insight-empty">{empty}</p> : <ul className="insight-list">{items.map(item => <li key={item.name}><span>{item.name}</span>
      <strong>{item.previous === 0 ? cash(item.current) : <Delta amount={item.change_abs} percent={item.change_pct} />}</strong></li>)}</ul>}
  </section>
}

export function FinancialInsights({ authenticated, api, onLogin, onConnections }: { authenticated: boolean; api: Api; onLogin: () => void; onConnections: () => void }) {
  const [report, setReport] = useState<InsightsReport | null>(null)
  const [status, setStatus] = useState<'loading' | 'ready' | 'unlinked' | 'error'>('loading')
  const [message, setMessage] = useState('')

  async function load() {
    setStatus('loading'); setMessage('')
    try {
      const result = await api('/v1/finance/insights')
      if (!isInsightsReport(result)) throw new Error('El servicio devolvió un análisis no válido. Intentá actualizar.')
      setReport(result); setStatus('ready')
    } catch (error) {
      const text = error instanceof Error ? error.message : ''
      setMessage(text || 'No pudimos cargar el análisis.')
      setStatus(/vincul|conectad|403|503/i.test(text) ? 'unlinked' : 'error')
    }
  }
  useEffect(() => { if (authenticated) void load(); else setStatus('ready') }, [authenticated]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!authenticated) return <section className="workspace-card"><h1>Análisis de gastos</h1><p>Ingresá para ver tus gastos, variaciones y tendencias.</p><button className="primary" onClick={onLogin}>Ingresar</button></section>
  if (status === 'loading') return <section className="insight-page" aria-busy="true"><h1>Análisis de gastos</h1><p className="insight-empty">Calculando tus gastos…</p></section>
  if (status === 'unlinked') return <section className="workspace-card"><h1>Análisis de gastos</h1><p>{message}</p><p>Vinculá tu historial financiero desde Conexiones para ver tus datos de BigQuery acá.</p><button className="primary" onClick={onConnections}>Ir a Conexiones</button></section>
  if (status === 'error' || !report) return <section className="workspace-card"><h1>Análisis de gastos</h1><p role="alert" className="balance-error">{message}</p><button className="outline" onClick={() => void load()}>Reintentar</button></section>

  const { latest, current_month: now } = report
  const top = report.categories[0]
  return <section className="insight-page">
    <header className="insight-heading"><div><span className="insight-eyebrow">MIS FINANZAS</span><h1>Análisis de gastos</h1>
      <p>Lo que se gastó, en qué y qué cambió respecto de los meses anteriores. Datos de tu ETL, en pesos.</p></div>
      <button className="outline" onClick={() => void load()}>Actualizar</button></header>

    {!report.has_history || !latest ? <section className="insight-card"><h2>Todavía hay poco historial</h2>
      <p>Para comparar variaciones necesito al menos dos meses completos con gastos. Mes en curso: {cash(now.spent)} en {now.days_elapsed} días.</p></section> : <>
      <FxNote fx={report.fx} />
      <div className="insight-kpis">
        <article className="insight-card"><span>{longMonth(latest.month)}</span><strong>{cash(latest.total)}</strong><Delta amount={latest.change_abs} percent={latest.change_pct} /><small>contra {longMonth(latest.previous_month)}</small></article>
        <article className="insight-card"><span>Contra tu promedio</span><strong>{pct(latest.vs_baseline_pct)}</strong><small>Promedio de los 3 meses anteriores: {cash(latest.baseline_total)}</small></article>
        <article className="insight-card"><span>Mes en curso</span><strong>{cash(now.spent)}</strong><small>{now.days_elapsed} de {now.days_in_month} días · proyección {cash(now.projected)}{now.vs_latest_closed_pct === null ? '' : ` (${pct(now.vs_latest_closed_pct)})`}</small></article>
        <article className="insight-card"><span>Categoría principal</span><strong>{top ? top.name : '—'}</strong><small>{top ? `${(top.share * 100).toLocaleString('es-AR', { maximumFractionDigits: 0 })}% de los gastos de ${shortMonth(latest.month)}` : ''}</small></article>
      </div>

      <section className="insight-card"><h2>Qué cambió</h2>
        {report.insights.length === 0 ? <p className="insight-empty">Sin variaciones relevantes: tus gastos se mantuvieron en línea con el mes anterior.</p>
          : <ul className="insight-findings">{report.insights.map((item, index) => <li key={`${item.kind}-${index}`} className={item.severity}>{describeInsight(item)}</li>)}</ul>}
      </section>

      <section className="insight-card"><h2>Gasto por mes</h2><MonthlyChart months={report.months} /><small>* Mes en curso, todavía incompleto.</small></section>

      <section className="insight-card"><h2>Categorías de {longMonth(latest.month)}</h2>
        <ExportMenu title={`Categorías ${longMonth(latest.month)}`} filename={`categorias-${latest.month}`} columns={CATEGORY_EXPORT_COLUMNS} rows={report.categories} />
        <div className="insight-table-wrap"><table className="insight-table"><caption className="sr-only">Gasto y variación por categoría</caption>
          <thead><tr><th>Categoría</th><th>Este mes</th><th>Mes anterior</th><th>Variación</th><th>Contra tu promedio</th><th>Peso</th></tr></thead>
          <tbody>{report.categories.map(c => <tr key={c.name}><td>{c.name}</td><td>{cash(c.current)}</td><td>{cash(c.previous)}</td>
            <td><Delta amount={c.change_abs} percent={c.change_pct} /></td><td>{pct(c.vs_baseline_pct)}</td>
            <td><span className="insight-share"><i style={{ width: `${Math.min(c.share * 100, 100)}%` }} /></span>{(c.share * 100).toLocaleString('es-AR', { maximumFractionDigits: 0 })}%</td></tr>)}</tbody></table></div>
      </section>

      <div className="insight-columns">
        <MerchantList title="Comercios que más subieron" items={report.merchants_up} empty="Ningún comercio subió respecto del mes anterior." />
        <MerchantList title="Comercios que más bajaron" items={report.merchants_down} empty="Ningún comercio bajó respecto del mes anterior." />
        <MerchantList title="Comercios nuevos" items={report.new_merchants} empty="No hubo comercios nuevos este mes." />
      </div>

      {Object.keys(report.category_trend).length > 0 && <section className="insight-card"><h2>Tendencia por categoría</h2>
        <ul className="insight-trends">{Object.entries(report.category_trend).map(([name, values]) => <li key={name}><span>{name}</span><Sparkline values={values} /><strong>{cash(values[values.length - 1] ?? 0)}</strong></li>)}</ul>
      </section>}
    </>}
    <FinancialComparisons api={api} />
  </section>
}
