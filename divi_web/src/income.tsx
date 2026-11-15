import { useEffect, useState } from 'react'
import './phase1.css'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Step = { from: string; to: string; months: number; gross_change_pct: number | null; net_change_pct: number | null
  inflation_pct: number | null; real_change_pct: number | null; beat_inflation: boolean | null; raise: boolean }
type Summary = { first_period: string; last_period: string; gross_growth_pct: number | null; inflation_pct: number | null
  real_growth_pct: number | null; raises_count: number; largest_raise: Step | null; raise_months: string[]; months_beating_inflation: number; months_compared: number }
type Analysis = { count: number; steps: Step[]; raises: Step[]; largest_raise: Step | null; all_time: Summary | null; year: (Summary & { year: number }) | null }

const pct = (v: number | null | undefined, signed = true) => v === null || v === undefined ? 'sin datos' : `${signed && v > 0 ? '+' : ''}${v.toLocaleString('es-AR', { maximumFractionDigits: 1 })}%`
const monthLabel = (m: string) => new Date(`${m}-01T12:00:00`).toLocaleDateString('es-AR', { month: 'short', year: '2-digit' }).replace('.', '')

function StepBars({ steps }: { steps: Step[] }) {
  const usable = steps.filter(s => s.gross_change_pct !== null)
  if (!usable.length) return <p className="alert-note">Necesitás dos recibos de distintos meses para comparar.</p>
  const max = Math.max(1, ...usable.flatMap(s => [Math.abs(s.gross_change_pct ?? 0), Math.abs(s.inflation_pct ?? 0)]))
  const width = 760, rowH = 46, left = 130, barMax = width - left - 80
  const height = usable.length * rowH + 20
  return <svg className="income-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Aumento del bruto frente a la inflación, por tramo">
    {usable.map((s, i) => {
      const y = 10 + i * rowH
      const g = Math.abs(s.gross_change_pct ?? 0) / max * barMax
      const inf = Math.abs(s.inflation_pct ?? 0) / max * barMax
      return <g key={`${s.from}-${s.to}`}>
        <text x={left - 10} y={y + 14} textAnchor="end" className="evo-label">{monthLabel(s.from)} → {monthLabel(s.to)}</text>
        <rect x={left} y={y} width={g} height={16} rx={3} fill="#a1ffd0"><title>{`Bruto ${pct(s.gross_change_pct)}`}</title></rect>
        <text x={left + g + 6} y={y + 13} className="evo-value">{pct(s.gross_change_pct)}</text>
        <rect x={left} y={y + 20} width={inf} height={12} rx={3} fill="#ffb494"><title>{`Inflación ${pct(s.inflation_pct, false)}`}</title></rect>
        <text x={left + inf + 6} y={y + 30} className="evo-value">{pct(s.inflation_pct, false)} inflación</text>
      </g>
    })}
  </svg>
}

export function IncomeAnalysis({ api, hasPayslips }: { api: Api; hasPayslips: boolean }) {
  const [data, setData] = useState<Analysis | null>(null)
  const [error, setError] = useState('')
  const year = new Date().getFullYear()

  function load() {
    setError('')
    api(`/v1/finance/income/analysis?year=${year}`).then(r => setData(r as Analysis)).catch(e => setError(e instanceof Error ? e.message : 'No pudimos calcular tus ingresos.'))
  }
  useEffect(() => { if (hasPayslips) load() }, [hasPayslips]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!hasPayslips) return null
  if (error) return <section className="data-card"><p role="alert" className="balance-error">{error}</p><button className="outline" onClick={load}>Reintentar</button></section>
  if (!data) return <section className="data-card"><p className="data-empty">Calculando ingresos…</p></section>

  const all = data.all_time
  const yr = data.year
  return <div className="data-stack">
    <section className="data-card">
      <h3>Ingresos y aumentos</h3>
      <div className="data-cards">
        <article className="data-card"><span className="data-kicker">Crecimiento del bruto desde {all ? monthLabel(all.first_period) : ''}</span>
          <strong>{pct(all?.gross_growth_pct)}</strong><small>Inflación en el mismo período: {pct(all?.inflation_pct, false)}</small></article>
        <article className="data-card"><span className="data-kicker">Frente a la inflación</span>
          <strong>{pct(all?.real_growth_pct)}</strong><small>{all && all.months_beating_inflation} de {all?.months_compared ?? 0} tramos le ganaron a la inflación</small></article>
        <article className="data-card"><span className="data-kicker">Aumentos registrados</span>
          <strong>{data.raises.length}</strong><small>{data.largest_raise ? `Mayor: ${pct(data.largest_raise.gross_change_pct)} en ${monthLabel(data.largest_raise.to)}` : 'Sin aumentos todavía'}</small></article>
      </div>
      {yr && <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Año {yr.year}</th><th>Crecimiento</th><th>Inflación</th><th>Real</th><th>Aumentos</th><th>Meses con aumento</th></tr></thead>
        <tbody><tr><td>{monthLabel(yr.first_period)} → {monthLabel(yr.last_period)}</td><td>{pct(yr.gross_growth_pct)}</td><td>{pct(yr.inflation_pct, false)}</td>
          <td>{pct(yr.real_growth_pct)}</td><td>{yr.raises_count}</td><td>{yr.raise_months.map(monthLabel).join(', ') || '—'}</td></tr></tbody></table></div>}
      {!yr && <p className="alert-note">Para ver el año necesitás al menos dos recibos de {year}.</p>}
    </section>

    <section className="data-card">
      <h3>Cada tramo, frente a la inflación</h3>
      <StepBars steps={data.steps} />
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Desde</th><th>Hasta</th><th>Meses</th><th>Bruto</th><th>Neto</th><th>Inflación</th><th>Real</th><th>Aumento</th></tr></thead>
        <tbody>{data.steps.slice().reverse().map(s => <tr key={`${s.from}-${s.to}`}>
          <td>{monthLabel(s.from)}</td><td>{monthLabel(s.to)}</td><td>{s.months}</td><td>{pct(s.gross_change_pct)}</td><td>{pct(s.net_change_pct)}</td>
          <td>{pct(s.inflation_pct, false)}</td><td className={s.beat_inflation ? 'insight-delta down' : s.beat_inflation === false ? 'insight-delta up' : ''}>{pct(s.real_change_pct)}</td>
          <td>{s.raise ? 'Sí' : 'No'}</td></tr>)}</tbody></table></div>
      <p className="alert-note">Un aumento es una suba del bruto mayor a 0,5% entre recibos. Si faltan meses entre dos recibos, el tramo abarca todos esos meses y la inflación se acumula sobre ellos.</p>
    </section>
  </div>
}
