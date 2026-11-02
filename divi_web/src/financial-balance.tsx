import { useEffect, useMemo, useState, type FormEvent } from 'react'
import { isBalanceReport, monthlySummary, parseIncome, previousMonth, variation, type BalanceReport, type Metric } from './finance-summary'
import './financial-balance.css'

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 2 })
const cash = (value: number | null) => value === null ? 'Sin datos' : money.format(value)
const percent = (value: number | null) => value === null ? 'Sin base de ingresos' : `${value.toLocaleString('es-AR', { maximumFractionDigits: 1 })}%`
const monthName = (month: string) => new Date(`${month}-01T12:00:00`).toLocaleDateString('es-AR', { month: 'long', year: 'numeric' })
const labels: Record<Metric, string> = { income: 'Ingresos', expenses: 'Gastos', savings: 'Ahorro neto' }
type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>

function Difference({ value, base }: { value: number | null; base: number | null }) {
  const delta = variation(value, base)
  return <span className="balance-difference">{delta ? `${delta.amount >= 0 ? '+' : '−'}${cash(Math.abs(delta.amount))} · ${delta.percent === null ? 'sin base porcentual' : `${delta.percent > 0 ? '+' : ''}${percent(delta.percent)}`}` : 'Sin comparación disponible'}</span>
}

export function FinancialBalance({ authenticated, api, onLogin }: { authenticated: boolean; api: Api; onLogin: () => void }) {
  const [report, setReport] = useState<BalanceReport | null>(null)
  const [selected, setSelected] = useState('')
  const [lookback, setLookback] = useState(6)
  const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading')
  const [error, setError] = useState('')
  const [saving, setSaving] = useState(false)
  const [income, setIncome] = useState('')
  const [notice, setNotice] = useState('')
  async function load() {
    setStatus('loading'); setError(''); setReport(null)
    try {
      const result = await api('/v1/finance/balance')
      if (!isBalanceReport(result)) throw new Error('El servicio devolvió un balance no válido. Intentá actualizar.')
      setReport(result); setSelected(current => result.months.some(row => row.month === current) ? current : result.months.at(-1)?.month || ''); setStatus('ready')
    } catch (failure) { setStatus('error'); setError(failure instanceof Error ? failure.message : 'No pudimos cargar tu balance.') }
  }
  useEffect(() => {
    let active = true
    setReport(null); setSelected(''); setError(''); setNotice('')
    if (!authenticated) { setStatus('ready'); return }
    setStatus('loading')
    void api('/v1/finance/balance').then(result => {
      if (!active) return
      if (!isBalanceReport(result)) throw new Error('No pudimos validar los datos del balance.')
      setReport(result); setSelected(result.months.at(-1)?.month || ''); setStatus('ready')
    }).catch(failure => { if (active) { setStatus('error'); setError(failure instanceof Error ? failure.message : 'No pudimos cargar tu balance.') } })
    return () => { active = false }
  }, [authenticated, api])
  const row = report?.months.find(item => item.month === selected)
  useEffect(() => { setIncome(row?.income === null || row?.income === undefined ? '' : String(row.income)); setNotice('') }, [row?.month, row?.income])
  const summary = useMemo(() => monthlySummary(report?.months || [], selected, lookback), [report, selected, lookback])
  const previous = report?.months.find(item => item.month === previousMonth(selected))
  const chart = report?.months.filter(item => item.month <= selected).slice(-(lookback + 1)) || []
  const maximum = Math.max(...chart.flatMap(item => [item.income || 0, item.expenses || 0]), 1)
  async function save(event: FormEvent) {
    event.preventDefault(); setError(''); setNotice('')
    const amount = parseIncome(income)
    if (amount === null) { setError('Usá un importe no negativo, sin separador de miles y con hasta dos decimales.'); return }
    setSaving(true)
    try {
      await api(`/v1/finance/income/${selected}`, 'PUT', { amount, currency: 'ARS' })
      await load(); setNotice('Total de ingresos guardado en tu cuenta.')
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos guardar el ingreso.') }
    finally { setSaving(false) }
  }
  return <div className="balance-page">
    <header className="balance-heading"><div><span className="kicker">FINANZAS PERSONALES · RESULTADOS REALES</span><h1>Tu mes,<br /><em>en balance.</em></h1><p>Lo que entra, lo que sale y lo que queda. Sin confundir el ahorro registrado con una proyección.</p></div>{authenticated && <button className="outline" onClick={() => void load()} disabled={status === 'loading' || saving}>↻ Actualizar</button>}</header>
    {!authenticated ? <section className="balance-panel"><h2>Tu balance es privado</h2><p>Ingresá con la cuenta vinculada al ETL para consultar tus gastos y registrar ingresos. No mostramos datos de ejemplo como si fueran tuyos.</p><button className="primary" onClick={onLogin}>Ingresar</button></section> : <>
      {error && <p className="balance-error" role="alert">{error}</p>}{notice && <p role="status">{notice}</p>}
      {status === 'loading' && <p role="status">Cargando tu balance…</p>}
      {report && row && <>
        <div className="balance-controls"><label>Mes<select value={selected} onChange={event => setSelected(event.target.value)} disabled={saving}>{[...report.months].reverse().map(item => <option key={item.month} value={item.month}>{monthName(item.month)}{item.partial ? ' · en curso' : ''}</option>)}</select></label><label>Promedios históricos<select value={lookback} onChange={event => setLookback(Number(event.target.value))}><option value={3}>Últimos 3 meses completos</option><option value={6}>Últimos 6 meses completos</option></select></label><small>Actualizado al {report.as_of} · ARS</small></div>
        {row.partial && <p className="balance-note">Mes en curso: son importes acumulados, no un cierre. No calculamos variaciones contra un mes completo.</p>}
        <div className="balance-metrics">{(['income', 'expenses', 'savings'] as Metric[]).map(metric => <article key={metric} className={metric === 'savings' && row.savings !== null && row.savings < 0 ? 'negative' : ''}><span>{labels[metric]}</span><strong>{cash(row[metric])}</strong><small>{metric === 'income' ? 'Total mensual informado por vos' : metric === 'expenses' ? 'Gastos registrados en el ETL' : 'Ingresos menos gastos registrados'}</small>{!row.partial && <Difference value={row[metric]} base={previous?.[metric] ?? null} />}</article>)}<article><span>Tasa de ahorro</span><strong>{row.savings_rate === null ? '—' : percent(row.savings_rate)}</strong><small>{row.income === null ? 'Falta registrar el ingreso mensual' : row.income === 0 ? 'No se divide por ingresos cero' : row.expenses === null ? 'Sin gastos registrados para calcular' : 'Ahorro neto / ingresos × 100'}</small>{!row.partial && row.savings_rate !== null && previous?.savings_rate != null && <small>{(row.savings_rate - previous.savings_rate).toFixed(1)} puntos porcentuales vs. mes anterior</small>}</article></div>
        <section className="balance-panel"><div className="balance-section-title"><h2>Tu referencia de los últimos meses</h2><span>Previos al mes seleccionado · sin meses en curso</span></div><div className="balance-averages">{(['income', 'expenses', 'savings'] as Metric[]).map(metric => <article key={metric}><span>Promedio de {labels[metric].toLowerCase()}</span><strong>{cash(summary[metric].value)}</strong><small>{summary[metric].count} de {summary.previous.length} meses con datos</small>{!row.partial && <><small>Mes seleccionado vs. promedio</small><Difference value={row[metric]} base={summary[metric].value} /></>}</article>)}<article><span>Tasa histórica ponderada</span><strong>{summary.savingsRate === null ? '—' : percent(summary.savingsRate)}</strong><small>{summary.rateCount} meses con ingresos y gastos · suma de ahorro / suma de ingresos</small></article></div></section>
        <div className="balance-details"><section className="balance-panel"><h2>Ingresos y gastos por mes</h2><p className="balance-legend"><span>● Ingresos</span><span>● Gastos</span></p><div className="balance-chart">{chart.map(item => <div key={item.month}><b>{monthName(item.month)}{item.partial ? ' *' : ''}</b><div>{(['income', 'expenses'] as const).map(metric => <div className={`balance-track ${metric}`} key={metric}><i style={{ width: `${(item[metric] || 0) / maximum * 100}%` }} /><span>{labels[metric]}: {cash(item[metric])}</span></div>)}</div></div>)}</div></section><section className="balance-panel"><span className="kicker">CARGA MANUAL · ARS</span><h2>Ingreso total de {monthName(selected)}</h2><p>Sumá sueldo, honorarios y otros ingresos efectivamente recibidos. No incluyas transferencias entre tus propias cuentas.</p><form onSubmit={event => void save(event)}><label>Total del mes<input type="text" inputMode="decimal" value={income} onChange={event => setIncome(event.target.value)} placeholder="Ej. 1500000,50" required maxLength={16} disabled={saving} /></label><small>Guardar reemplaza el total de este mes; no lo suma de nuevo. Se guarda en tu cuenta, no en este navegador.</small><button className="primary" type="submit" disabled={saving}>{saving ? 'Guardando…' : 'Guardar ingreso mensual'}</button></form></section></div>
        <section className="balance-panel balance-table"><h2>Historial y variaciones mensuales</h2><table><caption>Las variaciones se calculan solo entre meses cerrados consecutivos con datos. Ahorro negativo significa déficit.</caption><thead><tr><th>Mes</th><th>Ingresos / variación</th><th>Gastos / variación</th><th>Ahorro / variación</th><th>% ahorro</th></tr></thead><tbody>{chart.map((item) => { const index = report.months.indexOf(item); const prior = report.months[index - 1]; return <tr key={item.month}><th>{monthName(item.month)}{item.partial ? ' *' : ''}</th>{(['income', 'expenses', 'savings'] as Metric[]).map(metric => <td key={metric}>{cash(item[metric])}{!item.partial && <Difference value={item[metric]} base={prior?.[metric] ?? null} />}</td>)}<td>{item.savings_rate === null ? '—' : percent(item.savings_rate)}</td></tr> })}</tbody></table></section>
        <p className="balance-note">Gastos: solo movimientos del ETL expresados en ARS o convertidos a ARS por el modelo existente. No se suman gastos de grupos por separado para evitar duplicarlos. Los meses sin registros no se suponen de gasto cero; ingresos faltantes no se suponen cero. El ahorro es un saldo de flujo registrado, no el saldo bancario ni un rendimiento de inversión.</p>
      </>}
    </>}
  </div>
}
