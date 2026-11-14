import { useEffect, useState, type ChangeEvent } from 'react'
import { ExportMenu, type ExportColumn } from './export-menu'
import './phase1.css'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Concept = { concept: string; amount: number }
type Payslip = {
  period: string; period_label: string; pay_date: string | null; employer: string | null; position: string | null
  level: string | null; bank: string | null; identifiers: { cuil: string; legajo: string; account: string }
  haberes: Concept[]; descuentos: Concept[]; gross: number; deductions: number; net: number; deductions_pct_of_gross: number | null
}
type Summary = {
  count: number
  months: { period: string; period_label: string; gross: number; deductions: number; net: number; deductions_pct_of_gross: number | null }[]
  averages: { gross: number; deductions: number; net: number; deductions_pct_of_gross: number | null } | null
  deductions_by_concept: { concept: string; average: number; pct_of_gross: number | null }[]
}
type Report = { payslips: Payslip[]; summary: Summary; currency: string }

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 2 })
const cash = (v: number | null | undefined) => v === null || v === undefined ? '—' : money.format(v)
const pct = (v: number | null | undefined) => v === null || v === undefined ? '—' : `${v.toLocaleString('es-AR', { maximumFractionDigits: 1 })}%`

const HISTORY_COLUMNS: ExportColumn[] = [
  { key: 'period_label', label: 'Período' }, { key: 'gross', label: 'Bruto' }, { key: 'deductions', label: 'Descuentos' },
  { key: 'net', label: 'Neto' }, { key: 'deductions_pct_of_gross', label: 'Descuentos % del bruto' },
]

function readAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = String(reader.result || '')
      resolve(result.includes(',') ? result.split(',')[1] : result)
    }
    reader.onerror = () => reject(new Error('No pudimos leer el archivo.'))
    reader.readAsDataURL(file)
  })
}

export function PayslipsTab({ api }: { api: Api }) {
  const [report, setReport] = useState<Report | null>(null)
  const [status, setStatus] = useState<'loading' | 'ready' | 'busy'>('loading')
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')

  async function load() {
    setStatus('loading'); setError('')
    try { setReport(await api('/v1/finance/payslips') as Report); setStatus('ready') }
    catch (e) { setError(e instanceof Error ? e.message : 'No pudimos cargar los recibos.'); setStatus('ready') }
  }
  useEffect(() => { void load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function upload(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return
    setMessage(''); setError('')
    if (file.type !== 'application/pdf' && !file.name.toLowerCase().endsWith('.pdf')) { setError('Subí un archivo PDF.'); return }
    if (file.size > 3 * 1024 * 1024) { setError('El PDF supera 3 MB.'); return }
    setStatus('busy')
    try {
      const pdf_base64 = await readAsBase64(file)
      setReport(await api('/v1/finance/payslips', 'POST', { pdf_base64 }) as Report)
      setMessage('Recibo guardado. Los importes cuadran con los totales del recibo.')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'No pudimos guardar el recibo.')
    } finally { setStatus('ready') }
  }

  async function remove(period: string) {
    if (!window.confirm(`¿Borrar el recibo de ${period}?`)) return
    setStatus('busy')
    try { setReport(await api(`/v1/finance/payslips/${period}`, 'DELETE') as Report); setMessage('Recibo borrado.') }
    catch (e) { setError(e instanceof Error ? e.message : 'No pudimos borrar el recibo.') }
    finally { setStatus('ready') }
  }

  if (status === 'loading' && !report) return <p className="data-empty">Leyendo tus recibos…</p>
  const latest = report?.payslips[0]
  const avg = report?.summary.averages

  return <div className="data-stack">
    <section className="data-card">
      <div className="data-section-head"><h3>Cargar recibo de sueldo</h3>
        <label className="outline export-btn" style={{ cursor: 'pointer' }}>Elegir PDF
          <input type="file" accept="application/pdf" onChange={upload} disabled={status === 'busy'} style={{ display: 'none' }} /></label></div>
      <p className="alert-note">Se leen los importes del recibo y se guardan. El PDF no se guarda. El CUIL, el legajo y la cuenta quedan enmascarados (solo los últimos 4 dígitos).</p>
      {message && <p role="status" className="alert-message">{message}</p>}
      {error && <p role="alert" className="balance-error">{error}</p>}
    </section>

    {report && report.payslips.length === 0 && <section className="data-card"><p className="data-empty">Todavía no cargaste recibos.</p></section>}

    {avg && <section className="data-card">
      <h3>Promedio de {report!.summary.count} recibo{report!.summary.count === 1 ? '' : 's'}</h3>
      <div className="data-cards">
        <article className="data-card"><span className="data-kicker">Bruto</span><strong>{cash(avg.gross)}</strong></article>
        <article className="data-card"><span className="data-kicker">Descuentos</span><strong>{cash(avg.deductions)}</strong>
          <small>{pct(avg.deductions_pct_of_gross)} del bruto</small></article>
        <article className="data-card"><span className="data-kicker">Neto</span><strong>{cash(avg.net)}</strong></article>
      </div>
      <h3>Cuánto representa cada descuento</h3>
      <div className="data-table-wrap"><table className="data-table"><thead><tr><th>Concepto</th><th>Promedio mensual</th><th>% del bruto</th></tr></thead>
        <tbody>{report!.summary.deductions_by_concept.map(d => <tr key={d.concept}><td>{d.concept}</td><td>{cash(d.average)}</td><td>{pct(d.pct_of_gross)}</td></tr>)}</tbody></table></div>
    </section>}

    {report && report.summary.months.length > 0 && <section className="data-card">
      <div className="data-section-head"><h3>Historial</h3>
        <ExportMenu title="Recibos de sueldo" filename="recibos-de-sueldo" columns={HISTORY_COLUMNS} rows={report.summary.months as unknown as Record<string, unknown>[]} /></div>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Período</th><th>Bruto</th><th>Descuentos</th><th>Neto</th><th>% del bruto</th><th></th></tr></thead>
        <tbody>{report.summary.months.slice().reverse().map(m => <tr key={m.period}>
          <td>{m.period_label}</td><td>{cash(m.gross)}</td><td>{cash(m.deductions)}</td><td>{cash(m.net)}</td><td>{pct(m.deductions_pct_of_gross)}</td>
          <td><button className="text-link" onClick={() => void remove(m.period)}>Borrar</button></td></tr>)}</tbody></table></div>
    </section>}

    {latest && <section className="data-card">
      <h3>Detalle de {latest.period_label}</h3>
      <p className="alert-note">{latest.employer ?? ''}{latest.position ? ` · ${latest.position}` : ''}{latest.level ? ` · ${latest.level}` : ''}{latest.pay_date ? ` · pagado el ${latest.pay_date}` : ''}{latest.bank ? ` · ${latest.bank}` : ''}</p>
      <div className="insight-columns">
        <div><h4>Haberes</h4><ul className="insight-list">{latest.haberes.map(h => <li key={h.concept}><span>{h.concept}</span><strong>{cash(h.amount)}</strong></li>)}</ul>
          <p><strong>Bruto: {cash(latest.gross)}</strong></p></div>
        <div><h4>Descuentos</h4><ul className="insight-list">{latest.descuentos.map(d => <li key={d.concept}><span>{d.concept}</span><strong>{cash(d.amount)} · {pct(latest.gross ? d.amount / latest.gross * 100 : null)}</strong></li>)}</ul>
          <p><strong>Total: {cash(latest.deductions)} ({pct(latest.deductions_pct_of_gross)} del bruto)</strong></p></div>
      </div>
      <p><strong>Neto cobrado: {cash(latest.net)}</strong></p>
      <p className="alert-note">Identificadores: CUIL {latest.identifiers.cuil} · legajo {latest.identifiers.legajo} · cuenta {latest.identifiers.account}</p>
    </section>}
  </div>
}
