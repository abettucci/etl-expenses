import { useEffect, useState, type ChangeEvent } from 'react'
import './phase1.css'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Position = { section: string; ticker: string; description: string; quantity: number; price: number; value: number }
type Holdings = {
  as_of: string; positions: Position[]; section_totals: Record<string, number>; holdings_total: number
  printed_total: number; cash_and_dollars_note: string
}
type Movement = { date: string; ticker: string; description: string; kind: string; currency: string; amount: number }
type Trade = { trade_date: string; ticker: string; side: string; quantity: number; price: number; gross: number; fee: number; net: number }
type Income = { month: string; currency: string; amount: number }
type Summary = {
  holdings: Holdings | null
  holdings_history: { as_of: string; holdings_total: number }[]
  income_by_month: Income[]
  movements: Movement[]
  trades: Trade[]
}

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 })
const cash = (value: number | null | undefined) => value === null || value === undefined ? '—' : money.format(value)
const num = (value: number | null | undefined) => value === null || value === undefined ? '—' : value.toLocaleString('es-AR', { maximumFractionDigits: 2 })
const KIND_LABEL: Record<string, string> = { dividend: 'Dividendo', amortization: 'Amortización / renta', interest: 'Intereses', other: 'Otro' }

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

export function InvestmentsTab({ api }: { api: Api }) {
  const [summary, setSummary] = useState<Summary | null>(null)
  const [status, setStatus] = useState<'loading' | 'ready' | 'busy'>('loading')
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')

  async function load() {
    setStatus('loading'); setError('')
    try { setSummary(await api('/v1/finance/investments') as Summary) }
    catch (e) { setError(e instanceof Error ? e.message : 'No pudimos cargar las inversiones.') }
    finally { setStatus('ready') }
  }
  useEffect(() => { void load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function upload(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return
    setMessage(''); setError('')
    const name = file.name.toLowerCase()
    if (!name.endsWith('.pdf') && !name.endsWith('.xlsx')) { setError('Subí el resumen de cuenta (PDF) o los boletos / movimientos (Excel) de Balanz.'); return }
    if (file.size > 5 * 1024 * 1024) { setError('El archivo supera 5 MB.'); return }
    setStatus('busy')
    try {
      const file_base64 = await readAsBase64(file)
      setSummary(await api('/v1/finance/investments/import', 'POST', { file_base64 }) as Summary)
      setMessage('Archivo importado. Las posiciones cuadran con los totales del resumen.')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'No pudimos importar el archivo.')
    } finally { setStatus('ready') }
  }

  if (status === 'loading' && !summary) return <p className="data-empty">Leyendo tus inversiones…</p>
  const holdings = summary?.holdings ?? null
  const sections = holdings ? Object.entries(holdings.section_totals) : []

  return <div className="data-stack">
    <section className="data-card">
      <div className="data-section-head"><h3>Importar desde Balanz</h3>
        <label className="outline export-btn" style={{ cursor: 'pointer' }}>Elegir archivo
          <input type="file" accept=".pdf,.xlsx,application/pdf" onChange={upload} disabled={status === 'busy'} style={{ display: 'none' }} /></label></div>
      <p className="alert-note">Resumen de cuenta en PDF (posición), y boletos o movimientos en Excel. Un archivo solo se guarda si sus totales cuadran. No se guardan el número de comitente, el CUIT ni los nombres; el archivo original no se guarda.</p>
      {message && <p role="status" className="alert-message">{message}</p>}
      {error && <p role="alert" className="balance-error">{error}</p>}
    </section>

    {!holdings && summary && <section className="data-card"><p className="data-empty">Todavía no importaste un resumen de cuenta.</p></section>}

    {holdings && <section className="data-card">
      <h3>Posición al {holdings.as_of}</h3>
      <div className="data-cards">
        <article className="data-card"><span className="data-kicker">Total instrumentos</span><strong>{cash(holdings.holdings_total)}</strong>
          <small>Total del resumen: {cash(holdings.printed_total)}</small></article>
        {sections.map(([name, value]) => <article key={name} className="data-card"><span className="data-kicker">{name}</span><strong>{cash(value)}</strong></article>)}
      </div>
      <p className="alert-note">{holdings.cash_and_dollars_note}</p>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Sección</th><th>Especie</th><th>Descripción</th><th>Cantidad</th><th>Precio</th><th>Valor</th></tr></thead>
        <tbody>{holdings.positions.map(p => <tr key={`${p.section}-${p.ticker}`}><td>{p.section}</td><td>{p.ticker}</td><td>{p.description}</td>
          <td>{num(p.quantity)}</td><td>{num(p.price)}</td><td>{cash(p.value)}</td></tr>)}</tbody></table></div>
    </section>}

    {summary && summary.holdings_history.length > 1 && <section className="data-card">
      <h3>Evolución de la posición</h3>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Fecha del resumen</th><th>Total instrumentos</th></tr></thead>
        <tbody>{summary.holdings_history.map(h => <tr key={h.as_of}><td>{h.as_of}</td><td>{cash(h.holdings_total)}</td></tr>)}</tbody></table></div>
    </section>}

    {summary && summary.income_by_month.length > 0 && <section className="data-card">
      <h3>Ingresos por dividendos y amortizaciones</h3>
      <p className="alert-note">Sumados por mes y por moneda, tal como figuran en el Excel. Las compras y ventas no cuentan como ingreso.</p>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Mes</th><th>Moneda</th><th>Importe</th></tr></thead>
        <tbody>{summary.income_by_month.slice().reverse().map(i => <tr key={`${i.month}-${i.currency}`}><td>{i.month}</td><td>{i.currency}</td><td>{num(i.amount)}</td></tr>)}</tbody></table></div>
    </section>}

    {summary && summary.trades.length > 0 && <section className="data-card">
      <h3>Compras y ventas</h3>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Fecha</th><th>Operación</th><th>Especie</th><th>Cantidad</th><th>Precio</th><th>Bruto</th><th>Arancel</th></tr></thead>
        <tbody>{summary.trades.map((t, i) => <tr key={i}><td>{t.trade_date}</td><td>{t.side}</td><td>{t.ticker}</td>
          <td>{num(t.quantity)}</td><td>{num(t.price)}</td><td>{cash(t.gross)}</td><td>{cash(t.fee)}</td></tr>)}</tbody></table></div>
    </section>}

    {summary && summary.movements.length > 0 && <section className="data-card">
      <h3>Movimientos de efectivo</h3>
      <div className="data-table-wrap"><table className="data-table">
        <thead><tr><th>Fecha</th><th>Tipo</th><th>Especie</th><th>Descripción</th><th>Moneda</th><th>Importe</th></tr></thead>
        <tbody>{summary.movements.slice(0, 200).map((m, i) => <tr key={i}><td>{m.date}</td><td>{KIND_LABEL[m.kind] ?? m.kind}</td><td>{m.ticker}</td>
          <td>{m.description}</td><td>{m.currency}</td><td>{num(m.amount)}</td></tr>)}</tbody></table></div>
    </section>}
  </div>
}
