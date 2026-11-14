import { useEffect, useRef, useState, type FormEvent } from 'react'

type Api = (path: string, method?: string, body?: unknown) => Promise<any>
type Status = { gmail: { connected: boolean; has_more: boolean; last_sync_at?: number | null }, binance: { connected: boolean } }
type Expense = { source_id: string; date: string; merchant: string; amount: string; currency: string }
type Balance = { asset: string; free: string; locked: string }
type Movement = { asset: string; amount: string; date: string; kind: 'deposit' | 'withdrawal'; status: number | null }
type Trade = { quantity: string; price: string; time: number; side: 'buy' | 'sell' }
let pendingGmailReturn = (() => {
  const url = new URL(window.location.href)
  const fragment = new URLSearchParams(url.hash.slice(1))
  if (fragment.get('gmail') !== 'confirm') return null
  const result = { state: fragment.get('state'), code: fragment.get('code') }
  window.history.replaceState({}, '', url.pathname + url.search)
  return result
})()
const movementDate = (value: string) => /^\d{12,13}$/.test(value) ? new Date(Number(value)).toLocaleString('es-AR') : value
const movementStatus = (row: Movement) => row.kind === 'deposit' ? ({ 0: 'pendiente', 1: 'acreditado', 6: 'acreditado, pendiente de desbloqueo' } as Record<number, string>)[row.status ?? -1] || 'consultar en Binance' : ({ 0: 'confirmación pendiente', 1: 'cancelado', 2: 'pendiente', 3: 'rechazado', 4: 'procesando', 5: 'fallido', 6: 'completado' } as Record<number, string>)[row.status ?? -1] || 'consultar en Binance'

export function ProviderConnections({ api, provider }: { api: Api; provider: 'gmail' | 'binance' }) {
  const [status, setStatus] = useState<Status | null>(null)
  const [error, setError] = useState(''), [notice, setNotice] = useState('')
  const [pending, setPending] = useState(false), [showForm, setShowForm] = useState(false)
  const [apiKey, setApiKey] = useState(''), [apiSecret, setApiSecret] = useState('')
  const [expenses, setExpenses] = useState<Expense[]>([]), [balances, setBalances] = useState<Balance[] | null>(null)
  const [movements, setMovements] = useState<Movement[] | null>(null), [nextPage, setNextPage] = useState<{ kind: 'deposit' | 'withdrawal'; offset: number } | null>(null)
  const [symbol, setSymbol] = useState('BTCUSDT'), [queriedSymbol, setQueriedSymbol] = useState(''), [trades, setTrades] = useState<Trade[] | null>(null), [tradeLimit, setTradeLimit] = useState(false)
  const mounted = useRef(true)
  const syncInFlight = useRef(false)
  useEffect(() => { mounted.current = true; return () => { mounted.current = false } }, [])
  async function refresh() {
    const result = await api('/v1/integrations/status')
    if (!result?.gmail || !result?.binance) throw new Error('No pudimos obtener el estado de las conexiones.')
    setStatus(result)
    if (provider === 'gmail' && result.gmail.connected) {
      const rows = await api('/v1/integrations/gmail/expenses')
      if (!Array.isArray(rows)) throw new Error('No pudimos obtener los gastos importados.')
      setExpenses(rows)
    }
  }
  useEffect(() => { let active = true
    void refresh().catch(() => { if (active) setError('No pudimos cargar la conexión. Reintentá.') })
    return () => { active = false }
  }, [api, provider])
  useEffect(() => {
    if (provider !== 'gmail') return
    const pendingReturn = pendingGmailReturn
    pendingGmailReturn = null
    if (pendingReturn) {
      setPending(true)
      void api('/v1/integrations/gmail/confirm', 'POST', pendingReturn).then(async () => {
        await refresh()
        if (mounted.current) { setNotice('Gmail conectado. Importando tus gastos…'); await syncGmail() }
      }).catch(failure => { if (mounted.current) setError(failure instanceof Error ? failure.message : 'No pudimos confirmar la conexión con Gmail.') })
        .finally(() => { if (mounted.current && !syncInFlight.current) setPending(false) })
    }
    const url = new URL(window.location.href)
    const result = url.searchParams.get('gmail')
    if (!result) return
    url.searchParams.delete('gmail')
    window.history.replaceState({}, '', url.pathname + url.search + url.hash)
    setError(result === 'cancelled' ? 'Cancelaste la autorización de Gmail.' : 'No pudimos completar la conexión con Gmail. Reintentá.')
  }, [provider])
  useEffect(() => {
    if (provider === 'gmail' && status?.gmail.connected && (status.gmail.has_more || !status.gmail.last_sync_at || Date.now() / 1000 - status.gmail.last_sync_at > 3600)) void syncGmail()
  }, [provider, status])
  async function startGmail() {
    setPending(true); setError(''); setNotice('')
    try {
      const result = await api('/v1/integrations/gmail/authorize', 'POST')
      const target = new URL(result?.authorization_url)
      if (target.protocol !== 'https:' || target.hostname !== 'accounts.google.com' || target.pathname !== '/o/oauth2/v2/auth') throw new Error('No recibimos un enlace válido de Google.')
      window.location.assign(target.href)
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos conectar Gmail.'); setPending(false) }
  }
  async function syncGmail() {
    if (syncInFlight.current) return
    syncInFlight.current = true
    setPending(true); setError(''); setNotice('')
    try {
      let more = true, count = 0
      while (more && mounted.current) {
        const result = await api('/v1/integrations/gmail/sync', 'POST')
        if (!Number.isInteger(result?.imported) || typeof result?.has_more !== 'boolean') throw new Error('No pudimos verificar el avance de la importación.')
        count += result.imported; more = result.has_more
        if (mounted.current) setNotice(`Procesando gastos compatibles: ${count} revisados…`)
      }
      if (mounted.current) { await refresh(); setNotice(`Importación terminada: ${count} gastos compatibles procesados.`) }
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos sincronizar Gmail.') }
    finally { syncInFlight.current = false; setPending(false) }
  }
  async function connectBinance(event: FormEvent) {
    event.preventDefault(); setPending(true); setError(''); setNotice('')
    try {
      await api('/v1/integrations/binance/connect', 'POST', { api_key: apiKey, api_secret: apiSecret })
      setApiKey(''); setApiSecret(''); setShowForm(false); await refresh(); setNotice('Binance conectado con permisos de lectura.')
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos conectar Binance.') }
    finally { setPending(false) }
  }
  async function loadBalances() {
    setPending(true); setError(''); setNotice('')
    try { const result = await api('/v1/integrations/binance/balances'); if (!Array.isArray(result?.balances)) throw new Error('No recibimos los saldos.'); setBalances(result.balances) }
    catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos consultar Binance.') }
    finally { setPending(false) }
  }
  async function loadMovements(kind: 'deposit' | 'withdrawal', offset = 0) {
    setPending(true); setError(''); setNotice('')
    try {
      const result = await api(`/v1/integrations/binance/movements/${kind}/${offset}`)
      if (!Array.isArray(result?.items) || !(result.next_offset === null || Number.isInteger(result.next_offset))) throw new Error('No recibimos los movimientos.')
      setMovements(current => offset ? [...(current || []), ...result.items] : result.items)
      setNextPage(result.next_offset === null ? null : { kind, offset: result.next_offset })
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos consultar los movimientos.') }
    finally { setPending(false) }
  }
  async function loadTrades(event: FormEvent) {
    event.preventDefault(); setPending(true); setError(''); setNotice('')
    try {
      if (!/^[A-Z0-9]{5,24}$/.test(symbol)) throw new Error('Ingresá un par válido, por ejemplo BTCUSDT.')
      const result = await api(`/v1/integrations/binance/trades/${symbol}`)
      if (!Array.isArray(result?.items) || typeof result?.limited !== 'boolean') throw new Error('No recibimos las operaciones.')
      setTrades(result.items); setTradeLimit(result.limited); setQueriedSymbol(symbol)
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos consultar las operaciones.') }
    finally { setPending(false) }
  }
  async function disconnect() {
    setPending(true); setError(''); setNotice('')
    try { await api(`/v1/integrations/${provider}`, 'DELETE'); setBalances(null); setMovements(null); setTrades(null); setExpenses([]); await refresh(); setNotice(provider === 'gmail' ? 'Gmail desconectado. Los gastos ya importados permanecen guardados.' : 'Binance desconectado. Si querés desactivar la clave también en Binance, eliminála desde Binance.') }
    catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos desconectar la cuenta.') }
    finally { setPending(false) }
  }
  const connected = status?.[provider].connected === true
  return <div className="connection-instructions">
    <p><strong>{status ? connected ? 'Conectada a tu usuario' : 'Sin conectar' : 'Consultando estado…'}</strong></p>
    {provider === 'gmail' ? <>
      <p>Google pedirá permiso de lectura. Podés importar avisos compatibles de compras Santander de los últimos 90 días. Se muestran acá por separado del historial administrado.</p>
      {!connected ? <button className="outline" disabled={pending || !status} onClick={() => void startGmail()}>Conectar Gmail ↗</button> : <>
        <button className="outline" disabled={pending} onClick={() => void syncGmail()}>{pending ? 'Sincronizando…' : status?.gmail.has_more ? 'Continuar importación' : 'Sincronizar Gmail'}</button>
        <button className="outline" disabled={pending} onClick={() => void disconnect()}>Desconectar Gmail</button>
        <p>Gastos importados: {expenses.length}</p>{expenses.map(row => <p key={row.source_id}>{row.date} · {row.merchant} · {row.amount} {row.currency}</p>)}
      </>}
    </> : <>
      <p>Creá en Binance una API Key con lectura habilitada, sin permisos de trading, transferencias ni retiros.</p>
      {!connected && !showForm && <button className="outline" disabled={pending || !status} onClick={() => setShowForm(true)}>Conectar Binance</button>}
      {!connected && showForm && <form onSubmit={event => void connectBinance(event)}><label>API Key<input autoComplete="off" type="password" value={apiKey} onChange={event => setApiKey(event.target.value)} required /></label><label>API Secret<input autoComplete="off" type="password" value={apiSecret} onChange={event => setApiSecret(event.target.value)} required /></label><button className="outline" disabled={pending}>{pending ? 'Conectando…' : 'Conectar con lectura'}</button><button type="button" className="outline" onClick={() => { setShowForm(false); setApiKey(''); setApiSecret('') }}>Cancelar</button></form>}
      {connected && <><button className="outline" disabled={pending} onClick={() => void loadBalances()}>Consultar saldos</button><button className="outline" disabled={pending} onClick={() => void loadMovements('deposit')}>Ver depósitos</button><button className="outline" disabled={pending} onClick={() => void loadMovements('withdrawal')}>Ver retiros</button><button className="outline" disabled={pending} onClick={() => void disconnect()}>Desconectar Binance</button><form onSubmit={event => void loadTrades(event)}><label>Par de operaciones<input value={symbol} onChange={event => setSymbol(event.target.value.toUpperCase().replace(/[^A-Z0-9]/g, ''))} maxLength={24} required /></label><button className="outline" disabled={pending}>Consultar operaciones del par</button></form>{trades && <div><p>Operaciones de {queriedSymbol}</p>{trades.length ? trades.map((row, index) => <p key={`${row.time}-${index}`}>{row.side === 'buy' ? 'Compra' : 'Venta'} · {row.quantity} a {row.price} · {movementDate(String(row.time))}</p>) : <p>Sin operaciones para mostrar.</p>}{tradeLimit && <small>Binance devolvió las 1.000 operaciones más recientes de este par.</small>}</div>}{balances && (balances.length ? balances.map(row => <p key={row.asset}>{row.asset} · disponible {row.free} · bloqueado {row.locked}</p>) : <p>Sin saldos para mostrar.</p>)}{movements && <div><p>Depósitos o retiros de los últimos 90 días</p>{movements.length ? movements.map((row, index) => <p key={`${row.kind}-${row.asset}-${row.date}-${index}`}>{row.kind === 'deposit' ? 'Depósito' : 'Retiro'} · {row.amount} {row.asset} · {movementDate(row.date)} · {movementStatus(row)}</p>) : <p>Sin movimientos para mostrar.</p>}{nextPage && <button className="outline" disabled={pending} onClick={() => void loadMovements(nextPage.kind, nextPage.offset)}>Cargar más movimientos</button>}</div>}</>}
    </>}
    {notice && <p role="status">{notice}</p>}{error && <p role="alert" className="balance-error">{error}{!status && <button className="outline" onClick={() => { setError(''); void refresh().catch(() => setError('No pudimos cargar la conexión. Reintentá.')) }}>Reintentar</button>}</p>}
  </div>
}
