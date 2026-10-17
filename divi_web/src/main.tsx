import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'

if ('serviceWorker' in navigator) window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js'))

type View = 'home' | 'groups' | 'activity' | 'wallet' | 'settings' | 'gifts' | 'panorama'
type Gift = { id: string; title: string; price: number; available: boolean; link?: string }
type Expense = { title: string; category: string; amount: number; payer: string; date: string }
type GroupSummary = { group_id: string; name: string; currency: string; role?: string; legacy_source_key?: string }
type GroupMember = { user_id: string; display_name: string; role: string; active: boolean; claim_state: string }
type GroupExpenseRecord = { expense_id: string; description: string; amount: string; currency: string; category?: string; created_at: string; deleted_at?: string; allocations?: Record<string, string> }
type GroupSettlement = { from_user_id: string; to_user_id: string; amount: string }
type Forecast = {
  mode?: 'live' | 'preview'
  data_source?: string
  currency: string
  as_of: string
  period: { history_start: string; history_end: string; forecast_month: string; months_observed: number }
  forecast: { monthly_total: number; recurring_total: number; flexible_total: number; current_month_spent: number; projected_month_end: number; trend_vs_last_month: number | null; confidence: string }
  monthly_history: Array<{ month: string; amount: number }>
  categories: Array<{ category: string; expected_monthly: number; share: number }>
  recurring: Array<{ merchant: string; category: string; expected_monthly: number; typical_charge: number; months_observed: number; observation_rate: number }>
}

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 })
const apiBase = import.meta.env.VITE_API_URL || ''
const previewForecast: Forecast = {
  mode: 'preview', currency: 'ARS', as_of: '2026-10-10',
  period: { history_start: '2026-04-01', history_end: '2026-09-30', forecast_month: '2026-10-01', months_observed: 6 },
  forecast: { monthly_total: 482000, recurring_total: 171500, flexible_total: 310500, current_month_spent: 168900, projected_month_end: 506700, trend_vs_last_month: 0.06, confidence: 'media' },
  monthly_history: [
    { month: '2026-04-01', amount: 421000 }, { month: '2026-05-01', amount: 448000 }, { month: '2026-06-01', amount: 465000 },
    { month: '2026-07-01', amount: 439000 }, { month: '2026-08-01', amount: 508000 }, { month: '2026-09-01', amount: 471000 },
  ],
  categories: [
    { category: 'Comida', expected_monthly: 157000, share: .33 }, { category: 'Servicios', expected_monthly: 119000, share: .25 },
    { category: 'Transporte', expected_monthly: 73000, share: .15 }, { category: 'Educación', expected_monthly: 58000, share: .12 },
    { category: 'Otros', expected_monthly: 75000, share: .15 },
  ],
  recurring: [
    { merchant: 'Alquiler', category: 'Vivienda', expected_monthly: 92000, typical_charge: 92000, months_observed: 6, observation_rate: 1 },
    { merchant: 'UDEMY', category: 'Educación', expected_monthly: 41000, typical_charge: 41000, months_observed: 5, observation_rate: .83 },
    { merchant: 'Spotify', category: 'Servicios', expected_monthly: 8500, typical_charge: 8500, months_observed: 6, observation_rate: 1 },
    { merchant: 'Internet', category: 'Servicios', expected_monthly: 30000, typical_charge: 30000, months_observed: 6, observation_rate: 1 },
  ],
}

async function api(path: string, method = 'GET', body?: unknown) {
  if (!apiBase) return null
  const token = localStorage.getItem('divi_access_token')
  const result = await fetch(`${apiBase}${path}`, { method, headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) }, body: body ? JSON.stringify(body) : undefined })
  if (!result.ok) throw new Error((await result.json()).error || 'No pudimos completar la acción')
  return result.status === 204 ? null : result.json()
}

function Mark() { return <span className="mark"><i /><i /><i /><i /></span> }
function Avatar({ name, tone = 'violet' }: { name: string; tone?: string }) { return <span className={`avatar ${tone}`}>{name.slice(0, 1).toUpperCase()}</span> }
function Action({ icon, title, caption, onClick }: { icon: string; title: string; caption: string; onClick?: () => void }) { return <button className="list-action" onClick={onClick}><span className="action-icon">{icon}</span><span><strong>{title}</strong><small>{caption}</small></span><b>›</b></button> }

function App() {
  const [view, setView] = useState<View>('home')
  const [overlay, setOverlay] = useState<'expense' | 'assistant' | 'pool' | 'premium' | null>(null)
  const [notice, setNotice] = useState('')
  const [gifts, setGifts] = useState<Gift[]>([
    { id: '1', title: 'Auriculares Sony WH-1000XM5', price: 579000, available: true },
    { id: '2', title: 'Cafetera espresso compacta', price: 198000, available: false },
    { id: '3', title: 'Clase de cerámica', price: 48000, available: true },
  ])
  const [expenses, setExpenses] = useState<Expense[]>([
    { title: 'Birras en la terraza', category: 'Comida', amount: 14900, payer: 'Matías', date: '14 sep' },
    { title: 'Uber de vuelta', category: 'Transporte', amount: 9800, payer: 'Betu', date: '14 sep' },
  ])
  const [groups, setGroups] = useState<GroupSummary[]>([])
  const [activeGroupId, setActiveGroupId] = useState('demo')
  const [groupRefresh, setGroupRefresh] = useState(0)
  const [chat, setChat] = useState<string[]>(['Hola, soy Divia. Puedo ayudarte a ordenar gastos, ver tu balance o armar el regalo de Fran ✦'])
  const balance = useMemo(() => expenses.reduce((sum, expense) => sum + expense.amount, 0), [expenses])
  const notify = (message: string) => { setNotice(message); window.setTimeout(() => setNotice(''), 3200) }
  const openExpense = (groupId = activeGroupId) => {
    if (!apiBase || !groupId || groupId === 'demo') { notify('Primero conectá la API, iniciá sesión y creá o elegí un grupo.'); return }
    setActiveGroupId(groupId); setOverlay('expense')
  }
  const reserve = async (gift: Gift) => {
    if (!gift.available) return
    try { await api(`/v1/groups/demo/events/fran-2026/wishlist/${gift.id}/reserve`, 'POST') } catch (e) { notify(e instanceof Error ? e.message : 'No disponible') ; return }
    setGifts(items => items.map(item => item.id === gift.id ? { ...item, available: false } : item)); notify('Regalo reservado para este grupo. Los demás grupos ya no lo verán disponible.')
  }
  useEffect(() => {
    if (!apiBase) return
    void api('/v1/groups').then(result => {
      if (Array.isArray(result)) {
        setGroups(result as GroupSummary[])
        if (result[0]?.group_id) setActiveGroupId(result[0].group_id)
      }
    }).catch(() => undefined)
  }, [])
  useEffect(() => {
    if (!apiBase) return
    const token = new URLSearchParams(window.location.search).get('invite')
    if (!token) return
    void api(`/v1/invitations/${encodeURIComponent(token)}/accept`, 'POST').then(() => {
      window.history.replaceState({}, '', window.location.pathname)
      notify('Invitación aceptada. El grupo ya aparece en tus grupos.')
      return api('/v1/groups')
    }).then(result => { if (Array.isArray(result)) setGroups(result as GroupSummary[]) })
      .catch(error => notify(error instanceof Error ? error.message : 'No pudimos aceptar la invitación.'))
  }, [])
  return <main>
    <div className="aurora" /><div className="grain" />
    <header className="topbar"><button className="brand" onClick={() => setView('home')}><Mark /><span>divi</span></button><div className="desktop-meta"><button className={`panorama-link ${view === 'panorama' ? 'active' : ''}`} onClick={() => setView('panorama')}>⌁ Panorama</button><span className="pill live">● grupo activo</span><Avatar name="Betu" /><span>Betu</span></div></header>
    <section className="shell">
      {view === 'home' && <Home balance={balance} onExpense={() => openExpense()} onPanorama={() => setView('panorama')} onGift={() => setView('gifts')} />}
      {view === 'groups' && <Groups groups={groups} refreshSignal={groupRefresh} onRefresh={() => void api('/v1/groups').then(result => Array.isArray(result) && setGroups(result as GroupSummary[])).catch(() => notify('No pudimos actualizar tus grupos.'))} onExpense={openExpense} onGift={() => setView('gifts')} />}
      {view === 'activity' && <Activity expenses={expenses} />}
      {view === 'wallet' && <Wallet onPremium={() => setOverlay('premium')} />}
      {view === 'settings' && <Settings notify={notify} />}
      {view === 'gifts' && <Gifts gifts={gifts} reserve={reserve} onPool={() => setOverlay('pool')} />}
      {view === 'panorama' && <Panorama />}
    </section>
    <nav className="dock" aria-label="Navegación principal">
      <button className={view === 'home' ? 'active' : ''} onClick={() => setView('home')}><span>⌂</span>Inicio</button>
      <button className={view === 'groups' || view === 'gifts' ? 'active' : ''} onClick={() => setView('groups')}><span>♧</span>Grupos</button>
      <button className="plus" onClick={() => openExpense()} aria-label="Agregar gasto">+</button>
      <button className={view === 'activity' ? 'active' : ''} onClick={() => setView('activity')}><span>♙</span>Actividad</button>
      <button className={view === 'wallet' ? 'active' : ''} onClick={() => setView('wallet')}><span>▣</span>Billetera</button>
    </nav>
    <button className="divia" onClick={() => setOverlay('assistant')} aria-label="Abrir asistente Divia">✧</button>
    {notice && <div className="toast" role="status">✓ {notice}</div>}
    {overlay === 'expense' && <ExpenseModal groupId={activeGroupId} close={() => setOverlay(null)} submit={expense => { setExpenses([expense, ...expenses]); setOverlay(null); notify('Gasto agregado y dividido en el grupo.') }} onSaved={() => setGroupRefresh(value => value + 1)} />}
    {overlay === 'assistant' && <Assistant chat={chat} setChat={setChat} close={() => setOverlay(null)} />}
    {overlay === 'premium' && <PremiumModal close={() => setOverlay(null)} notify={notify} />}
    {overlay === 'pool' && <PoolModal close={() => setOverlay(null)} notify={notify} />}
  </main>
}

function Home({ balance, onExpense, onPanorama, onGift }: { balance: number; onExpense: () => void; onPanorama: () => void; onGift: () => void }) { return <>
  <div className="eyebrow">La cuenta del grupo <span>〰</span></div><h1>Que la plata<br /><em>no corte el plan.</em></h1>
  <section className="hero-panel stagger"><div className="group-line"><div><span className="kicker">GRUPO ACTUAL</span><h2>Militantes del mood</h2></div><span className="members">♧ 4</span></div><div className="hero-actions"><button onClick={onExpense}>＋<span>Agregar gasto</span></button><button className="pink" onClick={onGift}>♔<span>Regalo de Fran</span></button></div><div className="balance"><span>MI BALANCE</span><strong>− {money.format(6175)}</strong><small>Te queda una liquidación pendiente</small></div><div className="settlement"><Avatar name="B" /><p><b>Betu</b> le debe a <b>matias.arbues7</b><strong>{money.format(6175)}</strong></p><button>Resolver →</button></div></section>
  <section className="feature-card"><span className="action-icon">⌘</span><div><h3>Tu plata, con memoria</h3><p>Gastos compartidos, tickets y hábitos en una sola vista.</p></div><button onClick={onPanorama}>Ver mi panorama <b>→</b></button></section>
  <div className="metrics"><div><span>GASTO DEL GRUPO</span><strong>{money.format(balance)}</strong><small>últimos 7 días</small></div><div><span>PRÓXIMO HITO</span><strong>Fran · 21 sep</strong><small>Regalo en marcha</small></div></div>
</> }

function monthShort(value: string) {
  return new Intl.DateTimeFormat('es-AR', { month: 'short' }).format(new Date(`${value}T12:00:00`)).replace('.', '')
}

function Panorama() {
  const [forecast, setForecast] = useState<Forecast>(previewForecast)
  const [status, setStatus] = useState<'preview' | 'loading' | 'live'>('preview')
  const [message, setMessage] = useState('Vista previa · conectá tu historial ETL para ver tus importes.')
  const maxHistory = Math.max(...forecast.monthly_history.map(item => item.amount), 1)
  const reload = async () => {
    if (!apiBase) { setStatus('preview'); setMessage('Vista previa · configurá VITE_API_URL para conectar el panorama.'); return }
    setStatus('loading')
    try {
      const result = await api('/v1/finance/forecast') as Forecast | null
      if (result?.mode === 'live') {
        setForecast(result)
        setStatus('live')
        setMessage(`Datos ETL agregados · ${result.period.months_observed} meses de historia.`)
      } else {
        setStatus('preview')
        setMessage('Vista previa · el panorama aún no está habilitado para esta cuenta.')
      }
    } catch {
      setStatus('preview')
      setMessage('Vista previa · no pudimos conectar tu historial en este momento.')
    }
  }
  useEffect(() => { void reload() }, [])
  const trend = forecast.forecast.trend_vs_last_month

  return <>
    <div className="panorama-title"><div><span className="kicker">ESPACIO PERSONAL · ETL</span><h1>Tu flujo de plata,<br /><em>antes de que pase.</em></h1><p>Una proyección mensual construida con los gastos que ya registraste.</p></div><button className="sync-button" onClick={() => void reload()} disabled={status === 'loading'}>{status === 'loading' ? 'Sincronizando…' : '↻ Sincronizar panorama'}</button></div>
    <div className={`forecast-status ${status}`}><span>{status === 'live' ? '●' : '◌'}</span>{message}</div>

    <section className="signal-flow" aria-label="Flujo de señales del ETL">
      <div className="signal-copy"><span className="kicker">SEÑALES QUE SUMAN</span><h2>Del movimiento<br />a la claridad.</h2><p>El panorama no te pide cargar todo de nuevo: ordena las señales que ya llegan a tu sistema.</p></div>
      <div className="signal-sources">
        <article><span>✉</span><div><b>Correo</b><small>Comprobantes y avisos</small></div><i>→</i></article>
        <article><span>▤</span><div><b>Tickets</b><small>Fotos y PDFs procesados</small></div><i>→</i></article>
        <article><span>◉</span><div><b>Telegram</b><small>Voz y gastos manuales</small></div><i>→</i></article>
      </div>
      <div className="signal-core"><Mark /><span>ETL</span><small>normaliza</small></div>
      <div className="signal-output"><span className="kicker">OCTUBRE</span><b>Flujo previsto</b><strong>{money.format(forecast.forecast.monthly_total)}</strong><small>{forecast.period.months_observed} meses de señales comparables</small><div><i style={{ width: `${Math.min((forecast.forecast.recurring_total / Math.max(forecast.forecast.monthly_total, 1)) * 100, 100)}%` }} /></div></div>
    </section>

    <section className="forecast-grid">
      <article className="forecast-hero"><div><span className="kicker">PROYECCIÓN DEL MES</span><strong>{money.format(forecast.forecast.projected_month_end)}</strong><p>Estimación de cierre usando tu ritmo actual y los últimos {forecast.period.months_observed} meses completos.</p></div><div className="forecast-orbit"><i /><i /><b>◔</b></div><footer><span>Ya registrado <b>{money.format(forecast.forecast.current_month_spent)}</b></span><span className={trend !== null && trend > 0 ? 'up' : ''}>{trend === null ? 'sin comparación' : `${trend > 0 ? '↗' : '↘'} ${Math.abs(trend * 100).toFixed(0)}% vs. mes anterior`}</span></footer></article>
      <article className="cash-card"><span className="kicker">CAJA MENSUAL ESTIMADA</span><h2>Lo que suele<br />salir de tu cuenta.</h2><div className="cash-line"><span><i className="recurring-dot" />Recurrente</span><b>{money.format(forecast.forecast.recurring_total)}</b></div><div className="cash-line"><span><i className="flexible-dot" />Flexible</span><b>{money.format(forecast.forecast.flexible_total)}</b></div><div className="cash-total"><span>Promedio mensual</span><strong>{money.format(forecast.forecast.monthly_total)}</strong></div></article>
      <article className="history-card"><header><div><span className="kicker">HISTORIAL</span><h2>Ritmo mensual</h2></div><span>{forecast.forecast.confidence} confianza</span></header><div className="history-bars">{forecast.monthly_history.map(item => <div key={item.month}><i style={{ height: `${Math.max((item.amount / maxHistory) * 100, 8)}%` }} title={money.format(item.amount)} /><span>{monthShort(item.month)}</span></div>)}</div></article>
    </section>

    <section className="panorama-detail-grid">
      <article className="category-card"><header><div><span className="kicker">POR CATEGORÍA</span><h2>Tu base mensual</h2></div><span>promedio</span></header>{forecast.categories.map((item, index) => <div className="category-row" key={item.category}><span className={`category-swatch c${index}`} /><b>{item.category}</b><i><em style={{ width: `${Math.round(item.share * 100)}%` }} /></i><strong>{money.format(item.expected_monthly)}</strong></div>)}</article>
      <article className="recurring-card"><header><div><span className="kicker">RECURRENTES DETECTADOS</span><h2>Pagos que vuelven</h2></div><span>◌ {forecast.recurring.length}</span></header>{forecast.recurring.length ? forecast.recurring.map(item => <div className="recurring-row" key={`${item.merchant}-${item.category}`}><span>↻</span><div><b>{item.merchant}</b><small>{item.category} · {item.months_observed}/{forecast.period.months_observed} meses</small></div><strong>{money.format(item.expected_monthly)}</strong></div>) : <p className="empty-state">Con más meses de historia vamos a poder identificar tus gastos recurrentes.</p>}<footer>Los recurrentes se infieren por su repetición. Podés revisarlos antes de tomarlos como compromiso fijo.</footer></article>
    </section>
  </>
}

function Groups({ groups, refreshSignal, onRefresh, onExpense, onGift }: { groups: GroupSummary[]; refreshSignal: number; onRefresh: () => void; onExpense: (groupId: string) => void; onGift: () => void }) {
  const [selectedId, setSelectedId] = useState('')
  const [members, setMembers] = useState<GroupMember[]>([])
  const [groupExpenses, setGroupExpenses] = useState<GroupExpenseRecord[]>([])
  const [settlement, setSettlement] = useState<{ balances?: Record<string, string>; settlements?: GroupSettlement[] }>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [creating, setCreating] = useState(false)
  const [groupName, setGroupName] = useState('')
  const [createError, setCreateError] = useState('')
  const [inviteUrl, setInviteUrl] = useState('')
  const [inviteError, setInviteError] = useState('')
  const selected = groups.find(group => group.group_id === selectedId) || groups[0]

  useEffect(() => { if (selected && selected.group_id !== selectedId) setSelectedId(selected.group_id) }, [groups, selected, selectedId])
  useEffect(() => {
    if (!selected) { setMembers([]); setGroupExpenses([]); setSettlement({}); return }
    let active = true
    setLoading(true); setError('')
    Promise.all([
      api(`/v1/groups/${selected.group_id}/members`),
      api(`/v1/groups/${selected.group_id}/expenses`),
      api(`/v1/groups/${selected.group_id}/settlements`),
    ]).then(([memberRows, expenseRows, settlementResult]) => {
      if (!active) return
      setMembers(Array.isArray(memberRows) ? memberRows as GroupMember[] : [])
      setGroupExpenses(Array.isArray(expenseRows) ? expenseRows as GroupExpenseRecord[] : [])
      setSettlement(settlementResult && typeof settlementResult === 'object' ? settlementResult as typeof settlement : {})
    }).catch(e => { if (active) setError(e instanceof Error ? e.message : 'No pudimos cargar el grupo.') })
      .finally(() => { if (active) setLoading(false) })
    return () => { active = false }
  }, [selected?.group_id, refreshSignal])

  const createGroup = async (event: FormEvent) => {
    event.preventDefault(); setCreateError('')
    try {
      const result = await api('/v1/groups', 'POST', { name: groupName.trim() }) as GroupSummary | null
      if (!result?.group_id) throw new Error('La API no devolvió el grupo creado. Revisá tu sesión e intentá de nuevo.')
      setGroupName(''); setCreating(false); onRefresh(); setSelectedId(result.group_id)
    } catch (e) { setCreateError(e instanceof Error ? e.message : 'No se pudo crear el grupo.') }
  }
  const createInvite = async () => {
    if (!selected) return
    setInviteError(''); setInviteUrl('')
    try {
      const result = await api(`/v1/groups/${selected.group_id}/invites`, 'POST') as { token?: string } | null
      if (!result?.token) throw new Error('La API no devolvió una invitación válida.')
      const url = `${window.location.origin}${window.location.pathname}?invite=${encodeURIComponent(result.token)}`
      setInviteUrl(url)
      try { await navigator.clipboard.writeText(url) } catch { /* The link remains selectable below. */ }
    } catch (e) { setInviteError(e instanceof Error ? e.message : 'No se pudo crear la invitación.') }
  }
  const nameFor = (id: string) => members.find(member => member.user_id === id)?.display_name || 'Integrante'
  const balances = Object.entries(settlement.balances || {}).filter(([, amount]) => Number(amount) !== 0)
  const visibleExpenses = groupExpenses.filter(expense => !expense.deleted_at)

  return <>
    <div className="page-title"><div><span className="kicker">TU GENTE</span><h1>Mis grupos</h1></div><button className="outline" onClick={() => { setCreating(value => !value); setCreateError('') }}>＋ Crear grupo</button></div>
    {creating && <form className="group-create" onSubmit={createGroup}><label>Nombre del grupo<input autoFocus maxLength={80} value={groupName} onChange={event => setGroupName(event.target.value)} placeholder="Ej. Viaje a Bariloche" required minLength={2} /></label><button className="primary" type="submit">Crear grupo</button><button type="button" className="outline" onClick={() => setCreating(false)}>Cancelar</button>{createError && <p className="form-error" role="alert">{createError}</p>}</form>}
    {!apiBase && <section className="insight"><span>ⓘ</span><div><h3>Conectá la API para gestionar grupos</h3><p>Esta vista no inventa grupos ni saldos: configurá VITE_API_URL e iniciá sesión para cargar tus datos.</p></div></section>}
    {apiBase && groups.length === 0 && !loading && <section className="insight"><span>♧</span><div><h3>Todavía no tenés grupos</h3><p>Creá uno para empezar a cargar gastos compartidos e invitar a tus amigos.</p></div></section>}
    {groups.length > 0 && <>
      <section className="group-grid">{groups.map(group => <button className={`group-card ${group.group_id === selected?.group_id ? 'selected' : ''}`} onClick={() => setSelectedId(group.group_id)} key={group.group_id}><span className="orb cyan">♧</span><div><h2>{group.name}</h2><p>{group.legacy_source_key ? 'Grupo migrado · vinculá a sus miembros' : `Grupo activo · ${group.currency}`}</p></div><strong>{group.group_id === selected?.group_id ? 'Seleccionado' : 'Ver grupo'}</strong><i>→</i></button>)}</section>
      {selected && <section className="group-detail">
        <header className="group-detail-head"><div><span className="kicker">DETALLE DEL GRUPO</span><h2>{selected.name}</h2><p>{members.length} integrantes · moneda {selected.currency}</p></div><div><button className="outline" onClick={createInvite}>↗ Invitar</button><button className="outline" onClick={() => onExpense(selected.group_id)}>＋ Agregar gasto</button><button className="outline" onClick={onGift}>Regalos</button></div></header>
        {inviteUrl && <div className="invite-link" role="status"><b>Link de invitación listo</b><input value={inviteUrl} readOnly aria-label="Link de invitación" /><small>Se copió al portapapeles si tu navegador lo permitió. Es de un solo uso y vence en 7 días.</small></div>}
        {inviteError && <p className="form-error" role="alert">{inviteError}</p>}
        {loading && <p className="empty-state" role="status">Cargando movimientos y balances…</p>}
        {error && <div className="form-error" role="alert">{error} <button className="outline" onClick={onRefresh}>Reintentar</button></div>}
        {!loading && !error && <>
          <div className="group-detail-grid"><article className="group-subpanel"><h3>Integrantes</h3>{members.length ? members.map(member => <div className="member-row" key={member.user_id}><Avatar name={member.display_name} /><div><b>{member.display_name}</b><small>{member.role === 'owner' ? 'Organizador' : 'Integrante'}{member.claim_state === 'pending' ? ' · pendiente de vincular Telegram' : ''}</small></div><span className="pill">{member.active ? 'Activo' : 'Inactivo'}</span></div>) : <p className="empty-state">No hay integrantes para mostrar.</p>}</article>
            <article className="group-subpanel"><h3>Balances</h3>{balances.length ? balances.map(([id, amount]) => <div className="balance-row" key={id}><span>{nameFor(id)}</span><strong className={Number(amount) > 0 ? 'credit' : 'debit'}>{Number(amount) > 0 ? 'recibe ' : 'debe '}{money.format(Math.abs(Number(amount)))}</strong></div>) : <p className="empty-state">Todos están al día por ahora.</p>}{settlement.settlements?.length ? <div className="settlement-list"><h4>Liquidaciones sugeridas</h4>{settlement.settlements.map((item, index) => <div className="balance-row" key={`${item.from_user_id}-${item.to_user_id}-${index}`}><span>{nameFor(item.from_user_id)} → {nameFor(item.to_user_id)}</span><strong>{money.format(Number(item.amount))}</strong></div>)}</div> : null}</article></div>
          <article className="group-subpanel group-ledger"><h3>Gastos recientes <span>{visibleExpenses.length}</span></h3>{visibleExpenses.length ? visibleExpenses.slice(0, 20).map(expense => <div className="expense" key={expense.expense_id}><span className="category c0">⌇</span><div><h3>{expense.description}</h3><p>{expense.category || 'Otro'} · {new Date(expense.created_at).toLocaleDateString('es-AR')}</p></div><strong>{money.format(Number(expense.amount))}</strong></div>) : <p className="empty-state">Todavía no hay gastos en este grupo.</p>}</article>
        </>}
      </section>}
    </>}
  </>
}

function Activity({ expenses }: { expenses: Expense[] }) { return <><div className="page-title"><div><span className="kicker">TODO EN ORDEN</span><h1>Actividad</h1></div><button className="outline">⇩ Exportar</button></div><div className="filters"><input placeholder="Buscar gastos…" /><button className="selected">Todos</button><button>Comida</button><button>Transporte</button><button>Compras</button></div><section className="total-bar"><span>Total gastado</span><strong>{money.format(expenses.reduce((a, e) => a + e.amount, 0))}</strong></section><div className="timeline">{expenses.map((expense, index) => <article className="expense" key={`${expense.title}${index}`}><span className={`category c${index}`}>{expense.category === 'Comida' ? '⌇' : '⌁'}</span><div><h3>{expense.title}</h3><p>{expense.category} · {expense.date}</p></div><div><strong>{money.format(expense.amount)}</strong><p>Pagó {expense.payer}</p></div></article>)}</div></> }

function Wallet({ onPremium }: { onPremium: () => void }) { return <><div className="page-title"><div><span className="kicker">TU RESUMEN FINANCIERO</span><h1>Mi billetera</h1></div><button className="outline">⚙ Ajustes</button></div><section className="wallet-banner"><div><span className="crown">♔</span><span className="kicker">PLAN GRATUITO</span><h2>Tu dinero ya habla.<br />Escuchalo completo.</h2><p>Wallet Premium suma patrones, historial ilimitado y exportaciones.</p></div><div className="wallet-stats"><span>5</span><p>escaneos<br />restantes</p></div><button onClick={onPremium}>Ver plan Premium <b>→</b></button></section><section className="blurred-ledger"><div><span>INGRESOS</span><strong>$ 1.820.000</strong></div><div><span>GASTOS</span><strong>$ 928.400</strong></div><div><span>AHORRO</span><strong>$ 891.600</strong></div></section></> }

function Settings({ notify }: { notify: (message: string) => void }) { return <><div className="page-title"><div><span className="kicker">TU ESPACIO</span><h1>Ajustes</h1></div><Avatar name="Betu" /></div><section className="settings-list"><Action icon="◉" title="Perfil" caption="Nombre, alias y foto" onClick={() => notify('Perfil listo para editar.')} /><Action icon="♧" title="Notificaciones" caption="Recordatorios de grupos y pagos" onClick={() => notify('Solicitamos permiso de notificaciones.')} /><Action icon="◒" title="Apariencia" caption="Noche aurora" onClick={() => notify('La apariencia se adapta al sistema.')} /><Action icon="⊙" title="Mercado Pago" caption="Conectá tu cuenta para organizar pools" onClick={() => { api('/v1/mercadopago/oauth/start', 'POST').then((result: any) => result?.authorization_url && (window.location.href = result.authorization_url)).catch(() => notify('Configuraremos tu cuenta de Mercado Pago.')) }} /><Action icon="?" title="Ayuda" caption="Preguntas frecuentes y soporte" /></section><p className="version">DIVI / 0.1.0<br />hecho para compartir</p></> }

function Gifts({ gifts, reserve, onPool }: { gifts: Gift[]; reserve: (gift: Gift) => void; onPool: () => void }) { return <><button className="back" onClick={() => history.back()}>← Volver a grupos</button><div className="gift-hero"><span className="kicker">CUMPLEAÑOS · 21 SEP</span><h1>El regalo de<br /><em>Fran.</em></h1><p>Su wishlist es una sola, aunque esté en varios grupos. Sin regalos repetidos.</p></div><section className="pool-progress"><div><span>POOL ACTIVO</span><strong>$ 32.000 <small>de $ 48.000</small></strong></div><div className="progress"><i /></div><button onClick={onPool}>Aportar al pool →</button></section><div className="wishlist-head"><div><h2>Wishlist de Fran</h2><p>Solo Fran puede editar esta lista.</p></div><span className="privacy">◌ sin spoilers</span></div><div className="gift-list">{gifts.map(gift => <article className={`gift ${!gift.available ? 'reserved' : ''}`} key={gift.id}><div className="gift-visual">{gift.title.includes('Auriculares') ? '◖◗' : gift.title.includes('Cafetera') ? '♨' : '◒'}</div><div><h3>{gift.title}</h3><p>{money.format(gift.price)}</p>{!gift.available && <small>Este regalo ya fue elegido</small>}</div><button disabled={!gift.available} onClick={() => reserve(gift)}>{gift.available ? 'Elegir regalo' : 'No disponible'}</button></article>)}</div></> }

function ExpenseModal({ groupId, close, submit, onSaved }: { groupId: string; close: () => void; submit: (expense: Expense) => void; onSaved: () => void }) {
  const [amount, setAmount] = useState(''); const [description, setDescription] = useState(''); const [members, setMembers] = useState<GroupMember[]>([]); const [participants, setParticipants] = useState<string[]>([]); const [error, setError] = useState(''); const [saving, setSaving] = useState(false)
  useEffect(() => { let active = true; api(`/v1/groups/${groupId}/members`).then(result => { if (!active) return; const rows = Array.isArray(result) ? result as GroupMember[] : []; setMembers(rows); setParticipants(rows.filter(member => member.active || member.claim_state === 'pending').map(member => member.user_id)) }).catch(e => { if (active) setError(e instanceof Error ? e.message : 'No pudimos cargar los integrantes.') }); return () => { active = false } }, [groupId])
  const send = async (event: FormEvent) => {
    event.preventDefault(); setError('')
    const normalizedAmount = amount.includes(',') ? amount.replace(/\./g, '').replace(',', '.') : /^\d{1,3}(\.\d{3})+$/.test(amount) ? amount.replace(/\./g, '') : amount
    const amountValue = Number(normalizedAmount)
    if (!description.trim() || !Number.isFinite(amountValue) || amountValue <= 0 || !participants.length) { setError('Completá descripción, monto válido y al menos un participante.'); return }
    setSaving(true)
    try {
      const result = await api(`/v1/groups/${groupId}/expenses`, 'POST', { amount: normalizedAmount, description: description.trim(), participants, split_method: 'equal', category: 'Otro' }) as GroupExpenseRecord | null
      if (!result?.expense_id) throw new Error('No se confirmó el gasto. Verificá tu sesión e intentá de nuevo.')
      onSaved(); submit({ title: result.description, category: result.category || 'Otro', amount: Number(result.amount), payer: 'Vos', date: 'ahora' })
    } catch (e) { setError(e instanceof Error ? e.message : 'No se pudo guardar el gasto.') }
    finally { setSaving(false) }
  }
  return <Modal title="Nuevo gasto" subtitle="El monto se divide en partes iguales entre las personas seleccionadas." close={close}><form className="expense-form" onSubmit={send}><label>Monto<input autoFocus value={amount} onChange={e => setAmount(e.target.value.replace(/[^0-9.,]/g, ''))} placeholder="$ 0" inputMode="decimal" required /></label><label>Descripción<input value={description} onChange={e => setDescription(e.target.value)} placeholder="¿Qué pagaste?" required /></label><fieldset><legend>Dividir entre</legend>{members.length ? members.filter(member => member.active || member.claim_state === 'pending').map(member => <label className="member-check" key={member.user_id}><input type="checkbox" checked={participants.includes(member.user_id)} onChange={event => setParticipants(current => event.target.checked ? [...current, member.user_id] : current.filter(id => id !== member.user_id))} /><Avatar name={member.display_name} /> {member.display_name}</label>) : <p className="empty-state">Cargando integrantes…</p>}</fieldset>{error && <p className="form-error" role="alert">{error}</p>}<button className="primary" type="submit" disabled={saving || members.length === 0}>{saving ? 'Guardando…' : 'Confirmar gasto'} <b>→</b></button></form></Modal>
}

function Assistant({ chat, setChat, close }: { chat: string[]; setChat: (value: string[]) => void; close: () => void }) { const [prompt, setPrompt] = useState(''); const ask = (event: FormEvent) => { event.preventDefault(); if (!prompt.trim()) return; setChat([...chat, `Vos: ${prompt}`, 'Divia: Para este grupo, lo más simple es cargarlo como gasto igual y cerrar la liquidación después.']); setPrompt('') }; return <Modal title="Divia" subtitle="Tu asistente financiera" close={close} wide><div className="chat">{chat.map((line, index) => <p className={line.startsWith('Vos:') ? 'user' : ''} key={index}>{line}</p>)}</div><div className="suggestions"><button onClick={() => setChat([...chat, 'Divia: Esta semana gastaste más en salidas, principalmente el viernes.'])}>¿En qué gasté más?</button><button onClick={() => setChat([...chat, 'Divia: Fran tiene 3 regalos posibles; el pool ya completó el 67%.'])}>¿Cómo va el regalo?</button></div><form className="ask" onSubmit={ask}><input value={prompt} onChange={e => setPrompt(e.target.value)} placeholder="Escribile a Divia…" /><button>↗</button></form></Modal> }

function PremiumModal({ close, notify }: { close: () => void; notify: (message: string) => void }) { const [card, setCard] = useState(''); const start = async (event: FormEvent) => { event.preventDefault(); try { const result: any = await api('/v1/premium/checkout', 'POST', { card_token_id: card }); if (result) notify('Suscripción autorizada. Tu prueba de 60 días ya está activa.') } catch (e) { notify(e instanceof Error ? e.message : 'Usá Mercado Pago para completar la suscripción.') } }; return <Modal title="Subirse a Premium" subtitle="$3.000 ARS / mes · 60 días sin cargo" close={close}><section className="premium-in-modal"><span className="crown">♔</span><h2>Más señal. Menos fricción.</h2><ul><li>30 escaneos de tickets por mes</li><li>Wallet personal completa</li><li>Exportar a CSV e historial ilimitado</li><li>Soporte prioritario</li></ul></section><form className="pay-form" onSubmit={start}><label>Token de tarjeta de Mercado Pago<input value={card} onChange={e => setCard(e.target.value)} placeholder="Generado por Mercado Pago Bricks" /></label><small>No guardamos datos de tarjeta. El token se envía directo a Mercado Pago.</small><button className="primary">Activar 60 días gratis <b>→</b></button></form></Modal> }

function PoolModal({ close, notify }: { close: () => void; notify: (message: string) => void }) { const [amount, setAmount] = useState('5000'); const contribute = () => { notify(`Abrimos Mercado Pago para aportar ${money.format(Number(amount))} al organizador.`); close() }; return <Modal title="Aportar al regalo" subtitle="El dinero se acredita directo al organizador" close={close}><div className="pool-modal"><span className="pool-icon">♔</span><h2>Auriculares para Fran</h2><p>Juntamos $32.000 de $48.000. Tu aporte no queda guardado en Divi.</p><label>Tu aporte<input value={amount} onChange={e => setAmount(e.target.value.replace(/[^0-9]/g, ''))} inputMode="numeric" /></label><button className="primary" onClick={contribute}>Pagar con Mercado Pago <b>↗</b></button><small>El pago lo recibe quien organiza este regalo.</small></div></Modal> }

function Modal({ title, subtitle, close, children, wide = false }: { title: string; subtitle?: string; close: () => void; children: ReactNode; wide?: boolean }) { return <div className="modal-backdrop" role="presentation"><section className={`modal ${wide ? 'wide' : ''}`} role="dialog" aria-modal="true" aria-label={title}><button className="close" onClick={close} aria-label="Cerrar">×</button><header><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</header>{children}</section></div> }

createRoot(document.getElementById('root')!).render(<App />)
