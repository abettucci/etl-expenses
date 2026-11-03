import { useEffect, useMemo, useState, type FormEvent, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'
import './premium.css'
import { TicketSplitter } from './ticket_splitter/TicketSplitter'
import { EtlOverview } from './etl-overview'
import { ProductNavigation } from './product-navigation'
import { FinancialBalance } from './financial-balance'
import { PersonalProfile } from './personal-profile'
import { FinanceChat } from './finance-chat'
import { UnifiedLanding } from './unified-landing'
import { UnifiedDashboard } from './unified-dashboard'
import { legacyWorkspaceView, loginDestination, workspacePaths } from './workspace-routing'
import { landingArea, personalFinanceAnchor } from './landing-navigation'
import './unified-theme.css'
import { createAuthSession } from './auth-session'

if ('serviceWorker' in navigator) {
  const wasControlled = Boolean(navigator.serviceWorker.controller)
  let refreshing = false
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (wasControlled && !refreshing && window.location.pathname !== '/auth/callback') {
      refreshing = true
      window.location.reload()
    }
  })
  window.addEventListener('load', () => { void navigator.serviceWorker.register('/sw.js').catch(() => { /* The app still works without offline caching. */ }) })
}

type View = 'welcome' | 'splitter' | 'home' | 'groups' | 'activity' | 'wallet' | 'settings' | 'gifts' | 'panorama' | 'etl' | 'balance' | 'profile' | 'connections'
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
// Public production settings are fallbacks so a missing Vercel build variable
// does not make the deployed application unusable. Vercel/local values override.
const apiBase = import.meta.env.VITE_API_URL || 'https://308jvhhj9f.execute-api.us-east-2.amazonaws.com'
const cognitoDomain = (import.meta.env.VITE_COGNITO_DOMAIN || 'https://divi-expenses-039434644707.auth.us-east-2.amazoncognito.com').replace(/\/$/, '')
const cognitoClientId = import.meta.env.VITE_COGNITO_CLIENT_ID || '132dokn7kaddbfmoba6e9882qq'
const authRedirectUri = import.meta.env.VITE_COGNITO_REDIRECT_URI || 'https://etl-expenses.vercel.app/auth/callback'
const mercadoPagoPublicKey = import.meta.env.VITE_MERCADOPAGO_PUBLIC_KEY || ''
const session = createAuthSession({ config: { domain: cognitoDomain, clientId: cognitoClientId, redirectUri: authRedirectUri, apiBase }, storage: sessionStorage, legacyStorage: localStorage, onExpired: () => window.dispatchEvent(new Event('split-session-expired')) })

type MercadoPagoCardForm = {
  getCardFormData: () => { token?: string }
  unmount?: () => void
}
type MercadoPagoSdk = {
  cardForm: (options: Record<string, unknown>) => MercadoPagoCardForm
}

function loadMercadoPagoSdk(): Promise<void> {
  if ((window as Window & { MercadoPago?: unknown }).MercadoPago) return Promise.resolve()
  return new Promise((resolve, reject) => {
    const current = document.querySelector<HTMLScriptElement>('script[data-mercadopago-sdk]')
    if (current) {
      current.addEventListener('load', () => resolve(), { once: true })
      current.addEventListener('error', () => reject(new Error('No pudimos cargar el formulario seguro de Mercado Pago.')), { once: true })
      return
    }
    const script = document.createElement('script')
    script.src = 'https://sdk.mercadopago.com/js/v2'
    script.async = true
    script.dataset.mercadopagoSdk = 'true'
    script.onload = () => { script.dataset.loaded = 'true'; resolve() }
    script.onerror = () => reject(new Error('No pudimos cargar el formulario seguro de Mercado Pago.'))
    document.head.appendChild(script)
  })
}

async function startCognitoLogin(returnView: View = 'home') {
  window.location.assign(await session.begin(returnView, window.location.origin))
}
async function api(path: string, method = 'GET', body?: unknown) {
  return session.request(path, method, body)
}

function Mark() { return <span className="mark"><i /><i /><i /><i /></span> }
function Avatar({ name, tone = 'violet' }: { name: string; tone?: string }) { return <span className={`avatar ${tone}`}>{name.slice(0, 1).toUpperCase()}</span> }
function Action({ icon, title, caption, onClick }: { icon: string; title: string; caption: string; onClick?: () => void }) { return <button className="list-action" onClick={onClick}><span className="action-icon">{icon}</span><span><strong>{title}</strong><small>{caption}</small></span><b>›</b></button> }

const viewPaths: Record<View, string> = {
  welcome: '/', splitter: '/dividir', ...workspacePaths,
  activity: '/actividad', wallet: '/billetera', settings: '/ajustes',
  gifts: '/regalos', etl: '/como-funciona',
}
function viewFromLocation(): View {
  const legacy = legacyWorkspaceView(window.location.pathname)
  if (legacy) return legacy
  const landing = landingArea(window.location.pathname, window.location.hash)
  if (landing) return landing
  const pathname = window.location.pathname.replace(/\/$/, '') || '/'
  return (Object.entries(viewPaths).find(([, value]) => value === pathname)?.[0] as View | undefined)
    || (pathname === '/settings' ? 'settings' : 'welcome')
}

function App() {
  const [view, setView] = useState<View>(() => viewFromLocation() === 'welcome' && !window.location.hash && session.hasSession() && !window.location.search ? 'home' : viewFromLocation())
  useEffect(() => {
    if (legacyWorkspaceView(window.location.pathname) || (window.location.pathname === '/' && view === 'home')) window.history.replaceState({}, '', viewPaths[view] + window.location.search)
  }, [])
  const navigate = (nextView: View) => {
    const pool = new URLSearchParams(window.location.search).get('pool')
    window.history.pushState({}, '', viewPaths[nextView] + (pool ? `?pool=${encodeURIComponent(pool)}` : ''))
    setView(nextView)
    window.scrollTo(0, 0)
    if (nextView === 'etl') window.requestAnimationFrame(() => document.getElementById(personalFinanceAnchor)?.scrollIntoView())
  }
  useEffect(() => {
    // /etl and the personal-finance anchor both open the same document.
    if (view === 'etl') {
      const frame = window.requestAnimationFrame(() => document.getElementById(window.location.hash === '#etl-process' ? 'etl-process' : personalFinanceAnchor)?.scrollIntoView())
      return () => window.cancelAnimationFrame(frame)
    }
  }, [view])
  useEffect(() => {
    const onPopState = () => { setView(viewFromLocation()); window.scrollTo(0, 0) }
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [])
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
  const [groupsStatus, setGroupsStatus] = useState<'loading' | 'ready' | 'error'>('loading')
  const [activeGroupId, setActiveGroupId] = useState('')
  const [groupRefresh, setGroupRefresh] = useState(0)
  const [authenticated, setAuthenticated] = useState(() => session.hasSession())
  const [authMessage, setAuthMessage] = useState('')
  const [authError, setAuthError] = useState(false)
  const [authPending, setAuthPending] = useState(false)
  const balance = useMemo(() => expenses.reduce((sum, expense) => sum + expense.amount, 0), [expenses])
  const notify = (message: string) => { setNotice(message); window.setTimeout(() => setNotice(''), 3200) }
  const authenticate = async (returnView: View = view) => {
    setAuthError(false); setAuthMessage(''); setAuthPending(true)
    try { await startCognitoLogin(returnView) } catch (error) { setAuthError(true); setAuthPending(false); setAuthMessage(error instanceof Error ? error.message : 'No pudimos iniciar sesión.') }
  }
  const signOut = () => {
    session.clear()
    sessionStorage.removeItem('split_open_premium_after_login')
    setAuthenticated(false)
    setOverlay(null)
    setGroups([])
    setActiveGroupId('')
    navigate('welcome')
    setAuthMessage('Sesión cerrada.')
    window.location.assign(session.logoutUrl())
  }
  const openPremium = () => {
    if (authenticated) { setOverlay('premium'); return }
    sessionStorage.setItem('split_open_premium_after_login', 'true')
    void authenticate('wallet')
  }
  const openExpense = (groupId = activeGroupId) => {
    if (!authenticated) { void authenticate('groups'); return }
    if (!groupId) { navigate('groups'); notify('Creá o elegí un grupo antes de cargar un gasto.'); return }
    setActiveGroupId(groupId); setOverlay('expense')
  }
  const reserve = async (gift: Gift) => {
    if (!gift.available) return
    try { await api(`/v1/groups/demo/events/fran-2026/wishlist/${gift.id}/reserve`, 'POST') } catch (e) { notify(e instanceof Error ? e.message : 'No disponible') ; return }
    setGifts(items => items.map(item => item.id === gift.id ? { ...item, available: false } : item)); notify('Regalo reservado para este grupo. Los demás grupos ya no lo verán disponible.')
  }
  useEffect(() => {
    const callbackUrl = new URL(window.location.href)
    if (callbackUrl.searchParams.get('signin') === '1' && callbackUrl.origin === new URL(authRedirectUri).origin) {
      window.history.replaceState({}, '', '/inicio')
      void authenticate(loginDestination(callbackUrl.searchParams.get('next')) as View)
      return
    }
    if (callbackUrl.pathname !== '/auth/callback') return
    window.history.replaceState({}, '', '/auth/callback')
    setAuthPending(true)
    void session.complete(callbackUrl).then(destination => {
        const nextView = loginDestination(destination) as View
        const resumePremium = sessionStorage.getItem('split_open_premium_after_login') === 'true'
        sessionStorage.removeItem('split_open_premium_after_login')
        window.history.replaceState({}, '', viewPaths[nextView])
        setAuthenticated(true)
        setView(nextView)
        setAuthPending(false); setAuthError(false)
        if (resumePremium) setOverlay('premium')
        setAuthMessage('Sesión iniciada.')
      })
      .catch(error => { setAuthPending(false); setAuthError(true); setAuthMessage(error instanceof Error ? error.message : 'No pudimos iniciar sesión.') })
  }, [])
  useEffect(() => {
    const expired = () => { setAuthenticated(false); setOverlay(null); setAuthError(true); setAuthMessage('Tu sesión venció. Volvé a ingresar para continuar.'); setGroups([]); setActiveGroupId('') }
    window.addEventListener('split-session-expired', expired)
    return () => window.removeEventListener('split-session-expired', expired)
  }, [])
  useEffect(() => {
    if (!authenticated) { setGroups([]); setActiveGroupId(''); return }
    let active = true
    setGroupsStatus('loading')
    void api('/v1/groups').then(result => {
      if (!active) return
      if (!Array.isArray(result)) throw new Error('No pudimos validar tus grupos.')
      setGroups(result as GroupSummary[])
      setActiveGroupId(result[0]?.group_id || '')
      setGroupsStatus('ready')
    }).catch(error => { if (active) { setGroupsStatus('error'); setAuthMessage(error instanceof Error ? error.message : 'No pudimos cargar tus grupos.') } })
    return () => { active = false }
  }, [authenticated])
  useEffect(() => {
    const token = new URLSearchParams(window.location.search).get('invite') || sessionStorage.getItem('split_pending_invite')
    if (!token) return
    if (!authenticated) {
      sessionStorage.setItem('split_pending_invite', token)
      void authenticate('groups')
      return
    }
    void api(`/v1/invitations/${encodeURIComponent(token)}/accept`, 'POST').then(() => {
      sessionStorage.removeItem('split_pending_invite')
      window.history.replaceState({}, '', window.location.pathname)
      notify('Invitación aceptada. El grupo ya aparece en tus grupos.')
      return api('/v1/groups')
    }).then(result => { if (Array.isArray(result)) { setGroups(result as GroupSummary[]); setActiveGroupId(result[0]?.group_id || '') } })
      .catch(error => notify(error instanceof Error ? error.message : 'No pudimos aceptar la invitación.'))
  }, [authenticated])
  return <main className={`product-root ${authenticated ? 'account-layout' : ''}`}>
    <ProductNavigation area={view === 'home' ? 'home' : view === 'balance' || view === 'panorama' ? 'personal' : view === 'connections' ? 'connections' : view === 'groups' || view === 'splitter' ? 'groups' : 'account'} authenticated={authenticated} onNavigate={navigate} onLogin={() => void authenticate(view === 'balance' ? 'balance' : 'home')} onLogout={signOut} onAssistant={() => setOverlay('assistant')} />
    {authPending && <section className="auth-feedback" role="status"><h2>Completando el acceso seguro…</h2><p>Esperá un momento. No cierres esta pestaña.</p></section>}
    {authError && <section className="auth-feedback" role="alert"><h2>No pudimos completar el acceso</h2><p>{authMessage}</p><div><button className="primary" onClick={() => void authenticate(view)}>Volver a ingresar</button><button className="outline" onClick={() => { setAuthError(false); setAuthMessage(''); navigate('welcome') }}>Volver al inicio</button></div></section>}
    {view === 'welcome' || view === 'etl' || view === 'splitter' ? <>
      {view === 'splitter' ? <section className="calculator-workspace" aria-label="División rápida"><TicketSplitter hideNavigation page="calculator" authenticated={authenticated} onLogin={() => void authenticate('home')} onNavigate={destination => navigate(destination === '/app' ? 'home' : 'welcome')} /></section> : <UnifiedLanding onStart={() => authenticated ? navigate('home') : void authenticate('home')} onDivide={() => navigate('splitter')} onLearn={() => navigate('etl')} />}
      {view !== 'splitter' && <section id={personalFinanceAnchor} className="unified-personal" aria-label="Finanzas personales con ETL Expenses"><EtlOverview embedded onNavigate={navigate} onHome={() => navigate('welcome')} /></section>}
    </> : <>
      <section className="shell">
        {(view === 'balance' || view === 'panorama') && <nav className="finance-subnav" aria-label="Finanzas personales"><span>MIS FINANZAS</span><button aria-current={view === 'balance' ? 'page' : undefined} onClick={() => navigate('balance')}>Balance mensual</button><button aria-current={view === 'panorama' ? 'page' : undefined} onClick={() => navigate('panorama')}>Proyección y recurrentes</button></nav>}
        {view === 'balance' && <FinancialBalance authenticated={authenticated} api={api} onLogin={() => void authenticate('balance')} onConnections={() => navigate('connections')} />}
        {view === 'home' && <UnifiedDashboard authenticated={authenticated} api={api} groups={groups} groupsStatus={groupsStatus} onLogin={() => void authenticate('home')} onFinance={() => navigate('balance')} onIncome={() => navigate('balance')} onGroups={id => { if (id) setActiveGroupId(id); navigate('groups') }} onExpense={() => openExpense()} onAssistant={() => setOverlay('assistant')} onConnections={() => navigate('connections')} />}
        {(view === 'profile' || view === 'connections' || view === 'settings') && <PersonalProfile section={view === 'connections' ? 'connections' : 'account'} authenticated={authenticated} api={api} onLogin={() => void authenticate(view)} onGroups={() => navigate('groups')} onAssistant={() => setOverlay('assistant')} />}
        {view === 'settings' && authenticated && <Settings notify={notify} onPremium={openPremium} />}
        {view === 'groups' && <Groups initialGroupId={activeGroupId} authenticated={authenticated} onLogin={() => void authenticate('groups')} groups={groups} groupsStatus={groupsStatus} refreshSignal={groupRefresh} onRefresh={() => { setGroupsStatus('loading'); void api('/v1/groups').then(result => { if (!Array.isArray(result)) throw new Error('Respuesta inválida'); setGroups(result as GroupSummary[]); setGroupsStatus('ready'); setGroupRefresh(value => value + 1) }).catch(() => { setGroupsStatus('error'); notify('No pudimos actualizar tus grupos.') }) }} onSelectGroup={setActiveGroupId} onExpense={openExpense} />}
        {view === 'panorama' && (authenticated ? <Panorama onConnections={() => navigate('connections')} /> : <section className="workspace-card"><h1>Proyección y recurrentes</h1><p>Ingresá para consultar tu historial.</p><button className="primary" onClick={() => void authenticate('panorama')}>Ingresar</button></section>)}
      </section>
    </>}
    {(notice || authMessage && !authError) && <div className="toast" role="status">{notice || authMessage}</div>}
    {overlay === 'expense' && <ExpenseModal groupId={activeGroupId} close={() => setOverlay(null)} submit={expense => { setExpenses([expense, ...expenses]); setOverlay(null); notify('Gasto agregado y dividido en el grupo.') }} onSaved={() => setGroupRefresh(value => value + 1)} />}
    {authenticated && <button className="assistant-fab" onClick={() => setOverlay('assistant')} aria-label="Abrir asistente financiero" aria-expanded={overlay === 'assistant'}>✧</button>}
    {overlay === 'assistant' && authenticated && <FinanceChat api={api} close={() => setOverlay(null)} />}
    {overlay === 'premium' && <PremiumModal close={() => setOverlay(null)} notify={notify} />}
    {overlay === 'pool' && <PoolModal poolId={new URLSearchParams(window.location.search).get('pool')} close={() => setOverlay(null)} />}
  </main>
}

function Home({ balance, onDivide, onExpense, onGroups, onPanorama, onGift }: { balance: number; onDivide: () => void; onExpense: () => void; onGroups: () => void; onPanorama: () => void; onGift: () => void }) { return <>
  <div className="eyebrow">La cuenta del grupo <span>〰</span></div><h1>Que la plata<br /><em>no corte el plan.</em></h1>
  <section className="hero-panel stagger"><div className="group-line"><div><span className="kicker">GRUPO ACTUAL</span><h2>Militantes del mood</h2></div><span className="members">♧ 4</span></div><div className="hero-actions"><button onClick={onExpense}>＋<span>Agregar gasto</span></button><button className="pink" onClick={onGift}>♔<span>Regalo de Fran</span></button></div><div className="balance"><span>MI BALANCE</span><strong>− {money.format(6175)}</strong><small>Te queda una liquidación pendiente</small></div><div className="settlement"><Avatar name="B" /><p><b>Betu</b> le debe a <b>matias.arbues7</b><strong>{money.format(6175)}</strong></p><button>Resolver →</button></div></section>
  <section className="experience-switcher" aria-label="Elegí qué espacio abrir"><button className="experience-tile" onClick={onDivide}><span>✂</span><div><b>Ticket Splitter clásico</b><small>División rápida sin registro, guardada en este navegador</small></div><i>→</i></button><button className="experience-tile" onClick={onGroups}><span>♧</span><div><b>Gastos compartidos</b><small>Grupos, integrantes, balances y liquidaciones</small></div><i>→</i></button><button className="experience-tile personal" onClick={onPanorama}><span>⌁</span><div><b>Mis finanzas</b><small>Actividad personal y panorama del ETL</small></div><i>→</i></button></section>
  <section className="feature-card"><span className="action-icon">⌘</span><div><h3>Tu plata, con memoria</h3><p>Gastos compartidos, tickets y hábitos en una sola vista.</p></div><button onClick={onPanorama}>Ver mi panorama <b>→</b></button></section>
  <div className="metrics"><div><span>GASTO DEL GRUPO</span><strong>{money.format(balance)}</strong><small>últimos 7 días</small></div><div><span>PRÓXIMO HITO</span><strong>Fran · 21 sep</strong><small>Regalo en marcha</small></div></div>
</> }

function monthShort(value: string) {
  return new Intl.DateTimeFormat('es-AR', { month: 'short' }).format(new Date(`${value}T12:00:00`)).replace('.', '')
}

function Panorama({ onConnections }: { onConnections: () => void }) {
  const [forecast, setForecast] = useState<Forecast | null>(null)
  const [status, setStatus] = useState<'unavailable' | 'loading' | 'live'>('loading')
  const [message, setMessage] = useState('Cargando tu historial…')
  const reload = async () => {
    setForecast(null); setStatus('loading')
    try {
      const result = await api('/v1/finance/forecast') as Forecast | null
      if (result?.mode === 'live') {
        setForecast(result)
        setStatus('live')
        setMessage(`Datos ETL agregados · ${result.period.months_observed} meses de historia.`)
      } else {
        setStatus('unavailable')
        setMessage('La proyección aún no está habilitada para esta cuenta. Revisá tus fuentes en Conexiones.')
      }
    } catch {
      setStatus('unavailable')
      setMessage('No pudimos conectar tu historial en este momento. Revisá Conexiones o intentá nuevamente.')
    }
  }
  useEffect(() => { void reload() }, [])
  if (!forecast) return <section className="workspace-card"><span className="kicker">MIS FINANZAS</span><h1>Proyección y recurrentes</h1><p role="status">{status === 'loading' ? 'Cargando tu historial…' : message}</p><button className="outline" onClick={() => void reload()} disabled={status === 'loading'}>Reintentar</button>{status === 'unavailable' && <button className="text-link" onClick={onConnections}>Revisar conexiones →</button>}</section>
  const maxHistory = Math.max(...forecast.monthly_history.map(item => item.amount), 1)
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
      <div className="signal-output"><span className="kicker">{new Date(`${forecast.period.forecast_month.slice(0, 7)}-01T12:00:00`).toLocaleDateString('es-AR', { month: 'long', year: 'numeric' })}</span><b>Flujo previsto</b><strong>{money.format(forecast.forecast.monthly_total)}</strong><small>{forecast.period.months_observed} meses de señales comparables</small><div><i style={{ width: `${Math.min((forecast.forecast.recurring_total / Math.max(forecast.forecast.monthly_total, 1)) * 100, 100)}%` }} /></div></div>
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

function Groups({ initialGroupId, authenticated, onLogin, groups, groupsStatus, refreshSignal, onRefresh, onSelectGroup, onExpense }: { initialGroupId: string; authenticated: boolean; onLogin: () => void; groups: GroupSummary[]; groupsStatus: 'loading' | 'ready' | 'error'; refreshSignal: number; onRefresh: () => void; onSelectGroup: (groupId: string) => void; onExpense: (groupId: string) => void }) {
  const [selectedId, setSelectedId] = useState(initialGroupId)
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

  const startCreatingGroup = () => {
    if (!authenticated) { onLogin(); return }
    setCreating(value => !value); setCreateError('')
  }
  const createGroup = async (event: FormEvent) => {
    event.preventDefault(); setCreateError('')
    try {
      const result = await api('/v1/groups', 'POST', { name: groupName.trim() }) as GroupSummary | null
      if (!result?.group_id) throw new Error('La API no devolvió el grupo creado. Revisá tu sesión e intentá de nuevo.')
      setGroupName(''); setCreating(false); setSelectedId(result.group_id); onSelectGroup(result.group_id); onRefresh()
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
    <div className="page-title"><div><span className="kicker">TU ESPACIO · GASTOS COMPARTIDOS</span><h1>Mis grupos</h1><p className="page-description">Dividí gastos, invitá integrantes y cerrá cuentas sin mezclarlo con tu balance personal.</p></div><button className="outline" onClick={startCreatingGroup}>{authenticated ? '＋ Crear grupo' : 'Ingresar'}</button></div>
    {creating && <form className="group-create" onSubmit={createGroup}><label>Nombre del grupo<input autoFocus maxLength={80} value={groupName} onChange={event => setGroupName(event.target.value)} placeholder="Ej. Viaje a Bariloche" required minLength={2} /></label><button className="primary" type="submit">Crear grupo</button><button type="button" className="outline" onClick={() => setCreating(false)}>Cancelar</button>{createError && <p className="form-error" role="alert">{createError}</p>}</form>}
    {!authenticated && <section className="insight"><span>♧</span><div><h3>Ingresá para abrir tus grupos</h3><p>Acá vas a encontrar gastos divididos, integrantes, balances y liquidaciones.</p><button onClick={onLogin}>Ingresar →</button></div></section>}
    {authenticated && groupsStatus === 'loading' && <p role="status">Cargando tus grupos…</p>}
    {authenticated && groupsStatus === 'error' && <p className="form-error" role="alert">No pudimos cargar tus grupos. <button className="outline" onClick={onRefresh}>Reintentar</button></p>}
    {authenticated && groupsStatus === 'ready' && groups.length === 0 && !loading && <section className="insight"><span>♧</span><div><h3>Todavía no tenés grupos</h3><p>Creá uno para empezar a cargar gastos compartidos e invitar a tus amigos.</p><button onClick={() => setCreating(true)}>＋ Crear mi primer grupo</button></div></section>}
    {authenticated && groups.length > 0 && <>
      <section className="group-grid">{groups.map(group => <button className={`group-card ${group.group_id === selected?.group_id ? 'selected' : ''}`} onClick={() => { setSelectedId(group.group_id); onSelectGroup(group.group_id) }} key={group.group_id}><span className="orb cyan">♧</span><div><h2>{group.name}</h2><p>{group.legacy_source_key ? 'Grupo migrado · vinculá a sus miembros' : `Grupo activo · ${group.currency}`}</p></div><strong>{group.group_id === selected?.group_id ? 'Seleccionado' : 'Ver grupo'}</strong><i>→</i></button>)}</section>
      {selected && <section className="group-detail">
        <header className="group-detail-head"><div><span className="kicker">DETALLE DEL GRUPO</span><h2>{selected.name}</h2><p>{members.length} integrantes · moneda {selected.currency}</p></div><div><button className="outline" onClick={createInvite}>↗ Invitar</button><button className="outline" onClick={() => onExpense(selected.group_id)}>＋ Agregar gasto</button></div></header>
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

function Settings({ notify, onPremium }: { notify: (message: string) => void; onPremium: () => void }) { return <section className="workspace-card"><h2>Plan y cobros</h2><p>Accedé al checkout existente o conectá tu cuenta de Mercado Pago para recibir aportes. Estas acciones requieren configuración del servicio; no se activan al iniciar sesión.</p><div className="dashboard-actions"><button className="outline" onClick={onPremium}>Ver suscripción</button><button className="outline" onClick={() => { api('/v1/mercadopago/oauth/start', 'POST').then((result: any) => { if (result?.authorization_url) window.location.href = result.authorization_url; else notify('El servicio no devolvió un enlace para conectar Mercado Pago.') }).catch(() => notify('No pudimos iniciar la conexión con Mercado Pago.')) }}>Conectar Mercado Pago ↗</button></div></section> }

function Gifts({ gifts, reserve, onBack, onPool }: { gifts: Gift[]; reserve: (gift: Gift) => void; onBack: () => void; onPool: () => void }) { return <><button className="back" onClick={onBack}>← Volver a grupos</button><div className="gift-hero"><span className="kicker">CUMPLEAÑOS · 21 SEP</span><h1>El regalo de<br /><em>Fran.</em></h1><p>Su wishlist es una sola, aunque esté en varios grupos. Sin regalos repetidos.</p></div><section className="pool-progress"><div><span>POOL ACTIVO</span><strong>$ 32.000 <small>de $ 48.000</small></strong></div><div className="progress"><i /></div><button onClick={onPool}>Aportar al pool →</button></section><div className="wishlist-head"><div><h2>Wishlist de Fran</h2><p>Solo Fran puede editar esta lista.</p></div><span className="privacy">◌ sin spoilers</span></div><div className="gift-list">{gifts.map(gift => <article className={`gift ${!gift.available ? 'reserved' : ''}`} key={gift.id}><div className="gift-visual">{gift.title.includes('Auriculares') ? '◖◗' : gift.title.includes('Cafetera') ? '♨' : '◒'}</div><div><h3>{gift.title}</h3><p>{money.format(gift.price)}</p>{!gift.available && <small>Este regalo ya fue elegido</small>}</div><button disabled={!gift.available} onClick={() => reserve(gift)}>{gift.available ? 'Elegir regalo' : 'No disponible'}</button></article>)}</div></> }

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


function PremiumModal({ close, notify }: { close: () => void; notify: (message: string) => void }) {
  const [formLoading, setFormLoading] = useState(true)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    let cardForm: MercadoPagoCardForm | undefined
    if (!mercadoPagoPublicKey) {
      setFormLoading(false)
      setError('Falta configurar VITE_MERCADOPAGO_PUBLIC_KEY en Vercel para habilitar el formulario seguro.')
      return () => { active = false }
    }
    void loadMercadoPagoSdk().then(() => {
      if (!active) return
      const sdk = (window as Window & { MercadoPago?: MercadoPagoSdk }).MercadoPago
      if (!sdk) throw new Error('No pudimos iniciar el formulario de Mercado Pago.')
      cardForm = sdk.cardForm({
        amount: '3000',
        iframe: true,
        form: {
          id: 'premium-card-checkout',
          cardNumber: { id: 'premium-card-number', placeholder: 'Número de tarjeta' },
          expirationDate: { id: 'premium-card-expiry', placeholder: 'MM/YY' },
          securityCode: { id: 'premium-card-security', placeholder: 'Código de seguridad' },
          cardholderName: { id: 'premium-cardholder-name', placeholder: 'Nombre como aparece en la tarjeta' },
          issuer: { id: 'premium-card-issuer', placeholder: 'Banco emisor' },
          installments: { id: 'premium-card-installments', placeholder: 'Cuotas' },
          identificationType: { id: 'premium-id-type', placeholder: 'Tipo de documento' },
          identificationNumber: { id: 'premium-id-number', placeholder: 'Número de documento' },
          cardholderEmail: { id: 'premium-cardholder-email', placeholder: 'Correo electrónico' },
        },
        callbacks: {
          onFormMounted: (mountError: unknown) => {
            if (!active) return
            setFormLoading(false)
            if (mountError) setError('No se pudo cargar el formulario de Mercado Pago. Cerralo y volvé a intentar.')
          },
          onSubmit: async (event: Event) => {
            event.preventDefault()
            if (!active) return
            setError('')
            setSubmitting(true)
            try {
              const cardToken = cardForm?.getCardFormData().token
              if (!cardToken) throw new Error('Completá los datos de la tarjeta para continuar.')
              const result = await api('/v1/premium/checkout', 'POST', { card_token_id: cardToken }) as { status?: string; subscription_id?: string } | null
              if (String(result?.status || '').toLowerCase() === 'authorized') {
                notify('Mercado Pago autorizó la suscripción. Premium ya está activo.')
                close()
              } else {
                setError(`Mercado Pago recibió la solicitud, pero la suscripción aún no está autorizada${result?.status ? ` (estado: ${result.status})` : ''}.`)
              }
            } catch (submitError) {
              if (active) setError(submitError instanceof Error ? submitError.message : 'No pudimos completar la suscripción.')
            } finally {
              if (active) setSubmitting(false)
            }
          },
        },
      })
    }).catch(loadError => {
      if (active) { setFormLoading(false); setError(loadError instanceof Error ? loadError.message : 'No pudimos cargar Mercado Pago.') }
    })
    return () => { active = false; cardForm?.unmount?.() }
  }, [])

  return <Modal title="Subirse a Premium" subtitle="$3.000 ARS / mes · 60 días sin cargo" close={close}>
    <section className="premium-in-modal"><span className="crown">♔</span><h2>Más señal. Menos fricción.</h2><ul><li>30 escaneos de tickets por mes</li><li>Wallet personal completa</li><li>Exportar a CSV e historial ilimitado</li><li>Soporte prioritario</li></ul></section>
    <form id="premium-card-checkout" className="mp-card-form" onSubmit={event => event.preventDefault()}>
      <label className="mp-field">Número de tarjeta<div id="premium-card-number" className="mp-card-frame" /></label>
      <div className="mp-field-row"><label className="mp-field">Vencimiento<div id="premium-card-expiry" className="mp-card-frame" /></label><label className="mp-field">Código de seguridad<div id="premium-card-security" className="mp-card-frame" /></label></div>
      <label className="mp-field">Titular<input id="premium-cardholder-name" autoComplete="cc-name" /></label>
      <div className="mp-field-row"><label className="mp-field">Banco emisor<select id="premium-card-issuer" defaultValue=""><option value="" disabled>Seleccionar</option></select></label><label className="mp-field">Cuotas<select id="premium-card-installments" defaultValue=""><option value="" disabled>Seleccionar</option></select></label></div>
      <div className="mp-field-row"><label className="mp-field">Documento<select id="premium-id-type" defaultValue=""><option value="" disabled>Tipo</option></select></label><label className="mp-field">Número de documento<input id="premium-id-number" autoComplete="off" /></label></div>
      <label className="mp-field">Correo electrónico<input id="premium-cardholder-email" type="email" autoComplete="email" /></label>
      <small>Los datos de tarjeta se capturan en campos seguros de Mercado Pago; Split solo recibe el token temporal.</small>
      {formLoading && <p className="mp-form-status" role="status">Cargando formulario seguro…</p>}
      {error && <p className="form-error" role="alert">{error}</p>}
      <button className="primary" type="submit" disabled={!mercadoPagoPublicKey || formLoading || submitting}>{submitting ? 'Enviando a Mercado Pago…' : 'Activar 60 días gratis'} <b>→</b></button>
    </form>
  </Modal>
}

function PoolModal({ poolId, close }: { poolId: string | null; close: () => void }) {
  const [amount, setAmount] = useState('5000')
  const [error, setError] = useState('')
  const [redirecting, setRedirecting] = useState(false)
  const isRealPool = Boolean(poolId)

  const contribute = async () => {
    setError('')
    const contributionAmount = Number(amount)
    if (!Number.isFinite(contributionAmount) || contributionAmount <= 0) {
      setError('Ingresá un monto mayor a $0.')
      return
    }
    if (!poolId) {
      setError('Este pool es una vista de ejemplo. Para pagar, abrí un pool creado dentro de un grupo real.')
      return
    }
    if (!apiBase) {
      setError('Falta configurar la API de Split. Intentá nuevamente cuando la app esté desplegada.')
      return
    }

    setRedirecting(true)
    try {
      const result = await api(`/v1/pools/${encodeURIComponent(poolId)}/checkout`, 'POST', { amount: contributionAmount }) as { checkout_url?: string } | null
      if (!result?.checkout_url) throw new Error('No pudimos obtener el link de pago de Mercado Pago.')
      window.location.assign(result.checkout_url)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'No pudimos iniciar el pago. Verificá que el organizador haya conectado Mercado Pago.')
      setRedirecting(false)
    }
  }

  return <Modal title="Aportar al regalo" subtitle="El dinero se acredita directo al organizador" close={close}><div className="pool-modal"><span className="pool-icon">♔</span><h2>Auriculares para Fran</h2><p>Juntamos $32.000 de $48.000. Tu aporte no queda guardado en Split.</p><label>Tu aporte<input value={amount} onChange={e => setAmount(e.target.value.replace(/[^0-9]/g, ''))} inputMode="numeric" disabled={redirecting} /></label>{!isRealPool && <small>Este ejemplo todavía no está asociado a un pool real.</small>}{error && <p className="form-error" role="alert">{error}</p>}<button className="primary" onClick={() => void contribute()} disabled={redirecting}>{redirecting ? 'Abriendo Mercado Pago…' : 'Pagar con Mercado Pago'} <b>↗</b></button><small>El pago lo recibe quien organiza este regalo.</small></div></Modal>
}

function Modal({ title, subtitle, close, children, wide = false }: { title: string; subtitle?: string; close: () => void; children: ReactNode; wide?: boolean }) { return <div className="modal-backdrop" role="presentation"><section className={`modal ${wide ? 'wide' : ''}`} role="dialog" aria-modal="true" aria-label={title}><button className="close" onClick={close} aria-label="Cerrar">×</button><header><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</header>{children}</section></div> }

createRoot(document.getElementById('root')!).render(<App />)
