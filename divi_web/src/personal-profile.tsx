import { useEffect, useState, type FormEvent } from 'react'
import './personal-workspace.css'
import { FinanceSetup, TelegramLink } from './connection-setup'
import { ProviderConnections } from './provider-connections'
import { AlertSettingsPanel } from './alert-settings'
import { MercadoPagoSetup } from './mercadopago-setup'
type Api = (path: string, method?: string, body?: unknown) => Promise<any>
type Overview = { profile: { display_name: string; email: string }; finance_linked: boolean; sources: { id: string; name: string; status: string; detail: string }[]; telegram: { finance_url: string | null; groups_url: string | null; personal_access: boolean; detail: string } }
const safeBot = (url: string | null) => url && /^https:\/\/t\.me\/[A-Za-z0-9_]{5,32}$/.test(url) ? url : null

export function PersonalProfile({ section = 'account', authenticated, api, onLogin, onGroups, onAssistant }: { section?: 'account' | 'connections'; authenticated: boolean; api: Api; onLogin: () => void; onGroups: () => void; onAssistant: () => void }) {
  const [tab, setTab] = useState<'connections' | 'telegram'>('connections')
  const [data, setData] = useState<Overview | null>(null)
  const [name, setName] = useState('')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [retry, setRetry] = useState(0)
  useEffect(() => {
    let active = true; setData(null); setError(''); setNotice('')
    if (!authenticated) return
    setLoading(true)
    void api('/v1/profile/overview').then(result => {
      if (!active) return
      if (!result?.profile || !Array.isArray(result.sources) || !result.telegram) throw new Error('No pudimos validar tu perfil.')
      setData(result); setName(result.profile.display_name)
    }).catch(failure => { if (active) setError(failure instanceof Error ? failure.message : 'No pudimos cargar tu perfil.') }).finally(() => { if (active) setLoading(false) })
    return () => { active = false }
  }, [authenticated, api, retry])
  async function save(event: FormEvent) {
    event.preventDefault(); if (!name.trim() || saving) return
    setSaving(true); setError(''); setNotice('')
    try { await api('/v1/me', 'PATCH', { display_name: name.trim() }); setNotice('Nombre actualizado.') }
    catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos guardar el nombre.') }
    finally { setSaving(false) }
  }
  if (!authenticated) return <section className="workspace-card"><h1>Tu espacio personal</h1><p>Ingresá para ver tu dashboard, tus conexiones y el asistente.</p><button className="primary" onClick={onLogin}>Ingresar</button></section>
  return <div className="personal-workspace"><header className="workspace-heading"><div><span className="kicker">TU ESPACIO · CONFIGURACIÓN</span><h1>{section === 'account' ? 'Mi perfil' : 'Conexiones'}</h1><p>{section === 'account' ? 'Tus datos de cuenta. El resumen de tus gastos está en Inicio y Mis finanzas.' : 'Revisá cómo llegan tus gastos y cómo acceder a los bots de Telegram.'}</p></div><button className="outline" onClick={onAssistant}>✦ Preguntar al asistente</button></header>
    {error && <div role="alert" className="balance-error">{error}<button onClick={() => setRetry(value => value + 1)}>Reintentar</button></div>}{loading && <p role="status">Cargando tu perfil…</p>}{notice && <p role="status">{notice}</p>}
    {section === 'account' && <AlertSettingsPanel api={api} />}
    {data && <>{section === 'account' ? <section className="workspace-account"><form onSubmit={event => void save(event)}><label>Nombre<input value={name} onChange={event => setName(event.target.value)} maxLength={80} required /></label><button className="outline" disabled={saving}>{saving ? 'Guardando…' : 'Guardar nombre'}</button></form><div><small>Correo de tu cuenta</small><b>{data.profile.email}</b></div><button className="outline" onClick={onGroups}>Mis grupos →</button></section> : <>
      <nav className="workspace-tabs" aria-label="Conexiones">{([['connections', 'Fuentes y carga'], ['telegram', 'Telegram y alertas']] as const).map(([key, label]) => <button key={key} aria-current={tab === key ? 'page' : undefined} onClick={() => setTab(key)}>{label}</button>)}</nav>
      {!data.finance_linked && <aside className="workspace-hint"><b>Tu historial financiero todavía no está vinculado.</b> Conectalo con el bot financiero que ya usás. <button onClick={() => { setTab('connections'); requestAnimationFrame(() => document.getElementById('vincular-historial')?.scrollIntoView({ behavior: 'smooth' })) }}>Vincular historial →</button></aside>}
      {tab === 'connections' && <FinanceSetup api={api} linked={data.finance_linked} botUrl={safeBot(data.telegram.finance_url)} onRefresh={() => setRetry(value => value + 1)} />}
      {tab === 'connections' && <><section className="workspace-card"><h2>Cómo llegan tus gastos</h2><ol><li>Configurá una fuente compatible en el ETL o usá el bot financiero autorizado.</li><li>Enviá un ticket, una nota de voz o un gasto escrito; revisá el resultado y confirmalo.</li><li>El dashboard muestra los movimientos procesados cuando tu cuenta tiene acceso vinculado.</li><li>Registrá tus ingresos mensuales desde el dashboard para calcular ahorro y déficit.</li></ol><p>No pegues contraseñas, claves bancarias ni API keys en el chat.</p></section><div className="workspace-sources">{data.sources.map(source => <article className="workspace-card" key={source.id}>{source.id !== 'gmail' && source.id !== 'binance' && <span className={`workspace-status ${source.status}`}>{source.status === 'not_available' ? 'No disponible aún' : source.status === 'linked' ? 'Fuente habilitada para tu cuenta' : 'Requiere configuración'}</span>}<h2>{source.name}</h2>{source.id === 'gmail' || source.id === 'binance' ? <ProviderConnections api={api} provider={source.id} /> : source.id === 'mercadopago' ? <><p>{source.detail}</p><MercadoPagoSetup /></> : <p>{source.detail}</p>}{source.id === 'telegram' && <button className="outline" onClick={() => setTab('telegram')}>Ver configuración del bot →</button>}</article>)}</div></>}
      {tab === 'telegram' && <><p className="workspace-hint">{data.telegram.detail}</p><div className="workspace-sources"><section className="workspace-card"><h2>Bot de finanzas personales</h2><p>Para registrar tickets, preguntar por tus gastos y recibir alertas de variación. El acceso al chat y el destino de notificaciones se habilitan en el ETL administrado; iniciar sesión en la web no los habilita por sí solo.</p><ol><li>Abrí el bot y enviá <code>/start</code> o <code>/ayuda</code>.</li><li>Confirmá que el administrador haya autorizado tu chat y lo haya configurado como destino de alertas.</li><li>Probá una consulta con período, por ejemplo: “¿Cuánto gasté el mes pasado?”</li></ol>{safeBot(data.telegram.finance_url) ? <a className="workspace-bot-link" href={data.telegram.finance_url!} target="_blank" rel="noopener noreferrer">Abrir bot financiero ↗</a> : <p className="workspace-hint">Falta configurar <code>DIVI_FINANCE_BOT_USERNAME</code> en GitHub Actions. No se puede abrir un enlace todavía.</p>}</section><section className="workspace-card"><h2>Bot de grupos</h2><p>Para saldos y gastos compartidos. Usá <code>/ayuda</code> para consultar los comandos disponibles. No es el bot del historial personal.</p>{safeBot(data.telegram.groups_url) ? <a className="workspace-bot-link" href={data.telegram.groups_url!} target="_blank" rel="noopener noreferrer">Abrir bot de grupos ↗</a> : <p className="workspace-hint">Falta configurar <code>DIVI_GROUPS_BOT_USERNAME</code> en GitHub Actions.</p>}<TelegramLink api={api} available={Boolean(safeBot(data.telegram.groups_url))} /></section></div></>}
    </>}</>}
  </div>
}
