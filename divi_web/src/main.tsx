import { useMemo, useState, type FormEvent, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import './styles.css'

if ('serviceWorker' in navigator) window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js'))

type View = 'home' | 'groups' | 'activity' | 'wallet' | 'settings' | 'gifts'
type Gift = { id: string; title: string; price: number; available: boolean; link?: string }
type Expense = { title: string; category: string; amount: number; payer: string; date: string }

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 })
const apiBase = import.meta.env.VITE_API_URL || ''

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
  const [chat, setChat] = useState<string[]>(['Hola, soy Divia. Puedo ayudarte a ordenar gastos, ver tu balance o armar el regalo de Fran ✦'])
  const balance = useMemo(() => expenses.reduce((sum, expense) => sum + expense.amount, 0), [expenses])
  const notify = (message: string) => { setNotice(message); window.setTimeout(() => setNotice(''), 3200) }
  const reserve = async (gift: Gift) => {
    if (!gift.available) return
    try { await api(`/v1/groups/demo/events/fran-2026/wishlist/${gift.id}/reserve`, 'POST') } catch (e) { notify(e instanceof Error ? e.message : 'No disponible') ; return }
    setGifts(items => items.map(item => item.id === gift.id ? { ...item, available: false } : item)); notify('Regalo reservado para este grupo. Los demás grupos ya no lo verán disponible.')
  }
  return <main>
    <div className="aurora" /><div className="grain" />
    <header className="topbar"><button className="brand" onClick={() => setView('home')}><Mark /><span>divi</span></button><div className="desktop-meta"><span className="pill live">● grupo activo</span><Avatar name="Betu" /><span>Betu</span></div></header>
    <section className="shell">
      {view === 'home' && <Home balance={balance} onExpense={() => setOverlay('expense')} onPremium={() => setOverlay('premium')} onGift={() => setView('gifts')} />}
      {view === 'groups' && <Groups onExpense={() => setOverlay('expense')} onGift={() => setView('gifts')} />}
      {view === 'activity' && <Activity expenses={expenses} />}
      {view === 'wallet' && <Wallet onPremium={() => setOverlay('premium')} />}
      {view === 'settings' && <Settings notify={notify} />}
      {view === 'gifts' && <Gifts gifts={gifts} reserve={reserve} onPool={() => setOverlay('pool')} />}
    </section>
    <nav className="dock" aria-label="Navegación principal">
      <button className={view === 'home' ? 'active' : ''} onClick={() => setView('home')}><span>⌂</span>Inicio</button>
      <button className={view === 'groups' || view === 'gifts' ? 'active' : ''} onClick={() => setView('groups')}><span>♧</span>Grupos</button>
      <button className="plus" onClick={() => setOverlay('expense')} aria-label="Agregar gasto">+</button>
      <button className={view === 'activity' ? 'active' : ''} onClick={() => setView('activity')}><span>♙</span>Actividad</button>
      <button className={view === 'wallet' ? 'active' : ''} onClick={() => setView('wallet')}><span>▣</span>Billetera</button>
    </nav>
    <button className="divia" onClick={() => setOverlay('assistant')} aria-label="Abrir asistente Divia">✧</button>
    {notice && <div className="toast" role="status">✓ {notice}</div>}
    {overlay === 'expense' && <ExpenseModal close={() => setOverlay(null)} submit={expense => { setExpenses([expense, ...expenses]); setOverlay(null); notify('Gasto agregado y dividido en el grupo.') }} />}
    {overlay === 'assistant' && <Assistant chat={chat} setChat={setChat} close={() => setOverlay(null)} />}
    {overlay === 'premium' && <PremiumModal close={() => setOverlay(null)} notify={notify} />}
    {overlay === 'pool' && <PoolModal close={() => setOverlay(null)} notify={notify} />}
  </main>
}

function Home({ balance, onExpense, onPremium, onGift }: { balance: number; onExpense: () => void; onPremium: () => void; onGift: () => void }) { return <>
  <div className="eyebrow">Hola, Betu <span>〰</span></div><h1>Compartir gastos<br /><em>sin pensar de más.</em></h1>
  <section className="hero-panel stagger"><div className="group-line"><div><span className="kicker">GRUPO ACTUAL</span><h2>Militantes del mood</h2></div><span className="members">♧ 4</span></div><div className="hero-actions"><button onClick={onExpense}>＋<span>Agregar gasto</span></button><button className="pink" onClick={onGift}>♔<span>Regalo de Fran</span></button></div><div className="balance"><span>MI BALANCE</span><strong>− {money.format(6175)}</strong><small>Te queda una liquidación pendiente</small></div><div className="settlement"><Avatar name="B" /><p><b>Betu</b> le debe a <b>matias.arbues7</b><strong>{money.format(6175)}</strong></p><button>Resolver →</button></div></section>
  <section className="feature-card"><span className="action-icon">⌘</span><div><h3>Tu bolsillo, con contexto</h3><p>Centralizá gastos, tickets y hábitos personales.</p></div><button onClick={onPremium}>Probar Premium <b>→</b></button></section>
  <div className="metrics"><div><span>GASTO DEL GRUPO</span><strong>{money.format(balance)}</strong><small>últimos 7 días</small></div><div><span>PRÓXIMO HITO</span><strong>Fran · 21 sep</strong><small>Regalo en marcha</small></div></div>
</> }

function Groups({ onExpense, onGift }: { onExpense: () => void; onGift: () => void }) { return <><div className="page-title"><div><span className="kicker">TU GENTE</span><h1>Mis grupos</h1></div><button className="outline">＋ Crear grupo</button></div><section className="group-grid"><button className="group-card selected" onClick={onExpense}><span className="orb cyan">♧</span><div><h2>Militantes del mood</h2><p>4 personas · ARS</p></div><strong>− $ 6.175</strong><i>→</i></button><button className="group-card" onClick={onGift}><span className="orb pink">♔</span><div><h2>Cumple de Fran</h2><p>Regalo compartido · 8 días</p></div><strong className="mint">$ 32.000</strong><i>→</i></button></section><section className="insight"><span>✦</span><div><h3>El cumpleaños se acerca</h3><p>Fran ya cargó 3 ideas. Coordiná el regalo sin spoilers ni duplicados.</p></div><button onClick={onGift}>Ver wishlist →</button></section></> }

function Activity({ expenses }: { expenses: Expense[] }) { return <><div className="page-title"><div><span className="kicker">TODO EN ORDEN</span><h1>Actividad</h1></div><button className="outline">⇩ Exportar</button></div><div className="filters"><input placeholder="Buscar gastos…" /><button className="selected">Todos</button><button>Comida</button><button>Transporte</button><button>Compras</button></div><section className="total-bar"><span>Total gastado</span><strong>{money.format(expenses.reduce((a, e) => a + e.amount, 0))}</strong></section><div className="timeline">{expenses.map((expense, index) => <article className="expense" key={`${expense.title}${index}`}><span className={`category c${index}`}>{expense.category === 'Comida' ? '⌇' : '⌁'}</span><div><h3>{expense.title}</h3><p>{expense.category} · {expense.date}</p></div><div><strong>{money.format(expense.amount)}</strong><p>Pagó {expense.payer}</p></div></article>)}</div></> }

function Wallet({ onPremium }: { onPremium: () => void }) { return <><div className="page-title"><div><span className="kicker">TU RESUMEN FINANCIERO</span><h1>Mi billetera</h1></div><button className="outline">⚙ Ajustes</button></div><section className="wallet-banner"><div><span className="crown">♔</span><span className="kicker">PLAN GRATUITO</span><h2>Tu dinero ya habla.<br />Escuchalo completo.</h2><p>Wallet Premium suma patrones, historial ilimitado y exportaciones.</p></div><div className="wallet-stats"><span>5</span><p>escaneos<br />restantes</p></div><button onClick={onPremium}>Ver plan Premium <b>→</b></button></section><section className="blurred-ledger"><div><span>INGRESOS</span><strong>$ 1.820.000</strong></div><div><span>GASTOS</span><strong>$ 928.400</strong></div><div><span>AHORRO</span><strong>$ 891.600</strong></div></section></> }

function Settings({ notify }: { notify: (message: string) => void }) { return <><div className="page-title"><div><span className="kicker">TU ESPACIO</span><h1>Ajustes</h1></div><Avatar name="Betu" /></div><section className="settings-list"><Action icon="◉" title="Perfil" caption="Nombre, alias y foto" onClick={() => notify('Perfil listo para editar.')} /><Action icon="♧" title="Notificaciones" caption="Recordatorios de grupos y pagos" onClick={() => notify('Solicitamos permiso de notificaciones.')} /><Action icon="◒" title="Apariencia" caption="Noche aurora" onClick={() => notify('La apariencia se adapta al sistema.')} /><Action icon="⊙" title="Mercado Pago" caption="Conectá tu cuenta para organizar pools" onClick={() => { api('/v1/mercadopago/oauth/start', 'POST').then((result: any) => result?.authorization_url && (window.location.href = result.authorization_url)).catch(() => notify('Configuraremos tu cuenta de Mercado Pago.')) }} /><Action icon="?" title="Ayuda" caption="Preguntas frecuentes y soporte" /></section><p className="version">DIVI / 0.1.0<br />hecho para compartir</p></> }

function Gifts({ gifts, reserve, onPool }: { gifts: Gift[]; reserve: (gift: Gift) => void; onPool: () => void }) { return <><button className="back" onClick={() => history.back()}>← Volver a grupos</button><div className="gift-hero"><span className="kicker">CUMPLEAÑOS · 21 SEP</span><h1>El regalo de<br /><em>Fran.</em></h1><p>Su wishlist es una sola, aunque esté en varios grupos. Sin regalos repetidos.</p></div><section className="pool-progress"><div><span>POOL ACTIVO</span><strong>$ 32.000 <small>de $ 48.000</small></strong></div><div className="progress"><i /></div><button onClick={onPool}>Aportar al pool →</button></section><div className="wishlist-head"><div><h2>Wishlist de Fran</h2><p>Solo Fran puede editar esta lista.</p></div><span className="privacy">◌ sin spoilers</span></div><div className="gift-list">{gifts.map(gift => <article className={`gift ${!gift.available ? 'reserved' : ''}`} key={gift.id}><div className="gift-visual">{gift.title.includes('Auriculares') ? '◖◗' : gift.title.includes('Cafetera') ? '♨' : '◒'}</div><div><h3>{gift.title}</h3><p>{money.format(gift.price)}</p>{!gift.available && <small>Este regalo ya fue elegido</small>}</div><button disabled={!gift.available} onClick={() => reserve(gift)}>{gift.available ? 'Elegir regalo' : 'No disponible'}</button></article>)}</div></> }

function ExpenseModal({ close, submit }: { close: () => void; submit: (expense: Expense) => void }) { const [amount, setAmount] = useState(''); const [description, setDescription] = useState(''); const send = async (event: FormEvent) => { event.preventDefault(); if (!description || !Number(amount)) return; try { await api('/v1/groups/demo/expenses', 'POST', { amount, description, paid_by: 'demo', participants: ['demo'], split_method: 'equal' }) } catch { /* demo remains usable without a deployed API */ } submit({ title: description, category: 'Otro', amount: Number(amount), payer: 'Betu', date: 'ahora' }) }; return <Modal title="Nuevo gasto" close={close}><form className="expense-form" onSubmit={send}><label>Monto<input autoFocus value={amount} onChange={e => setAmount(e.target.value.replace(/[^0-9]/g, ''))} placeholder="$ 0" inputMode="numeric" /></label><label>Descripción<input value={description} onChange={e => setDescription(e.target.value)} placeholder="¿Qué compraste?" /></label><fieldset><legend>Categoría</legend><button type="button" className="chip selected">⌇ Comida</button><button type="button" className="chip">⌁ Transporte</button><button type="button" className="chip">◒ Compras</button></fieldset><fieldset><legend>Dividir entre</legend><div className="member-check"><Avatar name="Matías" tone="pink" /> Matías <b>✓</b></div><div className="member-check"><Avatar name="Betu" /> Betu <b>✓</b></div><div className="member-check"><Avatar name="Lucas" /> Lucas <b>✓</b></div></fieldset><button className="primary" type="submit">Confirmar gasto <b>→</b></button></form></Modal> }

function Assistant({ chat, setChat, close }: { chat: string[]; setChat: (value: string[]) => void; close: () => void }) { const [prompt, setPrompt] = useState(''); const ask = (event: FormEvent) => { event.preventDefault(); if (!prompt.trim()) return; setChat([...chat, `Vos: ${prompt}`, 'Divia: Para este grupo, lo más simple es cargarlo como gasto igual y cerrar la liquidación después.']); setPrompt('') }; return <Modal title="Divia" subtitle="Tu asistente financiera" close={close} wide><div className="chat">{chat.map((line, index) => <p className={line.startsWith('Vos:') ? 'user' : ''} key={index}>{line}</p>)}</div><div className="suggestions"><button onClick={() => setChat([...chat, 'Divia: Esta semana gastaste más en salidas, principalmente el viernes.'])}>¿En qué gasté más?</button><button onClick={() => setChat([...chat, 'Divia: Fran tiene 3 regalos posibles; el pool ya completó el 67%.'])}>¿Cómo va el regalo?</button></div><form className="ask" onSubmit={ask}><input value={prompt} onChange={e => setPrompt(e.target.value)} placeholder="Escribile a Divia…" /><button>↗</button></form></Modal> }

function PremiumModal({ close, notify }: { close: () => void; notify: (message: string) => void }) { const [card, setCard] = useState(''); const start = async (event: FormEvent) => { event.preventDefault(); try { const result: any = await api('/v1/premium/checkout', 'POST', { card_token_id: card }); if (result) notify('Suscripción autorizada. Tu prueba de 60 días ya está activa.') } catch (e) { notify(e instanceof Error ? e.message : 'Usá Mercado Pago para completar la suscripción.') } }; return <Modal title="Subirse a Premium" subtitle="$3.000 ARS / mes · 60 días sin cargo" close={close}><section className="premium-in-modal"><span className="crown">♔</span><h2>Más señal. Menos fricción.</h2><ul><li>30 escaneos de tickets por mes</li><li>Wallet personal completa</li><li>Exportar a CSV e historial ilimitado</li><li>Soporte prioritario</li></ul></section><form className="pay-form" onSubmit={start}><label>Token de tarjeta de Mercado Pago<input value={card} onChange={e => setCard(e.target.value)} placeholder="Generado por Mercado Pago Bricks" /></label><small>No guardamos datos de tarjeta. El token se envía directo a Mercado Pago.</small><button className="primary">Activar 60 días gratis <b>→</b></button></form></Modal> }

function PoolModal({ close, notify }: { close: () => void; notify: (message: string) => void }) { const [amount, setAmount] = useState('5000'); const contribute = () => { notify(`Abrimos Mercado Pago para aportar ${money.format(Number(amount))} al organizador.`); close() }; return <Modal title="Aportar al regalo" subtitle="El dinero se acredita directo al organizador" close={close}><div className="pool-modal"><span className="pool-icon">♔</span><h2>Auriculares para Fran</h2><p>Juntamos $32.000 de $48.000. Tu aporte no queda guardado en Divi.</p><label>Tu aporte<input value={amount} onChange={e => setAmount(e.target.value.replace(/[^0-9]/g, ''))} inputMode="numeric" /></label><button className="primary" onClick={contribute}>Pagar con Mercado Pago <b>↗</b></button><small>El pago lo recibe quien organiza este regalo.</small></div></Modal> }

function Modal({ title, subtitle, close, children, wide = false }: { title: string; subtitle?: string; close: () => void; children: ReactNode; wide?: boolean }) { return <div className="modal-backdrop" role="presentation"><section className={`modal ${wide ? 'wide' : ''}`} role="dialog" aria-modal="true" aria-label={title}><button className="close" onClick={close} aria-label="Cerrar">×</button><header><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</header>{children}</section></div> }

createRoot(document.getElementById('root')!).render(<App />)
