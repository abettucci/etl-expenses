import { useEffect, useState } from 'react'
type Api = (path: string, method?: string, body?: unknown) => Promise<any>

export function FinanceSetup({ api, linked, botUrl, onRefresh }: { api: Api; linked: boolean; botUrl: string | null; onRefresh: () => void }) {
  const [code, setCode] = useState(''), [expiresAt, setExpiresAt] = useState(0), [pending, setPending] = useState(false), [verifying, setVerifying] = useState(false), [message, setMessage] = useState(''), [copied, setCopied] = useState(false)
  useEffect(() => {
    if (!expiresAt) return
    const timeout = setTimeout(() => { setCode(''); setMessage('El código venció. Generá uno nuevo para continuar.') }, Math.max(0, expiresAt - Date.now()))
    return () => clearTimeout(timeout)
  }, [expiresAt])
  async function prepare() {
    setPending(true); setMessage(''); setCode(''); setCopied(false)
    try {
      const result = await api('/v1/finance/connection', 'POST')
      if (typeof result?.code !== 'string' || !/^[A-F0-9]{48}$/.test(result.code) || result.expires_in_minutes !== 15) throw new Error('No recibimos un código válido. Reintentá.')
      setCode(result.code); setExpiresAt(Date.now() + result.expires_in_minutes * 60000)
    } catch (error) { setMessage(error instanceof Error ? error.message : 'No pudimos preparar la vinculación.') }
    finally { setPending(false) }
  }
  async function copy() {
    try { await navigator.clipboard.writeText(`/conectar_historial ${code}`); setCopied(true) }
    catch { setMessage('Seleccioná el comando y copialo para enviarlo al bot.') }
  }
  async function verify() {
    setVerifying(true); setMessage('')
    try {
      const result = await api('/v1/profile/overview')
      if (result?.finance_linked === true) onRefresh()
      else setMessage('Todavía no recibimos la confirmación. Enviá el comando al bot financiero y volvé a intentar.')
    } catch (error) { setMessage(error instanceof Error ? error.message : 'No pudimos verificar la vinculación.') }
    finally { setVerifying(false) }
  }
  return <section id="vincular-historial" className="workspace-card connection-setup"><span className={`workspace-status ${linked ? 'linked' : ''}`}>{linked ? 'Historial vinculado' : 'Sin vincular'}</span><h2>Vincular mi historial financiero</h2><p>{linked ? 'Tu cuenta ya puede consultar tu historial de gastos existente.' : 'Conectá el historial que ya usás con tu bot financiero. Para confirmar que es tuyo, enviá un código al mismo bot desde tu chat autorizado.'}</p>{!linked && <><button className="outline" disabled={pending || Boolean(code)} onClick={() => void prepare()}>{pending ? 'Generando código…' : 'Vincular mi historial'}</button>{code && <div className="connection-instructions"><ol><li>Copiá el comando y envialo en privado a tu bot financiero.</li><li>Cuando el bot confirme la vinculación, volvé acá y tocá «Ya lo envié».</li></ol><label>Comando para el bot financiero<input value={`/conectar_historial ${code}`} readOnly onFocus={event => event.target.select()} /></label><button className="outline" onClick={() => void copy()}>{copied ? 'Copiado' : 'Copiar comando'}</button>{botUrl && <a className="workspace-bot-link" href={botUrl} target="_blank" rel="noopener noreferrer">Abrir bot financiero ↗</a>}<button className="outline" disabled={verifying} onClick={() => void verify()}>{verifying ? 'Verificando…' : 'Ya lo envié · actualizar estado'}</button><small>El código vence a las {new Date(expiresAt).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })} y se usa una sola vez.</small></div>}</>}{message && <p role="alert" className="balance-error">{message}</p>}<p className="muted">Esta opción vincula el historial que ya procesa tu ETL. Conectar nuevas cuentas de Gmail, bancos, Balanz o Binance requiere sus respectivos conectores.</p></section>
}

export function TelegramLink({ api, available }: { api: Api; available: boolean }) {
  const [code, setCode] = useState(''), [expiresAt, setExpiresAt] = useState(0), [pending, setPending] = useState(false), [error, setError] = useState('')
  const [copied, setCopied] = useState(false)
  useEffect(() => {
    if (!expiresAt) return
    const timeout = setTimeout(() => { setCode(''); setError('El código venció. Generá uno nuevo para continuar.') }, Math.max(0, expiresAt - Date.now()))
    return () => clearTimeout(timeout)
  }, [expiresAt])
  async function create() {
    setPending(true); setError(''); setCode(''); setCopied(false)
    try {
      const result = await api('/v1/identity-links', 'POST')
      if (typeof result?.code !== 'string' || !/^[A-Z0-9]{8,64}$/.test(result.code) || !Number.isInteger(result.expires_in_minutes) || result.expires_in_minutes < 1 || result.expires_in_minutes > 15) throw new Error('No recibimos un código válido. Reintentá.')
      setCode(result.code); setExpiresAt(Date.now() + result.expires_in_minutes * 60000)
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos generar el código.') }
    finally { setPending(false) }
  }
  async function copy() {
    try { if (!navigator.clipboard) throw new Error(); await navigator.clipboard.writeText(`/vincular ${code}`); setCopied(true) }
    catch { setError('No pudimos copiar automáticamente. Tocá el campo para seleccionar el comando completo y copialo.') }
  }
  return <div className="connection-instructions"><h3>Vincular esta cuenta con Telegram</h3><p>Generá un código y enviá el comando al bot de grupos en un chat privado. No lo compartas: vincula tu identidad de Telegram con esta cuenta web.</p><button className="outline" onClick={() => void create()} disabled={!available || pending || Boolean(code)}>{pending ? 'Generando código…' : 'Generar código de vinculación'}</button>{!available && <p className="workspace-hint">Primero debe configurarse el nombre público del bot de grupos en el despliegue. No generamos un código que no podrías usar.</p>}{code && <div role="status"><label>Comando para el chat privado<input readOnly value={`/vincular ${code}`} onFocus={event => event.target.select()} /></label><button className="outline" onClick={() => void copy()}>{copied ? 'Comando copiado' : 'Copiar comando'}</button><small>Copialo y enviáselo al bot. Vence a las {new Date(expiresAt).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })}; se usa una sola vez.</small></div>}{error && <p role="alert" className="balance-error">{error}</p>}<p className="muted">Esto vincula los grupos, no el bot financiero ni permisos sobre BigQuery. Al terminar, actualizá Grupos para ver tus espacios.</p></div>
}
