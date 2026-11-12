import { useEffect, useState } from 'react'
type Api = (path: string, method?: string, body?: unknown) => Promise<any>

export function FinanceSetup({ api, linked }: { api: Api; linked: boolean }) {
  const [accountId, setAccountId] = useState(''), [pending, setPending] = useState(false), [message, setMessage] = useState('')
  async function prepare() {
    setPending(true); setMessage('')
    try {
      const result = await api('/v1/me')
      if (typeof result?.user_id !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(result.user_id)) throw new Error('No pudimos identificar tu cuenta. Volvé a ingresar.')
      setAccountId(result.user_id)
    } catch (error) { setMessage(error instanceof Error ? error.message : 'No pudimos preparar la vinculación.') }
    finally { setPending(false) }
  }
  return <section className="workspace-card connection-setup"><span className={`workspace-status ${linked ? 'linked' : ''}`}>{linked ? 'Historial habilitado para tu cuenta' : 'Pendiente de habilitación'}</span><h2>Vincular mi historial anterior</h2><p>{linked ? 'Tu cuenta tiene permiso para consultar el ETL existente. No hace falta reconectar las fuentes administradas desde esta pantalla.' : 'El historial existente tiene un único dueño. Para habilitarlo, primero identificá esta cuenta y pedí al administrador que confirme que ese historial es tuyo.'}</p>{!linked && <><button className="outline" disabled={pending} onClick={() => void prepare()}>{pending ? 'Identificando cuenta…' : 'Preparar vinculación'}</button>{accountId && <div className="connection-instructions"><label>ID de esta cuenta<input value={accountId} readOnly onFocus={event => event.target.select()} /></label><p>Compartí este identificador con el administrador del ETL. No es una contraseña y no concede acceso por sí solo.</p><details><summary>Configuración necesaria en GitHub Actions</summary><ol><li>Confirmar el propietario antes de habilitar los datos.</li><li>Configurar el secret <code>DIVI_FINANCE_FORECAST_OWNER_SUB</code> con el ID de esta cuenta.</li><li>Configurar la variable <code>DIVI_FINANCE_FORECAST_ENABLED</code> en <code>true</code> y ejecutar el despliegue.</li></ol><p>No uses el ID de otra persona ni habilites el mismo historial para todas las cuentas.</p></details></div>}</>}{message && <p role="alert" className="balance-error">{message}</p>}<p className="muted">Esta vinculación da acceso al historial del ETL administrado. Las conexiones personales de Gmail y Binance se gestionan en sus respectivas tarjetas.</p></section>
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
