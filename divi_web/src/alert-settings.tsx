import './phase1.css'
import { useEffect, useState, type FormEvent } from 'react'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type AlertSettings = {
  thresholds: { percent_threshold: number; absolute_threshold: number; source: 'stored' | 'default' }
  updated_at: string | null
  updated_by: string | null
  channels: { telegram: boolean; whatsapp: boolean }
  whatsapp_sender_available: boolean
}

const money = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 2 })

export function isAlertSettings(value: unknown): value is AlertSettings {
  const v = value as AlertSettings
  return Boolean(v) && typeof v === 'object' && typeof v.thresholds?.percent_threshold === 'number' && typeof v.channels?.telegram === 'boolean'
}

export function AlertSettingsPanel({ api }: { api: Api }) {
  const [settings, setSettings] = useState<AlertSettings | null>(null)
  const [percent, setPercent] = useState('')
  const [absolute, setAbsolute] = useState('')
  const [telegram, setTelegram] = useState(true)
  const [whatsapp, setWhatsapp] = useState(true)
  const [status, setStatus] = useState<'loading' | 'ready' | 'saving' | 'error'>('loading')
  const [message, setMessage] = useState('')

  function apply(next: AlertSettings) {
    setSettings(next)
    setPercent(String(next.thresholds.percent_threshold).replace('.', ','))
    setAbsolute(String(next.thresholds.absolute_threshold).replace('.', ','))
    setTelegram(next.channels.telegram)
    setWhatsapp(next.channels.whatsapp)
  }

  async function load() {
    setStatus('loading'); setMessage('')
    try {
      const result = await api('/v1/alerts/settings')
      if (!isAlertSettings(result)) throw new Error('La configuración de alertas no es válida. Intentá actualizar.')
      apply(result); setStatus('ready')
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'No pudimos cargar la configuración de alertas.')
      setStatus('error')
    }
  }
  useEffect(() => { void load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function save(event: FormEvent) {
    event.preventDefault()
    setStatus('saving'); setMessage('')
    try {
      const result = await api('/v1/alerts/settings', 'PUT', {
        percent_threshold: percent.replace(',', '.'),
        absolute_threshold: absolute.replace(',', '.'),
        telegram_enabled: telegram,
        whatsapp_enabled: whatsapp,
      })
      if (!isAlertSettings(result)) throw new Error('El servicio devolvió una respuesta no válida.')
      apply(result)
      setMessage('Configuración guardada. Se aplica en la próxima verificación de variaciones.')
      setStatus('ready')
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'No pudimos guardar la configuración.')
      setStatus('ready')
    }
  }

  if (status === 'loading') return <section className="workspace-card alert-settings"><h2>Alertas de variación</h2><p>Cargando…</p></section>
  if (!settings) return <section className="workspace-card alert-settings"><h2>Alertas de variación</h2><p role="alert" className="balance-error">{message}</p><button className="outline" onClick={() => void load()}>Reintentar</button></section>

  return <section className="workspace-card alert-settings">
    <h2>Alertas de variación</h2>
    <p className="alert-note">Te avisamos cuando un gasto sube más que el porcentaje <b>o</b> más que el monto que definas.</p>
    <form className="alert-form" onSubmit={event => void save(event)}>
      <label>Porcentaje de aumento (%)<input inputMode="decimal" value={percent} onChange={e => setPercent(e.target.value)} required /></label>
      <label>Monto absoluto (ARS)<input inputMode="decimal" value={absolute} onChange={e => setAbsolute(e.target.value)} required /></label>
      <fieldset className="alert-channels"><legend>Canales</legend>
        <label className="alert-switch"><input type="checkbox" checked={telegram} onChange={e => setTelegram(e.target.checked)} /> Telegram</label>
        <label className="alert-switch"><input type="checkbox" checked={whatsapp} onChange={e => setWhatsapp(e.target.checked)} /> WhatsApp</label>
        {!settings.whatsapp_sender_available && <small className="alert-warn">Todavía no hay envío de alertas por WhatsApp. El interruptor queda guardado para cuando se conecte.</small>}
      </fieldset>
      <button className="primary" type="submit" disabled={status === 'saving'}>{status === 'saving' ? 'Guardando…' : 'Guardar'}</button>
    </form>
    <p className="alert-current">Actual: {settings.thresholds.percent_threshold.toLocaleString('es-AR')}% y {money.format(settings.thresholds.absolute_threshold)}
      {settings.thresholds.source === 'default' && ' (valores por defecto)'}
      {settings.updated_at && ` · actualizado ${settings.updated_at.replace('T', ' ').slice(0, 16)}`}</p>
    {message && <p role="status" className="alert-message">{message}</p>}
  </section>
}
