import { useState } from 'react'
import './phase1.css'

const SETTINGS_URL = 'https://www.mercadopago.com.ar/balance/reports/settlement_v2/settings'
const API_BASE = (import.meta.env.VITE_API_URL || 'https://308jvhhj9f.execute-api.us-east-2.amazonaws.com').replace(/\/$/, '')
export const MERCADOPAGO_PAYMENTS_WEBHOOK = `${API_BASE}/v1/webhooks/mercadopago/payments`

export function MercadoPagoSetup() {
  const [copied, setCopied] = useState(false)
  async function copy() {
    try {
      await navigator.clipboard.writeText(MERCADOPAGO_PAYMENTS_WEBHOOK)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      setCopied(false)
    }
  }
  return <div className="mp-setup">
    <p className="alert-note">Mercado Pago avisa a la app cuando entra un pago. Para activarlo, copiá la URL y pegala en la configuración de notificaciones de Mercado Pago.</p>
    <label className="mp-url">URL del webhook
      <input readOnly value={MERCADOPAGO_PAYMENTS_WEBHOOK} onFocus={event => event.target.select()} />
    </label>
    <div className="mp-actions">
      <button className="outline" type="button" onClick={() => void copy()}>{copied ? 'Copiado' : 'Copiar URL'}</button>
      <a className="primary mp-link" href={SETTINGS_URL} target="_blank" rel="noopener noreferrer">Abrir configuración de Mercado Pago ↗</a>
    </div>
    <ol className="mp-steps">
      <li>En Mercado Pago, entrá a Configuración de notificaciones y elegí la opción de webhooks.</li>
      <li>Pegá la URL de arriba y guardá.</li>
      <li>Volvé acá cuando termines.</li>
    </ol>
    <p className="alert-note">No hace falta ingresar claves acá: la URL es pública y no contiene secretos.</p>
  </div>
}
