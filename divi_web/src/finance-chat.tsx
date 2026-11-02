import { useEffect, useRef, useState, type FormEvent } from 'react'
import './personal-workspace.css'
type Api = (path: string, method?: string, body?: unknown) => Promise<any>

export function FinanceChat({ api, close }: { api: Api; close: () => void }) {
  const [question, setQuestion] = useState('')
  const [messages, setMessages] = useState<{ role: string; text: string }[]>([])
  const [pending, setPending] = useState(false)
  const [error, setError] = useState('')
  const end = useRef<HTMLDivElement>(null)
  useEffect(() => { const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') close() }; window.addEventListener('keydown', escape); return () => window.removeEventListener('keydown', escape) }, [close])
  useEffect(() => { end.current?.scrollIntoView({ block: 'nearest' }) }, [messages, pending])
  async function send(event?: FormEvent, suggested?: string) {
    event?.preventDefault()
    const text = (suggested || question).trim()
    if (!text || text.length > 1000 || pending) return
    setPending(true); setError(''); setQuestion(''); setMessages(previous => [...previous.slice(-11), { role: 'user', text }])
    try {
      const result = await api('/v1/finance/assistant', 'POST', { question: text })
      if (!result || typeof result.answer !== 'string' || !result.answer.trim() || result.answer.length > 4000) throw new Error('No recibí una respuesta válida. Intentá nuevamente.')
      setMessages(previous => [...previous, { role: 'assistant', text: result.answer }])
    } catch (failure) { setError(failure instanceof Error ? failure.message : 'No pudimos consultar el asistente.') }
    finally { setPending(false) }
  }
  return <section className="finance-chat" role="dialog" aria-labelledby="finance-chat-title"><header><div><span>ETL EXPENSES · TU ASISTENTE</span><h2 id="finance-chat-title">Preguntale a tus gastos</h2></div><button onClick={close} aria-label="Cerrar asistente">×</button></header><div className="finance-chat-messages" aria-live="polite"><p className="workspace-hint">Mismo agente financiero de Telegram. Solo consulta datos: no registra ni modifica gastos desde este chat. Las preguntas son independientes; incluí el período y el comercio si corresponde.</p>{messages.length === 0 && <div className="workspace-shortcuts">{['¿Cuánto gasté el mes pasado?', 'Mostrame mis gastos por categoría este mes'].map(text => <button key={text} onClick={() => void send(undefined, text)} disabled={pending}>{text}</button>)}</div>}{messages.map((message, index) => <p className={`finance-chat-message ${message.role}`} key={index}>{message.text}</p>)}{pending && <p role="status">Consultando tus gastos…</p>}{error && <p className="balance-error" role="alert">{error}</p>}<div ref={end} /></div><form onSubmit={event => void send(event)}><label className="workspace-sr" htmlFor="finance-question">Tu pregunta</label><textarea autoFocus id="finance-question" value={question} onChange={event => setQuestion(event.target.value)} rows={2} maxLength={1000} placeholder="Ej. Mi gasto total de octubre de 2026" /><button type="submit" disabled={pending || !question.trim()} aria-label="Enviar pregunta">↑</button></form><footer>Hasta 30 consultas por hora · usa el proveedor de IA configurado · el historial se borra al cerrar.</footer></section>
}
