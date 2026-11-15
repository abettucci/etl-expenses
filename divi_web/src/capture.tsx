import { useEffect, useRef, useState } from 'react'
import './phase1.css'

type Api = (path: string, method?: string, body?: unknown) => Promise<unknown>
type Preview = { amount: string; merchant: string; expense_date: string; category: string; subcategory: string }
const MAX_SECONDS = 60

function toBase64(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = String(reader.result || '')
      resolve(result.includes(',') ? result.split(',')[1] : result)
    }
    reader.onerror = () => reject(new Error('No pudimos leer el archivo.'))
    reader.readAsDataURL(blob)
  })
}

function pickAudioType(): string {
  const candidates = ['audio/webm;codecs=opus', 'audio/ogg;codecs=opus', 'audio/webm']
  const supported = (window as any).MediaRecorder
  return candidates.find(type => supported?.isTypeSupported?.(type)) || ''
}

export function CaptureTab({ api }: { api: Api }) {
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [recording, setRecording] = useState(false)
  const [seconds, setSeconds] = useState(0)
  const [transcript, setTranscript] = useState('')
  const [draft, setDraft] = useState<Preview | null>(null)
  const recorder = useRef<MediaRecorder | null>(null)
  const chunks = useRef<Blob[]>([])
  const timer = useRef<number | null>(null)
  const started = useRef<number>(0)

  useEffect(() => () => { if (timer.current) window.clearInterval(timer.current) }, [])

  async function sendPhoto(file: File) {
    setError(''); setMessage(''); setBusy(true)
    try {
      if (file.size > 4 * 1024 * 1024) throw new Error('La foto supera 4 MB. Probá con una foto más chica.')
      const image_base64 = await toBase64(file)
      const result = await api('/v1/finance/capture/receipt', 'POST', { image_base64, filename: file.name }) as { message: string }
      setMessage(result.message || 'Ticket procesado.')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'No pudimos procesar el ticket.')
    } finally { setBusy(false) }
  }

  async function startRecording() {
    setError(''); setMessage(''); setTranscript(''); setDraft(null)
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      const type = pickAudioType()
      const rec = new MediaRecorder(stream, type ? { mimeType: type } : undefined)
      chunks.current = []
      rec.ondataavailable = e => { if (e.data.size) chunks.current.push(e.data) }
      rec.onstop = () => {
        stream.getTracks().forEach(track => track.stop())
        const blob = new Blob(chunks.current, { type: rec.mimeType || 'audio/webm' })
        const duration = Math.max(1, Math.round((Date.now() - started.current) / 1000))
        void sendAudio(blob, duration)
      }
      recorder.current = rec
      started.current = Date.now()
      rec.start()
      setRecording(true); setSeconds(0)
      timer.current = window.setInterval(() => {
        const elapsed = Math.round((Date.now() - started.current) / 1000)
        setSeconds(elapsed)
        if (elapsed >= MAX_SECONDS) stopRecording()
      }, 500)
    } catch {
      setError('No pudimos acceder al micrófono. Revisá el permiso del navegador.')
    }
  }

  function stopRecording() {
    if (timer.current) { window.clearInterval(timer.current); timer.current = null }
    if (recorder.current && recorder.current.state !== 'inactive') recorder.current.stop()
    setRecording(false)
  }

  async function sendAudio(blob: Blob, duration: number) {
    setBusy(true)
    try {
      const audio_base64 = await toBase64(blob)
      const result = await api('/v1/finance/capture/voice', 'POST', { audio_base64, duration_seconds: duration }) as { transcript: string; expense: Preview }
      setTranscript(result.transcript)
      setDraft(result.expense)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'No pudimos procesar el audio.')
    } finally { setBusy(false) }
  }

  async function save() {
    if (!draft) return
    setBusy(true); setError('')
    try {
      await api('/v1/finance/capture/voice/save', 'POST', draft)
      setMessage('Gasto guardado.'); setDraft(null); setTranscript('')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'No pudimos guardar el gasto.')
    } finally { setBusy(false) }
  }

  const field = (key: keyof Preview, label: string, type = 'text') => <label className="data-field"><span>{label}</span>
    <input type={type} value={draft ? draft[key] : ''} onChange={e => draft && setDraft({ ...draft, [key]: e.target.value })} /></label>

  return <div className="data-stack">
    <section className="data-card">
      <h3>Foto de ticket</h3>
      <p className="alert-note">Sacá una foto o elegí una imagen del ticket. Se lee igual que en el bot de Telegram.</p>
      <label className="outline export-btn" style={{ cursor: busy ? 'wait' : 'pointer', width: 'fit-content' }}>
        Sacar o elegir foto
        <input type="file" accept="image/*" capture="environment" disabled={busy}
          onChange={e => { const file = e.target.files?.[0]; e.target.value = ''; if (file) void sendPhoto(file) }} style={{ display: 'none' }} />
      </label>
      {message && <p role="status" className="alert-message">{message}</p>}
    </section>

    <section className="data-card">
      <h3>Gasto por audio</h3>
      <p className="alert-note">Grabá hasta {MAX_SECONDS} segundos, por ejemplo: «pizza cinco mil hoy». Revisás lo que entendimos antes de guardar.</p>
      {!recording ? <button className="primary" type="button" disabled={busy} onClick={() => void startRecording()}>Grabar audio</button>
        : <button className="outline" type="button" onClick={stopRecording}>Detener ({seconds}s)</button>}
      {transcript && <p><strong>Escuchamos:</strong> {transcript}</p>}
      {draft && <div className="data-filters">
        {field('amount', 'Monto (ARS)', 'number')}{field('merchant', 'Comercio')}{field('expense_date', 'Fecha', 'date')}
        {field('category', 'Categoría')}{field('subcategory', 'Subcategoría')}
        <div className="data-filter-actions"><button className="primary" type="button" disabled={busy} onClick={() => void save()}>Guardar gasto</button>
          <button className="outline" type="button" onClick={() => { setDraft(null); setTranscript('') }}>Descartar</button></div>
      </div>}
    </section>

    {busy && <p className="data-empty">Procesando…</p>}
    {error && <p role="alert" className="balance-error">{error}</p>}
  </div>
}
