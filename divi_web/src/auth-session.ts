/** Browser session lifecycle only. API Gateway remains the JWT authority. */
export const SESSION_KEY = 'split_session_v1'
const PENDING_KEY = 'split_oauth_pending_v1'
export type AuthConfig = { domain: string; clientId: string; redirectUri: string; apiBase: string }
type Session = { idToken: string; refreshToken?: string; expiresAt: number }
type Pending = { state: string; verifier: string; nonce: string; createdAt: number; destination: string }
export class SessionError extends Error {}

function payload(token: string): Record<string, unknown> {
  try {
    const parts = token.split('.')
    if (parts.length !== 3 || token.length > 20000) throw new Error()
    const claims = JSON.parse(atob(parts[1].replace(/-/g, '+').replace(/_/g, '/')))
    if (!claims || typeof claims !== 'object' || Array.isArray(claims)) throw new Error()
    return claims as Record<string, unknown>
  } catch { throw new SessionError('La respuesta de acceso no es válida. Volvé a ingresar.') }
}
function randomValue() {
  const bytes = crypto.getRandomValues(new Uint8Array(48))
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}
const destinations = new Set(['home', 'balance', 'panorama', 'groups', 'connections', 'profile', 'settings', 'splitter', 'etl'])
const safeDestination = (value: unknown) => typeof value === 'string' && destinations.has(value) ? value : 'home'

export function createAuthSession({ config, storage, legacyStorage, fetcher = fetch, clock = Date.now, onExpired = () => {} }: {
  config: AuthConfig; storage: Storage; legacyStorage?: Storage; fetcher?: typeof fetch; clock?: () => number; onExpired?: () => void
}) {
  const domain = new URL(config.domain)
  const callback = new URL(config.redirectUri)
  if (domain.protocol !== 'https:' || domain.username || domain.password || callback.protocol !== 'https:' && callback.hostname !== 'localhost') throw new Error('Configuración de acceso inválida')
  const tokenUrl = new URL('/oauth2/token', domain).href
  let generation = 0
  let refreshing: Promise<string> | null = null
  // Old releases retained tokens indefinitely. Require one fresh login, not an
  // apparent signed-in state based on a stale or malformed legacy string.
  legacyStorage?.removeItem('split_access_token')
  function read(): Session | null {
    try {
      const value = JSON.parse(storage.getItem(SESSION_KEY) || 'null')
      if (!value || typeof value.idToken !== 'string' || typeof value.expiresAt !== 'number' || !Number.isFinite(value.expiresAt)) return null
      if (value.refreshToken !== undefined && (typeof value.refreshToken !== 'string' || !value.refreshToken || value.refreshToken.length > 20000)) return null
      const claims = payload(value.idToken)
      if (claims.aud !== config.clientId || claims.token_use !== 'id' || typeof claims.exp !== 'number' || !Number.isFinite(claims.exp)) return null
      return { ...value, expiresAt: Math.min(value.expiresAt, claims.exp * 1000) }
    } catch { return null }
  }
  function clear() {
    generation += 1; refreshing = null
    storage.removeItem(SESSION_KEY); storage.removeItem(PENDING_KEY)
    for (const key of ['split_pkce_verifier', 'split_return_view']) storage.removeItem(key)
    legacyStorage?.removeItem('split_access_token')
  }
  function expire(): never { clear(); onExpired(); throw new SessionError('Tu sesión venció. Volvé a ingresar para continuar.') }
  function hasSession() {
    const value = read()
    return Boolean(value && (value.expiresAt > clock() || value.refreshToken))
  }
  function storeTokens(data: unknown, existingRefresh?: string, nonce?: string) {
    const value = data as Record<string, unknown> | null
    if (!value || typeof value.id_token !== 'string') throw new SessionError('Cognito no devolvió una sesión válida. Volvé a ingresar.')
    const claims = payload(value.id_token)
    if (claims.aud !== config.clientId || claims.token_use !== 'id' || typeof claims.exp !== 'number' || !Number.isFinite(claims.exp) || claims.exp * 1000 <= clock() || nonce && claims.nonce !== nonce) throw new SessionError('La confirmación de acceso no es válida. Iniciá sesión nuevamente.')
    const refreshToken = typeof value.refresh_token === 'string' && value.refresh_token ? value.refresh_token : existingRefresh
    const duration = typeof value.expires_in === 'number' && value.expires_in > 0 ? value.expires_in * 1000 : claims.exp * 1000 - clock()
    storage.setItem(SESSION_KEY, JSON.stringify({ idToken: value.id_token, refreshToken, expiresAt: Math.min(claims.exp * 1000, clock() + duration) }))
    return value.id_token
  }
  async function fetchBounded(url: string, options: RequestInit, timeoutMs = 15000) {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)
    try { return await fetcher(url, { ...options, signal: controller.signal }) }
    catch { throw new SessionError('No pudimos conectar el servicio. Revisá tu conexión y reintentá.') }
    finally { clearTimeout(timeout) }
  }
  async function begin(destination = 'home', currentOrigin = callback.origin) {
    if (currentOrigin !== callback.origin) return new URL(`/inicio?signin=1&next=${safeDestination(destination)}`, callback.origin).href
    const verifier = randomValue(), state = randomValue(), nonce = randomValue()
    const challenge = btoa(String.fromCharCode(...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
    const pending: Pending = { state, verifier, nonce, createdAt: clock(), destination: safeDestination(destination) }
    storage.setItem(PENDING_KEY, JSON.stringify(pending))
    const url = new URL('/oauth2/authorize', domain)
    url.search = new URLSearchParams({ response_type: 'code', client_id: config.clientId, redirect_uri: callback.href, scope: 'openid email profile', state, nonce, code_challenge_method: 'S256', code_challenge: challenge, lang: 'es' }).toString()
    return url.href
  }
  async function complete(url: URL) {
    if (url.origin !== callback.origin || url.pathname !== callback.pathname) throw new SessionError('El acceso debe completarse desde el dominio principal de la aplicación.')
    const pending = (() => { try { return JSON.parse(storage.getItem(PENDING_KEY) || 'null') as Pending | null } catch { return null } })()
    storage.removeItem(PENDING_KEY)
    if (url.searchParams.has('error')) throw new SessionError(url.searchParams.get('error') === 'access_denied' ? 'El acceso se canceló o fue rechazado. Podés volver a intentarlo.' : 'El proveedor no pudo completar el acceso. Volvé a intentarlo.')
    if (!pending || typeof pending.verifier !== 'string' || !/^[A-Za-z0-9_-]{64}$/.test(pending.verifier) || typeof pending.state !== 'string' || !pending.nonce || !Number.isFinite(pending.createdAt) || clock() - pending.createdAt > 600000 || pending.createdAt > clock() || url.searchParams.getAll('state').length !== 1 || url.searchParams.get('state') !== pending.state || url.searchParams.getAll('code').length !== 1 || !url.searchParams.get('code')) throw new SessionError('No pudimos verificar el regreso de Cognito. Ingresá nuevamente desde esta misma pestaña; no copies el enlace de confirmación.')
    const currentGeneration = generation
    const result = await fetchBounded(tokenUrl, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ grant_type: 'authorization_code', client_id: config.clientId, redirect_uri: callback.href, code: url.searchParams.get('code')!, code_verifier: pending.verifier }) })
    if (!result.ok) throw new SessionError('La confirmación venció o ya fue usada. Volvé a ingresar.')
    let data: unknown
    try { data = await result.json() } catch { throw new SessionError('El servicio de acceso respondió de forma inesperada. Reintentá.') }
    if (currentGeneration !== generation) throw new SessionError('El acceso se canceló. Volvé a ingresar.')
    storeTokens(data, undefined, pending.nonce)
    return safeDestination(pending.destination)
  }
  async function token(rejected?: string): Promise<string> {
    const value = read()
    if (!value) return expire()
    if (value.expiresAt > clock() + 30000 && (!rejected || value.idToken !== rejected)) return value.idToken
    if (!value.refreshToken) return expire()
    if (refreshing) return refreshing
    const currentGeneration = generation
    const operation = (async () => {
      const result = await fetchBounded(tokenUrl, { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ grant_type: 'refresh_token', client_id: config.clientId, refresh_token: value.refreshToken! }) })
      if (currentGeneration !== generation) throw new SessionError('La sesión se cerró.')
      if (result.status === 400 || result.status === 401) return expire()
      if (!result.ok) throw new SessionError('No pudimos renovar tu sesión. Reintentá en unos minutos.')
      let data: unknown
      try { data = await result.json() }
      catch { if (currentGeneration !== generation) throw new SessionError('La sesión se cerró.'); return expire() }
      if (currentGeneration !== generation) throw new SessionError('La sesión se cerró.')
      try { return storeTokens(data, value.refreshToken) } catch { return expire() }
    })()
    refreshing = operation
    try { return await operation } finally { if (refreshing === operation) refreshing = null }
  }
  async function request(path: string, method = 'GET', body?: unknown): Promise<any> {
    if (!path.startsWith('/v1/') || path.includes('\\') || path.includes('#') || path.includes('..')) throw new Error('Ruta de API inválida')
    const target = new URL(config.apiBase)
    target.pathname = target.pathname.replace(/\/$/, '') + path; target.search = ''; target.hash = ''
    let currentToken = await token()
    const send = () => fetchBounded(target.href, { method, headers: { 'content-type': 'application/json', authorization: `Bearer ${currentToken}` }, body: body === undefined ? undefined : JSON.stringify(body) }, 30000)
    let result = await send()
    if (result.status === 401) { currentToken = await token(currentToken); result = await send() }
    if (result.status === 401) return expire()
    if (result.status === 204) return null
    let data: any
    try { data = await result.json() } catch { throw new Error('El servicio respondió de forma inesperada. Intentá actualizar.') }
    if (!result.ok) throw new Error(typeof data?.error === 'string' && data.error.length <= 300 ? data.error : 'No pudimos completar la acción. Reintentá.')
    return data
  }
  function logoutUrl() { const url = new URL('/logout', domain); url.search = new URLSearchParams({ client_id: config.clientId, logout_uri: new URL('/', callback).href }).toString(); return url.href }
  return { hasSession, begin, complete, request, clear, logoutUrl }
}
