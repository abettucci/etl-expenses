import test from 'node:test'
import assert from 'node:assert/strict'
import { createAuthSession, SESSION_KEY } from '../src/auth-session.ts'

const NOW = 1793700000000
const config = { domain: 'https://synthetic.auth.us-east-2.amazoncognito.com', clientId: 'synthetic-client', redirectUri: 'https://synthetic.example.invalid/auth/callback', apiBase: 'https://synthetic-api.example.invalid' }
const jwt = (extra = {}) => `eyJhbGciOiJSUzI1NiJ9.${Buffer.from(JSON.stringify({ aud: config.clientId, token_use: 'id', exp: NOW / 1000 + 3600, ...extra })).toString('base64url')}.synthetic-not-a-real-signature`
function storage() { const values = new Map(); return { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value), removeItem: key => values.delete(key) } }
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
function setup(fetcher = async () => json([]), expiry = NOW + 3600000, refresh = 'synthetic-refresh') {
  const store = storage(), legacy = storage(); let expired = 0
  store.setItem(SESSION_KEY, JSON.stringify({ idToken: jwt({ exp: expiry / 1000 }), expiresAt: expiry, refreshToken: refresh }))
  const client = createAuthSession({ config, storage: store, legacyStorage: legacy, fetcher, clock: () => NOW, onExpired: () => expired++ })
  return { client, store, legacy, expired: () => expired }
}

test('login usa PKCE, state/nonce, scopes y destino local permitido', async () => {
  const { client, store } = setup()
  const url = new URL(await client.begin('https://evil.invalid'))
  assert.equal(url.pathname, '/oauth2/authorize')
  assert.equal(url.searchParams.get('scope'), 'openid email profile')
  assert.equal(url.searchParams.get('code_challenge_method'), 'S256')
  assert.equal(url.searchParams.get('lang'), 'es')
  assert.equal(url.searchParams.get('state').length, 64)
  assert.equal(url.searchParams.get('nonce').length, 64)
  assert.equal(JSON.parse(store.getItem('split_oauth_pending_v1')).destination, 'home')
})
test('preview vuelve primero al origen canónico sin guardar PKCE en otro dominio', async () => {
  const { client, store } = setup()
  assert.equal(await client.begin('connections', 'https://preview.example.invalid'), 'https://synthetic.example.invalid/inicio?signin=1&next=connections')
  assert.equal(store.getItem('split_oauth_pending_v1'), null)
})
test('callback correcto cambia código por tokens una sola vez y conserva destino', async () => {
  let calls = 0, expectedNonce
  const { client, store } = setup(async (url, options) => {
    calls++; assert.equal(url, config.domain + '/oauth2/token')
    const body = new URLSearchParams(options.body)
    assert.equal(body.get('grant_type'), 'authorization_code')
    assert.equal(body.get('code_verifier').length, 64)
    return json({ id_token: jwt({ nonce: expectedNonce }), refresh_token: 'synthetic-new-refresh', expires_in: 3600 })
  })
  const login = new URL(await client.begin('groups')); expectedNonce = login.searchParams.get('nonce')
  const callback = new URL(config.redirectUri + '?code=synthetic-code&state=' + login.searchParams.get('state'))
  assert.equal(await client.complete(callback), 'groups')
  assert.equal(client.hasSession(), true)
  assert.equal(store.getItem('split_oauth_pending_v1'), null)
  await assert.rejects(client.complete(callback), /misma pestaña/)
  assert.equal(calls, 1)
})
test('state erróneo, duplicado, callback externo y nonce erróneo no crean sesión', async () => {
  for (const kind of ['state', 'duplicate', 'origin', 'nonce']) {
    let calls = 0
    const { client, store } = setup(async () => { calls++; return json({ id_token: jwt({ nonce: 'wrong' }) }) })
    client.clear()
    const login = new URL(await client.begin())
    const callback = new URL(config.redirectUri + '?code=synthetic-code&state=' + login.searchParams.get('state'))
    if (kind === 'state') callback.searchParams.set('state', 'wrong')
    if (kind === 'duplicate') callback.searchParams.append('state', login.searchParams.get('state'))
    if (kind === 'origin') callback.hostname = 'other.example.invalid'
    await assert.rejects(client.complete(callback))
    assert.equal(store.getItem(SESSION_KEY), null)
    assert.equal(calls, kind === 'nonce' ? 1 : 0)
  }
})
test('error OAuth se informa sin mostrar error_description arbitrario', async () => {
  const { client } = setup()
  await assert.rejects(client.complete(new URL(config.redirectUri + '?error=access_denied&error_description=private-details')), error => /canceló/.test(error.message) && !error.message.includes('private-details'))
})
test('retorno vencido y token expirado sin refresh no llaman servicios externos', async () => {
  let calls = 0
  const { client, store } = setup(async () => { calls++; return json({}) }, NOW - 1000, undefined)
  // Explicitly omit the refresh token, rather than using the helper default.
  store.setItem(SESSION_KEY, JSON.stringify({ idToken: jwt({ exp: NOW / 1000 - 10 }), expiresAt: NOW - 1000 }))
  assert.equal(client.hasSession(), false)
  await assert.rejects(client.request('/v1/me'), /sesión venció/)
  const start = new URL(await client.begin())
  const pending = JSON.parse(store.getItem('split_oauth_pending_v1'))
  store.setItem('split_oauth_pending_v1', JSON.stringify({ ...pending, createdAt: NOW - 600001 }))
  await assert.rejects(client.complete(new URL(config.redirectUri + '?code=synthetic&state=' + start.searchParams.get('state'))), /misma pestaña/)
  assert.equal(calls, 0)
})
test('tokens vencidos renuevan una sola vez incluso con solicitudes concurrentes', async () => {
  let refreshes = 0, requests = 0
  const { client } = setup(async (url, options) => {
    if (url.endsWith('/oauth2/token')) { refreshes++; assert.equal(new URLSearchParams(options.body).get('grant_type'), 'refresh_token'); return json({ id_token: jwt(), expires_in: 3600 }) }
    requests++; assert.equal(options.headers.authorization, 'Bearer ' + jwt()); return json({ ok: true })
  }, NOW - 1000)
  await Promise.all([client.request('/v1/me'), client.request('/v1/groups')])
  assert.equal(refreshes, 1); assert.equal(requests, 2)
})
test('401 renueva y reintenta una vez; 403 no borra una sesión válida', async () => {
  let requests = 0, refreshes = 0
  const { client, expired } = setup(async url => {
    if (url.endsWith('/oauth2/token')) { refreshes++; return json({ id_token: jwt({ jti: 'synthetic-new' }), expires_in: 3600 }) }
    requests++; return requests === 1 ? json({ message: 'Unauthorized' }, 401) : json({ error: 'Sin fuente vinculada' }, 403)
  })
  await assert.rejects(client.request('/v1/finance/balance'), /Sin fuente vinculada/)
  assert.equal(refreshes, 1); assert.equal(requests, 2); assert.equal(expired(), 0); assert.equal(client.hasSession(), true)
})
test('refresh revocado y segundo 401 cierran sesión sin bucles', async () => {
  for (const revoked of [true, false]) {
    let requests = 0
    const { client, expired } = setup(async url => {
      if (url.endsWith('/oauth2/token')) return revoked ? json({ error: 'invalid_grant' }, 400) : json({ id_token: jwt({ jti: 'new' }) })
      requests++; return json({}, 401)
    }, revoked ? NOW - 1000 : NOW + 3600000)
    await assert.rejects(client.request('/v1/me'), /sesión venció/)
    assert.equal(client.hasSession(), false); assert.equal(expired(), 1); assert.equal(requests, revoked ? 0 : 2)
  }
})
test('error transitorio preserva refresh para reintentar; salida malformada es clara', async () => {
  const { client, expired } = setup(async () => { throw new Error('internal credential info') }, NOW - 1000)
  await assert.rejects(client.request('/v1/me'), error => /conectar/.test(error.message) && !error.message.includes('credential'))
  assert.equal(client.hasSession(), true); assert.equal(expired(), 0)
  const malformed = setup(async () => new Response('<html>invalid</html>', { status: 502 }))
  await assert.rejects(malformed.client.request('/v1/me'), /forma inesperada/)
})
test('logout durante refresh no restaura credenciales; tokens legados no dan sesión', async () => {
  let resolve
  const response = new Promise(done => { resolve = done })
  const { client, store } = setup(() => response, NOW - 1000)
  const request = client.request('/v1/me')
  client.clear(); resolve(json({ id_token: jwt() }))
  await assert.rejects(request)
  assert.equal(store.getItem(SESSION_KEY), null)
  const old = storage(); old.setItem('split_access_token', 'legacy-token')
  const fresh = createAuthSession({ config, storage: storage(), legacyStorage: old })
  assert.equal(old.getItem('split_access_token'), null); assert.equal(fresh.hasSession(), false)
})

test('la query de la API llega completa: filtros, orden y paginación', async () => {
  const seen = []
  const { client } = setup(async url => { seen.push(url); return json({ rows: [] }) })
  await client.request('/v1/finance/data/transactions?date_from=2026-10-01&date_to=2026-10-31&offset=50&sort=monto_total&dir=asc')
  const sent = new URL(seen[0])
  assert.equal(sent.pathname.endsWith('/v1/finance/data/transactions'), true)
  assert.equal(sent.searchParams.get('date_from'), '2026-10-01')
  assert.equal(sent.searchParams.get('date_to'), '2026-10-31')
  assert.equal(sent.searchParams.get('offset'), '50')
  assert.equal(sent.searchParams.get('sort'), 'monto_total')
  assert.equal(sent.hash, '')
})
