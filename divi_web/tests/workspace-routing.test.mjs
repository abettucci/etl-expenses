import test from 'node:test'
import assert from 'node:assert/strict'
import { legacyWorkspaceView, loginDestination, workspacePaths } from '../src/workspace-routing.ts'

test('el acceso general llega a Inicio y conserva destinos explícitos seguros', () => {
  assert.equal(loginDestination(null), 'home')
  assert.equal(loginDestination('https://invalid.example'), 'home')
  for (const destination of ['groups', 'balance', 'panorama', 'connections', 'profile', 'splitter']) {
    assert.equal(loginDestination(destination), destination)
  }
})
test('balance y proyección son rutas internas de finanzas', () => {
  assert.equal(workspacePaths.balance, '/finanzas/balance')
  assert.equal(workspacePaths.panorama, '/finanzas/proyeccion')
  assert.equal(legacyWorkspaceView('/app'), 'home')
  assert.equal(legacyWorkspaceView('/balance/'), 'balance')
  assert.equal(legacyWorkspaceView('/panorama'), 'panorama')
  assert.equal(legacyWorkspaceView('/regalos'), 'groups')
  assert.equal(legacyWorkspaceView('/finanzas/balance'), null)
})
