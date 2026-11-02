import test from 'node:test'
import assert from 'node:assert/strict'
import { landingArea } from '../src/landing-navigation.ts'

test('la portada reúne ambas secciones y permite volver a compartidos', () => {
  assert.equal(landingArea('/', ''), 'welcome')
  assert.equal(landingArea('/', '#finanzas-personales'), 'etl')
  assert.equal(landingArea('/', '#gastos-compartidos'), 'welcome')
})
test('conserva accesos anteriores y el enlace a las etapas del ETL', () => {
  assert.equal(landingArea('/etl', ''), 'etl')
  assert.equal(landingArea('/etl/', ''), 'etl')
  assert.equal(landingArea('/', '#etl-process'), 'etl')
})
test('no intercepta calculadora, grupos, sesión ni panorama', () => {
  for (const path of ['/dividir', '/grupos', '/auth/callback', '/panorama', '/app']) {
    assert.equal(landingArea(path, '#finanzas-personales'), null)
  }
})
