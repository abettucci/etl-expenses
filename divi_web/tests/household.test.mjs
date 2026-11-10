import test from 'node:test'
import assert from 'node:assert/strict'
import { householdMonth, expenseDate } from '../src/household.ts'

test('resume el mes por moneda y no mezcla gastos eliminados u otros meses', () => {
  const rows = [
    { amount: '100.01', currency: 'ARS', expense_date: '2026-10-01', created_at: '2026-10-02T01:00:00Z', allocations: { a: '50.01', b: '50.00' } },
    { amount: '50.00', currency: 'ARS', expense_date: '2026-10-31', created_at: '2026-10-31T20:00:00Z', allocations: { a: '25.00', b: '25.00' } },
    { amount: '2.00', currency: 'USD', expense_date: '2026-10-10', created_at: '2026-10-10T20:00:00Z', shares: { a: '1.00', b: '1.00' } },
    { amount: '999', currency: 'ARS', expense_date: '2026-10-10', created_at: '2026-10-10T20:00:00Z', deleted_at: 'x' },
    { amount: '999', currency: 'ARS', expense_date: '2026-11-01', created_at: '2026-11-01T20:00:00Z' },
  ]
  const result = householdMonth(rows, '2026-10', 'a')
  assert.equal(result.count, 3)
  assert.deepEqual(result.currencies, [{ currency: 'ARS', total: 150.01, share: 75.01 }, { currency: 'USD', total: 2, share: 1 }])
})

test('usa la fecha argentina para gastos anteriores y no inventa participación faltante', () => {
  const row = { amount: '10', currency: 'ARS', created_at: '2026-11-01T01:30:00Z' }
  assert.equal(expenseDate(row), '2026-10-31')
  assert.equal(householdMonth([row], '2026-10', 'a').currencies[0].share, null)
})
