import test from 'node:test'
import assert from 'node:assert/strict'
import { amountToCents, calculateDivision, cloneDivision, equalShares, resultText } from '../src/ticket_splitter/model.ts'

const sample = () => ({
  id: 'demo', name: 'División de prueba', currency: 'ARS',
  createdAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z',
  participants: ['a', 'b', 'c'].map(id => ({ id, name: id.toUpperCase() })),
  expenses: [{ id: 'expense', description: 'Compra de prueba', amountCents: 10000,
    paidBy: [{ participantId: 'a', amountCents: 10000 }],
    participantIds: ['a', 'b', 'c'], splitMode: 'equal', exactShares: {} }],
  payments: [],
})

test('repartir centavos conserva el total incluso con resto', () => {
  assert.deepEqual(equalShares(10000, ['a', 'b', 'c']), { a: 3334, b: 3333, c: 3333 })
  const calculation = calculateDivision(sample())
  assert.deepEqual(calculation.issues, [])
  assert.deepEqual(calculation.balances, { a: 6666, b: -3333, c: -3333 })
  assert.equal(Object.values(calculation.balances).reduce((sum, value) => sum + value, 0), 0)
  assert.equal(calculation.settlements.length, 2)
})

test('varios pagadores descuentan lo que ya adelantó cada persona', () => {
  const division = sample()
  division.participants.pop()
  division.expenses[0].participantIds = ['a', 'b']
  division.expenses[0].paidBy = [{ participantId: 'a', amountCents: 7000 }, { participantId: 'b', amountCents: 3000 }]
  assert.deepEqual(calculateDivision(division).settlements, [{ debtorParticipantId: 'b', creditorParticipantId: 'a', amountCents: 2000 }])
})

test('el reparto exacto usa los importes asignados', () => {
  const division = sample()
  division.expenses[0].splitMode = 'exact'
  division.expenses[0].exactShares = { a: 2000, b: 5000, c: 3000 }
  assert.deepEqual(calculateDivision(division).balances, { a: 8000, b: -5000, c: -3000 })
})

test('no propone transferencias con repartos negativos, sumas incorrectas ni importes inválidos', () => {
  for (const mutate of [
    expense => { expense.splitMode = 'exact'; expense.exactShares = { a: -1000, b: 6000, c: 5000 } },
    expense => { expense.paidBy[0].amountCents = 9999 },
    expense => { expense.amountCents = NaN },
    expense => { expense.participantIds = ['inexistente'] },
  ]) {
    const division = sample()
    mutate(division.expenses[0])
    const calculation = calculateDivision(division)
    assert.ok(calculation.issues.length > 0)
    assert.deepEqual(calculation.settlements, [])
  }
})

test('marcar las transferencias salda la cuenta y deshacer restablece la deuda', () => {
  const division = sample()
  const original = calculateDivision(division)
  division.payments = original.settlements.map((settlement, index) => ({ ...settlement, id: String(index), paidAt: division.createdAt }))
  assert.deepEqual(calculateDivision(division).settlements, [])
  assert.match(resultText(division, calculateDivision(division)), /No hay transferencias pendientes/)
  division.payments.pop()
  assert.equal(calculateDivision(division).settlements.length, 1)
})

test('duplicar crea identificadores nuevos y no arrastra pagos', () => {
  const division = sample()
  division.payments = [{ id: 'paid', debtorParticipantId: 'b', creditorParticipantId: 'a', amountCents: 3333, paidAt: division.createdAt }]
  const copy = cloneDivision(division)
  assert.notEqual(copy.id, division.id)
  assert.deepEqual(copy.payments, [])
  assert.ok(copy.participants.every(person => !division.participants.some(original => original.id === person.id)))
  assert.deepEqual(calculateDivision(copy).issues, [])
  assert.equal(copy.expenses[0].amountCents, division.expenses[0].amountCents)
})

test('la entrada decimal acepta coma y no convierte texto inválido en dinero', () => {
  assert.equal(amountToCents('1234,56'), 123456)
  assert.equal(amountToCents('12.50'), 1250)
  assert.equal(amountToCents('no es un monto'), 0)
})
