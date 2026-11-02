import test from 'node:test'
import assert from 'node:assert/strict'
import { variation, monthlySummary, parseIncome, isBalanceReport, previousMonth } from '../src/finance-summary.ts'
const row = (month, income, expenses, partial = false) => ({ month, income, expenses, savings: income === null || expenses === null ? null : income - expenses, savings_rate: income > 0 && expenses !== null ? (income - expenses) / income * 100 : null, partial })

test('la comparación usa el mes calendario previo incluso al cambiar de año', () => {
  assert.equal(previousMonth('2026-01'), '2025-12')
  assert.equal(previousMonth('2026-10'), '2026-09')
})

test('promedios excluyen mes seleccionado, parciales e importes faltantes; tasa ponderada', () => {
  const summary = monthlySummary([row('2026-06', null, 100), row('2026-07', 100, 50), row('2026-08', 900, 450), row('2026-09', 100, 999, true), row('2026-10', 0, 100)], '2026-10', 6)
  assert.equal(summary.income.value, 500)
  assert.equal(summary.savings.value, 250)
  assert.equal(summary.savings.count, 2)
  assert.equal(summary.savingsRate, 50)
  assert.equal(summary.expenses.value, 200)
})
test('variaciones soportan déficit, base cero, faltantes y cero real', () => {
  assert.deepEqual(variation(-20, -10), { amount: -10, percent: -100 })
  assert.deepEqual(variation(10, 0), { amount: 10, percent: null })
  assert.deepEqual(variation(0, 100), { amount: -100, percent: -100 })
  assert.equal(variation(null, 100), null)
  assert.equal(monthlySummary([], '2026-10', 3).savingsRate, null)
})
test('ingresos con centavos, cero y formato argentino sin separadores de miles', () => {
  assert.equal(parseIncome('1500000,50'), '1500000.50')
  assert.equal(parseIncome('0'), '0.00')
  for (const value of ['1.500.000', '-1', '', '1e6', 'NaN', '1.234', '9'.repeat(40)]) assert.equal(parseIncome(value), null)
})
test('no se muestra una vista previa ni una respuesta malformada como datos reales', () => {
  const report = { mode: 'live', currency: 'ARS', as_of: '2026-10-10', months: [row('2026-10', 100, 50)] }
  assert.equal(isBalanceReport(report), true)
  assert.equal(isBalanceReport({ ...report, mode: 'preview' }), false)
  assert.equal(isBalanceReport({ ...report, months: [row('2026-10', NaN, 10)] }), false)
  assert.equal(isBalanceReport({ ...report, months: [report.months[0], report.months[0]] }), false)
})
