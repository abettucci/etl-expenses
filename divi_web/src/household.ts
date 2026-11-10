export type HouseholdExpense = {
  amount: string; currency?: string; expense_date?: string; created_at: string; deleted_at?: string
  allocations?: Record<string, string>; shares?: Record<string, string>
}

export function argentinaDate(value: string): string {
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return ''
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'America/Argentina/Buenos_Aires', year: 'numeric', month: '2-digit', day: '2-digit' }).format(date)
}

export function expenseDate(expense: HouseholdExpense): string {
  return expense.expense_date || argentinaDate(expense.created_at)
}

export function householdMonth(expenses: HouseholdExpense[], month: string, userId: string) {
  const rows = expenses.filter(row => !row.deleted_at && expenseDate(row).slice(0, 7) === month)
  const cents = (value: string) => Math.round(Number(value) * 100)
  const currencies: Record<string, { total: number; share: number; complete: boolean }> = {}
  for (const row of rows) {
    const currency = row.currency || 'ARS'
    const current = currencies[currency] ||= { total: 0, share: 0, complete: true }
    const allocations = row.allocations || row.shares
    current.total += cents(row.amount)
    if (!allocations) current.complete = false
    else current.share += cents(allocations[userId] || '0')
  }
  return { currencies: Object.entries(currencies).map(([currency, value]) => ({ currency, total: value.total / 100, share: value.complete ? value.share / 100 : null })), count: rows.length }
}
