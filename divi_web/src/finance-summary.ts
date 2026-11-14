export type MonthlyActual = { month: string; income: number | null; expenses: number | null; savings: number | null; savings_rate: number | null; partial: boolean }
export type BalanceReport = { currency: 'ARS'; as_of: string; mode: 'live'; months: MonthlyActual[]; notice?: string }
export type Metric = 'income' | 'expenses' | 'savings'

export function previousMonth(month: string) {
  const index = Number(month.slice(0, 4)) * 12 + Number(month.slice(5, 7)) - 2
  return `${Math.floor(index / 12)}-${String(index % 12 + 1).padStart(2, '0')}`
}

export function variation(value: number | null, base: number | null) {
  if (value === null || base === null) return null
  return { amount: value - base, percent: base === 0 ? null : (value - base) / Math.abs(base) * 100 }
}

export function monthlySummary(months: MonthlyActual[], selected: string, lookback: number) {
  const previous = [...months].filter(row => row.month < selected && !row.partial).sort((a, b) => a.month.localeCompare(b.month)).slice(-lookback)
  const average = (metric: Metric) => {
    const values = previous.flatMap(row => row[metric] === null ? [] : [row[metric] as number])
    return { value: values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : null, count: values.length }
  }
  const paired = previous.filter(row => row.savings !== null && row.income !== null)
  const income = paired.reduce((sum, row) => sum + row.income!, 0)
  return { income: average('income'), expenses: average('expenses'), savings: average('savings'),
    savingsRate: income > 0 ? paired.reduce((sum, row) => sum + row.savings!, 0) / income * 100 : null,
    rateCount: paired.length, previous }
}

export function parseIncome(value: string): string | null {
  // Input has no thousands separators: native number input, dot or comma decimal.
  const normalized = value.trim().replace(',', '.')
  if (!/^\d{1,12}(\.\d{1,2})?$/.test(normalized)) return null
  const amount = Number(normalized)
  return Number.isFinite(amount) && amount >= 0 ? amount.toFixed(2) : null
}

export function isBalanceReport(value: unknown): value is BalanceReport {
  if (!value || typeof value !== 'object') return false
  const report = value as BalanceReport
  return report.mode === 'live' && report.currency === 'ARS' && typeof report.as_of === 'string' && (report.notice === undefined || typeof report.notice === 'string' && report.notice.length <= 500) && Array.isArray(report.months)
    && report.months.length <= 13 && new Set(report.months.map(row => row?.month)).size === report.months.length
    && report.months.every(row => row && /^20\d{2}-(0[1-9]|1[0-2])$/.test(row.month)
      && typeof row.partial === 'boolean' && ['income', 'expenses', 'savings', 'savings_rate'].every(key => {
        const item = row[key as keyof MonthlyActual]; return item === null || typeof item === 'number' && Number.isFinite(item)
      }))
}
