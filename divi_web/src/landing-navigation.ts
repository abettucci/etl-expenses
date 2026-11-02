export const personalFinanceAnchor = 'finanzas-personales'
export const sharedExpensesAnchor = 'gastos-compartidos'

export function landingArea(pathname: string, hash: string): 'welcome' | 'etl' | null {
  const path = pathname.replace(/\/$/, '') || '/'
  if (path === '/etl') return 'etl' // Preserve existing bookmarks.
  if (path !== '/') return null
  return hash === `#${personalFinanceAnchor}` || hash === '#etl-process' ? 'etl' : 'welcome'
}
