import type { MouseEvent } from 'react'
import './product-navigation.css'

export function ProductNavigation({ area, authenticated, onNavigate, onLogin, onLogout }: {
  area: 'shared' | 'personal' | 'workspace' | 'balance'
  authenticated: boolean
  onNavigate: (destination: 'welcome' | 'etl' | 'home' | 'balance') => void
  onLogin: () => void
  onLogout: () => void
}) {
  function follow(event: MouseEvent<HTMLAnchorElement>, destination: 'welcome' | 'etl' | 'home' | 'balance') {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    event.preventDefault()
    onNavigate(destination)
  }
  return <header className="product-nav">
    <a className="product-brand" href="/" onClick={event => follow(event, 'welcome')} aria-label="Split, volver al inicio">split<span>compartí · organizá · entendé</span></a>
    <nav aria-label="Secciones del producto">
      <a href="/#gastos-compartidos" aria-current={area === 'shared' ? 'location' : undefined} onClick={event => follow(event, 'welcome')}>Gastos compartidos</a>
      <a href="/#finanzas-personales" aria-current={area === 'personal' ? 'location' : undefined} onClick={event => follow(event, 'etl')}>Finanzas personales</a>
      <a href="/balance" aria-current={area === 'balance' ? 'page' : undefined} onClick={event => follow(event, 'balance')}>Balance mensual</a>
      <a href="/app" aria-current={area === 'workspace' ? 'page' : undefined} onClick={event => follow(event, 'home')}>Mi espacio <span aria-hidden="true">↗</span></a>
    </nav>
    <button className="product-session" onClick={authenticated ? onLogout : onLogin}>{authenticated ? 'Salir' : 'Ingresar'}</button>
  </header>
}
