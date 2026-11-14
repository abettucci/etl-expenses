import { useRef, type MouseEvent } from 'react'
import { House, ChartNoAxesCombined, Users, Plug, Sparkles } from 'lucide-react'
import './product-navigation.css'
type Destination = 'welcome' | 'etl' | 'home' | 'insights' | 'balance' | 'profile' | 'groups' | 'connections' | 'settings' | 'splitter'
export function ProductNavigation({ area, authenticated, onNavigate, onLogin, onLogout, onAssistant }: {
  area: string; authenticated: boolean; onNavigate: (destination: Destination) => void; onLogin: () => void; onLogout: () => void; onAssistant: () => void
}) {
  const menu = useRef<HTMLDetailsElement>(null)
  const mobileMenu = useRef<HTMLDetailsElement>(null)
  function follow(event: MouseEvent<HTMLAnchorElement>, destination: Destination) {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    event.preventDefault(); if (menu.current) menu.current.open = false; if (mobileMenu.current) mobileMenu.current.open = false; onNavigate(destination)
  }
  const primary = [
    { to: 'home', href: '/inicio', label: 'Inicio', icon: House, active: area === 'home' },
    { to: 'insights', href: '/finanzas/analisis', label: 'Mis finanzas', icon: ChartNoAxesCombined, active: area === 'balance' || area === 'personal' },
    { to: 'groups', href: '/grupos', label: 'Grupos', icon: Users, active: area === 'groups' || area === 'shared' },
  ] as const
  return <header className={`product-nav ${authenticated ? 'workspace-nav' : 'public-nav'}`}>
    <a className="product-brand" href={authenticated ? '/inicio' : '/'} onClick={event => follow(event, authenticated ? 'home' : 'welcome')}>split<span>tu dinero, en contexto</span></a>
    {authenticated ? <>
      <span className="nav-eyebrow">TU ESPACIO</span>
      <nav className="primary-nav" aria-label="Navegación principal">{primary.map(item => <a key={item.to} href={item.href} aria-current={item.active ? 'page' : undefined} onClick={event => follow(event, item.to)}><item.icon size={18} aria-hidden="true" />{item.label}</a>)}</nav>
      <nav className="secondary-nav" aria-label="Herramientas"><a href="/conexiones" aria-current={area === 'connections' ? 'page' : undefined} onClick={event => follow(event, 'connections')}><Plug size={18} aria-hidden="true" />Conexiones</a><button onClick={onAssistant}><Sparkles size={18} aria-hidden="true" />Asistente</button><a href="/dividir" onClick={event => follow(event, 'splitter')}>División rápida ↗</a></nav>
      <details className="account-menu" ref={menu}><summary>Tu cuenta <span aria-hidden="true">⌄</span></summary><div><a href="/perfil" onClick={event => follow(event, 'profile')}>Mi perfil</a><a href="/ajustes" onClick={event => follow(event, 'settings')}>Ajustes</a><a href="/" onClick={event => follow(event, 'welcome')}>Ver portada</a><button onClick={onLogout}>Cerrar sesión</button></div></details>
      <details className="mobile-more" ref={mobileMenu}><summary>Más</summary><div><a href="/conexiones" onClick={event => follow(event, 'connections')}>Conexiones</a><button onClick={() => { if (mobileMenu.current) mobileMenu.current.open = false; onAssistant() }}>Asistente</button><a href="/perfil" onClick={event => follow(event, 'profile')}>Mi perfil</a><a href="/ajustes" onClick={event => follow(event, 'settings')}>Ajustes</a><a href="/dividir" onClick={event => follow(event, 'splitter')}>División rápida</a><a href="/" onClick={event => follow(event, 'welcome')}>Ver portada</a><button onClick={onLogout}>Cerrar sesión</button></div></details>
    </> : <><nav aria-label="Producto"><a href="/como-funciona" onClick={event => follow(event, 'etl')}>Cómo funciona</a><a href="/dividir" onClick={event => follow(event, 'splitter')}>Dividir sin registro</a></nav><button className="product-session" onClick={onLogin}>Ingresar →</button></>}
  </header>
}
