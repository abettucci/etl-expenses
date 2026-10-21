import { lazy, Suspense } from 'react'
import { ArrowRight, Calculator, Layers3, ReceiptText } from 'lucide-react'
import { Hero } from './Hero'
import { Features } from './Features'
import { HowItWorks } from './HowItWorks'
import { Link, NavigationProvider } from './navigation'
import './classic.css'

const Splitter = lazy(() => import('./Splitter'))

export function TicketSplitter({ page, authenticated, onLogin, onNavigate }: {
  page: 'landing' | 'calculator'
  authenticated: boolean
  onLogin: () => void
  onNavigate: (destination: string) => void
}) {
  return <div id="ticket-splitter"><NavigationProvider navigate={onNavigate}>
    {page === 'calculator' ? <Suspense fallback={<p className="p-8 text-center" role="status">Abriendo tus divisiones…</p>}><Splitter /></Suspense> : <>
      <header className="classic-nav">
        <Link to="/" className="classic-logo"><ReceiptText aria-hidden="true" /><span>split<span className="classic-logo-detail"> / Ticket Splitter</span></span></Link>
        <nav aria-label="Espacios de la aplicación">
          <Link to="/dividir" className="classic-nav-divide">Dividir gastos</Link>
          <Link to="/app" className="classic-nav-space">Mi espacio <ArrowRight aria-hidden="true" /></Link>
          {!authenticated && <button onClick={onLogin} className="classic-nav-login">Ingresar</button>}
        </nav>
      </header>
      <Hero />
      <section className="classic-spaces" aria-labelledby="classic-spaces-title">
        <div className="container">
          <p className="classic-eyebrow">DOS FORMAS DE ORDENAR LA PLATA</p>
          <h2 id="classic-spaces-title">La cuenta de hoy.<br /><span>Y el panorama de todos los días.</span></h2>
          <div className="classic-space-grid">
            <article><Calculator aria-hidden="true" /><span className="classic-space-label">TICKET SPLITTER CLÁSICO</span><h3>Dividí una cuenta.</h3><p>Agregá personas y gastos, elegí quién pagó y compartí las transferencias. Sin registro, con guardado en este navegador.</p><Link to="/dividir">Empezar a dividir <ArrowRight aria-hidden="true" /></Link></article>
            <article><Layers3 aria-hidden="true" /><span className="classic-space-label">MI ESPACIO</span><h3>Seguí todas tus cuentas.</h3><p>Entrá a tus grupos con cuenta, finanzas personales, actividad, panorama de gastos y regalos compartidos.</p><Link to="/app">Abrir mi espacio <ArrowRight aria-hidden="true" /></Link></article>
          </div>
        </div>
      </section>
      <Features />
      <HowItWorks />
      <section className="classic-closing"><div className="container"><span className="classic-eyebrow">UNA CUENTA MENOS EN LA CABEZA</span><h2>Compartir el plan.<br />Hacer simples las cuentas.</h2><p>La calculadora de Ticket Splitter es gratuita. En Mi espacio también encontrás tus grupos, tu panorama y el plan Premium opcional.</p><div><Link to="/dividir">Empezar a dividir <ArrowRight aria-hidden="true" /></Link><Link to="/app">Ir a Mi espacio</Link></div></div></section>
      <footer className="classic-footer"><span>split / Ticket Splitter</span><nav aria-label="Navegación del pie"><Link to="/dividir">División rápida</Link><Link to="/app">Mi espacio</Link><a href="#how-it-works">Cómo funciona</a></nav></footer>
    </>}
  </NavigationProvider></div>
}
