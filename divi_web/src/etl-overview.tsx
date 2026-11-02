import { ArrowDownRight, ArrowRight, AudioLines, BellRing, Bot, Boxes, Braces, Database, FileImage, Mail, MessageCircle, ReceiptText, Search, Sparkles, WalletCards } from 'lucide-react'
import './etl-overview.css'

export function EtlOverview({ onNavigate, onHome, embedded = false }: { onNavigate: (destination: 'panorama' | 'balance') => void; onHome: () => void; embedded?: boolean }) {
  return <div className="etl-page">
    <header className="etl-topline">
      <div className="etl-wordmark"><span className="etl-wave">∿</span><span>ETL<span> expenses</span></span></div>
      <div className="etl-top-actions"><span className="etl-tagline">TUS MOVIMIENTOS, CON CONTEXTO</span><button onClick={() => onNavigate('balance')}>Mis finanzas</button><button onClick={onHome}>↑ Volver al inicio</button></div>
    </header>

    <section className="etl-hero" aria-labelledby="etl-title">
      <div className="etl-hero-copy">
        <span className="etl-kicker">PERSONAL FINANCE · PIPELINE</span>
        <h1 id="etl-title">Cada gasto,<br />desde su origen<br /><em>hasta una decisión.</em></h1>
        <p>ETL Expenses reúne movimientos de bancos, billeteras, tickets y cargas manuales. Los normaliza para que puedas consultarlos por conversación, analizarlos en BigQuery y recibir alertas cuando algo se desvía.</p>
        <div className="etl-hero-actions">
          <button className="etl-button-primary" onClick={() => onNavigate('panorama')}>Ver mi panorama <ArrowRight size={16} /></button>
          <a className="etl-button-quiet" href="#etl-process">Ver etapas del ETL <ArrowRight size={16} /></a>
        </div>
        <div className="etl-trustline"><span><i /> Gmail y bancos</span><span><i /> Mercado Pago</span><span><i /> Telegram</span></div>
      </div>

      <div className="etl-flow-card" aria-label="Fuentes financieras conectadas con BigQuery, consultas y alertas">
        <div className="etl-flow-heading"><div><span className="etl-kicker">UN SOLO FLUJO</span><h2>De movimiento a claridad.</h2></div><span className="etl-live-mark"><i /> ETL</span></div>
        <div className="etl-flow-canvas">
          <div className="etl-source-stack">
            <div className="etl-source-node"><span className="etl-node-icon gmail"><Mail size={17} /></span><div><b>Gmail · bancos</b><small>Débitos y transferencias</small></div><span className="etl-node-dot" /></div>
            <div className="etl-source-node"><span className="etl-node-icon wallet"><WalletCards size={17} /></span><div><b>Mercado Pago</b><small>Pagos y transferencias</small></div><span className="etl-node-dot" /></div>
            <div className="etl-source-node"><span className="etl-node-icon ticket"><ReceiptText size={17} /></span><div><b>Tickets</b><small>PDF, imagen y supermercado</small></div><span className="etl-node-dot" /></div>
            <div className="etl-source-node"><span className="etl-node-icon telegram"><MessageCircle size={17} /></span><div><b>Telegram</b><small>Texto, voz y foto</small></div><span className="etl-node-dot" /></div>
          </div>
          <div className="etl-converge"><span /><span /><span /><span /><div className="etl-core"><span>∿</span><small>ETL</small></div><i /></div>
          <div className="etl-output-stack">
            <div className="etl-output-main"><div className="etl-output-brand"><span>∿</span> BigQuery</div><b>Un modelo para consultar</b><small>Fechas, comercios, categorías y montos con una estructura común.</small><div className="etl-query-chip"><Braces size={14} /> gastos_totales <span>read</span></div></div>
            <div className="etl-output-mini"><span className="etl-mini-icon"><Bot size={15} /></span><div><b>Conversación</b><small>Preguntá por tus gastos</small></div><ArrowDownRight size={15} /></div>
            <div className="etl-output-mini"><span className="etl-mini-icon alert"><BellRing size={15} /></span><div><b>Alertas</b><small>Variaciones por período</small></div><ArrowDownRight size={15} /></div>
          </div>
        </div>
        <div className="etl-flow-caption"><span>INGESTA</span><i /><span>TRANSFORMACIÓN</span><i /><span>ANÁLISIS</span></div>
      </div>
    </section>

    <section id="etl-process" className="etl-process" aria-labelledby="etl-process-title">
      <div className="etl-section-heading"><div><span className="etl-kicker">EL RECORRIDO</span><h2 id="etl-process-title">Una capa común para fuentes distintas.</h2></div><p>El ETL convierte comprobantes y movimientos de formatos distintos en información comparable.</p></div>
      <div className="etl-step-grid">
        <article><span className="etl-step-index">01</span><span className="etl-step-icon"><Mail size={20} /></span><h3>Ingesta</h3><p>Recibe avisos bancarios, reportes de billeteras, tickets y registros que mandás al bot.</p><small>GMAIL · S3 · TELEGRAM</small></article>
        <article><span className="etl-step-index">02</span><span className="etl-step-icon"><FileImage size={20} /></span><h3>Extracción</h3><p>Lee datos de mails, PDF e imágenes; mantiene los datos de origen para poder rastrearlos.</p><small>DOCUMENTOS · OCR · MENSAJES</small></article>
        <article><span className="etl-step-index">03</span><span className="etl-step-icon"><Boxes size={20} /></span><h3>Normalización</h3><p>Unifica fechas, divisas, comercios y categorías. Los mapeos pueden revisarse antes de corregirlos.</p><small>REGLAS · TAXONOMÍA · FX</small></article>
        <article><span className="etl-step-index">04</span><span className="etl-step-icon"><Database size={20} /></span><h3>BigQuery</h3><p>Deja los movimientos listos para consultas conversacionales, análisis propios y alertas.</p><small>PRD · SQL · HISTORIAL</small></article>
      </div>
    </section>

    <section className="etl-intelligence">
      <div className="etl-conversation-panel">
        <div className="etl-panel-label"><span><MessageCircle size={14} /> CONSULTAS CONVERSACIONALES</span><span>TELEGRAM</span></div>
        <h2>Hablale a tus datos<br />como hablás vos.</h2>
        <p>El bot traduce preguntas cotidianas a consultas sobre el modelo de gastos y devuelve resultados en el chat.</p>
        <div className="etl-chat-example"><span className="etl-chat-avatar">T</span><div><small>VOS · EJEMPLO</small><p>¿Cuánto gasté en comida el mes pasado?</p></div></div>
        <div className="etl-chat-answer"><Sparkles size={15} /><div><small>ETL EXPENSES</small><p>Busca el período y agrupa los movimientos por categoría.</p></div></div>
        <div className="etl-chat-footer"><AudioLines size={14} /> También podés registrar un gasto por texto, voz o foto.</div>
      </div>
      <div className="etl-alert-panel">
        <div className="etl-panel-label"><span><BellRing size={14} /> ALERTAS DE DESVÍOS</span><span>REGLAS CONFIGURABLES</span></div>
        <h2>Detectá cambios<br />antes de perderlos de vista.</h2>
        <p>Compara montos y porcentajes entre ventanas temporales para comercio, categoría y subcategoría.</p>
        <div className="etl-alert-chart" aria-label="Gráfico ilustrativo de comparación de períodos">
          <div className="etl-chart-legend"><span><i /> período previo</span><span><i /> período actual</span></div>
          <div className="etl-chart-columns"><div><i style={{ height: '44%' }} /><i style={{ height: '59%' }} /><i style={{ height: '38%' }} /><i style={{ height: '53%' }} /></div><div><i style={{ height: '51%' }} /><i style={{ height: '78%' }} /><i style={{ height: '47%' }} /><i style={{ height: '86%' }} /></div></div>
          <div className="etl-chart-foot"><span>Comercio</span><span>Categoría</span><span>Subcategoría</span><span>Ventanas</span></div>
        </div>
        <div className="etl-period-pills"><span>1 semana</span><span>2 semanas</span><span>1 mes</span><span>Comparación histórica</span></div>
      </div>
    </section>

    <section className="etl-data-cta">
      <div className="etl-data-icon"><Search size={23} /></div>
      <div><span className="etl-kicker">TUS DATOS, TU CRITERIO</span><h2>Consultas a demanda. Reglas que podés revisar.</h2><p>Explorá el panorama personal o ajustá las reglas que clasifican tus comercios.</p></div>
      <div className="etl-data-actions"><button onClick={() => onNavigate('panorama')}>Abrir panorama <ArrowRight size={16} /></button></div>
    </section>
    <footer className="etl-footer"><span>ETL EXPENSES · PERSONAL FINANCE</span><span>Fuentes → BigQuery → Conversación y alertas</span></footer>
  </div>
}
