import { useEffect, useMemo, useState, type FormEvent } from 'react'
import { ArrowLeft, Check, LoaderCircle, MapPin, RefreshCw, Search, Sparkles, Store } from 'lucide-react'
import './merchant-mappings.css'

type Mapping = { flow: string; match_type: string; match_value: string; comercio_raw?: string; comercio_depurado: string; categoria: string; subcategoria?: string }
type Candidate = { candidate_id: string; flow: string; comercio_raw: string; comercio_normalizado?: string; appearance_count?: number; status?: string; suggested_match_type?: string; suggested_match_value?: string; suggested_comercio_depurado?: string; suggested_categoria?: string; suggested_subcategoria?: string; confidence?: number; suggestion_source?: string; rationale?: string }
type ExistingSuggestion = { flow: string; match_type: string; match_value: string; merchant: string; category: string; subcategory: string; confidence: number; margin: number; source: string } | null
type PlaceSuggestion = { place_id?: string; merchant: string; primary_type?: string; address?: string; category?: string | null; source: string }
type Options = { mappings: Mapping[]; merchants: string[]; categories: string[]; subcategories: Record<string, string[]>; flows: Array<{ value: string; label: string }>; places_enabled: boolean }

const emptyOptions: Options = { mappings: [], merchants: [], categories: [], subcategories: {}, flows: [], places_enabled: false }

export function MerchantMappings({ authenticated, onLogin, api, notify, onBack }: {
  authenticated: boolean
  onLogin: () => void
  api: (path: string, method?: string, body?: unknown) => Promise<unknown>
  notify: (message: string) => void
  onBack: () => void
}) {
  const [options, setOptions] = useState<Options>(emptyOptions)
  const [candidates, setCandidates] = useState<Candidate[]>([])
  const [selectedId, setSelectedId] = useState('')
  const [flow, setFlow] = useState('')
  const [raw, setRaw] = useState('')
  const [matchType, setMatchType] = useState('contains')
  const [matchValue, setMatchValue] = useState('')
  const [merchant, setMerchant] = useState('')
  const [category, setCategory] = useState('')
  const [subcategory, setSubcategory] = useState('')
  const [source, setSource] = useState('manual')
  const [existing, setExisting] = useState<ExistingSuggestion>(null)
  const [places, setPlaces] = useState<PlaceSuggestion[]>([])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [search, setSearch] = useState('')

  const reload = async () => {
    setLoading(true)
    setError('')
    try {
      const [loadedOptions, loadedCandidates] = await Promise.all([
        api('/v1/finance/merchant-mapping/options') as Promise<Options>,
        api('/v1/finance/merchant-mapping/candidates') as Promise<Candidate[]>,
      ])
      setOptions(loadedOptions || emptyOptions)
      setCandidates(Array.isArray(loadedCandidates) ? loadedCandidates : [])
      setFlow(current => current || loadedOptions?.flows?.[0]?.value || '')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'No pudimos cargar la cola de comercios.')
    } finally { setLoading(false) }
  }

  useEffect(() => { if (authenticated) void reload() }, [authenticated])

  const filteredCandidates = useMemo(() => {
    const query = search.trim().toLocaleLowerCase('es-AR')
    return candidates.filter(item => !query || `${item.comercio_raw} ${item.flow} ${item.suggested_comercio_depurado || ''}`.toLocaleLowerCase('es-AR').includes(query))
  }, [candidates, search])
  const categorySubcategories = options.subcategories[category] || []

  const chooseCandidate = (candidate: Candidate) => {
    setSelectedId(candidate.candidate_id)
    setFlow(candidate.flow)
    setRaw(candidate.comercio_raw || '')
    setMatchType(candidate.suggested_match_type || 'contains')
    setMatchValue(candidate.suggested_match_value || candidate.comercio_normalizado || candidate.comercio_raw || '')
    setMerchant(candidate.suggested_comercio_depurado || '')
    setCategory(candidate.suggested_categoria || '')
    setSubcategory(candidate.suggested_subcategoria || '')
    setSource(candidate.suggestion_source === 'google_places' ? 'google_places' : candidate.suggested_comercio_depurado ? 'existing_mapping' : 'manual')
    setExisting(null)
    setPlaces([])
  }

  const requestSuggestions = async (includePlaces = false) => {
    if (!raw.trim() || !flow) { setError('Elegí el origen y escribí el nombre del comercio para buscar sugerencias.'); return }
    setLoading(true); setError(''); setPlaces([])
    try {
      const result = await api('/v1/finance/merchant-mapping/suggest', 'POST', { flow, raw_merchant: raw, include_places: includePlaces }) as { existing_match?: ExistingSuggestion; places?: PlaceSuggestion[] }
      setExisting(result.existing_match || null)
      setPlaces(result.places || [])
      if (!result.existing_match && !result.places?.length) notify('No encontramos coincidencias claras; podés completar el mapeo manualmente.')
    } catch (cause) { setError(cause instanceof Error ? cause.message : 'No pudimos buscar sugerencias.') }
    finally { setLoading(false) }
  }

  const useExisting = (suggestion: NonNullable<ExistingSuggestion>) => {
    setMatchType(suggestion.match_type)
    setMatchValue(suggestion.match_value || raw)
    setMerchant(suggestion.merchant)
    setCategory(suggestion.category)
    setSubcategory(suggestion.subcategory || '')
    setSource('existing_mapping')
  }

  const usePlace = (place: PlaceSuggestion) => {
    setMerchant(place.merchant)
    if (place.category) setCategory(place.category)
    setSource('google_places')
  }

  const save = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault(); setSaving(true); setError('')
    try {
      await api('/v1/finance/merchant-mapping/save', 'POST', {
        candidate_id: selectedId || undefined, flow, match_type: matchType,
        raw_merchant: raw, match_value: matchValue, merchant, category, subcategory: subcategory || null, source,
      })
      notify(`Mapeo guardado: ${merchant} · ${category}${subcategory ? ` / ${subcategory}` : ''}`)
      setSelectedId(''); setRaw(''); setMatchValue(''); setMerchant(''); setCategory(''); setSubcategory(''); setExisting(null); setPlaces([])
      await reload()
    } catch (cause) { setError(cause instanceof Error ? cause.message : 'No pudimos guardar el mapeo.') }
    finally { setSaving(false) }
  }

  if (!authenticated) return <section className="mapping-gate"><div className="mapping-gate-icon"><Store /></div><span className="mapping-eyebrow">ESPACIO PERSONAL · ETL</span><h1>Revisá cómo se clasifican tus comercios.</h1><p>Iniciá sesión con la cuenta dueña de los datos para revisar sugerencias y guardar reglas de mapeo.</p><button className="mapping-primary" onClick={onLogin}>Ingresar <ArrowLeft className="mapping-arrow-right" size={16} /></button></section>

  return <div className="mapping-page">
    <div className="mapping-heading"><button className="mapping-back" onClick={onBack}><ArrowLeft size={16} /> Mi espacio</button><div className="mapping-title-row"><div><span className="mapping-eyebrow">REVISIÓN DE DATOS · ETL</span><h1>Comercios por clasificar</h1><p>Usá las coincidencias sugeridas como punto de partida y confirmá cada regla antes de guardarla.</p></div><button className="mapping-refresh" onClick={() => void reload()} disabled={loading}><RefreshCw size={15} className={loading ? 'mapping-spinning' : ''} /> Actualizar</button></div></div>
    <div className="mapping-statusline"><span className="mapping-status-dot" />{candidates.length} pendientes de revisión <span>·</span> {options.mappings.length} reglas disponibles <span className="mapping-owner-note">Solo visible para la cuenta dueña</span></div>
    {error && <div className="mapping-error" role="alert">{error}</div>}

    <div className="mapping-layout">
      <aside className="mapping-queue">
        <div className="mapping-queue-head"><div><h2>Por revisar</h2><span>Priorizados por frecuencia</span></div><span className="mapping-count">{candidates.length}</span></div>
        <label className="mapping-search"><Search size={15} /><input value={search} onChange={event => setSearch(event.target.value)} placeholder="Buscar comercio" /></label>
        {loading && !candidates.length ? <div className="mapping-empty"><LoaderCircle className="mapping-spinning" /> Cargando pendientes…</div> : filteredCandidates.length ? <div className="mapping-candidate-list">{filteredCandidates.map(candidate => <button key={candidate.candidate_id} className={`mapping-candidate ${selectedId === candidate.candidate_id ? 'selected' : ''}`} onClick={() => chooseCandidate(candidate)}><span className="mapping-candidate-icon"><Store size={15} /></span><span className="mapping-candidate-info"><b>{candidate.comercio_raw || 'Comercio sin nombre'}</b><small>{options.flows.find(item => item.value === candidate.flow)?.label || candidate.flow}</small><small>{candidate.suggested_comercio_depurado ? `Sugerencia: ${candidate.suggested_comercio_depurado}` : 'Sin sugerencia automática'}</small></span><span className="mapping-frequency">{candidate.confidence != null ? `${candidate.confidence}%` : ''}<br />×{candidate.appearance_count || 1}</span></button>)}</div> : <div className="mapping-empty"><Check size={17} />No hay pendientes para mostrar.</div>}
      </aside>

      <section className="mapping-editor">
        <div className="mapping-editor-head"><div><span className="mapping-eyebrow">REGLA DE CLASIFICACIÓN</span><h2>{selectedId ? 'Revisar sugerencia' : 'Crear o corregir mapeo'}</h2></div><span className="mapping-safe-badge"><Check size={13} /> Se guarda al confirmar</span></div>
        <form onSubmit={event => void save(event)}>
          <div className="mapping-fields two-columns">
            <label>Origen de los datos<select required value={flow} onChange={event => setFlow(event.target.value)}><option value="">Elegir origen…</option>{options.flows.map(item => <option key={item.value} value={item.value}>{item.label}</option>)}</select></label>
            <label>Tipo de coincidencia<select value={matchType} onChange={event => setMatchType(event.target.value)}><option value="contains">Contiene</option><option value="exact">Exacta</option><option value="fuzzy:80">Similaridad ≥ 80%</option><option value="regex">Expresión regular</option></select></label>
          </div>
          <div className="mapping-fields two-columns">
            <label>Nombre original del comercio<input required value={raw} onChange={event => { setRaw(event.target.value); if (!selectedId) setMatchValue(event.target.value) }} placeholder="Ej.: OPESSA NORDELTA" /></label>
            <label>Texto que debe coincidir<input required value={matchValue} onChange={event => setMatchValue(event.target.value)} placeholder={matchType === 'regex' ? '^OPESSA.*' : 'Se completa con el original'} /></label>
          </div>
          <div className="mapping-fields three-columns">
            <label>Comercio depurado<input required list="mapping-merchants" value={merchant} onChange={event => setMerchant(event.target.value)} placeholder="Elegí uno o escribí otro" /><datalist id="mapping-merchants">{options.merchants.map(value => <option key={value} value={value} />)}</datalist></label>
            <label>Categoría<input required list="mapping-categories" value={category} onChange={event => { setCategory(event.target.value); setSubcategory('') }} placeholder="Elegí o escribí una" /><datalist id="mapping-categories">{options.categories.map(value => <option key={value} value={value} />)}</datalist></label>
            <label>Subcategoría <span className="mapping-optional">opcional</span><input list="mapping-subcategories" value={subcategory} onChange={event => setSubcategory(event.target.value)} placeholder="Podés dejarla vacía" /><datalist id="mapping-subcategories">{categorySubcategories.map(value => <option key={value} value={value} />)}</datalist></label>
          </div>

          <div className="mapping-suggestions"><div className="mapping-suggestion-heading"><div><Sparkles size={16} /><div><b>Sugerencias para ahorrar tiempo</b><small>Siempre revisás el resultado antes de guardarlo.</small></div></div><button type="button" onClick={() => void requestSuggestions(false)} disabled={loading || !raw.trim()}><Search size={14} /> Buscar coincidencias</button></div>
            {existing && <button type="button" className="mapping-suggestion-card" onClick={() => useExisting(existing)}><span className="mapping-match-icon"><Sparkles size={15} /></span><span><b>{existing.merchant}</b><small>{existing.category}{existing.subcategory ? ` · ${existing.subcategory}` : ''} · misma fuente</small></span><strong>{existing.confidence}%</strong><span className="mapping-use">Usar</span></button>}
            {options.places_enabled && <div className="mapping-place-action"><span><MapPin size={14} /> Buscar nombre real y tipo en Google Maps</span><button type="button" onClick={() => void requestSuggestions(true)} disabled={loading || !raw.trim()}>{loading ? 'Buscando…' : 'Buscar en Maps'}</button></div>}
            {places.map(place => <button type="button" key={place.place_id || place.merchant} className="mapping-suggestion-card" onClick={() => usePlace(place)}><span className="mapping-match-icon place"><MapPin size={15} /></span><span><b>{place.merchant}</b><small>{[place.category, place.primary_type, place.address].filter(Boolean).join(' · ')}</small></span><span className="mapping-use">Usar</span></button>)}
          </div>
          <div className="mapping-form-footer"><p>La regla impacta el proceso de mapeo ETL. La categoría es obligatoria; la subcategoría no.</p><button type="submit" className="mapping-primary" disabled={saving || !raw.trim() || !merchant.trim() || !category.trim()}>{saving ? <><LoaderCircle size={16} className="mapping-spinning" /> Guardando…</> : <><Check size={16} /> Confirmar mapeo</>}</button></div>
        </form>
      </section>
    </div>
  </div>
}
