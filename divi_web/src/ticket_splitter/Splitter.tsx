import { useEffect, useMemo, useState, type InputHTMLAttributes } from "react";
import { Link } from "./navigation";
import {
  ArrowLeft,
  Check,
  ChevronRight,
  Clipboard,
  Copy,
  Download,
  Landmark,
  Minus,
  PencilLine,
  Plus,
  ReceiptText,
  Share2,
  Trash2,
  Undo2,
  UsersRound,
  WalletCards,
} from "lucide-react";
import { Button } from "./ui";
import {
  amountToCents,
  calculateDivision,
  centsToInput,
  cloneDivision,
  createDivision,
  equalShares,
  formatARS,
  newId,
  nowIso,
  resultText,
  STORAGE_KEY,
  type Expense,
  type Participant,
  type Split,
  type SplitMode,
} from "./model";

const loadDivisions = (): Split[] => {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    const parsed = saved ? JSON.parse(saved) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const participantName = (division: Split, participantId: string) =>
  division.participants.find((participant) => participant.id === participantId)?.name ?? "Persona eliminada";

const newExpense = (participants: Participant[]): Expense => ({
  id: newId(),
  description: "Nuevo gasto",
  amountCents: 0,
  paidBy: [],
  participantIds: participants.map((participant) => participant.id),
  splitMode: "equal",
  exactShares: {},
});

const EmptyState = ({ onCreate }: { onCreate: () => void }) => (
  <section className="relative overflow-hidden rounded-3xl bg-gradient-to-br from-[#0088cc] via-[#0077b5] to-[#005a87] px-6 py-12 text-white shadow-2xl shadow-[#0077b5]/25 sm:px-12 sm:py-16">
    <div className="absolute -right-16 -top-20 h-64 w-64 rounded-full bg-white/15 blur-3xl" />
    <div className="absolute -bottom-24 left-16 h-52 w-52 rounded-full bg-cyan-300/20 blur-3xl" />
    <div className="relative max-w-xl">
      <p className="mb-4 inline-flex items-center gap-2 rounded-full border border-white/15 bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-[#7dd3fc]">
        <WalletCards className="h-3.5 w-3.5" /> Ticket Splitter
      </p>
      <h2 className="text-4xl font-bold leading-tight tracking-tight sm:text-5xl">Dividí los gastos de tu grupo.</h2>
      <p className="mt-5 max-w-md text-base leading-relaxed text-slate-300">
        La calculadora de siempre para hacer cuentas rápidas, sin registro y desde cualquier dispositivo.
      </p>
      <Button onClick={onCreate} className="mt-8 h-12 rounded-full bg-white px-6 text-base font-bold text-[#0088cc] hover:bg-cyan-50">
        <Plus className="h-5 w-5" /> Crear primera división
      </Button>
    </div>
  </section>
);

export default function Splitter() {
  const [divisions, setDivisions] = useState<Split[]>(loadDivisions);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [newParticipantName, setNewParticipantName] = useState("");
  const [storageError, setStorageError] = useState(false);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(divisions));
      setStorageError(false);
    } catch {
      setStorageError(true);
    }
  }, [divisions]);

  const activeDivision = divisions.find((division) => division.id === activeId) ?? null;
  const calculation = useMemo(
    () => (activeDivision ? calculateDivision(activeDivision) : null),
    [activeDivision],
  );

  const replaceDivision = (nextDivision: Split) => {
    setDivisions((current) => current.map((division) => (division.id === nextDivision.id ? nextDivision : division)));
  };

  const updateActive = (updater: (division: Split) => Split, resetsPayments = true) => {
    if (!activeDivision) return;
    if (resetsPayments && activeDivision.payments.length > 0) {
      const confirmed = window.confirm(
        "Este cambio recalculará las transferencias y reiniciará los pagos marcados. ¿Querés continuar?",
      );
      if (!confirmed) return;
    }
    const updated = updater(activeDivision);
    replaceDivision({
      ...updated,
      payments: resetsPayments ? [] : updated.payments,
      updatedAt: nowIso(),
    });
  };

  const createNewDivision = () => {
    const division = createDivision();
    setDivisions((current) => [division, ...current]);
    setActiveId(division.id);
  };

  const removeDivision = (division: Split) => {
    if (!window.confirm(`¿Borrar “${division.name}”? Esta acción no se puede deshacer.`)) return;
    setDivisions((current) => current.filter((item) => item.id !== division.id));
    if (activeId === division.id) setActiveId(null);
  };

  const duplicateDivision = (division: Split) => {
    const copy = cloneDivision(division);
    setDivisions((current) => [copy, ...current]);
    setActiveId(copy.id);
  };

  const exportDivision = (division: Split) => {
    const blob = new Blob([JSON.stringify(division, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${division.name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "division"}.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  };

  const addParticipant = () => {
    const name = newParticipantName.trim();
    if (!name || !activeDivision) return;
    updateActive((division) => ({
      ...division,
      participants: [...division.participants, { id: newId(), name }],
    }));
    setNewParticipantName("");
  };

  const deleteParticipant = (participantId: string) => {
    if (!activeDivision) return;
    const participant = participantName(activeDivision, participantId);
    if (!window.confirm(`¿Quitar a ${participant}? También se eliminarán sus asignaciones en los gastos.`)) return;
    updateActive((division) => ({
      ...division,
      participants: division.participants.filter((item) => item.id !== participantId),
      expenses: division.expenses.map((expense) => ({
        ...expense,
        participantIds: expense.participantIds.filter((id) => id !== participantId),
        paidBy: expense.paidBy.filter((payer) => payer.participantId !== participantId),
        exactShares: Object.fromEntries(
          Object.entries(expense.exactShares).filter(([id]) => id !== participantId),
        ),
      })),
    }));
  };

  const updateExpense = (expenseId: string, updater: (expense: Expense) => Expense) => {
    updateActive((division) => ({
      ...division,
      expenses: division.expenses.map((expense) => (expense.id === expenseId ? updater(expense) : expense)),
    }));
  };

  const shareSummary = async () => {
    if (!activeDivision || !calculation || calculation.issues.length > 0) return;
    const text = resultText(activeDivision, calculation);
    try {
      if (navigator.share) {
        await navigator.share({ title: activeDivision.name, text });
      } else {
        await navigator.clipboard.writeText(text);
        window.alert("Resumen copiado al portapapeles.");
      }
    } catch (error) {
      if ((error as DOMException).name !== "AbortError") window.alert("No se pudo compartir el resumen.");
    }
  };

  if (!activeDivision) {
    return (
      <div className="min-h-screen bg-gradient-to-b from-[#edf9ff] via-slate-50 to-white text-slate-900">
        <div className="mx-auto max-w-6xl px-4 py-7 sm:px-6 lg:px-8">
          {storageError && <p role="alert" className="mb-4 rounded-xl bg-amber-100 p-4 text-sm text-amber-900">No pudimos guardar en este navegador. Exportá tus divisiones antes de cerrar esta pestaña.</p>}
          <header className="mb-10 flex items-center justify-between">
            <Link to="/" className="inline-flex items-center gap-2 text-sm font-semibold text-slate-600 transition hover:text-slate-900">
              <ArrowLeft className="h-4 w-4" /> Volver al inicio
            </Link>
            <div className="flex flex-wrap items-center gap-4"><Link to="/app" className="text-sm font-semibold text-[#0077b5]">Mi espacio →</Link><span className="hidden text-xl font-bold tracking-tight text-[#0088cc] sm:inline">Ticket Splitter</span></div>
          </header>

          <div className="mb-8 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <p className="mb-2 text-xs font-bold uppercase tracking-[0.18em] text-[#0088cc]">Sin registro · guardado en este navegador</p>
              <h1 className="text-4xl tracking-tight sm:text-5xl">Mis divisiones</h1>
            </div>
            {divisions.length > 0 && (
              <Button onClick={createNewDivision} className="h-11 rounded-full bg-[#0088cc] px-5 font-bold hover:bg-[#0077b5]">
                <Plus className="h-4 w-4" /> Nueva división
              </Button>
            )}
          </div>

          {divisions.length === 0 ? (
            <EmptyState onCreate={createNewDivision} />
          ) : (
            <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {divisions.map((division) => {
                const divisionCalculation = calculateDivision(division);
                const total = division.expenses.reduce((sum, expense) => sum + expense.amountCents, 0);
                return (
                  <article key={division.id} className="group rounded-3xl border border-[#e2e8f0] bg-white p-6 shadow-sm transition hover:-translate-y-0.5 hover:shadow-lg hover:shadow-slate-900/5">
                    <div className="mb-8 flex items-start justify-between gap-4">
                      <div className="min-w-0">
                        <p className="truncate text-2xl font-bold tracking-tight">{division.name}</p>
                        <p className="mt-1 text-sm text-slate-500">
                          {division.participants.length} {division.participants.length === 1 ? "persona" : "personas"} · {division.expenses.length} {division.expenses.length === 1 ? "gasto" : "gastos"}
                        </p>
                      </div>
                      <span className="rounded-full bg-[#e8f7ff] px-2.5 py-1 text-xs font-bold text-[#0077b5]">ARS</span>
                    </div>
                    <p className="text-3xl tracking-tight">{formatARS(total)}</p>
                    <p className="mt-1 text-sm text-slate-500">
                      {divisionCalculation.issues.length > 0
                        ? "Faltan datos por completar"
                        : divisionCalculation.settlements.length === 0
                          ? "Todo está saldado"
                          : `${divisionCalculation.settlements.length} transferencia${divisionCalculation.settlements.length === 1 ? "" : "s"} pendiente${divisionCalculation.settlements.length === 1 ? "" : "s"}`}
                    </p>
                    <div className="mt-7 flex items-center gap-2">
                      <Button onClick={() => setActiveId(division.id)} className="h-10 flex-1 rounded-full bg-[#0088cc] font-bold hover:bg-[#0077b5]">
                        Abrir <ChevronRight className="h-4 w-4" />
                      </Button>
                      <Button aria-label={`Duplicar ${division.name}`} onClick={() => duplicateDivision(division)} size="icon" variant="outline" className="rounded-full border-[#e2e8f0]">
                        <Copy className="h-4 w-4" />
                      </Button>
                      <Button aria-label={`Borrar ${division.name}`} onClick={() => removeDivision(division)} size="icon" variant="ghost" className="rounded-full text-slate-500 hover:bg-red-50 hover:text-red-600">
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </article>
                );
              })}
            </section>
          )}
        </div>
      </div>
    );
  }

  const hasPayments = activeDivision.payments.length > 0;
  const totalExpenses = activeDivision.expenses.reduce((sum, expense) => sum + expense.amountCents, 0);

  return (
    <div className="min-h-screen bg-gradient-to-b from-[#edf9ff] via-slate-50 to-white text-slate-900">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        {storageError && <p role="alert" className="mb-4 rounded-xl bg-amber-100 p-4 text-sm text-amber-900">No pudimos guardar en este navegador. Exportá esta división antes de cerrar la pestaña.</p>}
        <header className="mb-7 flex flex-wrap items-center gap-3 rounded-3xl bg-gradient-to-r from-[#0088cc] via-[#0077b5] to-[#005a87] p-3 text-white shadow-xl shadow-[#0077b5]/20 sm:p-4">
          <Button onClick={() => setActiveId(null)} variant="ghost" className="rounded-full text-white hover:bg-white/15 hover:text-white">
            <ArrowLeft className="h-4 w-4" /> Mis divisiones
          </Button>
          <div className="hidden h-5 w-px bg-white/30 sm:block" />
          <div className="flex min-w-0 flex-1 items-center gap-2">
            <PencilLine className="h-4 w-4 shrink-0 text-cyan-200" />
            <input
              aria-label="Nombre de la división"
              value={activeDivision.name}
              onChange={(event) => updateActive((division) => ({ ...division, name: event.target.value }), false)}
              onBlur={() => { if (!activeDivision.name.trim()) updateActive((division) => ({ ...division, name: "Nueva división" }), false); }}
              className="w-full min-w-0 bg-transparent text-2xl font-bold tracking-tight text-white outline-none placeholder:text-white/60 sm:text-3xl"
            />
          </div>
          <div className="ml-auto flex flex-wrap items-center gap-2"><Link to="/app" className="px-3 text-sm font-semibold text-white">Mi espacio →</Link>
            <Button onClick={() => exportDivision(activeDivision)} variant="outline" className="rounded-full border-white/30 bg-white/10 text-white hover:bg-white/20 hover:text-white">
              <Download className="h-4 w-4" /><span className="hidden sm:inline">Exportar</span>
            </Button>
            <Button onClick={shareSummary} disabled={!calculation || calculation.issues.length > 0} className="rounded-full bg-white font-bold text-[#0088cc] hover:bg-cyan-50">
              <Share2 className="h-4 w-4" /><span className="hidden sm:inline">Compartir</span>
            </Button>
          </div>
        </header>

        <div className="mb-7 grid gap-3 sm:grid-cols-3">
          <div className="rounded-2xl border border-[#e2e8f0] bg-white px-4 py-3">
            <p className="text-xs font-bold uppercase tracking-[0.12em] text-slate-500">Total cargado</p>
            <p className="mt-1 text-2xl">{formatARS(totalExpenses)}</p>
          </div>
          <div className="rounded-2xl border border-[#e2e8f0] bg-white px-4 py-3">
            <p className="text-xs font-bold uppercase tracking-[0.12em] text-slate-500">Personas</p>
            <p className="mt-1 text-2xl">{activeDivision.participants.length}</p>
          </div>
          <div className="rounded-2xl border border-[#e2e8f0] bg-white px-4 py-3">
            <p className="text-xs font-bold uppercase tracking-[0.12em] text-slate-500">Estado</p>
            <p className="mt-1 text-2xl">{hasPayments ? `${activeDivision.payments.length} pago${activeDivision.payments.length === 1 ? "" : "s"}` : "Al día"}</p>
          </div>
        </div>

        <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_380px]">
          <div className="space-y-6">
            <section className="rounded-3xl border border-[#e2e8f0] bg-white p-5 shadow-sm sm:p-7">
              <div className="mb-5 flex items-center justify-between gap-4">
                <div>
                  <p className="text-xs font-bold uppercase tracking-[0.14em] text-[#0077b5]">Paso 1</p>
                  <h2 className="mt-1 text-2xl tracking-tight">¿Quiénes están?</h2>
                </div>
                <UsersRound className="h-6 w-6 text-[#00a8e8]" />
              </div>
              <div className="flex flex-wrap gap-2">
                {activeDivision.participants.map((participant) => (
                  <div key={participant.id} className="inline-flex items-center gap-2 rounded-full bg-[#e8f7ff] py-1.5 pl-3 pr-1.5 text-sm font-semibold text-[#005a87]">
                    {participant.name}
                    <button onClick={() => deleteParticipant(participant.id)} aria-label={`Quitar a ${participant.name}`} className="rounded-full p-1 text-[#0077b5] transition hover:bg-white hover:text-red-600">
                      <Minus className="h-3.5 w-3.5" />
                    </button>
                  </div>
                ))}
              </div>
              <div className="mt-4 flex gap-2">
                <input
                  aria-label="Nombre de una persona"
                  maxLength={80}
                  value={newParticipantName}
                  onChange={(event) => setNewParticipantName(event.target.value)}
                  onKeyDown={(event) => { if (event.key === "Enter") addParticipant(); }}
                  placeholder="Nombre de una persona"
                  className="h-10 min-w-0 flex-1 rounded-xl border border-[#cbd5e1] bg-[#ffffff] px-3 text-sm outline-none transition focus:border-[#00a8e8] focus:ring-2 focus:ring-[#00a8e8]/15"
                />
                <Button aria-label="Agregar persona" onClick={addParticipant} variant="outline" className="h-10 rounded-xl border-[#cbd5e1] bg-white font-bold hover:bg-[#e8f7ff]">
                  <Plus className="h-4 w-4" /><span className="hidden sm:inline">Agregar</span>
                </Button>
              </div>
            </section>

            <section className="rounded-3xl border border-[#e2e8f0] bg-white p-5 shadow-sm sm:p-7">
              <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
                <div>
                  <p className="text-xs font-bold uppercase tracking-[0.14em] text-[#0088cc]">Paso 2</p>
                  <h2 className="mt-1 text-2xl tracking-tight">Cargá los gastos</h2>
                </div>
                <Button
                  onClick={() => {
                    if (activeDivision.participants.length === 0) {
                      window.alert("Primero agregá al menos una persona.");
                      return;
                    }
                    updateActive((division) => ({ ...division, expenses: [...division.expenses, newExpense(division.participants)] }));
                  }}
                  className="rounded-full bg-[#0088cc] font-bold hover:bg-[#0077b5]"
                >
                  <Plus className="h-4 w-4" /> Agregar gasto
                </Button>
              </div>

              {activeDivision.expenses.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-[#cbd5e1] bg-[#f8fafc] px-5 py-8 text-center">
                  <ReceiptText className="mx-auto h-7 w-7 text-[#0088cc]" />
                  <p className="mt-3 font-semibold">Todavía no hay gastos.</p>
                  <p className="mt-1 text-sm text-slate-500">Podés cargar cena, compras, alojamiento o cualquier adelanto.</p>
                </div>
              ) : (
                <div className="space-y-5">
                  {activeDivision.expenses.map((expense, expenseIndex) => (
                    <ExpenseEditor
                      key={expense.id}
                      division={activeDivision}
                      expense={expense}
                      index={expenseIndex}
                      onChange={(updater) => updateExpense(expense.id, updater)}
                      onRemove={() => updateActive((division) => ({ ...division, expenses: division.expenses.filter((item) => item.id !== expense.id) }))}
                    />
                  ))}
                </div>
              )}
            </section>
          </div>

          <aside className="xl:sticky xl:top-6">
            <BalancePanel
              division={activeDivision}
              calculation={calculation!}
              onMarkPaid={(settlement) => updateActive((division) => ({
                ...division,
                payments: [...division.payments, {
                  id: newId(),
                  debtorParticipantId: settlement.debtorParticipantId,
                  creditorParticipantId: settlement.creditorParticipantId,
                  amountCents: settlement.amountCents,
                  paidAt: nowIso(),
                }],
              }), false)}
              onUndoPayment={(paymentId) => updateActive((division) => ({
                ...division,
                payments: division.payments.filter((payment) => payment.id !== paymentId),
              }), false)}
            />
          </aside>
        </div>
      </div>
    </div>
  );
}

function AmountInput({ amountCents, onAmountChange, ...props }: Omit<InputHTMLAttributes<HTMLInputElement>, 'value' | 'onChange' | 'type' | 'onFocus' | 'onBlur'> & {
  amountCents: number;
  onAmountChange: (amountCents: number) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(() => centsToInput(amountCents));
  useEffect(() => { if (!editing) setDraft(centsToInput(amountCents)); }, [amountCents, editing]);
  return <input {...props} type="text" inputMode="decimal" value={draft}
    onFocus={() => setEditing(true)}
    onChange={event => { setDraft(event.target.value); onAmountChange(amountToCents(event.target.value)); }}
    onBlur={() => { setEditing(false); setDraft(centsToInput(amountCents)); }} />;
}

function ExpenseEditor({
  division,
  expense,
  index,
  onChange,
  onRemove,
}: {
  division: Split;
  expense: Expense;
  index: number;
  onChange: (updater: (expense: Expense) => Expense) => void;
  onRemove: () => void;
}) {
  const paidTotal = expense.paidBy.reduce((sum, payer) => sum + payer.amountCents, 0);
  const selectedParticipants = division.participants.filter((participant) => expense.participantIds.includes(participant.id));
  const setMode = (mode: SplitMode) => onChange((current) => ({
    ...current,
    splitMode: mode,
    exactShares: mode === "exact" ? equalShares(current.amountCents, current.participantIds) : current.exactShares,
  }));

  return (
    <article className="rounded-2xl border border-[#e2e8f0] bg-[#ffffff] p-4 sm:p-5">
      <div className="flex items-start gap-3">
        <span className="mt-1 flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-[#17212b] text-xs font-bold text-white">{index + 1}</span>
        <div className="grid min-w-0 flex-1 gap-3 sm:grid-cols-[minmax(0,1fr)_150px]">
          <label className="text-xs font-bold uppercase tracking-[0.1em] text-slate-500">
            Qué fue
            <input
              value={expense.description}
              onChange={(event) => onChange((current) => ({ ...current, description: event.target.value }))}
              className="mt-1.5 h-10 w-full rounded-xl border border-[#cbd5e1] bg-white px-3 text-sm font-medium normal-case tracking-normal text-slate-900 outline-none focus:border-[#00a8e8] focus:ring-2 focus:ring-[#00a8e8]/15"
            />
          </label>
          <label className="text-xs font-bold uppercase tracking-[0.1em] text-slate-500">
            Total ARS
            <AmountInput
              min="0"
              step="0.01"
              inputMode="decimal"
              amountCents={expense.amountCents}
              onAmountChange={amountCents => onChange((current) => ({ ...current, amountCents }))}
              placeholder="0,00"
              className="mt-1.5 h-10 w-full rounded-xl border border-[#cbd5e1] bg-white px-3 text-sm font-medium normal-case tracking-normal text-slate-900 outline-none focus:border-[#00a8e8] focus:ring-2 focus:ring-[#00a8e8]/15"
            />
          </label>
        </div>
        <button onClick={onRemove} aria-label={`Eliminar ${expense.description}`} className="rounded-full p-2 text-slate-400 transition hover:bg-red-50 hover:text-red-600">
          <Trash2 className="h-4 w-4" />
        </button>
      </div>

      <div className="mt-5 grid gap-5 border-t border-[#e2e8f0] pt-5 lg:grid-cols-2">
        <div>
          <p className="text-xs font-bold uppercase tracking-[0.12em] text-slate-500">Quién pagó</p>
          <div className="mt-2 space-y-2">
            {expense.paidBy.map((payer) => (
              <div key={payer.participantId} className="flex gap-2">
                <select
                  aria-label="Persona que pagó"
                  value={payer.participantId}
                  onChange={(event) => onChange((current) => ({
                    ...current,
                    paidBy: current.paidBy.map((item) => item.participantId === payer.participantId
                      ? { ...item, participantId: event.target.value }
                      : item),
                  }))}
                  className="h-9 min-w-0 flex-1 rounded-lg border border-[#cbd5e1] bg-white px-2 text-sm outline-none focus:border-[#00a8e8]"
                >
                  {division.participants.filter((participant) => participant.id === payer.participantId || !expense.paidBy.some((entry) => entry.participantId === participant.id)).map((participant) => <option key={participant.id} value={participant.id}>{participant.name}</option>)}
                </select>
                <AmountInput
                  min="0"
                  step="0.01"
                  inputMode="decimal"
                  aria-label={`Aporte de ${participantName(division, payer.participantId)}`}
                  amountCents={payer.amountCents}
                  onAmountChange={amountCents => onChange((current) => ({
                    ...current,
                    paidBy: current.paidBy.map((item) => item.participantId === payer.participantId
                      ? { ...item, amountCents }
                      : item),
                  }))}
                  placeholder="0,00"
                  className="h-9 w-28 rounded-lg border border-[#cbd5e1] bg-white px-2 text-right text-sm outline-none focus:border-[#00a8e8]"
                />
                <button onClick={() => onChange((current) => ({ ...current, paidBy: current.paidBy.filter((item) => item.participantId !== payer.participantId) }))} aria-label="Quitar pagador" className="rounded-lg px-1.5 text-slate-400 hover:bg-red-50 hover:text-red-600">
                  <Minus className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
          {expense.paidBy.length < division.participants.length && (
            <button
              onClick={() => onChange((current) => {
                const available = division.participants.find((participant) => !current.paidBy.some((payer) => payer.participantId === participant.id));
                return available ? { ...current, paidBy: [...current.paidBy, { participantId: available.id, amountCents: 0 }] } : current;
              })}
              className="mt-2 inline-flex items-center gap-1 text-sm font-bold text-[#0077b5] hover:text-[#005a87]"
            >
              <Plus className="h-3.5 w-3.5" /> Sumar pagador
            </button>
          )}
          <p className={`mt-2 text-xs font-medium ${paidTotal === expense.amountCents && expense.amountCents > 0 ? "text-[#0077b5]" : "text-[#c75a22]"}`}>
            Aportes: {formatARS(paidTotal)} / {formatARS(expense.amountCents)}
          </p>
        </div>

        <div>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs font-bold uppercase tracking-[0.12em] text-slate-500">Quiénes participan</p>
            <div className="inline-flex rounded-lg bg-[#e2e8f0] p-0.5 text-xs font-bold">
              <button aria-pressed={expense.splitMode === "equal"} onClick={() => setMode("equal")} className={`rounded-md px-2 py-1 ${expense.splitMode === "equal" ? "bg-white text-slate-900 shadow-sm" : "text-slate-500"}`}>Igual</button>
              <button aria-pressed={expense.splitMode === "exact"} onClick={() => setMode("exact")} className={`rounded-md px-2 py-1 ${expense.splitMode === "exact" ? "bg-white text-slate-900 shadow-sm" : "text-slate-500"}`}>Exacto</button>
            </div>
          </div>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {division.participants.map((participant) => {
              const selected = expense.participantIds.includes(participant.id);
              return (
                <button
                  key={participant.id}
                  aria-pressed={selected}
                  onClick={() => onChange((current) => ({
                    ...current,
                    participantIds: selected
                      ? current.participantIds.filter((id) => id !== participant.id)
                      : [...current.participantIds, participant.id],
                  }))}
                  className={`rounded-full px-2.5 py-1 text-xs font-bold transition ${selected ? "bg-[#17212b] text-white" : "bg-[#e2e8f0] text-slate-500 hover:bg-[#e2e8f0]"}`}
                >
                  {selected && <Check className="mr-1 inline h-3 w-3" />}{participant.name}
                </button>
              );
            })}
          </div>
          {expense.splitMode === "equal" ? (
            <p className="mt-3 text-sm text-slate-500">
              {selectedParticipants.length > 0 ? `Cada persona: ${formatARS(Math.floor(expense.amountCents / selectedParticipants.length))} aprox.` : "Elegí a quiénes les corresponde."}
            </p>
          ) : (
            <div className="mt-3 space-y-1.5">
              {selectedParticipants.map((participant) => (
                <label key={participant.id} className="flex items-center gap-2 text-sm text-slate-600">
                  <span className="flex-1 truncate">{participant.name}</span>
                  <AmountInput
                    min="0"
                    step="0.01"
                    inputMode="decimal"
                    aria-label={`Importe asignado a ${participant.name}`}
                    amountCents={expense.exactShares[participant.id] ?? 0}
                    onAmountChange={amountCents => onChange((current) => ({
                      ...current,
                      exactShares: { ...current.exactShares, [participant.id]: amountCents },
                    }))}
                    placeholder="0,00"
                    className="h-8 w-28 rounded-lg border border-[#cbd5e1] bg-white px-2 text-right text-sm outline-none focus:border-[#00a8e8]"
                  />
                </label>
              ))}
            </div>
          )}
        </div>
      </div>
    </article>
  );
}

function BalancePanel({
  division,
  calculation,
  onMarkPaid,
  onUndoPayment,
}: {
  division: Split;
  calculation: ReturnType<typeof calculateDivision>;
  onMarkPaid: (settlement: ReturnType<typeof calculateDivision>["settlements"][number]) => void;
  onUndoPayment: (paymentId: string) => void;
}) {
  return (
    <section className="overflow-hidden rounded-3xl bg-[#17212b] text-white shadow-xl shadow-slate-900/15">
      <div className="border-b border-white/10 px-5 py-5 sm:px-6">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-xs font-bold uppercase tracking-[0.14em] text-[#7dd3fc]">Paso 3</p>
            <h2 className="mt-1 text-2xl tracking-tight">El balance</h2>
          </div>
          <Landmark className="h-6 w-6 text-[#7dd3fc]" />
        </div>
      </div>

      {calculation.issues.length > 0 ? (
        <div className="px-5 py-6 sm:px-6">
          <p className="text-sm font-semibold text-[#cfeeff]">Faltan algunos datos antes de calcular.</p>
          <ul className="mt-4 space-y-2 text-sm leading-relaxed text-slate-300">
            {calculation.issues.map((issue) => <li key={issue} className="flex gap-2"><span className="text-[#7dd3fc]">•</span>{issue}</li>)}
          </ul>
        </div>
      ) : (
        <div className="px-5 py-6 sm:px-6">
          <p className="text-sm leading-relaxed text-slate-300">Consolidamos todos los gastos y adelantos para que hagan la menor cantidad de transferencias posible.</p>
          <div className="mt-5 space-y-3">
            {calculation.settlements.length === 0 ? (
              <div className="rounded-2xl bg-[#232e3c] px-4 py-5 text-center">
                <Check className="mx-auto h-6 w-6 text-[#22d3ee]" />
                <p className="mt-2 text-xl">Todo saldado</p>
                <p className="mt-1 text-sm text-slate-300">No quedan transferencias pendientes.</p>
              </div>
            ) : calculation.settlements.map((settlement) => (
              <div key={`${settlement.debtorParticipantId}-${settlement.creditorParticipantId}-${settlement.amountCents}`} className="rounded-2xl bg-[#232e3c] p-4">
                <div className="flex gap-2 text-sm leading-snug">
                  <span className="font-bold text-white">{participantName(division, settlement.debtorParticipantId)}</span>
                  <span className="text-slate-400">le paga</span>
                  <span className="font-bold text-[#7dd3fc]">{formatARS(settlement.amountCents)}</span>
                  <span className="text-slate-400">a</span>
                  <span className="font-bold text-white">{participantName(division, settlement.creditorParticipantId)}</span>
                </div>
                <Button onClick={() => onMarkPaid(settlement)} className="mt-3 h-9 w-full rounded-xl bg-[#00a8e8] text-xs font-bold text-[#ffffff] hover:bg-[#22d3ee]">
                  <Check className="h-3.5 w-3.5" /> Marcar como pagado
                </Button>
              </div>
            ))}
          </div>

          {division.payments.length > 0 && (
            <div className="mt-7 border-t border-white/10 pt-5">
              <p className="text-xs font-bold uppercase tracking-[0.14em] text-slate-400">Pagos registrados</p>
              <div className="mt-3 space-y-2">
                {division.payments.map((payment) => (
                  <div key={payment.id} className="flex items-center gap-2 rounded-xl border border-white/10 px-3 py-2.5 text-xs text-slate-300">
                    <Check className="h-4 w-4 shrink-0 text-[#22d3ee]" />
                    <span className="min-w-0 flex-1 truncate">{participantName(division, payment.debtorParticipantId)} → {participantName(division, payment.creditorParticipantId)} · {formatARS(payment.amountCents)}</span>
                    <button onClick={() => onUndoPayment(payment.id)} className="rounded-lg p-1 text-slate-400 hover:bg-white/10 hover:text-white" aria-label="Deshacer pago">
                      <Undo2 className="h-3.5 w-3.5" />
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="mt-7 border-t border-white/10 pt-5">
            <p className="text-xs font-bold uppercase tracking-[0.14em] text-slate-400">Saldos individuales</p>
            <div className="mt-3 space-y-2">
              {division.participants.map((participant) => {
                const balance = calculation.balances[participant.id] ?? 0;
                return (
                  <div key={participant.id} className="flex items-center justify-between text-sm">
                    <span className="text-slate-300">{participant.name}</span>
                    <span className={balance > 0 ? "font-bold text-[#7dd3fc]" : balance < 0 ? "font-bold text-[#cfeeff]" : "font-bold text-slate-400"}>
                      {balance > 0 ? "+" : ""}{formatARS(balance)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
      <div className="flex items-center gap-2 bg-black/15 px-5 py-3 text-xs text-slate-400 sm:px-6">
        <Clipboard className="h-3.5 w-3.5" /> Se guarda automáticamente en este navegador.
      </div>
    </section>
  );
}
