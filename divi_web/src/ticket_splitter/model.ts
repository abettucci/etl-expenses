export type SplitMode = "equal" | "exact";

export type Participant = {
  id: string;
  name: string;
};

export type Contribution = {
  participantId: string;
  amountCents: number;
};

export type Expense = {
  id: string;
  description: string;
  amountCents: number;
  paidBy: Contribution[];
  participantIds: string[];
  splitMode: SplitMode;
  exactShares: Record<string, number>;
};

export type Payment = {
  id: string;
  debtorParticipantId: string;
  creditorParticipantId: string;
  amountCents: number;
  paidAt: string;
};

export type Split = {
  id: string;
  name: string;
  currency: "ARS";
  participants: Participant[];
  expenses: Expense[];
  payments: Payment[];
  createdAt: string;
  updatedAt: string;
};

export type Settlement = {
  debtorParticipantId: string;
  creditorParticipantId: string;
  amountCents: number;
};

export type Calculation = {
  issues: string[];
  balances: Record<string, number>;
  settlements: Settlement[];
};

export const STORAGE_KEY = "splitbot-web-divisions-v1";

export const newId = () =>
  globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`;

export const nowIso = () => new Date().toISOString();

export const formatARS = (amountCents: number) =>
  new Intl.NumberFormat("es-AR", {
    style: "currency",
    currency: "ARS",
    maximumFractionDigits: 2,
  }).format(amountCents / 100);

export const amountToCents = (value: string) => {
  const normalized = value.trim().replace(",", ".");
  const amount = Number(normalized);
  return Number.isFinite(amount) ? Math.round(amount * 100) : 0;
};

export const centsToInput = (amountCents: number) =>
  amountCents === 0 ? "" : (amountCents / 100).toFixed(2);

export const createDivision = (): Split => {
  const timestamp = nowIso();
  return {
    id: newId(),
    name: "Nueva división",
    currency: "ARS",
    participants: [],
    expenses: [],
    payments: [],
    createdAt: timestamp,
    updatedAt: timestamp,
  };
};

export const equalShares = (amountCents: number, participantIds: string[]) => {
  const shares: Record<string, number> = {};
  if (participantIds.length === 0) return shares;

  const base = Math.floor(amountCents / participantIds.length);
  const remainder = amountCents % participantIds.length;
  participantIds.forEach((participantId, index) => {
    shares[participantId] = base + (index < remainder ? 1 : 0);
  });
  return shares;
};

const expenseIssues = (expense: Expense, index: number, participantIds: Set<string>) => {
  const label = `Gasto ${index + 1}`;
  const issues: string[] = [];
  if (!expense.description.trim()) issues.push(`${label}: ingresá una descripción.`);
  if (!Number.isSafeInteger(expense.amountCents) || expense.amountCents <= 0) issues.push(`${label}: el monto debe ser mayor a cero y expresarse en centavos válidos.`);
  if (expense.participantIds.length === 0) issues.push(`${label}: elegí al menos una persona.`);
  if (expense.participantIds.some((id) => !participantIds.has(id))) {
    issues.push(`${label}: tiene participantes inválidos.`);
  }
  if (expense.paidBy.length === 0) issues.push(`${label}: indicá quién pagó.`);
  if (expense.paidBy.some((payer) => !participantIds.has(payer.participantId) || !Number.isSafeInteger(payer.amountCents) || payer.amountCents <= 0)) {
    issues.push(`${label}: revisá los aportes de quienes pagaron.`);
  }
  const paid = expense.paidBy.reduce((sum, payer) => sum + payer.amountCents, 0);
  if (expense.paidBy.length > 0 && paid !== expense.amountCents) {
    issues.push(`${label}: los aportes deben sumar ${formatARS(expense.amountCents)}.`);
  }
  if (expense.splitMode === "exact") {
    if (expense.participantIds.some((id) => !Number.isSafeInteger(expense.exactShares[id] ?? 0) || (expense.exactShares[id] ?? 0) < 0)) {
      issues.push(`${label}: los importes asignados deben ser positivos o cero y expresarse en centavos válidos.`);
    }
    const assigned = expense.participantIds.reduce(
      (sum, participantId) => sum + (expense.exactShares[participantId] ?? 0),
      0,
    );
    if (assigned !== expense.amountCents) {
      issues.push(`${label}: los importes asignados deben sumar ${formatARS(expense.amountCents)}.`);
    }
  }
  return issues;
};

export const calculateDivision = (division: Split): Calculation => {
  const participantIds = new Set(division.participants.map((participant) => participant.id));
  const issues: string[] = [];
  if (division.participants.length === 0) issues.push("Agregá al menos una persona a la división.");
  if (division.expenses.length === 0) issues.push("Agregá al menos un gasto para calcular el balance.");

  division.expenses.forEach((expense, index) => {
    issues.push(...expenseIssues(expense, index, participantIds));
  });

  const balances = Object.fromEntries(division.participants.map((participant) => [participant.id, 0]));
  if (issues.length > 0) return { issues, balances, settlements: [] };

  division.expenses.forEach((expense) => {
    expense.paidBy.forEach((payer) => {
      balances[payer.participantId] += payer.amountCents;
    });

    const shares = expense.splitMode === "equal"
      ? equalShares(expense.amountCents, expense.participantIds)
      : expense.exactShares;
    expense.participantIds.forEach((participantId) => {
      balances[participantId] -= shares[participantId] ?? 0;
    });
  });

  division.payments.forEach((payment) => {
    if (!participantIds.has(payment.debtorParticipantId) || !participantIds.has(payment.creditorParticipantId)) return;
    balances[payment.debtorParticipantId] += payment.amountCents;
    balances[payment.creditorParticipantId] -= payment.amountCents;
  });

  const debtors = division.participants
    .map((participant) => ({ id: participant.id, amountCents: balances[participant.id] }))
    .filter((entry) => entry.amountCents < 0)
    .map((entry) => ({ ...entry, amountCents: -entry.amountCents }));
  const creditors = division.participants
    .map((participant) => ({ id: participant.id, amountCents: balances[participant.id] }))
    .filter((entry) => entry.amountCents > 0);
  const settlements: Settlement[] = [];
  let debtorIndex = 0;
  let creditorIndex = 0;

  while (debtorIndex < debtors.length && creditorIndex < creditors.length) {
    const debtor = debtors[debtorIndex];
    const creditor = creditors[creditorIndex];
    const amountCents = Math.min(debtor.amountCents, creditor.amountCents);
    if (amountCents > 0) {
      settlements.push({
        debtorParticipantId: debtor.id,
        creditorParticipantId: creditor.id,
        amountCents,
      });
    }
    debtor.amountCents -= amountCents;
    creditor.amountCents -= amountCents;
    if (debtor.amountCents === 0) debtorIndex += 1;
    if (creditor.amountCents === 0) creditorIndex += 1;
  }

  return { issues, balances, settlements };
};

export const cloneDivision = (division: Split): Split => {
  const participantIdMap = new Map(division.participants.map((participant) => [participant.id, newId()]));
  const timestamp = nowIso();
  return {
    ...division,
    id: newId(),
    name: `${division.name} copia`,
    participants: division.participants.map((participant) => ({
      ...participant,
      id: participantIdMap.get(participant.id)!,
    })),
    expenses: division.expenses.map((expense) => ({
      ...expense,
      id: newId(),
      participantIds: expense.participantIds.map((id) => participantIdMap.get(id)!),
      paidBy: expense.paidBy.map((payer) => ({
        ...payer,
        participantId: participantIdMap.get(payer.participantId)!,
      })),
      exactShares: Object.fromEntries(
        Object.entries(expense.exactShares).map(([id, amount]) => [participantIdMap.get(id)!, amount]),
      ),
    })),
    payments: [],
    createdAt: timestamp,
    updatedAt: timestamp,
  };
};

export const resultText = (division: Split, calculation: Calculation) => {
  const participants = new Map(division.participants.map((participant) => [participant.id, participant.name]));
  const pending = calculation.settlements.length
    ? calculation.settlements
      .map((settlement) => `• ${participants.get(settlement.debtorParticipantId)} le paga ${formatARS(settlement.amountCents)} a ${participants.get(settlement.creditorParticipantId)}`)
      .join("\n")
    : "No hay transferencias pendientes.";
  return `${division.name}\n\nTransferencias pendientes\n${pending}`;
};
