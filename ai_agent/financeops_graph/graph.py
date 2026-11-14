"""LangGraph orchestration for the local FinanceOps Copilot proof of concept."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Protocol

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from .repositories import FinanceRepository
from .schemas import (
    ApprovalDecision,
    DateRange,
    DraftCategoryMapping,
    InvestigationIntent,
    PlannedToolCall,
)
from .state import FinanceOpsState
from .tools import build_finance_tools


class IntentResolver(Protocol):
    """Boundary where an LLM may later be injected and mocked in tests.

    Its output is a validated intent, never SQL or arbitrary tool input.
    """

    def resolve(self, question: str, reference_date: date) -> InvestigationIntent: ...


class DurableCheckpointerProvider(Protocol):
    """Future production boundary for an encrypted, durable LangGraph saver.

    TODO: implement this only after retention, tenant isolation, encryption and
    access-control requirements are approved. The POC intentionally uses no
    durable store.
    """

    def build(self) -> Any: ...


def _month_range(reference_date: date, months_back: int = 0) -> DateRange:
    month = reference_date.month - months_back
    year = reference_date.year
    while month <= 0:
        month += 12
        year -= 1
    start = date(year, month, 1)
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return DateRange(start=start, end=next_month - timedelta(days=1))


class DeterministicIntentResolver:
    """Offline resolver used by the demo; it makes no network or model call."""

    def resolve(self, question: str, reference_date: date) -> InvestigationIntent:
        normalized = question.casefold()
        current_month = _month_range(reference_date)
        previous_month = _month_range(reference_date, months_back=1)

        if any(word in normalized for word in ("rule", "regla", "mapping", "mapeo")):
            match = re.search(
                r"(?:merchant|comercio)\s+(.+?)\s+(?:as|como)\s+([\wÁÉÍÓÚÜÑáéíóúüñ -]+?)(?:[?.!]|$)",
                question,
                re.IGNORECASE,
            )
            merchant = match.group(1).strip() if match else "Merchant X"
            category = match.group(2).strip().title() if match else "Software"
            return InvestigationIntent(
                kind="draft_mapping",
                merchant=merchant,
                suggested_category=category,
                rationale="El comercio aparece en transacciones pendientes de categorización.",
            )
        if any(word in normalized for word in ("categor", "uncategor", "sin clasificar", "pendient")):
            return InvestigationIntent(kind="uncategorized", period=current_month)
        if any(word in normalized for word in ("policy", "política", "diccionario", "guideline", "guía")):
            return InvestigationIntent(kind="policy_lookup")
        return InvestigationIntent(
            kind="spending_increase",
            period=current_month,
            previous_period=previous_month,
            category="Software" if "software" in normalized else None,
        )


class FinanceOpsCopilot:
    """A local-only StateGraph with short-term, in-memory checkpointing.

    InMemorySaver is intentionally non-durable and unsuitable for Lambda
    restarts. A future deployment should inject a durable LangGraph checkpointer
    and an auditable store; this POC provisions neither.
    """

    def __init__(
        self,
        repository: FinanceRepository,
        resolver: IntentResolver | None = None,
        reference_date: date | None = None,
        checkpointer: InMemorySaver | None = None,
    ):
        self.repository = repository
        self.resolver = resolver or DeterministicIntentResolver()
        self.reference_date = reference_date or date.today()
        self.checkpointer = checkpointer or InMemorySaver()
        self.tools = build_finance_tools(repository)
        self.graph = self._build()

    @staticmethod
    def _audit(event: str, mode: str, **details: Any) -> dict[str, Any]:
        return {
            "event": event,
            "mode": mode,
            "at": datetime.now(timezone.utc).isoformat(),
            **details,
        }

    def _normalize(self, state: FinanceOpsState) -> dict[str, Any]:
        question = str(state.get("user_question") or "").strip()
        if not question:
            raise ValueError("user_question is required")
        intent = self.resolver.resolve(question, self.reference_date)
        update: dict[str, Any] = {
            "normalized_investigation_intent": intent.model_dump(mode="json"),
            "execution_metadata": [
                *state.get("execution_metadata", []),
                self._audit("intent_normalized", self.repository.mode, intent=intent.kind),
            ],
        }
        if intent.period:
            update["date_range"] = intent.period.model_dump(mode="json")
        if intent.previous_period:
            update["previous_date_range"] = intent.previous_period.model_dump(mode="json")
        return update

    def _plan(self, state: FinanceOpsState) -> dict[str, Any]:
        intent = InvestigationIntent.model_validate(state["normalized_investigation_intent"])
        if intent.kind == "spending_increase":
            call = PlannedToolCall(
                name="compare_periods",
                arguments={
                    "current_period": intent.period.model_dump(mode="json"),
                    "previous_period": intent.previous_period.model_dump(mode="json"),
                },
            )
        elif intent.kind == "uncategorized":
            call = PlannedToolCall(
                name="find_uncategorized_or_suspicious_transactions",
                arguments={"period": intent.period.model_dump(mode="json")},
            )
        elif intent.kind == "policy_lookup":
            call = PlannedToolCall(
                name="search_finance_policy",
                arguments={"query": state["user_question"]},
            )
        else:
            merchant = intent.merchant or "Merchant X"
            call = PlannedToolCall(
                name="draft_category_mapping",
                arguments={
                    "merchant": merchant,
                    "suggested_category": intent.suggested_category or "Software",
                    "rationale": intent.rationale or "Revisión solicitada por el usuario.",
                    "affected_count": self.repository.affected_transaction_count(merchant),
                },
            )
        return {
            "tool_call": call.model_dump(mode="json"),
            "execution_metadata": [
                *state.get("execution_metadata", []),
                self._audit("tool_planned", self.repository.mode, tool=call.name),
            ],
        }

    def _execute_tool(self, state: FinanceOpsState) -> dict[str, Any]:
        call = PlannedToolCall.model_validate(state["tool_call"])
        result = self.tools[call.name].invoke(call.arguments)
        evidence_item = {"tool": call.name, "result": result}
        update: dict[str, Any] = {
            "evidence": [*state.get("evidence", []), evidence_item],
            "execution_metadata": [
                *state.get("execution_metadata", []),
                self._audit("tool_executed", self.repository.mode, tool=call.name, read_only=True),
            ],
        }
        if call.name == "draft_category_mapping":
            update["proposed_action_draft"] = DraftCategoryMapping.model_validate(result).model_dump(mode="json")
        return update

    @staticmethod
    def _route_after_tool(state: FinanceOpsState) -> str:
        return "approval" if state.get("proposed_action_draft") else "finalize"

    def _approval(self, state: FinanceOpsState) -> dict[str, Any]:
        draft = DraftCategoryMapping.model_validate(state["proposed_action_draft"])
        while True:
            payload = {
                "type": "approval_required",
                "draft": draft.model_dump(mode="json"),
                "rationale": draft.rationale,
                "affected_transaction_count": draft.affected_count,
                "evidence": state.get("evidence", []),
                "simulation_only": True,
            }
            decision = ApprovalDecision.model_validate(interrupt(payload))
            if decision.action == "edit":
                edits = decision.edits
                draft = draft.model_copy(update=edits.model_dump(exclude_none=True))
                continue
            if decision.action == "approve":
                draft = draft.model_copy(update={"status": "approved_in_simulation"})
                status = "approved_in_simulation"
                response = "Borrador aprobado en simulación. No se escribió ningún mapeo ni transacción."
            else:
                draft = draft.model_copy(update={"status": "rejected"})
                status = "rejected"
                response = "Borrador rechazado. No se escribió ningún mapeo ni transacción."
            return {
                "proposed_action_draft": draft.model_dump(mode="json"),
                "approval_status": status,
                "final_response": response,
                "execution_metadata": [
                    *state.get("execution_metadata", []),
                    self._audit("approval_resolved", self.repository.mode, status=status),
                ],
            }

    def _finalize(self, state: FinanceOpsState) -> dict[str, Any]:
        if state.get("final_response"):
            return {}
        evidence = state.get("evidence", [])
        result = evidence[-1]["result"] if evidence else {}
        call = PlannedToolCall.model_validate(state["tool_call"])
        if call.name == "compare_periods":
            intent = InvestigationIntent.model_validate(state["normalized_investigation_intent"])
            category = intent.category
            if category:
                current_amount = float(result["current_by_category"].get(category, 0))
                previous_amount = float(result["previous_by_category"].get(category, 0))
                difference = current_amount - previous_amount
                percentage = round((difference / previous_amount) * 100, 2) if previous_amount else None
                response = (
                    f"{category}: ARS {current_amount:,.2f} vs ARS {previous_amount:,.2f}; "
                    f"variación ARS {difference:,.2f} ({percentage}%)."
                )
            else:
                response = (
                    f"Comparación completada: ARS {result['current_total']:,.2f} vs ARS "
                    f"{result['previous_total']:,.2f}; variación ARS {result['difference']:,.2f} "
                    f"({result['percentage_change']}%)."
                )
        elif call.name == "find_uncategorized_or_suspicious_transactions":
            response = f"Se encontraron {result['count']} transacciones que requieren categorización o revisión."
        elif call.name == "search_finance_policy":
            response = f"Se encontraron {len(result)} documentos locales de política relevantes."
        else:
            response = "El borrador está listo para aprobación humana."
        return {
            "final_response": response,
            "execution_metadata": [
                *state.get("execution_metadata", []),
                self._audit("response_finalized", self.repository.mode),
            ],
        }

    def _build(self):
        builder = StateGraph(FinanceOpsState)
        builder.add_node("normalize", self._normalize)
        builder.add_node("plan", self._plan)
        builder.add_node("execute_tool", self._execute_tool)
        builder.add_node("approval", self._approval)
        builder.add_node("finalize", self._finalize)
        builder.add_edge(START, "normalize")
        builder.add_edge("normalize", "plan")
        builder.add_edge("plan", "execute_tool")
        builder.add_conditional_edges("execute_tool", self._route_after_tool, {"approval": "approval", "finalize": "finalize"})
        builder.add_edge("approval", "finalize")
        builder.add_edge("finalize", END)
        return builder.compile(checkpointer=self.checkpointer)
