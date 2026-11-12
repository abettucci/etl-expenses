"""Explicit short-term state for the FinanceOps LangGraph workflow."""

from __future__ import annotations

from typing import Any, TypedDict


class FinanceOpsState(TypedDict, total=False):
    thread_id: str
    user_question: str
    normalized_investigation_intent: dict[str, Any]
    date_range: dict[str, str]
    previous_date_range: dict[str, str]
    tool_call: dict[str, Any]
    evidence: list[dict[str, Any]]
    proposed_action_draft: dict[str, Any]
    approval_status: str
    final_response: str
    execution_metadata: list[dict[str, Any]]
