"""Strict, JSON-friendly contracts used by FinanceOps Copilot."""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class DateRange(StrictModel):
    start: date
    end: date

    @model_validator(mode="after")
    def end_must_not_precede_start(self) -> "DateRange":
        if self.end < self.start:
            raise ValueError("end must be on or after start")
        return self


class InvestigationIntent(StrictModel):
    kind: Literal["spending_increase", "uncategorized", "draft_mapping", "policy_lookup"]
    period: DateRange | None = None
    previous_period: DateRange | None = None
    category: str | None = Field(default=None, max_length=80)
    merchant: str | None = Field(default=None, max_length=120)
    suggested_category: str | None = Field(default=None, max_length=80)
    rationale: str | None = Field(default=None, max_length=500)


class GetSpendingSummaryArgs(StrictModel):
    period: DateRange
    category: str | None = Field(default=None, max_length=80)


class ComparePeriodsArgs(StrictModel):
    current_period: DateRange
    previous_period: DateRange

    @model_validator(mode="after")
    def previous_period_must_precede_current(self) -> "ComparePeriodsArgs":
        if self.previous_period.end >= self.current_period.start:
            raise ValueError("previous_period must end before current_period starts")
        return self


class FindTransactionsArgs(StrictModel):
    period: DateRange


class SearchFinancePolicyArgs(StrictModel):
    query: str = Field(min_length=3, max_length=300)


class DraftCategoryMappingArgs(StrictModel):
    merchant: str = Field(min_length=2, max_length=120)
    suggested_category: str = Field(min_length=2, max_length=80)
    rationale: str = Field(min_length=5, max_length=500)
    affected_count: int = Field(ge=0, le=100_000)

    @field_validator("merchant", "suggested_category")
    @classmethod
    def must_not_contain_control_characters(cls, value: str) -> str:
        if any(ord(char) < 32 for char in value):
            raise ValueError("control characters are not allowed")
        return value


class DraftCategoryMapping(StrictModel):
    merchant: str
    suggested_category: str
    rationale: str
    affected_count: int = Field(ge=0)
    status: Literal["draft", "approved_in_simulation", "rejected"] = "draft"


class MappingEdits(StrictModel):
    suggested_category: str | None = Field(default=None, min_length=2, max_length=80)
    rationale: str | None = Field(default=None, min_length=5, max_length=500)

    @model_validator(mode="after")
    def one_edit_is_required(self) -> "MappingEdits":
        if self.suggested_category is None and self.rationale is None:
            raise ValueError("at least one editable field is required")
        return self


class ApprovalDecision(StrictModel):
    action: Literal["approve", "reject", "edit"]
    edits: MappingEdits | None = None

    @model_validator(mode="after")
    def edits_are_only_allowed_for_edit_action(self) -> "ApprovalDecision":
        if self.action == "edit" and self.edits is None:
            raise ValueError("edits are required when action is edit")
        if self.action != "edit" and self.edits is not None:
            raise ValueError("edits are only valid when action is edit")
        return self


class PlannedToolCall(StrictModel):
    name: Literal[
        "get_spending_summary",
        "compare_periods",
        "find_uncategorized_or_suspicious_transactions",
        "search_finance_policy",
        "draft_category_mapping",
    ]
    arguments: dict[str, Any]
