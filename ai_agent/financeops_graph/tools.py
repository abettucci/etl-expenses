"""LangChain structured tools for the isolated FinanceOps POC."""

from __future__ import annotations

from typing import Any

from langchain_core.tools import StructuredTool

from .knowledge_base import search_local_policy
from .repositories import FinanceRepository
from .schemas import (
    ComparePeriodsArgs,
    DraftCategoryMapping,
    DraftCategoryMappingArgs,
    FindTransactionsArgs,
    GetSpendingSummaryArgs,
    SearchFinancePolicyArgs,
)


def build_finance_tools(repository: FinanceRepository) -> dict[str, StructuredTool]:
    """Bind strictly typed tools to one read-only repository instance."""

    def get_spending_summary(period, category=None) -> dict[str, Any]:
        args = GetSpendingSummaryArgs(period=period, category=category)
        return repository.get_spending_summary(args.period, args.category)

    def compare_periods(current_period, previous_period) -> dict[str, Any]:
        args = ComparePeriodsArgs(current_period=current_period, previous_period=previous_period)
        return repository.compare_periods(args.current_period, args.previous_period)

    def find_uncategorized_or_suspicious_transactions(period) -> dict[str, Any]:
        args = FindTransactionsArgs(period=period)
        return repository.find_uncategorized_or_suspicious_transactions(args.period)

    def search_finance_policy(query: str) -> list[dict[str, str]]:
        args = SearchFinancePolicyArgs(query=query)
        return search_local_policy(args.query)

    def draft_category_mapping(merchant: str, suggested_category: str, rationale: str, affected_count: int) -> dict[str, Any]:
        args = DraftCategoryMappingArgs(
            merchant=merchant,
            suggested_category=suggested_category,
            rationale=rationale,
            affected_count=affected_count,
        )
        # A draft is intentionally in-memory only. Do not persist or call a client here.
        return DraftCategoryMapping(**args.model_dump()).model_dump(mode="json")

    tools = [
        StructuredTool.from_function(
            func=get_spending_summary,
            name="get_spending_summary",
            description="Returns a read-only spending summary for a fixed date range.",
            args_schema=GetSpendingSummaryArgs,
        ),
        StructuredTool.from_function(
            func=compare_periods,
            name="compare_periods",
            description="Compares two non-overlapping, validated date ranges using read-only data.",
            args_schema=ComparePeriodsArgs,
        ),
        StructuredTool.from_function(
            func=find_uncategorized_or_suspicious_transactions,
            name="find_uncategorized_or_suspicious_transactions",
            description="Lists transactions requiring categorization or review without modifying them.",
            args_schema=FindTransactionsArgs,
        ),
        StructuredTool.from_function(
            func=search_finance_policy,
            name="search_finance_policy",
            description="Searches the local financial data dictionary and categorization policies.",
            args_schema=SearchFinancePolicyArgs,
        ),
        StructuredTool.from_function(
            func=draft_category_mapping,
            name="draft_category_mapping",
            description="Creates an in-memory category-mapping draft requiring human approval.",
            args_schema=DraftCategoryMappingArgs,
        ),
    ]
    return {tool.name: tool for tool in tools}
