"""Read-only data adapters for the FinanceOps proof of concept."""

from __future__ import annotations

import os
import re
from datetime import date
from typing import Any, Protocol

from .fixtures import SYNTHETIC_TRANSACTIONS
from .schemas import DateRange


class FinanceRepository(Protocol):
    mode: str

    def get_spending_summary(self, period: DateRange, category: str | None = None) -> dict[str, Any]: ...

    def compare_periods(self, current_period: DateRange, previous_period: DateRange) -> dict[str, Any]: ...

    def find_uncategorized_or_suspicious_transactions(self, period: DateRange) -> dict[str, Any]: ...

    def affected_transaction_count(self, merchant: str) -> int: ...


def _serialize_transaction(transaction: dict[str, Any]) -> dict[str, Any]:
    return {
        **transaction,
        "expense_date": transaction["expense_date"].isoformat(),
    }


class SyntheticFinanceRepository:
    """Offline repository backed only by fixtures.py."""

    mode = "synthetic"

    def __init__(self, transactions: list[dict[str, Any]] | None = None):
        self._transactions = list(transactions or SYNTHETIC_TRANSACTIONS)

    def _in_period(self, period: DateRange) -> list[dict[str, Any]]:
        return [
            transaction
            for transaction in self._transactions
            if period.start <= transaction["expense_date"] <= period.end
        ]

    def get_spending_summary(self, period: DateRange, category: str | None = None) -> dict[str, Any]:
        transactions = self._in_period(period)
        if category:
            transactions = [
                transaction
                for transaction in transactions
                if (transaction["category"] or "").casefold() == category.casefold()
            ]
        by_category: dict[str, float] = {}
        by_merchant: dict[str, float] = {}
        for transaction in transactions:
            category_name = transaction["category"] or "Sin categorizar"
            by_category[category_name] = by_category.get(category_name, 0.0) + transaction["amount"]
            by_merchant[transaction["merchant"]] = by_merchant.get(transaction["merchant"], 0.0) + transaction["amount"]
        return {
            "period": period.model_dump(mode="json"),
            "category_filter": category,
            "currency": "ARS",
            "transaction_count": len(transactions),
            "total_amount": round(sum(transaction["amount"] for transaction in transactions), 2),
            "by_category": dict(sorted(by_category.items())),
            "by_merchant": dict(sorted(by_merchant.items())),
        }

    def compare_periods(self, current_period: DateRange, previous_period: DateRange) -> dict[str, Any]:
        current = self.get_spending_summary(current_period)
        previous = self.get_spending_summary(previous_period)
        difference = round(current["total_amount"] - previous["total_amount"], 2)
        previous_amount = previous["total_amount"]
        percentage = round((difference / previous_amount) * 100, 2) if previous_amount else None
        return {
            "current_period": current_period.model_dump(mode="json"),
            "previous_period": previous_period.model_dump(mode="json"),
            "currency": "ARS",
            "current_total": current["total_amount"],
            "previous_total": previous_amount,
            "difference": difference,
            "percentage_change": percentage,
            "current_by_category": current["by_category"],
            "previous_by_category": previous["by_category"],
        }

    def find_uncategorized_or_suspicious_transactions(self, period: DateRange) -> dict[str, Any]:
        transactions = [
            transaction
            for transaction in self._in_period(period)
            if not transaction["category"] or transaction["suspicious_reason"]
        ]
        return {
            "period": period.model_dump(mode="json"),
            "count": len(transactions),
            "transactions": [_serialize_transaction(transaction) for transaction in transactions],
        }

    def affected_transaction_count(self, merchant: str) -> int:
        return sum(
            1 for transaction in self._transactions if transaction["merchant"].casefold() == merchant.casefold()
        )


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROJECT_IDENTIFIER = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")
_WRITE_SQL = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|EXPORT)\b", re.IGNORECASE)


def _safe_identifier(value: str, name: str, *, project: bool = False) -> str:
    pattern = _PROJECT_IDENTIFIER if project else _IDENTIFIER
    if not pattern.fullmatch(value):
        raise ValueError(f"invalid {name} identifier")
    return value


def assert_read_only_query(sql: str) -> None:
    normalized = sql.strip()
    if not normalized.upper().startswith(("SELECT", "WITH")) or _WRITE_SQL.search(normalized):
        raise ValueError("FinanceOps only permits predefined read-only SELECT/WITH queries")


class ReadOnlyBigQueryRepository:
    """Optional BigQuery adapter using fixed SQL templates and query parameters only.

    It intentionally uses Application Default Credentials. It does not read AWS
    Secrets Manager and exposes no write operation.
    """

    mode = "bigquery_read_only"

    def __init__(
        self,
        client: Any,
        project_id: str,
        dataset: str = "PRD",
        gastos_view: str = "gastos_totales",
    ):
        self.client = client
        self.project_id = _safe_identifier(project_id, "project", project=True)
        self.dataset = _safe_identifier(dataset, "dataset")
        self.gastos_view = _safe_identifier(gastos_view, "gastos view")

    @classmethod
    def from_environment(cls) -> "ReadOnlyBigQueryRepository":
        from google.cloud import bigquery

        project_id = os.environ.get("GCP_PROJECT_ID", "")
        if not project_id:
            raise RuntimeError("GCP_PROJECT_ID is required for FINANCEOPS_DATA_MODE=bigquery")
        client = bigquery.Client(project=project_id, location=os.environ.get("BQ_LOCATION", "US"))
        return cls(
            client=client,
            project_id=project_id,
            dataset=os.environ.get("BQ_DATASET_PROD", "PRD"),
            gastos_view=os.environ.get("FINANCEOPS_GASTOS_VIEW", "gastos_totales"),
        )

    def _fqn(self, table: str) -> str:
        return f"`{self.project_id}.{self.dataset}.{table}`"

    @staticmethod
    def _date_expr(column: str = "fecha") -> str:
        return f"""
        COALESCE(
          SAFE_CAST(NULLIF(TRIM(CAST({column} AS STRING)), '') AS DATE),
          DATE(SAFE_CAST(NULLIF(TRIM(CAST({column} AS STRING)), '') AS TIMESTAMP)),
          SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(CAST({column} AS STRING)), ''))
        )
        """

    def _query(self, sql: str, parameters: list[Any]) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        assert_read_only_query(sql)
        config = bigquery.QueryJobConfig(query_parameters=parameters, use_query_cache=True)
        job = self.client.query(sql, job_config=config)
        return [dict(row.items()) for row in job.result()]

    def get_spending_summary(self, period: DateRange, category: str | None = None) -> dict[str, Any]:
        from google.cloud import bigquery

        sql = f"""
        WITH expenses AS (
          SELECT
            {self._date_expr()} AS expense_date,
            NULLIF(TRIM(CAST(categoria AS STRING)), '') AS category,
            NULLIF(TRIM(CAST(comercio AS STRING)), '') AS merchant,
            SAFE_CAST(monto_total AS FLOAT64) AS amount
          FROM {self._fqn(self.gastos_view)}
        )
        SELECT COALESCE(category, 'Sin categorizar') AS category,
               COALESCE(merchant, 'Sin comercio') AS merchant,
               SUM(amount) AS total_amount,
               COUNT(*) AS transaction_count
        FROM expenses
        WHERE expense_date BETWEEN @start AND @end
          AND amount > 0
          AND (@category IS NULL OR LOWER(category) = LOWER(@category))
        GROUP BY category, merchant
        ORDER BY total_amount DESC
        """
        rows = self._query(sql, [
            bigquery.ScalarQueryParameter("start", "DATE", period.start),
            bigquery.ScalarQueryParameter("end", "DATE", period.end),
            bigquery.ScalarQueryParameter("category", "STRING", category),
        ])
        by_category: dict[str, float] = {}
        by_merchant: dict[str, float] = {}
        count = 0
        for row in rows:
            amount = float(row.get("total_amount") or 0)
            category_name = str(row.get("category") or "Sin categorizar")
            merchant = str(row.get("merchant") or "Sin comercio")
            by_category[category_name] = by_category.get(category_name, 0.0) + amount
            by_merchant[merchant] = by_merchant.get(merchant, 0.0) + amount
            count += int(row.get("transaction_count") or 0)
        return {
            "period": period.model_dump(mode="json"),
            "category_filter": category,
            "currency": "ARS",
            "transaction_count": count,
            "total_amount": round(sum(by_category.values()), 2),
            "by_category": by_category,
            "by_merchant": by_merchant,
        }

    def compare_periods(self, current_period: DateRange, previous_period: DateRange) -> dict[str, Any]:
        current = self.get_spending_summary(current_period)
        previous = self.get_spending_summary(previous_period)
        difference = round(current["total_amount"] - previous["total_amount"], 2)
        percentage = round((difference / previous["total_amount"]) * 100, 2) if previous["total_amount"] else None
        return {
            "current_period": current_period.model_dump(mode="json"),
            "previous_period": previous_period.model_dump(mode="json"),
            "currency": "ARS",
            "current_total": current["total_amount"],
            "previous_total": previous["total_amount"],
            "difference": difference,
            "percentage_change": percentage,
            "current_by_category": current["by_category"],
            "previous_by_category": previous["by_category"],
        }

    def find_uncategorized_or_suspicious_transactions(self, period: DateRange) -> dict[str, Any]:
        from google.cloud import bigquery

        sql = f"""
        WITH expenses AS (
          SELECT
            CAST(transaction_key AS STRING) AS transaction_id,
            {self._date_expr()} AS expense_date,
            NULLIF(TRIM(CAST(comercio AS STRING)), '') AS merchant,
            NULLIF(TRIM(CAST(categoria AS STRING)), '') AS category,
            SAFE_CAST(monto_total AS FLOAT64) AS amount
          FROM {self._fqn(self.gastos_view)}
        )
        SELECT transaction_id, expense_date, merchant, category, amount,
               IF(category IS NULL, 'missing_category', 'review_amount') AS reason
        FROM expenses
        WHERE expense_date BETWEEN @start AND @end
          AND (category IS NULL OR amount IS NULL OR amount <= 0)
        ORDER BY expense_date DESC
        LIMIT 100
        """
        rows = self._query(sql, [
            bigquery.ScalarQueryParameter("start", "DATE", period.start),
            bigquery.ScalarQueryParameter("end", "DATE", period.end),
        ])
        transactions = []
        for row in rows:
            row["expense_date"] = row["expense_date"].isoformat() if row.get("expense_date") else None
            row["amount"] = float(row["amount"]) if row.get("amount") is not None else None
            transactions.append(row)
        return {"period": period.model_dump(mode="json"), "count": len(transactions), "transactions": transactions}

    def affected_transaction_count(self, merchant: str) -> int:
        from google.cloud import bigquery

        sql = f"""
        SELECT COUNT(*) AS affected_count
        FROM {self._fqn(self.gastos_view)}
        WHERE LOWER(TRIM(CAST(comercio AS STRING))) = LOWER(@merchant)
        """
        rows = self._query(sql, [bigquery.ScalarQueryParameter("merchant", "STRING", merchant)])
        return int(rows[0]["affected_count"]) if rows else 0


def repository_from_environment() -> FinanceRepository:
    mode = os.environ.get("FINANCEOPS_DATA_MODE", "synthetic").strip().lower()
    if mode == "synthetic":
        return SyntheticFinanceRepository()
    if mode == "bigquery":
        return ReadOnlyBigQueryRepository.from_environment()
    raise ValueError("FINANCEOPS_DATA_MODE must be 'synthetic' or 'bigquery'")
