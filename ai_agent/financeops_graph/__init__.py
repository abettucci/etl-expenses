"""FinanceOps Copilot: isolated local LangGraph proof of concept.

This package deliberately has no dependency on the production Telegram or ETL
handlers. It is safe to import locally and defaults to synthetic data.
"""

from .graph import FinanceOpsCopilot
from .repositories import SyntheticFinanceRepository

__all__ = ["FinanceOpsCopilot", "SyntheticFinanceRepository"]
