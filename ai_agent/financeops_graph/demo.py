"""Run deterministic local FinanceOps Copilot demos."""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any

from langgraph.types import Command

from .graph import FinanceOpsCopilot
from .repositories import repository_from_environment


SCENARIOS = {
    "software-increase": "Why did my software expenses increase this month?",
    "uncategorized": "Show transactions that need categorization.",
    "draft-mapping": "Create a rule for merchant Merchant X as Software.",
}


def _json_default(value: Any):
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the isolated FinanceOps Copilot POC.")
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    args = parser.parse_args(argv)

    repository = repository_from_environment()
    copilot = FinanceOpsCopilot(repository, reference_date=date(2026, 9, 30))
    thread_id = f"demo-{args.scenario}"
    config = {"configurable": {"thread_id": thread_id}}
    result = copilot.graph.invoke(
        {"thread_id": thread_id, "user_question": SCENARIOS[args.scenario]},
        config=config,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))

    if args.scenario == "draft-mapping" and result.get("__interrupt__"):
        approved = copilot.graph.invoke(Command(resume={"action": "approve"}), config=config)
        print("\n--- approved in simulation ---")
        print(json.dumps(approved, ensure_ascii=False, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
