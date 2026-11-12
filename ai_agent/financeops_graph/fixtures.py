"""Synthetic fixture data. It contains no user or production financial data."""

from __future__ import annotations

from datetime import date


SYNTHETIC_TRANSACTIONS = [
    {
        "transaction_id": "demo-001",
        "expense_date": date(2026, 8, 3),
        "merchant": "Acme Cloud",
        "category": "Software",
        "amount": 12000.0,
        "currency": "ARS",
        "suspicious_reason": None,
    },
    {
        "transaction_id": "demo-002",
        "expense_date": date(2026, 8, 11),
        "merchant": "Code Forge",
        "category": "Software",
        "amount": 8000.0,
        "currency": "ARS",
        "suspicious_reason": None,
    },
    {
        "transaction_id": "demo-003",
        "expense_date": date(2026, 9, 2),
        "merchant": "Acme Cloud",
        "category": "Software",
        "amount": 24000.0,
        "currency": "ARS",
        "suspicious_reason": "amount_increased_from_prior_month",
    },
    {
        "transaction_id": "demo-004",
        "expense_date": date(2026, 9, 9),
        "merchant": "Code Forge",
        "category": "Software",
        "amount": 16000.0,
        "currency": "ARS",
        "suspicious_reason": None,
    },
    {
        "transaction_id": "demo-005",
        "expense_date": date(2026, 9, 14),
        "merchant": "Merchant X",
        "category": None,
        "amount": 6500.0,
        "currency": "ARS",
        "suspicious_reason": "missing_category",
    },
    {
        "transaction_id": "demo-006",
        "expense_date": date(2026, 9, 18),
        "merchant": "Merchant X",
        "category": None,
        "amount": 6500.0,
        "currency": "ARS",
        "suspicious_reason": "possible_duplicate",
    },
    {
        "transaction_id": "demo-007",
        "expense_date": date(2026, 9, 21),
        "merchant": "Transit Demo",
        "category": "Transport",
        "amount": 4300.0,
        "currency": "ARS",
        "suspicious_reason": None,
    },
]
