"""Pure helpers for merchant mapping suggestions and Telegram review flows.

This module deliberately has no AWS, BigQuery, Telegram, or OpenAI imports so
the scoring and validation rules can be tested without cloud credentials.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import re
import unicodedata
from typing import Iterable, Mapping, Sequence

try:
    from rapidfuzz import fuzz
except ImportError:  # Lightweight fallback for local validation environments.
    from difflib import SequenceMatcher

    class _FuzzFallback:
        @staticmethod
        def ratio(left: str, right: str) -> float:
            return SequenceMatcher(None, left, right).ratio() * 100

        @staticmethod
        def token_sort_ratio(left: str, right: str) -> float:
            return _FuzzFallback.ratio(" ".join(sorted(left.split())), " ".join(sorted(right.split())))

        @staticmethod
        def token_set_ratio(left: str, right: str) -> float:
            return _FuzzFallback.ratio(" ".join(sorted(set(left.split()))), " ".join(sorted(set(right.split()))))

    fuzz = _FuzzFallback()


PROCESSOR_TOKENS = {
    "AR",
    "COMPRA",
    "CREDITO",
    "DEBITO",
    "DLOCAL",
    "MERPAGO",
    "MERCADOPAGO",
    "ONLINE",
    "PAGO",
    "PAYMENT",
    "POS",
    "WWW",
}

REVIEWABLE_STATUSES = {"pending", "suggested", "needs_manual"}
TERMINAL_STATUSES = {"confirmed", "rejected", "corrected"}


def normalize_merchant(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").upper()
    return re.sub(r"[^A-Z0-9]+", "", text)


def merchant_tokens(value: str | None) -> list[str]:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").upper()
    tokens = re.findall(r"[A-Z]+|\d+", text)
    return [
        token
        for token in tokens
        if token not in PROCESSOR_TOKENS
        and not token.isdigit()
        and len(token) >= 3
        and not re.fullmatch(r"[A-Z]*\d{4,}[A-Z0-9]*", token)
    ]


def merchant_fingerprint(value: str | None) -> str:
    return " ".join(merchant_tokens(value))


def candidate_id(flow: str, comercio_normalizado: str) -> str:
    raw = f"{flow.strip().lower()}|{normalize_merchant(comercio_normalizado)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


@dataclass(frozen=True)
class MappingSuggestion:
    match_type: str
    match_value: str
    comercio_depurado: str
    categoria: str
    subcategoria: str
    confidence: float
    source: str
    rationale: str
    matched_flow: str | None = None
    place_id: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)


def _row_value(row: Mapping | object, key: str, default=""):
    if isinstance(row, Mapping):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, TypeError):
        return getattr(row, key, default)


def _mapping_fingerprints(rule: Mapping | object) -> set[str]:
    values = {
        merchant_fingerprint(_row_value(rule, "match_value")),
        merchant_fingerprint(_row_value(rule, "comercio_raw")),
        merchant_fingerprint(_row_value(rule, "comercio_depurado")),
    }
    return {value for value in values if value}


def _similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 100.0
    return float(max(
        fuzz.ratio(left, right),
        fuzz.token_sort_ratio(left, right),
        fuzz.token_set_ratio(left, right),
    ))


def suggest_from_existing_mappings(
    comercio_raw: str,
    target_flow: str,
    mapping_rows: Iterable[Mapping | object],
) -> tuple[MappingSuggestion | None, float]:
    """Return the best confirmed mapping and its margin over runner-up.

    Partial-ratio is intentionally excluded because short processor fragments
    such as ``NAN`` or ``PAGO`` produce dangerous false positives.
    """
    fingerprint = merchant_fingerprint(comercio_raw)
    if len(normalize_merchant(fingerprint)) < 4:
        return None, 0.0

    scored: list[tuple[float, Mapping | object]] = []
    for rule in mapping_rows:
        if not bool(_row_value(rule, "activo", True)):
            continue
        if str(_row_value(rule, "review_status", "confirmed") or "confirmed") == "rejected":
            continue
        names = _mapping_fingerprints(rule)
        if not names:
            continue
        score = max(_similarity(fingerprint, name) for name in names)
        scored.append((score, rule))

    if not scored:
        return None, 0.0
    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    margin = round(best_score - second_score, 2)

    if best_score < 80:
        return None, margin
    suggestion = MappingSuggestion(
        match_type="exact",
        match_value=normalize_merchant(comercio_raw),
        comercio_depurado=str(_row_value(best, "comercio_depurado", comercio_raw) or comercio_raw).strip(),
        categoria=str(_row_value(best, "categoria", "") or "").strip(),
        subcategoria=str(_row_value(best, "subcategoria", "") or "").strip(),
        confidence=round(best_score, 2),
        source="existing_mapping",
        rationale=(
            f"Coincide con un comercio confirmado de {_row_value(best, 'flow', 'otro origen')} "
            f"(margen {margin:.1f} puntos)."
        ),
        matched_flow=str(_row_value(best, "flow", target_flow)),
    )
    if not suggestion.categoria:
        return None, margin
    return suggestion, margin


def validate_taxonomy_pair(
    categoria: str,
    subcategoria: str,
    taxonomy: Sequence[tuple[str, str]],
) -> tuple[str, str] | None:
    wanted_category = categoria.strip().casefold()
    wanted_subcategory = subcategoria.strip().casefold()
    if not wanted_category:
        return None
    for existing_category, existing_subcategory in taxonomy:
        existing_category = str(existing_category or "")
        existing_subcategory = str(existing_subcategory or "")
        category_matches = wanted_category == existing_category.strip().casefold()
        subcategory_matches = wanted_subcategory == existing_subcategory.strip().casefold()
        if category_matches and subcategory_matches:
            return existing_category, existing_subcategory
        # Subcategory is intentionally optional.  When omitted, accept an
        # existing category without inventing a more specific classification.
        if category_matches and not wanted_subcategory:
            return existing_category, ""
    return None


def suggestion_from_structured_payload(
    payload: Mapping,
    comercio_raw: str,
    taxonomy: Sequence[tuple[str, str]],
    *,
    source: str = "llm",
    place_id: str | None = None,
) -> MappingSuggestion | None:
    pair = validate_taxonomy_pair(
        str(payload.get("categoria") or ""),
        str(payload.get("subcategoria") or ""),
        taxonomy,
    )
    comercio = str(payload.get("comercio_depurado") or "").strip()
    if not pair or not comercio:
        return None

    overlap = _similarity(merchant_fingerprint(comercio_raw), merchant_fingerprint(comercio))
    if overlap >= 80:
        confidence = 86.0 if source == "llm" else 89.0
    elif overlap >= 60:
        confidence = 82.0 if source == "llm" else 85.0
    else:
        confidence = 75.0

    return MappingSuggestion(
        match_type="exact",
        match_value=normalize_merchant(comercio_raw),
        comercio_depurado=comercio,
        categoria=pair[0],
        subcategoria=pair[1],
        confidence=confidence,
        source=source,
        rationale=(
            "Nombre inferido por IA dentro de la taxonomía existente."
            if source == "llm"
            else "La inferencia fue corroborada de forma temporal con Google Places."
        ),
        place_id=place_id,
    )


def parse_json_object(raw: str) -> dict | None:
    value = (raw or "").strip()
    if value.startswith("```"):
        value = value.replace("```json", "").replace("```", "").strip()
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def parse_human_correction(text: str) -> dict[str, str] | None:
    """Parse explicit key/value corrections without guessing financial data."""
    aliases = {
        "nombre": "comercio_depurado",
        "comercio": "comercio_depurado",
        "depurado": "comercio_depurado",
        "categoria": "categoria",
        "subcategoria": "subcategoria",
    }
    result: dict[str, str] = {}
    for part in re.split(r"[;\n|]+", text or ""):
        match = re.match(r"\s*([^:=]+)\s*[:=]\s*(.+?)\s*$", part)
        if not match:
            continue
        key = normalize_merchant(match.group(1)).lower()
        key = aliases.get(key)
        value = match.group(2).strip()
        if key and value and len(value) <= 120:
            if key == "subcategoria" and value.casefold() in {
                "ninguna", "sin subcategoria", "sin subcategoría", "quitar", "vacia", "vacía"
            }:
                value = ""
            result[key] = value
    return result or None


def is_auto_applicable(suggestion: MappingSuggestion, margin: float) -> bool:
    return (
        suggestion.source == "existing_mapping"
        and suggestion.confidence >= 95.0
        and margin >= 10.0
    )
