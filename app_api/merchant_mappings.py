"""Pure validation and suggestion helpers for the owner-only mapping UI."""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping


SUPPORTED_FLOWS = {
    "bank_payments",
    "bank_transfers",
    "mp_data",
    "mp_transfer_data",
    "supermarket_receipts",
    "carrefour_data",
}

FLOW_LABELS = {
    "bank_payments": "Tarjetas y débitos",
    "bank_transfers": "Transferencias bancarias",
    "mp_data": "Mercado Pago",
    "mp_transfer_data": "Transferencias de Mercado Pago",
    "supermarket_receipts": "Tickets de supermercado",
    "carrefour_data": "Carrefour",
}

PLACE_TYPE_HINTS = {
    "restaurant": "Comida",
    "cafe": "Comida",
    "bakery": "Comida",
    "grocery_store": "Comida",
    "supermarket": "Comida",
    "meal_takeaway": "Comida",
    "gas_station": "Auto",
    "car_wash": "Auto",
    "car_repair": "Auto",
    "parking": "Auto",
    "clothing_store": "Compras",
    "shopping_mall": "Compras",
    "pharmacy": "Salud",
    "dentist": "Salud",
    "hospital": "Salud",
    "gym": "Bienestar",
    "beauty_salon": "Bienestar",
    "movie_theater": "Entretenimiento",
    "night_club": "Entretenimiento",
}


def normalize_merchant(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def suggest_existing_mapping(
    raw: str,
    flow: str,
    mappings: Iterable[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Return a same-source existing rule only when the match is meaningful."""
    fingerprint = normalize_merchant(raw)
    if len(fingerprint) < 4:
        return None
    ranked: list[tuple[float, Mapping[str, Any]]] = []
    for row in mappings:
        if str(row.get("flow") or "") != flow:
            continue
        candidates = {
            normalize_merchant(row.get("comercio_depurado", "")),
            normalize_merchant(row.get("comercio_raw", "")),
            normalize_merchant(row.get("match_value", "")),
        } - {""}
        if not candidates:
            continue
        score = max(SequenceMatcher(None, fingerprint, value).ratio() for value in candidates)
        ranked.append((score, row))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    if not ranked or ranked[0][0] < 0.8:
        return None
    score, row = ranked[0]
    margin = score - (ranked[1][0] if len(ranked) > 1 else 0)
    return {
        "flow": flow,
        "match_type": str(row.get("match_type") or "contains"),
        "match_value": str(row.get("match_value") or ""),
        "merchant": str(row.get("comercio_depurado") or ""),
        "category": str(row.get("categoria") or ""),
        "subcategory": str(row.get("subcategoria") or ""),
        "confidence": round(score * 100, 1),
        "margin": round(margin * 100, 1),
        "source": "existing_mapping",
    }


def category_hint_for_place(primary_type: str, categories: Iterable[str]) -> str | None:
    """Use a Google place type only if it maps to a category already in the user's taxonomy."""
    hint = PLACE_TYPE_HINTS.get(str(primary_type or "").casefold())
    if not hint:
        return None
    choices = [str(category) for category in categories if str(category).strip()]
    exact = next((value for value in choices if value.casefold() == hint.casefold()), None)
    if exact:
        return exact
    family_terms = {
        "comida": ("comida", "alimento", "supermercado", "restaurante"),
        "auto": ("auto", "vehiculo", "vehículo", "transporte", "combustible"),
        "compras": ("compra", "indumentaria", "ropa", "hogar"),
        "salud": ("salud", "farmacia", "medico", "médico"),
        "bienestar": ("bienestar", "salud", "deporte"),
        "entretenimiento": ("entretenimiento", "ocio", "salida"),
    }
    terms = family_terms.get(hint.casefold(), (hint.casefold(),))
    return next(
        (value for value in choices if any(term in value.casefold() for term in terms)),
        None,
    )


def validate_mapping_payload(payload: Mapping[str, Any]) -> dict[str, str | None]:
    flow = str(payload.get("flow") or "").strip()
    if flow not in SUPPORTED_FLOWS:
        raise ValueError("Elegí un origen de gastos válido.")
    match_value = str(payload.get("match_value") or "").strip()
    raw_merchant = str(payload.get("raw_merchant") or match_value).strip()
    merchant = str(payload.get("merchant") or "").strip()
    category = str(payload.get("category") or "").strip()
    subcategory = str(payload.get("subcategory") or "").strip() or None
    match_type = str(payload.get("match_type") or "contains").strip().lower()
    candidate_id = str(payload.get("candidate_id") or "").strip() or None

    if not normalize_merchant(match_value):
        raise ValueError("El texto a buscar no puede quedar vacío.")
    if not raw_merchant or len(raw_merchant) > 200:
        raise ValueError("El nombre original del comercio debe tener hasta 200 caracteres.")
    if not merchant or len(merchant) > 160:
        raise ValueError("El nombre del comercio debe tener entre 1 y 160 caracteres.")
    if not category or len(category) > 80:
        raise ValueError("Elegí una categoría válida.")
    if subcategory and len(subcategory) > 100:
        raise ValueError("La subcategoría no puede superar 100 caracteres.")
    if match_type not in {"contains", "exact", "regex", "fuzzy"} and not re.fullmatch(r"fuzzy:(?:[5-9][0-9]|100)", match_type):
        raise ValueError("El tipo de coincidencia no es válido.")
    if match_type == "regex":
        try:
            re.compile(match_value)
        except re.error as exc:
            raise ValueError("La expresión regular no es válida.") from exc
    if candidate_id and not re.fullmatch(r"[A-Fa-f0-9]{20}", candidate_id):
        raise ValueError("El candidato seleccionado no es válido.")
    return {
        "flow": flow,
        "match_type": match_type,
        "match_value": match_value,
        "raw_merchant": raw_merchant,
        "merchant": merchant,
        "category": category,
        "subcategory": subcategory,
        "candidate_id": candidate_id,
    }
