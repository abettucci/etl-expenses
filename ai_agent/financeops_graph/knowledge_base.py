"""Small local policy corpus used for deterministic RAG-like retrieval."""

from __future__ import annotations

from collections import Counter


KNOWLEDGE_BASE = [
    {
        "id": "data-dictionary-gastos-totales",
        "title": "Diccionario de gastos_totales",
        "content": (
            "La vista gastos_totales consolida gastos. fecha representa la fecha del gasto, "
            "monto_total el importe, comercio el nombre del comercio y categoria la clasificación. "
            "Las consultas deben tratar fechas y montos vacíos de forma segura."
        ),
    },
    {
        "id": "categorization-guidelines",
        "title": "Guía de categorización",
        "content": (
            "Una categoría debe describir el propósito económico del gasto. Software incluye "
            "suscripciones SaaS, infraestructura cloud, herramientas de desarrollo y productividad. "
            "No se debe confirmar un mapeo ambiguo sin evidencia y revisión humana."
        ),
    },
    {
        "id": "merchant-mapping-examples",
        "title": "Ejemplos de reglas de comercio",
        "content": (
            "Ejemplo: comercio normalizado Acme Cloud, categoría Software, motivo suscripción de infraestructura. "
            "Ejemplo: Transit Demo, categoría Transport. Las reglas se preparan como borradores y nunca se "
            "aplican automáticamente."
        ),
    },
]


def search_local_policy(query: str, limit: int = 3) -> list[dict[str, str]]:
    """Return a deterministic token-overlap ranking without embeddings or network calls."""
    tokens = {token.lower() for token in query.split() if len(token) > 2}
    scored = []
    for document in KNOWLEDGE_BASE:
        document_tokens = Counter(document["content"].lower().split())
        score = sum(document_tokens[token] for token in tokens)
        scored.append((score, document))
    scored.sort(key=lambda item: (-item[0], item[1]["id"]))
    return [document for _, document in scored[:limit]]
