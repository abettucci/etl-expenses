"""Validation rules for Telegram voice expenses.

Only validated, structured data from this module is allowed to reach the
manual-expenses write path.  Audio and transcriptions deliberately have no
representation here so they cannot be persisted accidentally.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


VOICE_CALLBACK_PREFIX = "ve"
VOICE_CALLBACK_TOKEN_PATTERN = r"[A-Za-z0-9_-]{16,48}"


_CORRECTION_PREFIX = re.compile(
    r"^\s*(?:agregale|agregá|agrega|cambiale|cambiá|cambia|"
    r"corregile|corregí|corrige|ponele|poné|pone|actualizale|actualizá|actualiza)\b"
    r"\s*(?:la|el)?\s*:?\s*",
    re.IGNORECASE,
)

_CLASSIFICATION_SUGGESTIONS = (
    (("car wash", "lavadero", "lavado de auto", "lavado auto"), ("Auto", "Lavado")),
    (("ypf", "shell", "axion", "puma energy", "nafta", "combustible"), ("Auto", "Combustible")),
    (("estacionamiento", "parking", "garage"), ("Auto", "Estacionamiento")),
    (("uber", "cabify", "didi", "taxi", "remis"), ("Transporte", "Viajes")),
    (("rappi", "pedidos ya", "pedidosya"), ("Comida", "Delivery")),
    (("carrefour", "coto", "disco", "jumbo", "dia ", "supermercado"), ("Comida", "Supermercado")),
    (("netflix", "spotify", "disney", "youtube premium"), ("Entretenimiento", "Streaming")),
    (("farmacia", "farmacity"), ("Salud", "Farmacia")),
)

_MERCHANT_SUGGESTIONS = (
    (("exclusive car wash",), "Exclusive Car Wash"),
    (("car wash",), "Car Wash"),
)

_NATURAL_CORRECTION = re.compile(
    r"^\s*(?:cambia|cambiá|cambiale|corregí|corregile|actualiza|actualizá|pone|poné)\s+"
    r"(?:el|la)?\s*(nombre\s+del\s+comercio|comercio|monto|importe|fecha|"
    r"categor[ií]a|subcategor[ií]a)\s+(?:a|por)\s+(.+?)\s*$",
    re.IGNORECASE,
)


class TelegramVoice(BaseModel):
    """Allowlisted metadata for a Telegram voice note.

    extra="ignore" (not "forbid"): Telegram always sends file_unique_id
    on every voice note, which this model never reads. Forbidding it
    rejected every real voice message before transcription ever ran.
    """

    model_config = ConfigDict(extra="ignore")

    file_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    mime_type: Literal["audio/ogg", "audio/opus"]
    duration: int = Field(ge=1, le=120, strict=True)
    file_size: int = Field(ge=1, le=10 * 1024 * 1024, strict=True)

    @field_validator("mime_type", mode="before")
    @classmethod
    def normalize_mime_type(cls, value: object) -> str:
        return str(value).lower()


class TelegramVoiceMessage(BaseModel):
    """Validated Telegram fields used by the voice-expense path."""

    model_config = ConfigDict(extra="forbid")

    chat_id: int = Field(strict=True)
    message_id: int = Field(ge=1, strict=True)
    voice: TelegramVoice


class TelegramFileResponse(BaseModel):
    """Only the Telegram file path needed to download an already-validated voice note."""

    model_config = ConfigDict(extra="ignore")

    ok: Literal[True]
    result: dict[str, Any]

    @field_validator("result")
    @classmethod
    def validate_voice_file_path(cls, value: dict[str, Any]) -> dict[str, str]:
        file_path = value.get("file_path", "")
        if not re.fullmatch(r"voice/[A-Za-z0-9_.-]+\.(?:ogg|oga)", file_path):
            raise ValueError("invalid Telegram voice file path")
        return {"file_path": file_path}


class ManualExpenseIntent(BaseModel):
    """The only speech-derived fields permitted to reach BigQuery."""

    model_config = ConfigDict(extra="forbid")

    intent: Literal["create_manual_expense"]
    amount: Decimal = Field(gt=0, max_digits=14, decimal_places=2)
    merchant: str = Field(min_length=2, max_length=100)
    expense_date: date
    currency: Literal["ARS"]
    # Optional rather than ``str | None``: the production Lambda image still
    # runs Python 3.9, whose Pydantic type resolver cannot evaluate PEP 604
    # unions from postponed annotations.
    category: Optional[str] = Field(default=None, max_length=80)
    subcategory: Optional[str] = Field(default=None, max_length=80)

    @field_validator("merchant")
    @classmethod
    def merchant_must_be_readable(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9&.' -]+", normalized):
            raise ValueError("invalid merchant")
        return normalized

    @field_validator("category", "subcategory")
    @classmethod
    def classification_must_be_readable(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        if not normalized:
            return None
        if not re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9&.' /-]+", normalized):
            raise ValueError("invalid classification")
        return normalized


def _parse_ars_amount(raw_value: str) -> Decimal:
    value = raw_value.strip().replace("$", "").replace("ARS", "").replace("ars", "")
    value = re.sub(r"\s+", "", value)
    if not re.fullmatch(r"\d[\d.,]*", value):
        raise ValueError("invalid amount")
    last_group = re.search(r"[.,](\d+)$", value)
    if last_group and len(last_group.group(1)) in (1, 2):
        integer_part = re.sub(r"[.,]", "", value[: last_group.start()])
        value = f"{integer_part}.{last_group.group(1)}"
    else:
        value = re.sub(r"[.,]", "", value)
    try:
        amount = Decimal(value).quantize(Decimal("0.01"))
    except InvalidOperation as exc:
        raise ValueError("invalid amount") from exc
    if amount <= 0:
        raise ValueError("invalid amount")
    return amount


def parse_voice_expense_correction(text: object) -> dict[str, object] | None:
    """Parse labelled, partial corrections for a pending voice expense.

    Corrections deliberately use explicit labels so a normal question sent while
    a confirmation is open does not get mistaken for a financial update.
    Accepted example: ``Comercio: Lavadero\nCategoría: Auto\nSubcategoría: Lavado``.
    """
    raw_text = str(text or "").strip()
    if not raw_text or len(raw_text) > 1000:
        return None
    natural_match = _NATURAL_CORRECTION.fullmatch(raw_text)
    if natural_match:
        natural_label = natural_match.group(1).lower()
        raw_text = f"{'Comercio' if natural_label.startswith('nombre') else natural_match.group(1)}: {natural_match.group(2)}"
    # "Agregale categoría: Auto" and "Agregale: Categoría: Auto" are
    # corrections too. Remove only a leading imperative phrase; a normal
    # expense query remains untouched and can continue through the SQL flow.
    raw_text = _CORRECTION_PREFIX.sub("", raw_text, count=1)

    labels = {
        "comercio": "merchant",
        "merchant": "merchant",
        "monto": "amount",
        "importe": "amount",
        "fecha": "expense_date",
        "categoria": "category",
        "categoría": "category",
        "subcategoria": "subcategory",
        "subcategoría": "subcategory",
    }
    pattern = re.compile(
        r"(?:^|[\n;])\s*(comercio|merchant|monto|importe|fecha|categor[ií]a|subcategor[ií]a)\s*[:=;]\s*"
        r"(.*?)(?=(?:[\n;]\s*(?:comercio|merchant|monto|importe|fecha|categor[ií]a|subcategor[ií]a)\s*[:=;])|$)",
        re.IGNORECASE | re.DOTALL,
    )
    patch: dict[str, object] = {}
    for match in pattern.finditer(raw_text):
        field = labels[match.group(1).strip().lower()]
        value = " ".join(match.group(2).split())
        if not value:
            raise ValueError(f"missing {field}")
        if field == "amount":
            patch[field] = _parse_ars_amount(value)
        elif field == "expense_date":
            parsed_date = None
            for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
                try:
                    parsed_date = datetime.strptime(value, fmt).date()
                    break
                except ValueError:
                    continue
            if parsed_date is None:
                raise ValueError("invalid date")
            patch[field] = parsed_date
        elif field == "merchant":
            patch[field] = ManualExpenseIntent.model_validate({
                "intent": "create_manual_expense",
                "amount": "1",
                "merchant": value,
                "expense_date": date.today(),
                "currency": "ARS",
            }).merchant
        else:
            validated = ManualExpenseIntent.model_validate({
                "intent": "create_manual_expense",
                "amount": "1",
                "merchant": "Gasto manual",
                "expense_date": date.today(),
                "currency": "ARS",
                field: value,
            })
            patch[field] = getattr(validated, field)
    return patch or None


def suggest_manual_expense_classification(merchant: str) -> tuple[str, str] | None:
    """Return a transparent rule-based category suggestion for a merchant.

    Suggestions are never persisted automatically; the person can accept or
    override them in the confirmation preview.
    """
    normalized = unicodedata.normalize("NFKD", merchant).encode("ascii", "ignore").decode("ascii").lower()
    for keywords, suggestion in _CLASSIFICATION_SUGGESTIONS:
        if any(keyword in normalized for keyword in keywords):
            return suggestion
    return None


def suggest_manual_expense_values(merchant: str) -> dict[str, str]:
    """Return optional, rule-based values that a person can explicitly apply.

    The caller must present these values and request confirmation; this helper
    never mutates an expense by itself.
    """
    normalized = unicodedata.normalize("NFKD", merchant).encode("ascii", "ignore").decode("ascii").lower()
    values: dict[str, str] = {}
    for keywords, suggested_merchant in _MERCHANT_SUGGESTIONS:
        if any(keyword in normalized for keyword in keywords):
            if normalized != suggested_merchant.lower():
                values["merchant"] = suggested_merchant
            break
    classification = suggest_manual_expense_classification(merchant)
    if classification:
        values["category"], values["subcategory"] = classification
    return values


_SPANISH_MONTHS_AND_WEEKDAYS = (
    r"\b(lunes|martes|miércoles|jueves|viernes|sábado|domingo|"
    r"enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\b"
    r"|\bel\s+\d{1,2}\b"
)


def extract_manual_expense_regex(transcript: str, today: date) -> ManualExpenseIntent | None:
    """Rule-based parser for the canonical "<amount> pesos <merchant>" voice format.

    Returns None (never raises for "didn't match") whenever the transcript is
    ambiguous or outside this simple shape, so the caller can fall back to an
    LLM-based extraction instead of forcing a bad match.
    """
    text = " ".join(transcript.lower().split())
    if re.search(_SPANISH_MONTHS_AND_WEEKDAYS, text):
        return None  # fecha explicita no trivial -> que la resuelva el LLM

    # El monto siempre va al principio del mensaje: "<numero> pesos <comercio>",
    # "$<numero> <comercio>", o directamente "<numero> <comercio>" si el motor de
    # STT no agrego ninguna marca de moneda (distintos motores/versiones transcriben
    # el mismo monto hablado de formas distintas: "32.000"/"32,000"/"$32000"/"32000").
    # El marcador de moneda es opcional a proposito para no depender de que un motor
    # de STT elija una forma en particular.
    match = re.match(r"\$?\s*(\d[\d.,]*)\s*(?:pesos|ars)?\s*", text)
    if not match:
        return None  # el mensaje no arranca con un monto reconocible
    raw_amount = match.group(1)
    if re.search(r"\d", text[match.end():]):
        return None  # hay otro numero mas adelante -> ambiguo
    # El separador decimal solo se reconoce si el ultimo grupo tiene 1-2 digitos
    # (centavos). Un grupo de 3 digitos es agrupador de miles, sea "." (Whisper,
    # "32.000") o "," (Google STT, "32,000") -- ambos motores transcriben el
    # mismo monto hablado con separadores distintos.
    last_group = re.search(r"[.,](\d+)$", raw_amount)
    if last_group and len(last_group.group(1)) in (1, 2):
        integer_part = re.sub(r"[.,]", "", raw_amount[: last_group.start()])
        amount_str = f"{integer_part}.{last_group.group(1)}"
    else:
        amount_str = re.sub(r"[.,]", "", raw_amount)
    try:
        amount = Decimal(amount_str).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
    if amount <= 0:
        return None

    merchant_part = text[match.end():]

    expense_date = today
    if re.search(r"\banteayer\b", merchant_part):
        expense_date = today - timedelta(days=2)
        merchant_part = re.sub(r"\banteayer\b", "", merchant_part)
    elif re.search(r"\bayer\b", merchant_part):
        expense_date = today - timedelta(days=1)
        merchant_part = re.sub(r"\bayer\b", "", merchant_part)
    elif re.search(r"\bhoy\b", merchant_part):
        merchant_part = re.sub(r"\bhoy\b", "", merchant_part)

    merchant = " ".join(merchant_part.split())
    try:
        return ManualExpenseIntent(
            intent="create_manual_expense",
            amount=amount,
            merchant=merchant,
            expense_date=expense_date,
            currency="ARS",
        )
    except (ValidationError, ValueError):
        return None


class PendingVoiceExpense(BaseModel):
    """The short-lived confirmation record stored in DynamoDB."""

    model_config = ConfigDict(extra="forbid")

    confirmation_token: str = Field(pattern=rf"^{VOICE_CALLBACK_TOKEN_PATTERN}$")
    chat_id: int = Field(strict=True)
    message_id: int = Field(ge=1, strict=True)
    expense_id: Optional[str] = Field(default=None, pattern=r"^[0-9a-f-]{36}$")
    expense: ManualExpenseIntent
    expires_at: int = Field(ge=1)


class TelegramVoiceCallback(BaseModel):
    """Minimum validated Telegram callback envelope for voice confirmations."""

    model_config = ConfigDict(extra="forbid")

    callback_id: str = Field(min_length=1, max_length=128)
    chat_id: int = Field(strict=True)
    callback_data: str = Field(min_length=1, max_length=64)


def build_voice_callback(action: Literal["confirm", "cancel", "suggestion"], token: str) -> str:
    """Build a compact Telegram callback payload with no financial data."""
    if not re.fullmatch(VOICE_CALLBACK_TOKEN_PATTERN, token):
        raise ValueError("invalid confirmation token")
    return f"{VOICE_CALLBACK_PREFIX}:{action}:{token}"


def parse_voice_callback(value: object) -> tuple[Literal["confirm", "cancel", "suggestion"], str]:
    """Parse only the explicit actions allowed by the confirmation flow."""
    raw = str(value or "")
    match = re.fullmatch(rf"{VOICE_CALLBACK_PREFIX}:(confirm|cancel|suggestion):({VOICE_CALLBACK_TOKEN_PATTERN})", raw)
    if not match:
        raise ValueError("invalid voice callback")
    return match.group(1), match.group(2)
