"""Validation rules for Telegram voice expenses.

Only validated, structured data from this module is allowed to reach the
manual-expenses write path.  Audio and transcriptions deliberately have no
representation here so they cannot be persisted accidentally.
"""

import re
from datetime import date
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


VOICE_CALLBACK_PREFIX = "ve"
VOICE_CALLBACK_TOKEN_PATTERN = r"[A-Za-z0-9_-]{16,48}"


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

    @field_validator("merchant")
    @classmethod
    def merchant_must_be_readable(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9&.' -]+", normalized):
            raise ValueError("invalid merchant")
        return normalized


class PendingVoiceExpense(BaseModel):
    """The short-lived confirmation record stored in DynamoDB."""

    model_config = ConfigDict(extra="forbid")

    confirmation_token: str = Field(pattern=rf"^{VOICE_CALLBACK_TOKEN_PATTERN}$")
    chat_id: int = Field(strict=True)
    message_id: int = Field(ge=1, strict=True)
    expense: ManualExpenseIntent
    expires_at: int = Field(ge=1)


class TelegramVoiceCallback(BaseModel):
    """Minimum validated Telegram callback envelope for voice confirmations."""

    model_config = ConfigDict(extra="forbid")

    callback_id: str = Field(min_length=1, max_length=128)
    chat_id: int = Field(strict=True)
    callback_data: str = Field(min_length=1, max_length=64)


def build_voice_callback(action: Literal["confirm", "cancel"], token: str) -> str:
    """Build a compact Telegram callback payload with no financial data."""
    if not re.fullmatch(VOICE_CALLBACK_TOKEN_PATTERN, token):
        raise ValueError("invalid confirmation token")
    return f"{VOICE_CALLBACK_PREFIX}:{action}:{token}"


def parse_voice_callback(value: object) -> tuple[Literal["confirm", "cancel"], str]:
    """Parse only the two explicit actions allowed by this feature."""
    raw = str(value or "")
    match = re.fullmatch(rf"{VOICE_CALLBACK_PREFIX}:(confirm|cancel):({VOICE_CALLBACK_TOKEN_PATTERN})", raw)
    if not match:
        raise ValueError("invalid voice callback")
    return match.group(1), match.group(2)
