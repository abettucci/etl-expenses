import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys

from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).parent))
from voice_expenses import (
    ManualExpenseIntent,
    PendingVoiceExpense,
    TelegramFileResponse,
    TelegramVoice,
    build_voice_callback,
    parse_voice_callback,
)


class ManualExpenseIntentTest(unittest.TestCase):
    def test_valid_voice_expense_is_normalized(self):
        expense = ManualExpenseIntent.model_validate({
            "intent": "create_manual_expense", "amount": "42000.00", "merchant": "  Peluquería  Centro ",
            "expense_date": "2026-08-27", "currency": "ARS",
        })
        self.assertEqual(expense.amount, Decimal("42000.00"))
        self.assertEqual(expense.merchant, "Peluquería Centro")
        self.assertEqual(expense.expense_date, date(2026, 8, 27))

    def test_rejects_invalid_or_ambiguous_payloads(self):
        with self.assertRaises(ValidationError):
            ManualExpenseIntent.model_validate({"intent": "create_manual_expense", "amount": 0, "merchant": "Peluquería", "expense_date": "2026-08-27", "currency": "ARS"})
        with self.assertRaises(ValidationError):
            ManualExpenseIntent.model_validate({"intent": "create_manual_expense", "amount": 42000, "merchant": "Peluquería 💈", "expense_date": "2026-08-27", "currency": "ARS"})
        with self.assertRaises(ValidationError):
            ManualExpenseIntent.model_validate({"intent": "create_manual_expense", "amount": 42000, "merchant": "Peluquería", "expense_date": "2026-08-27", "currency": "USD"})
        with self.assertRaises(ValidationError):
            ManualExpenseIntent.model_validate({"intent": "query_expenses", "amount": 42000, "merchant": "Peluquería", "expense_date": "2026-08-27", "currency": "ARS"})

    def test_accepts_only_telegram_ogg_or_opus_with_bounded_metadata(self):
        voice = TelegramVoice.model_validate({
            "file_id": "AwACAgQAAxkBAAIB", "mime_type": "AUDIO/OGG", "duration": 30, "file_size": 4096,
        })
        self.assertEqual(voice.mime_type, "audio/ogg")
        with self.assertRaises(ValidationError):
            TelegramVoice.model_validate({"file_id": "../bad", "mime_type": "audio/mpeg", "duration": 121, "file_size": 1})

    def test_confirmation_callback_is_opaque_and_allowlisted(self):
        token = "zPq8Z9u5E2J7S3rK6T1vM4nQ"
        callback = build_voice_callback("confirm", token)
        self.assertEqual(parse_voice_callback(callback), ("confirm", token))
        with self.assertRaises(ValueError):
            parse_voice_callback("ve:confirm:42;DROP TABLE")

    def test_rejects_unexpected_telegram_file_path(self):
        valid = TelegramFileResponse.model_validate({"ok": True, "result": {"file_path": "voice/file.ogg"}})
        self.assertEqual(valid.result["file_path"], "voice/file.ogg")
        with self.assertRaises(ValidationError):
            TelegramFileResponse.model_validate({"ok": True, "result": {"file_path": "../secret.ogg"}})

    def test_pending_expense_excludes_audio_and_transcript(self):
        pending = PendingVoiceExpense.model_validate({
            "confirmation_token": "zPq8Z9u5E2J7S3rK6T1vM4nQ",
            "chat_id": 123,
            "message_id": 456,
            "expires_at": 1_800_000_000,
            "expense": {
                "intent": "create_manual_expense",
                "amount": "42000",
                "merchant": "Buenos Aires Barbershop",
                "expense_date": "2026-08-27",
                "currency": "ARS",
            },
        })
        self.assertNotIn("transcript", pending.model_dump())
        self.assertNotIn("audio", pending.model_dump())


if __name__ == "__main__":
    unittest.main()
