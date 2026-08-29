"""Unit tests for the voice-expense orchestration with external services mocked."""

import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


AI_AGENT_DIR = Path(__file__).parent
sys.path.insert(0, str(AI_AGENT_DIR))


class FakeTable:
    def __init__(self):
        self.item = None
        self.deleted = []

    def get_item(self, **_kwargs):
        return {"Item": self.item} if self.item else {}

    def delete_item(self, **kwargs):
        self.deleted.append(kwargs["Key"]["confirmation_token"])
        self.item = None


class FakeDynamo:
    def __init__(self, table):
        self.table = table

    def Table(self, _name):
        return self.table


def load_lambda_module():
    """Import the handler without installing or reaching external cloud SDKs."""
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.resource = lambda *_args, **_kwargs: types.SimpleNamespace(Table=lambda _name: FakeTable())
    fake_boto3.client = lambda *_args, **_kwargs: types.SimpleNamespace()

    fake_openai = types.ModuleType("openai")
    fake_openai.OpenAI = lambda **_kwargs: types.SimpleNamespace()

    fake_bigquery = types.ModuleType("google.cloud.bigquery")
    fake_service_account = types.ModuleType("google.oauth2.service_account")
    fake_cloud = types.ModuleType("google.cloud")
    fake_cloud.bigquery = fake_bigquery
    fake_oauth2 = types.ModuleType("google.oauth2")
    fake_oauth2.service_account = fake_service_account
    fake_google = types.ModuleType("google")
    fake_google.cloud = fake_cloud
    fake_google.oauth2 = fake_oauth2

    fake_requests = types.ModuleType("requests")
    fake_requests.get = lambda *_args, **_kwargs: None
    fake_requests.post = lambda *_args, **_kwargs: None

    fake_variations = types.ModuleType("variation_alerts")
    fake_variations.completed_comparison_periods = lambda *_args, **_kwargs: None
    fake_variations.exceeds_variation_threshold = lambda *_args, **_kwargs: False

    required_env = {
        "TELEGRAM_BOT_TOKEN": "test-token",
        "OPENAI_API_KEY": "test-openai-key",
        "GCP_PROJECT_ID": "test-project",
        "TELEGRAM_ALLOWED_CHAT_ID": "12345",
        "TELEGRAM_WEBHOOK_SECRET": "test-webhook-secret",
    }
    with patch.dict(os.environ, required_env, clear=False), patch.dict(sys.modules, {
        "boto3": fake_boto3,
        "openai": fake_openai,
        "requests": fake_requests,
        "google": fake_google,
        "google.cloud": fake_cloud,
        "google.cloud.bigquery": fake_bigquery,
        "google.oauth2": fake_oauth2,
        "google.oauth2.service_account": fake_service_account,
        "variation_alerts": fake_variations,
    }):
        spec = importlib.util.spec_from_file_location("voice_lambda_under_test", AI_AGENT_DIR / "lambda_function.py")
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


class VoiceExpenseFlowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_lambda_module()

    def setUp(self):
        self.table = FakeTable()
        self.module.dynamo = FakeDynamo(self.table)
        self.event = {"headers": {"X-Telegram-Bot-Api-Secret-Token": "test-webhook-secret"}}
        self.message = {
            "message_id": 456,
            "voice": {"file_id": "AwACAgQAAxkBAAIB", "mime_type": "audio/ogg", "duration": 30, "file_size": 4096},
        }
        self.expense = self.module.ManualExpenseIntent.model_validate({
            "intent": "create_manual_expense",
            "amount": "42000",
            "merchant": "Buenos Aires Barbershop",
            "expense_date": "2026-08-27",
            "currency": "ARS",
        })

    def test_voice_creates_preview_but_does_not_write_to_bigquery(self):
        with patch.object(self.module, "try_acquire_message_lock", return_value=True), \
             patch.object(self.module, "_download_telegram_voice", return_value=b"OggSvoice"), \
             patch.object(self.module, "_transcribe_voice", return_value="Gasté 42 mil"), \
             patch.object(self.module, "_extract_manual_expense", return_value=self.expense), \
             patch.object(self.module, "_save_pending_voice_expense", return_value="zPq8Z9u5E2J7S3rK6T1vM4nQ"), \
             patch.object(self.module, "_store_manual_expense") as store:
            text, keyboard = self.module.process_telegram_voice(self.event, self.message, 12345)

        self.assertIn("Buenos Aires Barbershop", text)
        self.assertIn("42.000", text)
        self.assertEqual(keyboard["inline_keyboard"][0][0]["callback_data"], "ve:confirm:zPq8Z9u5E2J7S3rK6T1vM4nQ")
        store.assert_not_called()

    def test_unauthorized_voice_never_downloads_audio(self):
        with patch.object(self.module, "_download_telegram_voice") as download:
            text, keyboard = self.module.process_telegram_voice({"headers": {}}, self.message, 12345)
        self.assertEqual(text, "No pude procesar este mensaje de voz.")
        self.assertIsNone(keyboard)
        download.assert_not_called()

    def test_confirmed_callback_writes_once_and_retry_does_not_duplicate(self):
        token = "zPq8Z9u5E2J7S3rK6T1vM4nQ"
        self.table.item = {
            "confirmation_token": token,
            "chat_id": "12345",
            "message_id": "456",
            "expense": self.expense.model_dump(mode="json"),
            "expires_at": 2_000_000_000,
        }
        update = {"callback_query": {"id": "callback-1", "data": f"ve:confirm:{token}", "message": {"chat": {"id": 12345}}}}
        with patch.object(self.module, "get_bigquery_client", return_value=object()), \
             patch.object(self.module, "_store_manual_expense") as store, \
             patch.object(self.module, "telegram_answer_callback"), \
             patch.object(self.module, "send_telegram_message"):
            self.module.handle_telegram_voice_callback(self.event, update)
            self.module.handle_telegram_voice_callback(self.event, update)

        store.assert_called_once()
        self.assertEqual(self.table.deleted, [token])

    def test_cancelled_or_expired_callback_does_not_write(self):
        token = "zPq8Z9u5E2J7S3rK6T1vM4nQ"
        self.table.item = {
            "confirmation_token": token,
            "chat_id": "12345",
            "message_id": "456",
            "expense": self.expense.model_dump(mode="json"),
            "expires_at": 2_000_000_000,
        }
        update = {"callback_query": {"id": "callback-1", "data": f"ve:cancel:{token}", "message": {"chat": {"id": 12345}}}}
        with patch.object(self.module, "_store_manual_expense") as store, \
             patch.object(self.module, "telegram_answer_callback"), \
             patch.object(self.module, "send_telegram_message"):
            self.module.handle_telegram_voice_callback(self.event, update)
            self.module.handle_telegram_voice_callback(self.event, update)

        store.assert_not_called()
        self.assertEqual(self.table.deleted, [token])


if __name__ == "__main__":
    unittest.main()
