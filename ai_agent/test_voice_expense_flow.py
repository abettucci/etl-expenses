"""Unit tests for the voice-expense orchestration with external services mocked."""

import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from datetime import date
from unittest.mock import patch


AI_AGENT_DIR = Path(__file__).parent
sys.path.insert(0, str(AI_AGENT_DIR))


class FakeTable:
    def __init__(self):
        self.item = None
        self.deleted = []

    def get_item(self, **_kwargs):
        return {"Item": self.item} if self.item else {}

    def put_item(self, **kwargs):
        self.item = kwargs["Item"]

    def scan(self, **kwargs):
        expected_chat = kwargs.get("ExpressionAttributeValues", {}).get(":chat_id")
        items = [self.item] if self.item and self.item.get("chat_id") == expected_chat else []
        return {"Items": items}

    def update_item(self, **kwargs):
        if not self.item or self.item.get("confirmation_token") != kwargs["Key"]["confirmation_token"]:
            raise ValueError("missing pending expense")
        self.item["expense"] = kwargs["ExpressionAttributeValues"][":expense"]

    def delete_item(self, **kwargs):
        key = kwargs["Key"]
        self.deleted.append(key.get("confirmation_token") or key.get("chat_id"))
        self.item = None


class FakeDynamo:
    def __init__(self, table):
        self.tables = {
            "telegram_pending_voice_expenses": table,
            "telegram_recent_manual_expenses": FakeTable(),
        }

    def Table(self, name):
        return self.tables.setdefault(name, FakeTable())


def load_lambda_module():
    """Import the handler without installing or reaching external cloud SDKs."""
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.resource = lambda *_args, **_kwargs: types.SimpleNamespace(Table=lambda _name: FakeTable())
    fake_boto3.client = lambda *_args, **_kwargs: types.SimpleNamespace()

    fake_openai = types.ModuleType("openai")
    fake_openai.OpenAI = lambda **_kwargs: types.SimpleNamespace()

    fake_bigquery = types.ModuleType("google.cloud.bigquery")
    fake_bigquery.QueryJobConfig = lambda query_parameters: types.SimpleNamespace(
        query_parameters=query_parameters
    )
    fake_bigquery.ScalarQueryParameter = lambda name, _type, value: types.SimpleNamespace(
        name=name, value=value
    )
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
        self.edit_table = self.module.dynamo.Table("telegram_recent_manual_expenses")
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

    def test_variation_query_resolves_mapping_table_placeholder(self):
        periods = types.SimpleNamespace(
            current_start=date(2026, 9, 7),
            current_end=date(2026, 9, 13),
            previous_start=date(2026, 8, 31),
            previous_end=date(2026, 9, 6),
        )
        query, _config = self.module._variation_query(
            periods,
            use_mapping=True,
            gastos_columns={"categoria", "subcategoria"},
            mapping_columns={"comercio_raw", "comercio_depurado", "activo", "categoria", "subcategoria"},
        )
        self.assertNotIn("{mapping}", query)
        self.assertIn("`test-project.PRD.dim_comercio_mapping`", query)
        self.assertIn("'categoria' AS dimension_type", query)
        self.assertIn("'subcategoria' AS dimension_type", query)

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

    def test_text_correction_updates_pending_preview_without_writing(self):
        token = "zPq8Z9u5E2J7S3rK6T1vM4nQ"
        self.table.item = {
            "confirmation_token": token,
            "chat_id": "12345",
            "message_id": "456",
            "expense": self.expense.model_dump(mode="json"),
            "expires_at": 2_000_000_000,
        }
        with patch.object(self.module, "_store_manual_expense") as store:
            handled, text, keyboard = self.module.handle_pending_voice_expense_correction(
                self.event,
                12345,
                "Agregale: Comercio: Exclusive Car Wash\nCategoría: Auto\nSubcategoría: Lavado",
            )

        self.assertTrue(handled)
        self.assertIn("Exclusive Car Wash", text)
        self.assertIn("Categoría: Auto", text)
        self.assertIn("Subcategoría: Lavado", text)
        self.assertEqual(keyboard["inline_keyboard"][0][0]["callback_data"], f"ve:confirm:{token}")
        self.assertEqual(self.table.item["expense"]["category"], "Auto")
        self.assertEqual(self.table.item["expense"]["subcategory"], "Lavado")
        store.assert_not_called()

    def test_preview_suggests_category_and_subcategory_without_applying_them(self):
        expense = self.expense.model_copy(update={"merchant": "lavadero de autos exclusive car wash"})

        text = self.module._voice_expense_preview(expense)

        self.assertIn("Sugerencias disponibles", text)
        self.assertIn("Comercio sugerido: Exclusive Car Wash", text)
        self.assertIn("Categoría sugerida: Auto", text)
        self.assertIn("Subcategoría sugerida: Lavado", text)
        self.assertIsNone(expense.category)
        self.assertIsNone(expense.subcategory)

    def test_suggestion_button_updates_pending_expense_without_writing_to_bigquery(self):
        token = "zPq8Z9u5E2J7S3rK6T1vM4nQ"
        expense = self.expense.model_copy(update={"merchant": "lavadero de autos exclusive car wash"})
        self.table.item = {
            "confirmation_token": token,
            "chat_id": "12345",
            "message_id": "456",
            "expense": expense.model_dump(mode="json"),
            "expires_at": 2_000_000_000,
        }
        update = {
            "callback_query": {
                "id": "callback-suggestion", "data": f"ve:suggestion:{token}",
                "message": {"chat": {"id": 12345}},
            },
        }
        with patch.object(self.module, "_store_manual_expense") as store, \
             patch.object(self.module, "telegram_answer_callback"), \
             patch.object(self.module, "send_telegram_message") as send:
            self.module.handle_telegram_voice_callback(self.event, update)

        store.assert_not_called()
        self.assertEqual(self.table.item["expense"]["merchant"], "Exclusive Car Wash")
        self.assertEqual(self.table.item["expense"]["category"], "Auto")
        self.assertEqual(self.table.item["expense"]["subcategory"], "Lavado")
        self.assertEqual(send.call_args.kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"], f"ve:confirm:{token}")

    def test_text_can_start_editing_confirmed_expense_and_accept_bare_merchant_value(self):
        self.module._save_recent_manual_expense(
            12345, 456, "5c5d5605-c512-4a54-93a4-e9c7f0ba9ffd", self.expense,
        )

        handled, text, keyboard = self.module.handle_confirmed_manual_expense_edit(
            self.event, 12345, "quiero corregir el nombre del comercio",
        )

        self.assertTrue(handled)
        self.assertIn("nuevo valor de comercio", text)
        self.assertIsNone(keyboard)
        self.assertEqual(self.edit_table.item["edit_field"], "merchant")

        handled, text, keyboard = self.module.handle_confirmed_manual_expense_edit(
            self.event, 12345, "Exclusive Car Wash",
        )

        self.assertTrue(handled)
        self.assertIn("¿Aplicar estos cambios", text)
        self.assertIn("Exclusive Car Wash", text)
        self.assertEqual(keyboard["inline_keyboard"][0][0]["callback_data"], "me:confirm")
        self.assertEqual(self.edit_table.item["edit_candidate"]["merchant"], "Exclusive Car Wash")

    def test_confirming_edit_updates_the_specific_expense_and_keeps_audit_path(self):
        expense_id = "5c5d5605-c512-4a54-93a4-e9c7f0ba9ffd"
        edited = self.expense.model_copy(update={"merchant": "Exclusive Car Wash"})
        self.module._save_recent_manual_expense(
            12345, 456, expense_id, self.expense, edit_candidate=edited,
        )
        update = {
            "callback_query": {
                "id": "callback-edit-1", "data": "me:confirm", "message": {"chat": {"id": 12345}},
            },
        }
        with patch.object(self.module, "get_bigquery_client", return_value=object()), \
             patch.object(self.module, "_update_confirmed_manual_expense") as update_expense, \
             patch.object(self.module, "telegram_answer_callback"), \
             patch.object(self.module, "send_telegram_message") as send:
            self.module.handle_manual_expense_edit_callback(self.event, update)

        update_expense.assert_called_once()
        self.assertEqual(update_expense.call_args.args[1]["expense_id"], expense_id)
        self.assertEqual(update_expense.call_args.args[2].merchant, "Exclusive Car Wash")
        self.assertEqual(self.edit_table.item["expense"]["merchant"], "Exclusive Car Wash")
        self.assertNotIn("edit_candidate", self.edit_table.item)
        self.assertEqual(send.call_args.kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "me:edit")

    def test_deleting_confirmed_expense_requires_delete_confirmation(self):
        self.module._save_recent_manual_expense(
            12345, 456, "5c5d5605-c512-4a54-93a4-e9c7f0ba9ffd", self.expense,
        )
        ask_delete = {
            "callback_query": {
                "id": "callback-delete-ask", "data": "me:delete", "message": {"chat": {"id": 12345}},
            },
        }
        with patch.object(self.module, "telegram_answer_callback"), \
             patch.object(self.module, "send_telegram_message") as send:
            self.module.handle_manual_expense_edit_callback(self.event, ask_delete)
        self.assertEqual(
            send.call_args.kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            "me:delete_confirm",
        )

        confirm_delete = {
            "callback_query": {
                "id": "callback-delete-confirm", "data": "me:delete_confirm", "message": {"chat": {"id": 12345}},
            },
        }
        with patch.object(self.module, "get_bigquery_client", return_value=object()), \
             patch.object(self.module, "_delete_confirmed_manual_expense") as delete_expense, \
             patch.object(self.module, "telegram_answer_callback"), \
             patch.object(self.module, "send_telegram_message"):
            self.module.handle_manual_expense_edit_callback(self.event, confirm_delete)
        delete_expense.assert_called_once()
        self.assertIsNone(self.edit_table.item)


if __name__ == "__main__":
    unittest.main()
