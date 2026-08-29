import os
from pathlib import Path
import sys
from unittest import TestCase, mock

sys.path.insert(0, str(Path(__file__).parent))
import test_notifications


class NotificationScriptTest(TestCase):
    def test_dry_run_validates_without_sending(self):
        env = {
            "TELEGRAM_BOT_TOKEN": "test-token",
            "TELEGRAM_ALERT_CHAT_ID": "123",
            "ALERT_SNS_TOPIC_ARN": "arn:aws:sns:us-east-2:123456789012:test-topic",
        }
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(test_notifications, "send_telegram") as telegram, mock.patch.object(test_notifications, "send_sns") as sns:
            self.assertEqual(test_notifications.main(["--channel", "both"]), 0)
        telegram.assert_not_called()
        sns.assert_not_called()

    def test_send_uses_only_selected_channel(self):
        env = {"TELEGRAM_BOT_TOKEN": "test-token", "TELEGRAM_ALERT_CHAT_ID": "123"}
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(test_notifications, "send_telegram") as telegram:
            self.assertEqual(test_notifications.main(["--channel", "telegram", "--send"]), 0)
        telegram.assert_called_once()
