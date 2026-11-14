import base64
import unittest
from integration_parsers import parse_santander_payment, validate_binance_read_only


def message(sender='mensajesyavisos@mails.santander.com.ar'):
    html = '<p>Monto</p><p>$ 12.500,50</p><p>Fecha</p><p>10/11/2026</p><p>Comercio</p><p>Supermercado</p>'
    encoded = base64.urlsafe_b64encode(html.encode()).decode().rstrip('=')
    return {'id': 'synthetic123', 'payload': {'headers': [{'name': 'From', 'value': sender}, {'name': 'Subject', 'value': 'Pagaste una compra'}], 'parts': [{'mimeType': 'text/html', 'body': {'data': encoded}}]}}


class IntegrationParserTests(unittest.TestCase):
    def test_santander_amount_and_date(self):
        self.assertEqual(parse_santander_payment(message()), {'source_id': 'synthetic123', 'date': '2026-11-10', 'merchant': 'Supermercado', 'amount': '12500.50', 'currency': 'ARS'})

    def test_unrecognized_sender_and_invalid_message_are_skipped(self):
        self.assertIsNone(parse_santander_payment(message('mensajesyavisos@mails.santander.com.ar.evil.invalid')))
        self.assertIsNone(parse_santander_payment({'id': '../path', 'payload': {}}))

    def test_binance_requires_read_only_and_rejects_missing_permissions(self):
        valid = {'enableReading': True, 'enableWithdrawals': False, 'enableSpotAndMarginTrading': False}
        validate_binance_read_only(valid)
        for invalid in [{}, {**valid, 'enableWithdrawals': True}, {**valid, 'enableFutures': True}, {'enableReading': True}]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_binance_read_only(invalid)
