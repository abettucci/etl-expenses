import base64
import json
from unittest.mock import patch

from test_finance_balance import ApiBalanceTests

PNG = base64.b64encode(b'\x89PNG\r\n\x1a\n' + b'0' * 64).decode()
JPEG = base64.b64encode(b'\xff\xd8\xff' + b'0' * 64).decode()
WEBM = base64.b64encode(b'\x1a\x45\xdf\xa3' + b'0' * 64).decode()
OGG = base64.b64encode(b'OggS' + b'0' * 64).decode()


class CaptureTests(ApiBalanceTests):
    def post(self, path, body, user='owner'):
        return self.api.lambda_handler(self.event('POST', path, body, user), None)

    def test_only_the_owner_can_capture_anything(self):
        with patch.object(self.api, '_invoke_agent') as agent:
            for path, body in [('/v1/finance/capture/receipt', {'image_base64': PNG}),
                               ('/v1/finance/capture/voice', {'audio_base64': WEBM, 'duration_seconds': 5}),
                               ('/v1/finance/capture/voice/save', {'amount': '1000', 'merchant': 'X', 'expense_date': '2026-11-01', 'category': 'Y'})]:
                self.assertEqual(self.post(path, body, user=None)['statusCode'], 401)
                self.assertEqual(self.post(path, body, user='other')['statusCode'], 403)
            agent.assert_not_called()

    def test_images_must_be_real_images(self):
        with patch.object(self.api, '_invoke_agent') as agent:
            result = self.post('/v1/finance/capture/receipt', {'image_base64': base64.b64encode(b'hola').decode()})
        self.assertEqual(result['statusCode'], 400)
        agent.assert_not_called()

    def test_a_valid_image_is_forwarded_to_the_agent(self):
        with patch.object(self.api, '_invoke_agent', return_value={'statusCode': 200, 'message': 'Ticket cargado'}) as agent:
            result = self.post('/v1/finance/capture/receipt', {'image_base64': JPEG, 'filename': 'ticket.jpg'})
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(json.loads(result['body'])['message'], 'Ticket cargado')
        action, identity, body = agent.call_args.args
        self.assertEqual(action, 'finance_web_receipt')
        self.assertEqual(body['filename'], 'ticket.jpg')

    def test_audio_needs_a_known_container_and_a_short_duration(self):
        with patch.object(self.api, '_invoke_agent') as agent:
            self.assertEqual(self.post('/v1/finance/capture/voice', {'audio_base64': PNG, 'duration_seconds': 5})['statusCode'], 400)
            self.assertEqual(self.post('/v1/finance/capture/voice', {'audio_base64': WEBM, 'duration_seconds': 0})['statusCode'], 400)
            self.assertEqual(self.post('/v1/finance/capture/voice', {'audio_base64': WEBM, 'duration_seconds': 61})['statusCode'], 400)
        agent.assert_not_called()

    def test_audio_is_forwarded_with_its_container(self):
        with patch.object(self.api, '_invoke_agent', return_value={'statusCode': 200, 'transcript': 'pizza 5000', 'expense': {'amount': '5000'}}) as agent:
            result = self.post('/v1/finance/capture/voice', {'audio_base64': OGG, 'duration_seconds': 4})
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(agent.call_args.args[2]['mime'], 'ogg')

    def test_saving_sends_only_the_reviewed_fields(self):
        with patch.object(self.api, '_invoke_agent', return_value={'statusCode': 200, 'saved': True, 'expense_id': 'e1'}) as agent:
            result = self.post('/v1/finance/capture/voice/save', {'amount': '5000', 'merchant': 'Pizza', 'expense_date': '2026-11-01',
                                                                  'category': 'Comida', 'subcategory': '', 'extra': 'x'})
        self.assertEqual(result['statusCode'], 201)
        body = agent.call_args.args[2]
        self.assertNotIn('extra', body)
        self.assertEqual(body['merchant'], 'Pizza')
        self.assertEqual(body['source'], 'web_voice')

    def test_agent_failures_do_not_leak_details(self):
        with patch.object(self.api, '_invoke_agent', side_effect=self.api.ApiError(503, 'No pudimos procesar la carga ahora.')):
            result = self.post('/v1/finance/capture/receipt', {'image_base64': PNG})
        self.assertEqual(result['statusCode'], 503)
