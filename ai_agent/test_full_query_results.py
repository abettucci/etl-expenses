"""Regression checks for complete financial reports without cloud requests."""
import ast
from datetime import date, datetime, timezone
from decimal import Decimal
import html
from html.parser import HTMLParser
from pathlib import Path
import re
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from agent_web import PlainText, run_web_question
from question_period import calendar_context, dates_match_question
from result_presenter import single_value_answer, split_html_messages


def load_reporting_functions():
    tree = ast.parse(Path(__file__).with_name('lambda_function.py').read_text())
    functions = {
        'uncap_generated_sql', 'generate_sql_with_openai2', 'retry_sql_generation',
        'query_bigquery', 'format_bigquery_results', 'send_telegram_message',
        '_friendly_column_label', '_looks_like_money_column', '_looks_like_date_column',
        '_format_number_ar', '_format_money_ar', '_format_datetime_for_chat', '_format_cell_for_chat',
    }
    constants = {'_COLUMN_LABELS_ES', 'FINANCIAL_QUERY_RULES', 'SQL_EXAMPLES'}
    nodes = [node for node in tree.body if (
        isinstance(node, ast.FunctionDef) and node.name in functions
    ) or (
        isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in constants for target in node.targets)
    )]
    namespace = {
        'html': html, 're': re, 'Decimal': Decimal, 'datetime': datetime,
        'date': date, 'timezone': timezone, '_TZ_AR': ZoneInfo('America/Argentina/Buenos_Aires'),
        'single_value_answer': single_value_answer, 'split_html_messages': split_html_messages,
        'calendar_context': calendar_context, 'dates_match_question': dates_match_question,
        'GCP_PROJECT_ID': 'test-project', 'BQ_DATASET_PROD': 'PRD',
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<reporting-functions>', 'exec'), namespace)
    return namespace


class Rows(list):
    schema = [SimpleNamespace(name=name) for name in ('fecha', 'comercio', 'monto_ars')]

    @property
    def total_rows(self):
        return len(self)


def visible(text):
    parser = PlainText()
    parser.feed(text)
    return ''.join(parser.parts)


class BalancedHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack = []

    def handle_starttag(self, tag, attrs):
        self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack and self.stack.pop() == tag


class CompleteReportTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_reporting_functions()
        self.rows = Rows([
            {'fecha': date(2026, 10, 1), 'comercio': f'Comercio-{i:03d} <&>', 'monto_ars': Decimal('100.25')}
            for i in range(36)
        ])

    def test_full_result_preserves_every_row_and_reports_reconciling_total(self):
        result = self.ns['format_bigquery_results'](self.rows)
        self.assertIn('(36 filas)', result)
        self.assertIn('$ 3.609,00', result)
        self.assertNotIn('truncado', result)
        self.assertGreater(len(result), 4000)
        for i in range(36):
            self.assertEqual(result.count(f'Comercio-{i:03d}'), 1)
        self.assertIn('&lt;&amp;&gt;', result)

    def test_missing_conversion_is_reported_without_counting_as_zero(self):
        self.rows[0]['monto_ars'] = None
        result = self.ns['format_bigquery_results'](self.rows)
        self.assertIn('$ 3.508,75', result)
        self.assertIn('1 movimientos sin conversión', result)

    def test_executor_has_no_result_cap_but_keeps_billing_guard(self):
        client = Mock()
        client.query.return_value.result.return_value = self.rows
        self.ns['validate_sql_dry_run'] = lambda *_: (True, None)
        self.ns['bigquery'] = SimpleNamespace(QueryJobConfig=lambda **kw: kw)
        result, mode = self.ns['query_bigquery'](client, 'SELECT * FROM t', timeout=5)
        self.assertEqual(mode, 'HTML')
        self.assertIn('Comercio-035', result)
        client.query.return_value.result.assert_called_once_with(timeout=5)
        self.assertEqual(client.query.call_args.kwargs['job_config']['maximum_bytes_billed'], 100_000_000)

    def test_terminal_model_limits_are_removed_without_editing_string_values(self):
        normalize = self.ns['uncap_generated_sql']
        self.assertEqual(normalize('SELECT * FROM t LIMIT 20;'), 'SELECT * FROM t')
        self.assertEqual(normalize('SELECT * FROM t limit 100 offset 20'), 'SELECT * FROM t')
        literal = "SELECT 'texto LIMIT 20' AS comercio"
        self.assertEqual(normalize(literal), literal)

    def test_generation_and_repair_share_currency_and_unlimited_report_policy(self):
        model = Mock()
        model.chat.completions.create.return_value = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='SELECT 1 LIMIT 20'))])
        self.ns['openai_client'] = model
        self.ns['build_enriched_schema_prompt'] = lambda: 'schema'
        self.ns['dates_match_question'] = lambda *_: True
        self.assertEqual(self.ns['generate_sql_with_openai2']('detalle de octubre', None), 'SELECT 1')
        self.assertEqual(self.ns['retry_sql_generation']('detalle', 'SELECT 1', 'error', None), 'SELECT 1')
        for call in model.chat.completions.create.call_args_list:
            prompt = call.kwargs['messages'][0]['content']
            self.assertIn('No agregues LIMIT ni OFFSET', prompt)
            self.assertIn('AS monto_ars', prompt)
            self.assertIn('moneda_original', prompt)
        self.assertNotRegex(self.ns['SQL_EXAMPLES'], r'\bLIMIT\s+\d+')

    def test_sender_delivers_all_chunks_and_keyboard_only_at_end(self):
        text = self.ns['format_bigquery_results'](self.rows)
        response = Mock()
        response.json.return_value = {'ok': True}
        requests = SimpleNamespace(post=Mock(return_value=response))
        self.ns['requests'] = requests
        self.ns['time'] = SimpleNamespace(sleep=Mock())
        keyboard = {'inline_keyboard': []}
        result = self.ns['send_telegram_message'](123, text, 'test-token', parse_mode='HTML', reply_markup=keyboard)
        self.assertEqual(result, {'ok': True})
        payloads = [call.kwargs['json'] for call in requests.post.call_args_list]
        self.assertGreater(len(payloads), 1)
        self.assertEqual(''.join(visible(payload['text']) for payload in payloads), visible(text))
        for payload in payloads[:-1]:
            self.assertNotIn('reply_markup', payload)
        self.assertEqual(payloads[-1]['reply_markup'], keyboard)
        self.assertEqual(self.ns['time'].sleep.call_count, len(payloads) - 1)

    def test_web_result_is_not_truncated_after_query_cap_removal(self):
        text = self.ns['format_bigquery_results'](self.rows)
        result = run_web_question({'user_id': 'owner', 'question': 'detalle de octubre'}, enabled=True, owner_sub='owner', generate=lambda _: 'SELECT 1', execute=lambda _: (text, 'HTML'))
        self.assertEqual(result['statusCode'], 200)
        self.assertIn('Comercio-035', result['answer'])


class HTMLDeliveryTests(unittest.TestCase):
    def test_long_rows_emoji_and_entities_remain_complete_and_balanced(self):
        original = '<b>Detalle &amp; gastos</b>\n' + '<i>' + ('🛒 &lt;comercio&gt; ' * 700) + '</i>'
        chunks = split_html_messages(original)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(''.join(visible(chunk) for chunk in chunks), visible(original))
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode('utf-16-le')) // 2, 4000)
            parser = BalancedHTML()
            parser.feed(chunk)
            self.assertEqual(parser.stack, [])


if __name__ == '__main__':
    unittest.main()
