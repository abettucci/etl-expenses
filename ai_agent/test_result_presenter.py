import ast
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import unittest
import sys
sys.path.insert(0, str(Path(__file__).parent))
from result_presenter import single_value_answer
from agent_web import run_web_question, looks_read_only
from unittest.mock import Mock
from datetime import date
from question_period import calendar_context, named_month_period, dates_match_question


class PresenterTests(unittest.TestCase):
    def test_october_uses_2026_not_hallucinated_2023(self):
        today = date(2026, 11, 1)
        question = 'decime cuanto fue mi gasto total durante todo octubre'
        self.assertEqual(named_month_period(question, today), (date(2026, 10, 1), date(2026, 11, 1)))
        self.assertIn('2026-10-01', calendar_context(question, today))
        self.assertFalse(dates_match_question("SELECT 1 WHERE DATE '2023-10-01' < CURRENT_DATE()", question, today))
        self.assertTrue(dates_match_question("SELECT 1 WHERE DATE '2026-10-01' < CURRENT_DATE()", question, today))
        self.assertIsNone(named_month_period('octubre de 2023', today))
        self.assertEqual(named_month_period('diciembre', date(2026, 1, 1))[0], date(2025, 12, 1))
    def test_money_scalar_without_query_metadata(self):
        for value in [Decimal('1234.50'), 1234.5, '1234.50']:
            self.assertEqual(single_value_answer(['total_gasto_ars'], {'total_gasto_ars': value}), '<b>$ 1.234,50 ARS</b>')
        self.assertEqual(single_value_answer(['total_ars'], {'total_ars': 0}), '<b>$ 0,00 ARS</b>')

    def test_null_is_not_zero_and_nonfinancial_is_escaped(self):
        answer = single_value_answer(['total_ars'], {'total_ars': None})
        self.assertIn('no equivale a gasto cero', answer)
        self.assertNotIn('—', answer)
        self.assertEqual(single_value_answer(['name'], {'name': '<b>test</b>'}), '&lt;b&gt;test&lt;/b&gt;')
        self.assertEqual(single_value_answer(['total_registros'], {'total_registros': 12}), '12')
        self.assertEqual(single_value_answer(['activo'], {'activo': True}), 'Sí')

    def test_production_formatter_uses_scalar_branch(self):
        tree = ast.parse(Path(__file__).with_name('lambda_function.py').read_text())
        node = next(item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == 'format_bigquery_results')
        namespace = {'single_value_answer': single_value_answer}
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<test-formatter>', 'exec'), namespace)
        class Rows(list):
            schema = [SimpleNamespace(name='total_gasto_ars')]
        for value in [123.45, 0, None]:
            answer = namespace['format_bigquery_results'](Rows([{'total_gasto_ars': value}]))
            self.assertNotIn('columnas', answer); self.assertNotIn('1 fila', answer)


class WebAgentTests(unittest.TestCase):
    def test_internal_owner_and_feature_guard(self):
        generate, execute = Mock(), Mock()
        for event, enabled in [({'user_id': 'other'}, True), ({'user_id': 'owner', 'body': '{}'}, True), ({'user_id': 'owner'}, False)]:
            answer = run_web_question(event, enabled=enabled, owner_sub='owner', generate=generate, execute=execute)
            self.assertIn(answer['statusCode'], [403, 503])
        generate.assert_not_called(); execute.assert_not_called()

    def test_plain_response_same_agent_no_html_execution(self):
        answer = run_web_question({'user_id': 'owner', 'question': 'total octubre'}, enabled=True, owner_sub='owner', generate=lambda _: 'SELECT 1', execute=lambda _: ('<b>$ 123,00 ARS</b>', 'HTML'))
        self.assertEqual(answer, {'statusCode': 200, 'answer': '$ 123,00 ARS'})

    def test_mutations_invalid_input_and_errors_never_exposed(self):
        execute = Mock()
        for question, sql in [('', 'SELECT 1'), ('a' * 1001, 'SELECT 1'), ('borrar gastos', 'DELETE FROM t')]:
            result = run_web_question({'user_id': 'owner', 'question': question}, enabled=True, owner_sub='owner', generate=lambda _: sql, execute=execute)
            self.assertEqual(result['statusCode'], 400)
        execute.assert_not_called()
        answer = run_web_question({'user_id': 'owner', 'question': 'total'}, enabled=True, owner_sub='owner', generate=lambda _: 'SELECT 1', execute=lambda _: ('❌ SQL and private data', False))
        self.assertEqual(answer['statusCode'], 503); self.assertNotIn('private', answer['answer'])

    def test_only_read_queries(self):
        self.assertTrue(looks_read_only('WITH t AS (SELECT 1) SELECT * FROM t;'))
        for sql in ['DELETE FROM t', 'SELECT 1; SELECT 2', 'CALL task()', "SELECT EXTERNAL_QUERY('x', 'y')", 'CREATE TABLE x AS SELECT 1']:
            self.assertFalse(looks_read_only(sql))

    def test_dry_run_rejects_writes_and_foreign_datasets(self):
        tree = ast.parse(Path(__file__).with_name('lambda_function.py').read_text())
        node = next(item for item in tree.body if isinstance(item, ast.FunctionDef) and item.name == 'validate_sql_dry_run')
        namespace = {'looks_read_only': looks_read_only, 'bigquery': SimpleNamespace(QueryJobConfig=lambda **kw: kw), 'GCP_PROJECT_ID': 'test-project', 'BQ_DATASET_PROD': 'PRD'}
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<test-validator>', 'exec'), namespace)
        client = Mock()
        self.assertFalse(namespace['validate_sql_dry_run'](client, 'DELETE FROM x')[0])
        client.query.assert_not_called()
        client.query.return_value = SimpleNamespace(statement_type='SELECT', referenced_tables=[SimpleNamespace(project='other', dataset_id='PRD')], total_bytes_processed=10)
        self.assertFalse(namespace['validate_sql_dry_run'](client, 'SELECT * FROM t')[0])
        client.query.return_value.referenced_tables = [SimpleNamespace(project='test-project', dataset_id='PRD')]
        self.assertTrue(namespace['validate_sql_dry_run'](client, 'SELECT * FROM t')[0])

    def test_docker_includes_both_modules(self):
        text = Path(__file__).with_name('ai_agent.dockerfile').read_text()
        self.assertIn('result_presenter.py', text); self.assertIn('agent_web.py', text)
