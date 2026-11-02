"""Local-only browser smoke tests against a built/previewed app.

Run: UNIFIED_WEB_URL=http://127.0.0.1:5178 python -m unittest discover -s tests -p test_unified_browser.py
Requires playwright + Chromium. All remote requests are mocked or blocked.
"""
import os
import unittest
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

BASE = os.environ.get('UNIFIED_WEB_URL', 'http://127.0.0.1:5178')
REPORT = {'mode': 'live', 'currency': 'ARS', 'as_of': '2026-11-02', 'months': [
    {'month': '2026-10', 'income': 100000, 'expenses': 60000, 'savings': 40000, 'savings_rate': 40, 'partial': False},
    {'month': '2026-11', 'income': None, 'expenses': 3000, 'savings': None, 'savings_rate': None, 'partial': True},
]}
PROFILE = {'profile': {'display_name': 'Persona de prueba', 'email': 'synthetic@example.invalid'}, 'finance_linked': False,
           'sources': [{'id': 'gmail', 'name': 'Gmail', 'status': 'setup_required', 'detail': 'Requiere configuración del ETL.'}],
           'telegram': {'detail': 'El chat requiere autorización.', 'finance_url': None, 'groups_url': None, 'personal_access': False}}


class UnifiedBrowser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver = sync_playwright().start()
        cls.browser = cls.driver.chromium.launch(headless=True)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.driver.stop()

    def setUp(self):
        self.context = self.browser.new_context(service_workers='block', viewport={'width': 1440, 'height': 960})
        self.errors = []
        self.writes = []
        self.failure = False
        self.empty_groups = False
        self.context.route('**/*', self.route)
        self.page = self.context.new_page()
        self.page.on('pageerror', lambda error: self.errors.append(str(error)))

    def tearDown(self):
        self.context.close()
        self.assertEqual(self.errors, [])
        self.assertEqual(self.writes, [], 'Browser tests must never send writes to financial systems')

    def route(self, route):
        request = route.request
        parsed = urlparse(request.url)
        if parsed.netloc == urlparse(BASE).netloc:
            route.continue_()
            return
        if parsed.path.startswith('/v1/'):
            if request.method != 'GET':
                self.writes.append(request.method + ' ' + parsed.path)
                route.abort()
                return
            data = []
            if parsed.path == '/v1/finance/balance':
                if self.failure:
                    route.fulfill(status=503, json={'error': 'Fuente financiera no disponible.'})
                    return
                data = REPORT
            elif parsed.path == '/v1/profile/overview':
                data = PROFILE
            elif parsed.path == '/v1/finance/forecast':
                data = {'mode': 'preview'}  # Must never present preview amounts as real.
            elif parsed.path == '/v1/groups':
                data = [] if self.empty_groups else [{'group_id': 'synthetic-group', 'name': 'Grupo de prueba', 'currency': 'ARS'}]
            elif parsed.path.endswith('/settlements'):
                data = {'balances': {}, 'settlements': []}
            route.fulfill(json=data)
            return
        route.abort()  # No fonts, Cognito, Telegram, analytics or production connections.

    def login(self):
        self.context.add_init_script("localStorage.setItem('split_access_token','synthetic-test-token')")

    def no_overflow(self):
        self.assertTrue(self.page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'Page overflows viewport')

    def test_public_funnel_and_anonymous_calculator(self):
        self.page.goto(BASE)
        self.page.get_by_role('heading', name='Entendé tus finanzas.', exact=False).wait_for()
        self.page.get_by_role('link', name='Cómo funciona', exact=True).click()
        self.page.get_by_role('heading', name='Cada gasto,', exact=False).wait_for()
        self.page.get_by_role('button', name='↑ Volver al inicio', exact=True).click()
        self.page.get_by_role('link', name='Dividir sin registro').click()
        self.page.get_by_role('heading', name='Mis divisiones', exact=True).wait_for()
        self.page.get_by_role('button', name='Crear primera división').click()
        self.page.reload()
        self.page.get_by_role('heading', name='Mis divisiones', exact=True).wait_for()
        self.no_overflow()
        for path in ['/', '/como-funciona', '/dividir']:
            for width in [320, 768, 1440]:
                with self.subTest(path=path, width=width):
                    self.page.set_viewport_size({'width': width, 'height': 960})
                    self.page.goto(BASE + path)
                    self.page.locator('#ticket-splitter' if path == '/dividir' else '.unified-hero').wait_for()
                    self.no_overflow()

    def test_logged_in_home_finance_groups_profile(self):
        self.login()
        self.page.goto(BASE)
        self.page.get_by_role('heading', name='Todo en perspectiva.').wait_for()
        self.assertTrue(self.page.url.endswith('/inicio'))
        self.page.get_by_role('button', name='Grupo de prueba', exact=False).click()
        self.page.get_by_role('heading', name='Grupo de prueba', exact=True).last.wait_for()
        self.page.get_by_role('link', name='Mis finanzas', exact=True).click()
        self.page.get_by_role('heading', name='Tu mes, en balance.', exact=False).wait_for()
        self.assertIn('/finanzas/balance', self.page.url)
        self.page.get_by_role('button', name='Proyección y recurrentes', exact=True).click()
        self.page.get_by_text('La proyección aún no está habilitada', exact=False).wait_for()
        self.assertNotIn('482.000', self.page.inner_text('body'))
        self.page.locator('.account-menu summary').click()
        self.page.locator('.account-menu').get_by_role('link', name='Mi perfil').click()
        self.page.get_by_role('heading', name='Mi perfil', exact=True).wait_for()
        self.assertEqual(self.page.locator('.balance-page').count(), 0)
        self.page.get_by_role('link', name='Conexiones', exact=True).first.click()
        self.page.get_by_role('button', name='Telegram y alertas').click()
        self.page.get_by_role('heading', name='Bot de finanzas personales').wait_for()
        self.page.locator('.account-menu summary').click()
        self.page.locator('.account-menu').get_by_role('link', name='Ajustes').click()
        self.page.get_by_role('button', name='Conectar Mercado Pago', exact=False).wait_for()
        self.page.get_by_role('button', name='Ver suscripción').wait_for()
        self.page.go_back()
        self.page.get_by_role('heading', name='Conexiones', exact=True).wait_for()
        self.no_overflow()

    def test_responsive_home_and_mobile_navigation(self):
        self.login()
        for width in [320, 768, 1440]:
            with self.subTest(width=width):
                self.page.set_viewport_size({'width': width, 'height': 960})
                self.page.goto(BASE + '/inicio')
                self.page.get_by_role('heading', name='Todo en perspectiva.').wait_for()
                self.page.get_by_role('button', name='Grupo de prueba', exact=False).wait_for()
                self.no_overflow()
        self.page.set_viewport_size({'width': 320, 'height': 760})
        self.page.locator('.mobile-more summary').click()
        self.page.locator('.mobile-more').get_by_role('link', name='Conexiones').click()
        self.page.get_by_role('heading', name='Conexiones', exact=True).wait_for()
        self.assertFalse(self.page.locator('.mobile-more').evaluate('(element) => element.open'))
        self.page.locator('.primary-nav').get_by_role('link', name='Inicio', exact=True).click()
        self.page.get_by_role('button', name='Preguntar al asistente', exact=False).click()
        self.page.get_by_role('heading', name='Preguntale a tus gastos').wait_for()
        self.no_overflow()

    def test_error_empty_and_legacy_routes(self):
        self.login()
        self.failure = True
        self.empty_groups = True
        self.page.goto(BASE + '/app')
        self.page.get_by_text('Fuente financiera no disponible.', exact=False).wait_for()
        self.page.get_by_role('button', name='Crear mi primer grupo', exact=False).wait_for()
        self.assertNotIn('$ 60.000', self.page.inner_text('body'))
        for old, new in [('/balance', '/finanzas/balance'), ('/panorama', '/finanzas/proyeccion'), ('/regalos', '/grupos')]:
            self.page.goto(BASE + old)
            self.page.wait_for_url(BASE + new)


if __name__ == '__main__':
    unittest.main()
