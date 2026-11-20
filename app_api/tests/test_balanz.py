import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import balanz  # noqa: E402

SUMMARY = """BALANZ
Posición consolidada por concertación
Información de cuenta
Fecha resumen 20/11/2026
Información de cuenta
Acciones $ 318.150
Bonos $ 429.856
Cedears $ 275.340
Corporativos $ 157.320
Fondos $ 300.000
Total $ 1.500.000
Acciones
Especie Descripción Cantidad Garantía Precio Valor Actual
BMA BANCO MACRO S.A."B" 1 V. ESCRIT 35,00 0.00 $ 9.090,00 $ 318.150
Bonos
Especie Descripción Cantidad Garantía Precio Valor Actual
AL30 BONO REP. ARGENTINA USD STEP UP 2030 532,00 0.00 $ 808,00 $ 429.856
Cedears
Especie Descripción Cantidad Garantía Precio Valor Actual
BA.C CEDEAR BANK OF AMERICA CORP. 13,00 0.00 $ 21.180,00 $ 275.340
Corporativos
Especie Descripción Cantidad Garantía Precio Valor Actual
IRCPO ON IRSA INVERSIONES 8% 31/03/35 92,00 0.00 $ 1.710,00 $ 157.320
Fondos
Especie Descripción Cantidad Garantía Precio Valor Actual
BCMMA Money Market Clase A 23.256,00 0.00 $ 12,91 $ 300.000
"""


class HoldingsTests(unittest.TestCase):
    def test_reads_each_section_and_reconciles(self):
        result = balanz.parse_holdings(SUMMARY)
        self.assertEqual(result['as_of'], '2026-11-20')
        self.assertEqual(len(result['positions']), 5)
        self.assertEqual(result['section_totals']['Cedears'], 275340.0)
        self.assertTrue(all(result['checks'].values()))

    def test_a_section_that_does_not_add_up_is_rejected(self):
        broken = SUMMARY.replace('Bonos $ 429.856', 'Bonos $ 500.000')
        with self.assertRaisesRegex(balanz.BalanzError, 'no cuadran'):
            balanz.parse_holdings(broken)

    def test_a_file_that_is_not_a_balanz_summary_is_rejected(self):
        with self.assertRaises(balanz.BalanzError):
            balanz.parse_holdings('Factura de luz Total $ 100,00')

    def test_identifiers_are_not_kept(self):
        text = SUMMARY.replace('Fecha resumen 20/11/2026', 'Fecha resumen 20/11/2026\nN° Comitente 123456789\nCUIT: 33-12345678-9')
        result = balanz.parse_holdings(text)
        self.assertNotIn('123456789', repr(result))
        self.assertNotIn('33-12345678-9', repr(result))


class NumberTests(unittest.TestCase):
    def test_argentine_numbers(self):
        self.assertEqual(balanz.ar_number('1.234.567,89'), 1234567.89)
        self.assertEqual(balanz.ar_number('$ 12,91'), 12.91)
        self.assertEqual(balanz.ar_number('0.00'), 0.0)
        self.assertEqual(balanz.ar_number('1.000'), 1000.0)


class MovementTests(unittest.TestCase):
    ROWS = [
        {'Descripcion': 'Dividendo en efectivo / KO', 'Ticker': 'KO', 'Tipo de Instrumento': 'Cedears', 'Concertacion': '2026-10-02',
         'Cantidad': 0, 'Precio': -1, 'Liquidacion': '2026-10-02', 'Moneda': 'Dólares C.V. 7000', 'Importe': 2.05},
        {'Descripcion': 'Renta y Amortización / TX26', 'Ticker': 'TX26', 'Tipo de Instrumento': 'Bonos', 'Concertacion': '2026-11-11',
         'Cantidad': -4060, 'Precio': -1, 'Liquidacion': '2026-11-11', 'Moneda': 'Pesos', 'Importe': 31229.61},
        {'Descripcion': 'Boleto / 1 / COMPRA / AAPL', 'Ticker': 'AAPL', 'Tipo de Instrumento': 'Cedears', 'Concertacion': '2026-08-19',
         'Cantidad': 13, 'Precio': 30000, 'Liquidacion': '2026-08-20', 'Moneda': 'Pesos', 'Importe': -390000},
    ]

    def test_movements_are_classified(self):
        kinds = [m['kind'] for m in balanz.parse_movements(self.ROWS)]
        self.assertEqual(kinds, ['dividend', 'amortization', 'other'])

    def test_income_leaves_out_purchases_and_keeps_currencies_apart(self):
        income = balanz.income_by_month(balanz.parse_movements(self.ROWS))
        self.assertEqual(income, [
            {'month': '2026-10', 'currency': 'Dólares C.V. 7000', 'amount': 2.05},
            {'month': '2026-11', 'currency': 'Pesos', 'amount': 31229.61},
        ])

    def test_movement_ids_are_stable(self):
        first = balanz.parse_movements(self.ROWS)[0]['id']
        second = balanz.parse_movements(self.ROWS)[0]['id']
        self.assertEqual(first, second)


class TradeTests(unittest.TestCase):
    def test_a_trade_must_match_quantity_times_price(self):
        ok = {'Ticker': 'AVGO', 'Tipo': 'COMPRA', 'Concertacion': '2026-09-22', 'Liquidacion': '2026-09-23', 'Cantidad': 42,
              'Precio': 14000, 'Bruto': 588000, 'Costos Mercado': 0, 'Arancel': 1000, 'Neto': -589000, 'Num Boleto': 1, 'Especie': 'CEDEAR'}
        bad = dict(ok, Bruto=600000)
        result = balanz.parse_trades([ok, bad])
        self.assertEqual(len(result['trades']), 1)
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(result['trades'][0]['side'], 'COMPRA')

    def test_rows_that_are_not_trades_are_ignored(self):
        self.assertEqual(balanz.parse_trades([{'Ticker': 'X', 'Tipo': 'DEPÓSITO'}])['trades'], [])


class UploadTests(unittest.TestCase):
    def _xlsx(self, header, rows):
        from io import BytesIO
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(header)
        for row in rows:
            ws.append(row)
        buffer = BytesIO()
        wb.save(buffer)
        return buffer.getvalue()

    def test_movements_file_is_detected(self):
        raw = self._xlsx(['Descripcion', 'Ticker', 'Tipo de Instrumento', 'Concertacion', 'Cantidad', 'Precio',
                          'Liquidacion', 'Moneda', 'Importe'],
                         [['Dividendo en efectivo / KO', 'KO', 'Cedears', '2026-10-02', 0, -1, '2026-10-02', 'Pesos', 2.05]])
        result = balanz.read_upload(raw)
        self.assertEqual(result['kind'], 'movements')
        self.assertEqual(result['data'][0]['kind'], 'dividend')

    def test_trades_file_is_detected(self):
        raw = self._xlsx(['Especie', 'Num Boleto', 'Ticker', 'Tipo', 'Concertacion', 'Liquidacion', 'Cantidad', 'Precio',
                          'Bruto', 'Costos Mercado', 'Arancel', 'Neto'],
                         [['CEDEAR', 1, 'AVGO', 'COMPRA', '2026-09-22', '2026-09-23', 42, 14000, 588000, 0, 1000, -589000]])
        result = balanz.read_upload(raw)
        self.assertEqual(result['kind'], 'trades')
        self.assertEqual(len(result['data']['trades']), 1)

    def test_unknown_excel_and_other_files_are_refused(self):
        raw = self._xlsx(['Foo', 'Bar'], [[1, 2]])
        with self.assertRaises(balanz.BalanzError):
            balanz.read_upload(raw)
        with self.assertRaises(balanz.BalanzError):
            balanz.read_upload(b'hola')


if __name__ == '__main__':
    unittest.main()
