import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import payslip as ps  # noqa: E402

# Second layout: contributions table first, a three-column income table, "TOTAL" and "TOTAL NETO $".
# Identifiers and amounts are fictitious.
LAYOUT_B = """Concepto Unidad Base Contribuciones
Contrib.SIJP sobre sueldo    10,77    4.000.000,00      430.800,00
TOTAL    430.800,00
Concepto Unidad Haberes con aportes Haberes sin aportes Descuentos
Sueldo Basico    30,00    3.000.000,00
Adicional IT      200.000,00
Aporte SIJP sobre sueldo    11,00      352.000,00
Aporte O.SOC. sobre suel.     3,00       96.000,00
Impuesto pagado/devuelto      100.000,00
TOTAL       3.200.000,00               0,00       548.000,00
Nombre y apellido
PEREZ, JUAN
CUIL
20-12345678-9
Legajo
11111111
Posición
Software Engineer
Periodo abonado
Agosto 2026
Fecha y lugar de pago
01.09.2026 Buenos Aires
Banco
Santander Rio
TOTAL NETO $       2.652.000,00
"""


class LayoutBTests(unittest.TestCase):
    def setUp(self):
        self.slip = ps.parse_payslip(LAYOUT_B)

    def test_period_is_read_even_when_the_accent_is_broken(self):
        self.assertEqual(self.slip['period'], '2026-08')
        self.assertEqual(self.slip['pay_date'], '2026-09-01')

    def test_only_the_income_table_is_read(self):
        self.assertEqual(self.slip['gross'], 3200000.0)
        self.assertEqual([h['concept'] for h in self.slip['haberes']], ['Sueldo Basico', 'Adicional IT'])
        self.assertNotIn('Contrib.SIJP sobre sueldo', [h['concept'] for h in self.slip['haberes']])

    def test_totals_and_net_reconcile(self):
        self.assertEqual(self.slip['deductions'], 548000.0)
        self.assertEqual(self.slip['net'], 2652000.0)
        self.assertTrue(all(self.slip['checks'].values()))

    def test_identifiers_are_masked(self):
        self.assertEqual(self.slip['identifiers']['cuil'], '••••6789')
        self.assertNotIn('12345678', repr(self.slip))

    def test_a_layout_whose_totals_do_not_reconcile_is_still_rejected(self):
        with self.assertRaises(ps.PayslipError):
            ps.parse_payslip(LAYOUT_B.replace('TOTAL NETO $       2.652.000,00', 'TOTAL NETO $       2.000.000,00'))


if __name__ == '__main__':
    unittest.main()
