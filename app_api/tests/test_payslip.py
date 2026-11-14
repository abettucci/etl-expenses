import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import payslip as ps  # noqa: E402

# Same layout as a MercadoLibre payslip. Identifiers are fictitious on purpose.
SAMPLE = """Años Meses Días
Pesos: DOS MILLONES OCHOCIENTOS CUARENTA Y UN MIL QUINIENTOS SETENTA Y SIETE con 98/100
Banco: Santander Rio            N° Cuenta: 1234567890
Nombre del empleado
PEREZ, JUAN
Cuil
20-12345678-9
Legajo
11111111
Posición
Software Engineer
Nivel
Analista Semi Senior - IT
Sector
IT - D&A - Commerce
Categoria
Fuera de Convenio
Periodo abonado
Diciembre 2025 -
Fecha y lugar de pago
01.01.2026 , Bs.As
F.Ingreso
08.01.2024
Cargas Sociales
F.Dep:10.12.2025  Período:11/2025  Banco:Santander Rio
 Concepto Unidad
Haberes
Con aporte Sin aporte Descuentos
Sueldo Basico        30,00    3.198.180,00
Adicional IT      373.003,00
Diferencia pago SAC          494,00
Aporte SIJP sobre sueldo        11,00      392.830,13
Aporte SIJP sobre Sac        11,00           54,34
Aporte INSSJP sobre suel         3,00      107.135,49
Aporte INSSJP sobre Sac         3,00           14,82
Aporte O.SOC. sobre suel.         3,00      107.135,49
Aporte O.SOC. sobre Sac         3,00           14,82
Impuesto pagado/devuelto      107.793,93
Dto Gimnasio       15.120,00
Totales    3.571.677,00      730.099,02
 Total Neto                  2.841.577,98
MercadoLibre S.R.L.
Av Caseros 3039, 2do Piso, Parque Patricios,
CABA
CUIT N° 30-70308853-4
"""


class ParsePayslipTests(unittest.TestCase):
    def setUp(self):
        self.slip = ps.parse_payslip(SAMPLE)

    def test_reads_period_pay_date_and_employment_details(self):
        self.assertEqual(self.slip['period'], '2025-12')
        self.assertEqual(self.slip['period_label'], 'Diciembre 2025')
        self.assertEqual(self.slip['pay_date'], '2026-01-01')
        self.assertEqual(self.slip['employer'], 'MercadoLibre S.R.L.')
        self.assertEqual(self.slip['position'], 'Software Engineer')
        self.assertEqual(self.slip['bank'], 'Santander Rio')

    def test_income_and_deductions_reconcile_with_the_printed_totals(self):
        self.assertEqual(self.slip['gross'], 3571677.0)
        self.assertEqual(self.slip['deductions'], 730099.02)
        self.assertEqual(self.slip['net'], 2841577.98)
        self.assertTrue(all(self.slip['checks'].values()))
        self.assertEqual(round(self.slip['gross'] - self.slip['deductions'], 2), self.slip['net'])

    def test_income_lines_are_the_haberes_with_their_amounts(self):
        concepts = {h['concept']: h['amount'] for h in self.slip['haberes']}
        self.assertEqual(concepts, {'Sueldo Basico': 3198180.0, 'Adicional IT': 373003.0, 'Diferencia pago SAC': 494.0})

    def test_deductions_are_grouped_with_plain_labels(self):
        by = {d['concept']: d['amount'] for d in self.slip['descuentos']}
        self.assertAlmostEqual(by['Jubilación (SIJP)'], 392830.13 + 54.34, places=2)
        self.assertAlmostEqual(by['PAMI (INSSJP)'], 107135.49 + 14.82, places=2)
        self.assertAlmostEqual(by['Obra social (O.SOC.)'], 107135.49 + 14.82, places=2)
        self.assertEqual(by['Impuesto a las ganancias'], 107793.93)
        self.assertEqual(by['Gimnasio'], 15120.0)

    def test_percentage_of_gross_is_computed(self):
        self.assertEqual(self.slip['deductions_pct_of_gross'], round(730099.02 / 3571677.0 * 100, 2))

    def test_identifiers_are_masked_and_never_appear_in_full(self):
        self.assertEqual(self.slip['identifiers'], {'cuil': '••••6789', 'legajo': '••••1111', 'account': '••••7890'})
        serialized = repr(self.slip)
        for secret in ('20-12345678-9', '12345678', '11111111', '1234567890'):
            self.assertNotIn(secret, serialized)

    def test_a_slip_whose_totals_do_not_reconcile_is_rejected(self):
        broken = SAMPLE.replace('Dto Gimnasio       15.120,00', 'Dto Gimnasio       15.121,00')
        with self.assertRaisesRegex(ps.PayslipError, 'no cuadran'):
            ps.parse_payslip(broken)

    def test_text_that_is_not_a_payslip_is_rejected(self):
        for bad in ('Factura de luz\nTotal 1.200,00', '', None):
            with self.assertRaises(ps.PayslipError):
                ps.parse_payslip(bad)

    def test_missing_period_is_rejected(self):
        with self.assertRaisesRegex(ps.PayslipError, 'período'):
            ps.parse_payslip(SAMPLE.replace('Periodo abonado', 'Otro dato'))


class SummaryTests(unittest.TestCase):
    def make(self, period, gross, deductions, label='Jubilación (SIJP)'):
        return {'period': period, 'period_label': period, 'gross': gross, 'deductions': deductions,
                'net': round(gross - deductions, 2), 'deductions_pct_of_gross': round(deductions / gross * 100, 2),
                'descuentos': [{'concept': label, 'amount': deductions}]}

    def test_averages_and_shares_across_months(self):
        result = ps.summarize([self.make('2025-12', 1000, 200), self.make('2025-11', 800, 160)])
        self.assertEqual(result['count'], 2)
        self.assertEqual(result['months'][0]['period'], '2025-11')
        self.assertEqual(result['averages'], {'gross': 900.0, 'deductions': 180.0, 'net': 720.0, 'deductions_pct_of_gross': 20.0})

    def test_deductions_by_concept_show_their_share(self):
        result = ps.summarize([self.make('2025-12', 1000, 200, 'Obra social (O.SOC.)')])
        self.assertEqual(result['deductions_by_concept'], [{'concept': 'Obra social (O.SOC.)', 'average': 200.0, 'pct_of_gross': 20.0}])

    def test_no_payslips_gives_an_empty_summary(self):
        self.assertEqual(ps.summarize([]), {'count': 0, 'months': [], 'averages': None})


if __name__ == '__main__':
    unittest.main()
