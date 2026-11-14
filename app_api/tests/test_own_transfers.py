import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import forecast  # noqa: E402


class OwnTransferExclusionTests(unittest.TestCase):
    def test_monthly_expenses_leave_out_transfers_between_own_accounts(self):
        sql = forecast.monthly_expenses_query(project_id='p-123456', dataset='PRD', gastos_view='gastos_totales',
                                              mapping_table='dim_comercio_mapping', has_monto_ars=False, use_mapping=True)
        self.assertIn("category <> 'Transferencias propias'", sql)

    def test_payment_share_leave_out_transfers_and_joins_the_mapping(self):
        sql = forecast.payment_methods_query(project_id='p-123456', dataset='PRD', gastos_view='gastos_totales',
                                             mapping_table='dim_comercio_mapping', has_monto_ars=False, use_mapping=True)
        self.assertIn("category <> 'Transferencias propias'", sql)
        self.assertIn('dim_comercio_mapping', sql)

    def test_payment_share_without_mapping_still_excludes_by_raw_category(self):
        sql = forecast.payment_methods_query(project_id='p-123456', dataset='PRD', gastos_view='gastos_totales',
                                             mapping_table='dim_comercio_mapping', has_monto_ars=False, use_mapping=False)
        self.assertIn("category <> 'Transferencias propias'", sql)
        self.assertNotIn('LEFT JOIN', sql)


if __name__ == '__main__':
    unittest.main()
