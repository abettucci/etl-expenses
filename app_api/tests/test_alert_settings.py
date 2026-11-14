import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import alert_settings as al  # noqa: E402


class ValidateUpdateTests(unittest.TestCase):
    def valid(self, **overrides):
        payload = {'percent_threshold': '10', 'absolute_threshold': '1000', 'telegram_enabled': True, 'whatsapp_enabled': False}
        payload.update(overrides)
        return payload

    def test_accepts_comma_decimals_and_returns_numbers(self):
        result = al.validate_update(self.valid(percent_threshold='12,5', absolute_threshold='1.000,50'))
        self.assertEqual(result['percent_threshold'], 12.5)
        self.assertEqual(result['absolute_threshold'], 1000.5)
        self.assertEqual(result['channels'], {'telegram': True, 'whatsapp': False})

    def test_thresholds_out_of_range_are_rejected(self):
        for bad in ({'percent_threshold': '0.1'}, {'percent_threshold': '101'}, {'absolute_threshold': '0'},
                    {'absolute_threshold': '2000000000'}, {'percent_threshold': 'mucho'}, {'absolute_threshold': None}):
            with self.assertRaises(ValueError, msg=str(bad)):
                al.validate_update(self.valid(**bad))

    def test_thousands_dots_are_read_as_separators(self):
        self.assertEqual(al.validate_update(self.valid(absolute_threshold='1.000'))['absolute_threshold'], 1000.0)
        self.assertEqual(al.validate_update(self.valid(absolute_threshold='2.500.000'))['absolute_threshold'], 2500000.0)

    def test_nan_and_booleans_are_not_numbers(self):
        for bad in ({'percent_threshold': float('nan')}, {'percent_threshold': True}):
            with self.assertRaises(ValueError, msg=str(bad)):
                al.validate_update(self.valid(**bad))

    def test_channel_switches_must_be_real_booleans(self):
        for bad in ({'telegram_enabled': 'si'}, {'whatsapp_enabled': 1}, {'telegram_enabled': None}):
            with self.assertRaises(ValueError, msg=str(bad)):
                al.validate_update(self.valid(**bad))

    def test_unknown_fields_are_rejected_so_nothing_is_smuggled_in(self):
        with self.assertRaisesRegex(ValueError, 'no permitidos'):
            al.validate_update(self.valid(user_id='someone-else'))

    def test_missing_fields_and_non_objects_are_rejected(self):
        with self.assertRaises(ValueError):
            al.validate_update({'percent_threshold': '10'})
        with self.assertRaises(ValueError):
            al.validate_update(['10', '1000'])


class ChannelDefaultsTests(unittest.TestCase):
    def test_missing_switches_stay_on_as_before(self):
        self.assertEqual(al.channels_from_item(None), {'telegram': True, 'whatsapp': True})
        self.assertEqual(al.channels_from_item({}), {'telegram': True, 'whatsapp': True})

    def test_explicit_off_is_respected_and_other_channel_is_untouched(self):
        self.assertEqual(al.channels_from_item({'telegram_enabled': False}), {'telegram': False, 'whatsapp': True})
        self.assertEqual(al.channels_from_item({'whatsapp_enabled': False, 'telegram_enabled': True}),
                         {'telegram': True, 'whatsapp': False})


class ThresholdReadTests(unittest.TestCase):
    def test_stored_row_wins_over_defaults(self):
        row = {'percent_threshold': 10.0, 'absolute_threshold': 1000.0}
        self.assertEqual(al.thresholds_from_row(row, (10.0, 5000.0)),
                         {'percent_threshold': 10.0, 'absolute_threshold': 1000.0, 'source': 'stored'})

    def test_missing_or_partial_row_falls_back_to_defaults(self):
        for row in (None, {}, {'percent_threshold': 3.0, 'absolute_threshold': None}):
            self.assertEqual(al.thresholds_from_row(row, (10.0, 5000.0))['source'], 'default', str(row))


class SqlTests(unittest.TestCase):
    def test_writes_use_parameters_and_fixed_identifiers(self):
        sql = al.upsert_thresholds_sql('hazel-pillar-400222', 'PRD')
        self.assertIn('MERGE `hazel-pillar-400222.PRD.alert_variation_settings`', sql)
        self.assertIn('@percent', sql)
        self.assertIn('@updated_by', sql)
        self.assertNotIn('1000', sql)

    def test_read_is_a_single_read_only_select(self):
        sql = al.read_thresholds_sql('hazel-pillar-400222', 'PRD')
        self.assertTrue(sql.lstrip().upper().startswith('SELECT'))
        self.assertIn("WHERE id = 'default'", sql)


if __name__ == '__main__':
    unittest.main()
