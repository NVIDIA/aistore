import unittest
from unittest.mock import MagicMock
from aistore.sdk.dataset.label_attribute import LabelAttribute


class TestLabelAttribute(unittest.TestCase):
    def setUp(self):
        self.label_identifier = MagicMock()
        self.label_attribute = LabelAttribute(
            name="label", label_identifier=self.label_identifier
        )

    def test_get_data_for_entry_with_label(self):
        filename = "sample_file"
        expected_label = "SampleLabel"
        self.label_identifier.return_value = expected_label

        key, data = self.label_attribute.get_data_for_entry(filename)

        expected_key = self.label_attribute.name
        self.assertEqual(key, expected_key)
        self.assertEqual(data, expected_label)
        self.label_identifier.assert_called_once_with(filename)

    def test_get_data_for_entry_no_label(self):
        filename = "unknown_file"
        self.label_identifier.return_value = None

        key, data = self.label_attribute.get_data_for_entry(filename)

        expected_key = self.label_attribute.name
        self.assertEqual(key, expected_key)
        self.assertIsNone(data)
        self.label_identifier.assert_called_once_with(filename)
