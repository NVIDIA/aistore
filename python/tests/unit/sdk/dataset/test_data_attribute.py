import unittest
from unittest.mock import patch
from pathlib import Path
from aistore.sdk.dataset.data_attribute import DataAttribute


class TestDataAttribute(unittest.TestCase):
    def setUp(self):
        self.data_attribute = DataAttribute(
            path=Path("/fake/path"), name="testfile", file_type="jpg"
        )

    @patch("aistore.sdk.dataset.data_attribute.read_file_bytes")
    def test_get_data_for_entry_file_not_found(self, mock_read_file_bytes):
        filename = "nonexistent"
        expected_key = f"{self.data_attribute.name}.{self.data_attribute.file_type}"
        mock_read_file_bytes.side_effect = FileNotFoundError("No such file")
        key, data = self.data_attribute.get_data_for_entry(filename)

        self.assertEqual(key, expected_key)
        self.assertIsNone(data)
        mock_read_file_bytes.assert_called_once_with(Path("/fake/path/nonexistent.jpg"))

    @patch("aistore.sdk.dataset.data_attribute.read_file_bytes")
    def test_get_data_for_entry_success(self, mock_read_file_bytes):
        filename = "sample"
        expected_key = f"{self.data_attribute.name}.{self.data_attribute.file_type}"
        expected_data = b"fake data"
        mock_read_file_bytes.return_value = expected_data
        key, data = self.data_attribute.get_data_for_entry(filename)

        self.assertEqual(key, expected_key)
        self.assertEqual(data, expected_data)
        mock_read_file_bytes.assert_called_once_with(Path("/fake/path/sample.jpg"))
