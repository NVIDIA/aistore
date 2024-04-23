import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from aistore.sdk.dataset.data_attribute import DataAttribute
from aistore.sdk.dataset.label_attribute import LabelAttribute
from aistore.sdk.dataset.dataset_config import DatasetConfig


class TestDatasetConfig(unittest.TestCase):
    def setUp(self):
        self.primary_data_attribute = DataAttribute(
            path=Path("/fake/path"), name="image", file_type="jpg"
        )
        self.secondary_label_attribute = LabelAttribute(
            name="label", label_identifier=MagicMock(return_value="label1")
        )

        self.dataset_config = DatasetConfig(
            primary_attribute=self.primary_data_attribute,
            secondary_attributes=[self.secondary_label_attribute],
        )

    @patch("aistore.sdk.dataset.dataset_config.Path.glob")
    def test_generate_dataset(self, mock_glob):
        mock_glob.return_value = [
            Path("/fake/path/image1.jpg"),
            Path("/fake/path/image2.jpg"),
        ]
        self.primary_data_attribute.get_data_for_entry = MagicMock(
            side_effect=[("image.jpg", b"fakeimage1"), ("image.jpg", b"fakeimage2")]
        )
        self.secondary_label_attribute.get_data_for_entry = MagicMock(
            return_value=("label", "label1")
        )

        items = list(self.dataset_config.generate_dataset(max_shard_items=10))

        self.assertEqual(len(items), 2)
        self.assertDictEqual(
            items[0],
            {"image.jpg": b"fakeimage1", "label": "label1", "__key__": "sample_00"},
        )
        self.assertDictEqual(
            items[1],
            {"image.jpg": b"fakeimage2", "label": "label1", "__key__": "sample_01"},
        )

    @patch("aistore.sdk.dataset.dataset_config.Path.glob")
    def test_generate_dataset_with_missing_data(self, mock_glob):
        mock_glob.return_value = [Path("/fake/path/image1.jpg")]
        self.primary_data_attribute.get_data_for_entry = MagicMock(
            return_value=(None, None)
        )
        self.secondary_label_attribute.get_data_for_entry = MagicMock(
            return_value=(None, None)
        )

        items = list(self.dataset_config.generate_dataset(max_shard_items=1))

        self.assertEqual(len(items), 0)
