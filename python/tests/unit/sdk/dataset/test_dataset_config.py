import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

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

    @patch("webdataset.ShardWriter")
    def test_write_shards(self, mock_shard_writer_class):
        sample_data = [
            ({"__key__": "sample_01", "image.jpg": "data1", "cls": "label1"}, []),
            ({"__key__": "sample_02", "image.jpg": "data2", "cls": "label2"}, []),
        ]
        mock_shard_writer = MagicMock()
        mock_shard_writer_class.return_value = mock_shard_writer
        mock_generate_dataset = MagicMock(return_value=iter(sample_data))
        self.dataset_config.generate_dataset = mock_generate_dataset

        self.dataset_config.write_shards(skip_missing=True, maxcount=2)

        mock_shard_writer_class.assert_called_once_with(
            pattern="dataset-%01d.tar", maxcount=2
        )
        calls = [call.write(data) for data, _ in sample_data]
        mock_shard_writer.assert_has_calls(calls)
        mock_shard_writer.close.assert_called_once()
