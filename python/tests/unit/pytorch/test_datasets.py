"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from unittest.mock import patch, Mock, MagicMock
from aistore.pytorch.dataset import AISDataset
from aistore.pytorch.iter_dataset import AISIterDataset
from aistore.pytorch.multishard_dataset import AISMultiShardStream
from aistore.pytorch.shard_reader import AISShardReader
from aistore.pytorch.utils import list_wds_samples_iter
from aistore.sdk.list_object_flag import ListObjectFlag


class TestAISDataset(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = Mock()
        mock_obj = Mock()
        mock_obj.get.return_value.read_all.return_value = b"mock data"
        self.mock_objects = [
            mock_obj,
            mock_obj,
        ]

        self.patcher_list_objects_iterator = patch(
            "aistore.pytorch.base_dataset.list_objects_iterator",
            return_value=iter(self.mock_objects),
        )
        self.patcher_list_objects = patch(
            "aistore.pytorch.base_dataset.list_objects", return_value=self.mock_objects
        )
        self.patcher_client = patch(
            "aistore.pytorch.base_dataset.Client", return_value=self.mock_client
        )
        self.patcher_list_objects_iterator.start()
        self.patcher_list_objects.start()
        self.patcher_client.start()

    def tearDown(self) -> None:
        self.patcher_list_objects_iterator.stop()
        self.patcher_list_objects.stop()
        self.patcher_client.stop()

    def test_map_dataset(self):
        ais_dataset = AISDataset(client_url="mock_client_url", urls_list="ais://test")
        self.assertIsNone(ais_dataset.etl_name)

        self.assertEqual(len(ais_dataset), 2)
        self.assertEqual(ais_dataset[0][1], b"mock data")

    def test_iter_dataset(self):
        ais_iter_dataset = AISIterDataset(
            client_url="mock_client_url", urls_list="ais://test"
        )
        self.assertIsNone(ais_iter_dataset.etl_name)

        self.assertEqual(len(ais_iter_dataset), 2)

        for _, obj in ais_iter_dataset:
            self.assertEqual(obj, b"mock data")

    def test_multi_shard_stream(self):
        self.patcher = unittest.mock.patch(
            "aistore.pytorch.multishard_dataset.list_shard_objects_iterator"
        )
        self.mock_list_shard_objects_iterator = self.patcher.start()

        self.data1 = iter([b"data1_1", b"data1_2", b"data1_3"])
        self.data2 = iter([b"data2_1", b"data2_2", b"data2_3"])
        self.data3 = iter([b"data3_1", b"data3_2", b"data3_3"])
        self.mock_list_shard_objects_iterator.side_effect = [
            self.data1,
            self.data2,
            self.data3,
        ]

        self.shards = [MagicMock(), MagicMock(), MagicMock()]

        stream = AISMultiShardStream(data_sources=self.shards)

        expected_results = [
            (b"data1_1", b"data2_1", b"data3_1"),
            (b"data1_2", b"data2_2", b"data3_2"),
            (b"data1_3", b"data2_3", b"data3_3"),
        ]

        results = list(iter(stream))

        self.assertEqual(results, expected_results)

    @patch("aistore.sdk.bucket")
    def test_list_wds_sample_iter(self, mock_bucket):
        # Mock the list_objects_iter method
        mock_shard = Mock()
        mock_shard.name = "shard.tar"
        mock_shard.list_objects_iter.return_value = iter([])
        mock_bucket.list_objects_iter.return_value = [mock_shard]

        # Call the function under test
        list(list_wds_samples_iter(None, [], [mock_bucket], None))

        # Assert that list_objects_iter was called exactly once for the shard
        mock_bucket.list_objects_iter.assert_any_call()

        # Assert that list_objects_iter was called exactly once with arch params
        mock_bucket.list_objects_iter.assert_called_with(
            prefix=mock_shard.name, props="name", flags=[ListObjectFlag.ARCH_DIR]
        )

    @patch("aistore.pytorch.shard_reader.list_wds_samples_iter")
    def test_shard_reader(self, mock_list_wds_samples_iter):
        # Mock list_wds_samples_iter
        mock_list_wds_samples_iter.return_value = [
            ("sample_1", {"cls": b"Content of class"}),
            ("sample_2", {"png": b"Content of class"}),
            ("sample_3", {"jpg": b"Content of class"}),
        ]

        # Create shard reader and get results and compare
        shard_reader = AISShardReader(
            client_url="http://example.com", urls_list="http://example.com/data"
        )

        result = list(shard_reader)

        expected_result = [
            ("sample_1", {"cls": b"Content of class"}),
            ("sample_2", {"png": b"Content of class"}),
            ("sample_3", {"jpg": b"Content of class"}),
        ]

        self.assertEqual(result, expected_result)

        # Ensure the iter is called correctly
        mock_list_wds_samples_iter.assert_called()
