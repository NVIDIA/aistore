"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022-2026, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from unittest.mock import patch, Mock, MagicMock
from aistore.pytorch.map_dataset import AISMapDataset
from aistore.pytorch.iter_dataset import AISIterDataset
from aistore.pytorch.multishard_dataset import AISMultiShardStream
from aistore.pytorch.shard_reader import AISShardReader
from aistore.pytorch.batch_iter_dataset import AISBatchIterDataset
from aistore.sdk import Bucket
from tarfile import open, TarInfo, DIRTYPE
from io import BytesIO


class TestAISDataset(unittest.TestCase):
    def setUp(self) -> None:
        mock_obj = Mock()
        mock_obj.get_reader.return_value.read_all.return_value = b"mock data"
        self.mock_objects = [
            mock_obj,
            mock_obj,
        ]
        self.mock_bck = Mock(Bucket)

        self.patcher_get_objects_iterator = patch(
            "aistore.pytorch.base_iter_dataset.AISBaseIterDataset._create_objects_iter",
            side_effect=lambda: iter(self.mock_objects),
        )
        self.patcher_get_objects = patch(
            "aistore.pytorch.base_map_dataset.AISBaseMapDataset._create_objects_list",
            return_value=self.mock_objects,
        )
        self.patcher_get_objects_iterator.start()
        self.patcher_get_objects.start()

    def tearDown(self) -> None:
        self.patcher_get_objects_iterator.stop()
        self.patcher_get_objects.stop()

    def test_map_dataset(self):
        self.mock_bck.list_all_objects_iter.return_value = iter(self.mock_objects)

        ais_dataset = AISMapDataset(ais_source_list=self.mock_bck)

        self.assertIsNone(ais_dataset._etl_name)

        self.assertEqual(len(ais_dataset), 2)
        self.assertEqual(ais_dataset[0][1], b"mock data")

    def test_iter_dataset(self):
        ais_iter_dataset = AISIterDataset(ais_source_list=self.mock_bck)
        self.assertIsNone(ais_iter_dataset._etl_name)

        self.assertEqual(len(ais_iter_dataset), 2)

        for _, obj in ais_iter_dataset:
            self.assertEqual(obj, b"mock data")

    def test_multi_shard_stream(self):
        self.patcher = patch(
            "aistore.pytorch.AISMultiShardStream._get_shard_objects_iterator"
        )
        self.mock_get_shard_objects_iterator = self.patcher.start()

        self.data1 = iter([b"data1_1", b"data1_2", b"data1_3"])
        self.data2 = iter([b"data2_1", b"data2_2", b"data2_3"])
        self.data3 = iter([b"data3_1", b"data3_2", b"data3_3"])
        self.mock_get_shard_objects_iterator.side_effect = [
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

        self.patcher.stop()

    def test_shard_reader(self):
        # Mock get_wds_samples_iter
        self.patcher = patch("aistore.pytorch.AISShardReader._create_objects_iter")
        mock_create_samples_iter = self.patcher.start()

        tar_buffer = BytesIO()
        # Open the tar file in write mode
        with open(fileobj=tar_buffer, mode="w") as tar:
            # Create some dummy content
            content = b"Content of class"

            # Create a TarInfo object to create samples
            tarinfo = TarInfo(name="sample_1.cls")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="sample_1.png")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="sample_1.jpg")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="README")
            tarinfo.size = len(content)
            tar.addfile(tarinfo, BytesIO(content))
            tarinfo = TarInfo(name="data/")
            tarinfo.type = DIRTYPE
            tar.addfile(tarinfo)

        tar_buffer.seek(0)

        mock_shard = Mock()
        mock_shard.name = "test_shard.tar"

        mock_get = Mock()
        mock_shard.get_reader.return_value = mock_get

        mock_get.read_all.return_value = tar_buffer.getvalue()

        mock_create_samples_iter.return_value = [mock_shard]

        # Create shard reader and get results and compare
        shard_reader = AISShardReader(bucket_list=self.mock_bck)

        result = list(shard_reader)

        expected_result = [
            (
                "sample_1",
                {
                    "cls": b"Content of class",
                    "png": b"Content of class",
                    "jpg": b"Content of class",
                },
            ),
        ]

        self.assertEqual(result, expected_result)

        # Ensure the iter is called correctly
        mock_create_samples_iter.assert_called()

        self.patcher.stop()

    def test_batch_iter_dataset(self):
        """Test AISBatchIterDataset functionality."""

        # Mock the client
        mock_client = Mock()

        # Create proper mock response items (now using MossOut format)
        mock_moss_out_1 = Mock()
        mock_moss_out_1.obj_name = "test_obj_1"
        mock_moss_out_2 = Mock()
        mock_moss_out_2.obj_name = "test_obj_2"

        # Create the response data as a list that can be iterated
        mock_response_data = [
            (mock_moss_out_1, b"batch data 1"),
            (mock_moss_out_2, b"batch data 2"),
        ]

        # Mock the batch object
        mock_batch = Mock()
        mock_batch.get.return_value = iter(mock_response_data)
        mock_client.batch.return_value = mock_batch

        # Create the batch dataset
        batch_dataset = AISBatchIterDataset(
            ais_source_list=self.mock_bck,
            client=mock_client,
        )

        # Test iteration
        results = list(batch_dataset)

        # Verify results
        expected_results = [
            ("test_obj_1", b"batch data 1"),
            ("test_obj_2", b"batch data 2"),
        ]
        self.assertEqual(results, expected_results)

        # Verify batch method was called
        mock_client.batch.assert_called()
        mock_batch.get.assert_called()

    def test_iter_dataset_preload_flag_stored(self):
        dataset = AISIterDataset(
            ais_source_list=self.mock_bck, partition_sources_by_worker=True
        )
        self.assertTrue(dataset._partition_sources_by_worker)

    def test_batch_iter_dataset_preload_flag_stored(self):
        mock_client = Mock()
        mock_client.batch.return_value = Mock()
        dataset = AISBatchIterDataset(
            ais_source_list=self.mock_bck,
            client=mock_client,
            partition_sources_by_worker=True,
        )
        self.assertTrue(dataset._partition_sources_by_worker)

    @patch("aistore.pytorch.base_iter_dataset.torch_utils.get_worker_info")
    def test_preload_calls_create_iter_with_source_slice(self, mock_worker_info):
        """With partition_sources_by_worker=True, each worker's _get_worker_iter_info calls
        _create_objects_iter with only that worker's assigned sources."""
        source_a, source_b, source_c, source_d = Mock(), Mock(), Mock(), Mock()
        dataset = AISIterDataset(
            ais_source_list=[source_a, source_b, source_c, source_d],
            partition_sources_by_worker=True,
        )
        mock_worker_info.return_value = Mock(id=1, num_workers=4)

        with patch.object(
            dataset, "_create_objects_iter", return_value=iter([])
        ) as mock_create:
            dataset._reset_iterator()
            _, worker_name = dataset._get_worker_iter_info()

            # Last call must be with sources[1::4] = [source_b]
            mock_create.assert_called_with([source_b])
            self.assertEqual(worker_name, " (Worker 1)")

    @patch("aistore.pytorch.base_iter_dataset.torch_utils.get_worker_info")
    def test_no_preload_does_not_call_create_iter_with_sources(self, mock_worker_info):
        """With partition_sources_by_worker=False, _create_objects_iter is only called once
        (by _reset_iterator) and _get_worker_iter_info uses islice on the object iterator.
        """
        dataset = AISIterDataset(
            ais_source_list=self.mock_bck,
            partition_sources_by_worker=False,
        )
        mock_worker_info.return_value = Mock(id=0, num_workers=2)

        with patch.object(
            dataset,
            "_create_objects_iter",
            side_effect=lambda sources=None: iter(self.mock_objects),
        ) as mock_create:
            dataset._reset_iterator()
            dataset._get_worker_iter_info()

            # Must only be called once (from _reset_iterator), never with a sources slice
            mock_create.assert_called_once_with()
