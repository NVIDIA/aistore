"""
Unit Test class for AIStore PyTorch Samplers

Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from unittest.mock import patch, Mock
import unittest.mock
from aistore.sdk import Bucket
from aistore.sdk.obj.object import Object
from aistore.pytorch import AISMapDataset, DynamicBatchSampler
from torch.utils.data import DataLoader


class TestAISSampler(unittest.TestCase):
    def setUp(self) -> None:
        mock_obj = Mock(Object)

        self.data = b"\0" * 1000  # 1kb
        mock_obj.get_reader.return_value.read_all.return_value = self.data
        mock_obj.props_cached.size = len(self.data)
        mock_obj.name = "test_obj"

        self.mock_objects = [mock_obj for _ in range(10)]  # 10 objects total

        self.mock_bck = Mock(Bucket)

        self.patcher_get_objects = patch(
            "aistore.pytorch.base_map_dataset.AISBaseMapDataset._create_objects_list",
            return_value=self.mock_objects,
        )

        self.patcher_get_objects.start()

        self.mock_bck.list_all_objects_iter.return_value = iter(self.mock_objects)

        self.ais_dataset = AISMapDataset(ais_source_list=self.mock_bck)

    def tearDown(self) -> None:
        self.patcher_get_objects.stop()

    def test_dynamic_sampler(self):
        loader = DataLoader(
            self.ais_dataset,
            batch_sampler=DynamicBatchSampler(
                data_source=self.ais_dataset,
                max_batch_size=2000,  # two objects (each is 1KB) per batch, 5 batches
            ),
        )

        num_batches = 0
        for names, content in loader:
            num_batches += 1
            self.assertEqual(len(names), 2)

            for data in content:
                self.assertEqual(data, self.data)

        self.assertEqual(num_batches, 5)

    def test_dynamic_sampler_drop_last(self):
        loader = DataLoader(
            self.ais_dataset,
            batch_sampler=DynamicBatchSampler(
                data_source=self.ais_dataset,
                max_batch_size=3000,  # three objects (each is 1KB) per batch
                drop_last=True,  # should result in 3 batches instead of four
            ),
        )

        num_batches = 0
        for names, content in loader:
            num_batches += 1
            self.assertEqual(len(names), 3)

            for data in content:
                self.assertEqual(data, self.data)

        self.assertEqual(num_batches, 3)

    def test_dynamic_sampler_oversized(self):
        loader = DataLoader(
            self.ais_dataset,
            batch_sampler=DynamicBatchSampler(
                data_source=self.ais_dataset,
                max_batch_size=500,  # even though objects are larger, include
                allow_oversized_samples=True,  # should result in 10 batches
            ),
        )

        num_batches = 0
        for names, content in loader:
            num_batches += 1
            self.assertEqual(len(names), 1)

            for data in content:
                self.assertEqual(data, self.data)

        self.assertEqual(num_batches, 10)

    def test_dynamic_sampler_oversized_drop_last(self):

        # add odd one odd one out 6kb object
        mock_obj = Mock(Object)
        large_data = b"\0" * 6000  # 6kb
        mock_obj.get_reader.return_value.read_all.return_value = large_data
        mock_obj.props_cached.size = len(large_data)
        mock_obj.name = "test_obj"

        self.mock_objects.append(mock_obj)
        self.mock_bck.list_all_objects_iter.return_value = iter(self.mock_objects)

        loader = DataLoader(
            self.ais_dataset,
            batch_sampler=DynamicBatchSampler(
                data_source=self.ais_dataset,
                max_batch_size=3000,  # three objects (each is 1KB) per batch
                drop_last=True,  # should result in 3 batches instead of four
                allow_oversized_samples=True,  # should add one batch since we added 6kb object above
            ),
        )

        num_batches = 0
        for names, content in loader:
            num_batches += 1
            self.assertTrue(len(names) == 3 or len(names) == 1)

            for data in content:
                self.assertTrue(data == self.data or data == large_data)

        self.assertEqual(num_batches, 4)

    @patch("aistore.pytorch.dynamic_sampler.torch.randperm")
    def test_dynamic_sampler_shuffle_materializes_python_ints(self, mock_randperm):
        """Verify shuffled indices are materialized as Python integers."""
        permutation = Mock()
        permutation.tolist.return_value = list(range(len(self.mock_objects)))
        mock_randperm.return_value = permutation
        sampler = DynamicBatchSampler(
            data_source=self.ais_dataset,
            max_batch_size=2000,
            shuffle=True,
        )

        first_batch = next(iter(sampler))

        mock_randperm.assert_called_once_with(len(self.mock_objects))
        permutation.tolist.assert_called_once_with()
        self.assertEqual([9, 8], first_batch)
        self.assertTrue(all(isinstance(index, int) for index in sampler._indices))

    def test_dynamic_sampler_reads_each_sample_size_once(self):
        """Verify each sample's cached size is read only once."""

        class CountingSample:
            def __init__(self, name):
                self.name = name
                self._props = Mock(size=1000)
                self.props_reads = 0

            @property
            def props_cached(self):
                self.props_reads += 1
                return self._props

        samples = [CountingSample("first"), CountingSample("second")]
        data_source = Mock()
        data_source.get_obj_list.return_value = samples
        sampler = DynamicBatchSampler(data_source, max_batch_size=2000)

        self.assertEqual([[0, 1]], list(sampler))
        self.assertEqual([1, 1], [sample.props_reads for sample in samples])
