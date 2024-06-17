"""
Integration Test class for AIStore PyTorch Samplers

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from unittest import TestCase
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import destroy_bucket, random_string
from aistore import Client
from random import randint
from aistore.pytorch import AISMapDataset, DynamicBatchSampler
from torch.utils.data import DataLoader
from sys import getsizeof
from aistore.pytorch.utils import convert_bytes_to_mb


MIN_OBJ_SIZE = 1000  # bytes = 1kb
MAX_OBJ_SIZE = 1000000  # bytes = 1mb
NUM_OBJECTS = 100
MAX_BATCH_SIZE = 1500000  # bytes = 1.5mb


class TestAISSampler(TestCase):
    """
    Integration tests for the AIS Pytorch Samplers
    """

    def setUp(self) -> None:
        self.bck_name = random_string()
        self.client = Client(CLUSTER_ENDPOINT)
        self.bck = self.client.bucket(self.bck_name)
        self.bck.create()

        for i in range(NUM_OBJECTS):
            content = b"\0" * (randint(0, (MAX_OBJ_SIZE - MIN_OBJ_SIZE)) + MIN_OBJ_SIZE)
            self.bck.object(f"object-{i}").put_content(content)

        self.dataset = AISMapDataset(ais_source_list=self.bck)

    def tearDown(self) -> None:
        """
        Cleanup after each test, destroy the bucket if it exists
        """
        destroy_bucket(self.client, self.bck_name)

    def test_dynamic_sampler(self):
        # Create dataloader using dynamic batch sampler
        loader = DataLoader(
            dataset=self.dataset,
            batch_sampler=DynamicBatchSampler(
                data_source=self.dataset,
                max_batch_size=MAX_BATCH_SIZE,
            ),
        )

        num_objects = 0
        for names, content in loader:
            # Test that batches are not empty and have consistent shape
            self.assertTrue(names is not None)
            self.assertTrue(content is not None)
            self.assertEqual(len(names), len(content))

            # Test that the size of each object is within the bounds
            batch_size = 0
            for data in content:
                data_size = getsizeof(data)
                self.assertTrue(data_size >= MIN_OBJ_SIZE and data_size < MAX_OBJ_SIZE)
                batch_size += data_size

            # Test that total batch size is within the bounds
            batch_size = convert_bytes_to_mb(batch_size)
            self.assertTrue(batch_size <= MAX_BATCH_SIZE)

            num_objects += len(names)

        # Test that all objects are included in our batch
        self.assertEqual(num_objects, NUM_OBJECTS)
