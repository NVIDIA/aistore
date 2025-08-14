"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
"""

import os
import unittest
import multiprocessing
from pathlib import Path
from torch.utils.data import DataLoader

from tests.integration import CLUSTER_ENDPOINT
from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import (
    create_and_put_object,
    random_string,
    cleanup_local,
    create_archive,
)

from aistore.sdk.dataset.data_shard import DataShard
from aistore.pytorch import (
    AISMapDataset,
    AISIterDataset,
    AISMultiShardStream,
    AISShardReader,
)
from aistore.pytorch.batch_iter_dataset import AISBatchIterDataset


class TestPytorchPlugin(unittest.TestCase):
    """Integration tests for the PyTorch plugin"""

    def setUp(self) -> None:
        self.client = DEFAULT_TEST_CLIENT
        self.bck = self.client.bucket(random_string())
        self.bck.create()
        self.local_test_files = (
            Path().absolute().joinpath("pytorch-plugin-test-" + random_string(8))
        )

    def tearDown(self) -> None:
        self.bck.delete(missing_ok=True)
        cleanup_local(str(self.local_test_files))

    # ----------------------
    # Shared helper methods
    # ----------------------

    def create_test_objects(self, count: int) -> dict:
        content_dict = {}
        for i in range(count):
            obj_name = f"temp/obj{i}"
            _, content = create_and_put_object(
                self.client, bck=self.bck.as_model(), obj_name=obj_name
            )
            content_dict[obj_name] = content
        return content_dict

    def verify_dataset_output(self, received: dict, expected: dict):
        self.assertEqual(len(received), len(expected))
        for k, v in received.items():
            self.assertEqual(v, expected[k])

    def get_pid_list(self):
        return multiprocessing.Manager().list()

    # ----------------------
    # Core dataset tests
    # ----------------------

    def test_ais_dataset(self):
        content_dict = self.create_test_objects(10)
        dataset = AISMapDataset(ais_source_list=[self.bck])

        self.assertEqual(len(dataset), len(content_dict))
        for obj_name in content_dict:
            name, content = dataset[list(content_dict).index(obj_name)]
            self.assertEqual(name, obj_name)
            self.assertEqual(content, content_dict[obj_name])

    def test_ais_iter_dataset(self):
        content_dict = self.create_test_objects(10)
        dataset = AISIterDataset(ais_source_list=self.bck)

        self.assertEqual(len(dataset), len(content_dict))
        for i, (name, content) in enumerate(dataset):
            self.assertEqual(name, f"temp/obj{i}")
            self.assertEqual(content, content_dict[name])

    # ----------------------
    # Dataloader tests
    # ----------------------

    def test_ais_iter_dataloader(self):
        content_dict = self.create_test_objects(10)
        dataset = AISIterDataset(ais_source_list=self.bck)
        dataloader = DataLoader(dataset, batch_size=1, shuffle=False, num_workers=4)

        received = {}
        for obj_name, content in dataloader:
            received[obj_name[0]] = content[0]

        self.verify_dataset_output(received, content_dict)

    def test_ais_map_dataloader(self):
        content_dict = self.create_test_objects(10)
        dataset = AISMapDataset(ais_source_list=self.bck)
        dataloader = DataLoader(dataset, batch_size=1, shuffle=False, num_workers=4)

        received = {}
        for obj_name, content in dataloader:
            received[obj_name[0]] = content[0]

        self.verify_dataset_output(received, content_dict)

    def test_ais_iter_dataloader_multiprocessing(self):
        content_dict = self.create_test_objects(10)
        pid_list = self.get_pid_list()

        class TestableDataset(AISIterDataset):
            def __iter__(self):
                pid_list.append(os.getpid())
                return super().__iter__()

        dataset = TestableDataset(ais_source_list=self.bck)
        dataloader = DataLoader(dataset, batch_size=1, shuffle=False, num_workers=4)

        received = {}
        for obj_name, content in dataloader:
            received[obj_name[0]] = content[0]

        self.verify_dataset_output(received, content_dict)
        self.assertGreater(
            len(set(pid_list)), 1, f"Expected multiple worker PIDs: {pid_list}"
        )

    def test_ais_map_dataloader_multiprocessing(self):
        content_dict = self.create_test_objects(10)
        pid_list = self.get_pid_list()

        class TestableDataset(AISMapDataset):
            def __getitem__(self, index):
                pid_list.append(os.getpid())
                return super().__getitem__(index)

        dataset = TestableDataset(ais_source_list=self.bck)
        dataloader = DataLoader(dataset, batch_size=1, shuffle=False, num_workers=4)

        received = {}
        for obj_name, content in dataloader:
            received[obj_name[0]] = content[0]

        self.verify_dataset_output(received, content_dict)
        self.assertGreater(
            len(set(pid_list)), 1, f"Expected multiprocessing, got PIDs: {pid_list}"
        )

    # ----------------------
    # Sharded dataset tests
    # ----------------------

    def test_multishard_stream(self):
        self.local_test_files.mkdir()
        # Create two shards and store them in the bucket
        shard1_content_dict = {
            "file1.txt": b"Content of file one",
            "file2.txt": b"Content of file two",
            "file3.txt": b"Content of file three",
        }
        shard1_archive_name = "test_multishard_shard1.tar"
        shard1_archive_path = self.local_test_files.joinpath(shard1_archive_name)
        create_archive(shard1_archive_path, shard1_content_dict)
        shard1_obj = self.bck.object(obj_name=shard1_archive_name)
        shard1_obj.get_writer().put_file(shard1_archive_path)

        shard2_content_dict = {
            "file1.cls": b"1",
            "file2.cls": b"2",
            "file3.cls": b"3",
        }
        shard2_archive_name = "test_multishard_shard2.tar"
        shard2_archive_path = self.local_test_files.joinpath(shard2_archive_name)
        create_archive(shard2_archive_path, shard2_content_dict)
        shard2_obj = self.bck.object(obj_name=shard2_archive_name)
        shard2_obj.get_writer().put_file(shard2_archive_path)

        shard1 = DataShard(
            client_url=CLUSTER_ENDPOINT,
            bucket_name=self.bck.name,
            prefix="test_multishard_shard1.tar",
        )
        shard2 = DataShard(
            client_url=CLUSTER_ENDPOINT,
            bucket_name=self.bck.name,
            prefix="test_multishard_shard2.tar",
        )
        dataset = AISMultiShardStream(data_sources=[shard1, shard2])
        combined_content = list(
            zip(shard1_content_dict.values(), shard2_content_dict.values())
        )

        for i, content in enumerate(dataset):
            self.assertEqual(content, combined_content[i])

    def test_shard_reader(self):
        self.local_test_files.mkdir()

        def prepare_shard(content_dict, name):
            path = self.local_test_files.joinpath(name)
            create_archive(path, content_dict)
            obj = self.bck.object(obj_name=name)
            obj.get_writer().put_file(path)

        shard1_data = {
            "sample_1.cls": b"Class content of sample one",
            "sample_1.jpg": b"Jpg content of sample one",
            "sample_1.png": b"Png content of sample one",
            "sample_2.cls": b"Class content of sample two",
            "sample_2.jpg": b"Jpg content of sample two",
            "sample_2.png": b"Png content of sample two",
        }

        shard2_data = {
            "sample_3.cls": b"Class content of sample three",
            "sample_3.jpg": b"Jpg content of sample three",
            "sample_3.png": b"Png content of sample three",
            "sample_4.cls": b"Class content of sample four",
            "sample_4.jpg": b"Jpg content of sample four",
            "sample_4.png": b"Png content of sample four",
        }

        prepare_shard(shard1_data, "shard1.tar")
        prepare_shard(shard2_data, "shard2.tar")

        expected = [
            {
                "cls": shard1_data["sample_1.cls"],
                "jpg": shard1_data["sample_1.jpg"],
                "png": shard1_data["sample_1.png"],
            },
            {
                "cls": shard1_data["sample_2.cls"],
                "jpg": shard1_data["sample_2.jpg"],
                "png": shard1_data["sample_2.png"],
            },
            {
                "cls": shard2_data["sample_3.cls"],
                "jpg": shard2_data["sample_3.jpg"],
                "png": shard2_data["sample_3.png"],
            },
            {
                "cls": shard2_data["sample_4.cls"],
                "jpg": shard2_data["sample_4.jpg"],
                "png": shard2_data["sample_4.png"],
            },
        ]

        sample_names = ["sample_1", "sample_2", "sample_3", "sample_4"]

        shard_reader = AISShardReader(
            bucket_list=[self.bck], prefix_map={self.bck: "shard1.tar"}
        )
        for i, (basename, sample) in enumerate(shard_reader):
            self.assertEqual(basename, sample_names[i])
            self.assertEqual(sample, expected[i])

        shard_reader = AISShardReader(bucket_list=[self.bck])
        for i, (basename, sample) in enumerate(shard_reader):
            self.assertEqual(basename, sample_names[i])
            self.assertEqual(sample, expected[i])

    def test_ais_batch_iter_dataset(self):
        content_dict = self.create_test_objects(10)
        dataset = AISBatchIterDataset(
            ais_source_list=self.bck,
            client=self.client,
        )

        # Test iteration
        results = {}
        for name, content in dataset:
            results[name] = content

        self.verify_dataset_output(results, content_dict)


if __name__ == "__main__":
    unittest.main()
