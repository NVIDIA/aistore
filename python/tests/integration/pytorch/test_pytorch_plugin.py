"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from pathlib import Path
from aistore.sdk import Client, Bucket
from aistore.sdk.dataset.data_shard import DataShard
from aistore.pytorch import (
    AISMapDataset,
    AISIterDataset,
    AISMultiShardStream,
    AISShardReader,
)
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import (
    create_and_put_object,
    random_string,
    cleanup_local,
    create_archive,
)


# pylint: disable=unused-variable
class TestPytorchPlugin(unittest.TestCase):
    """
    Integration tests for the Pytorch plugin
    """

    def setUp(self) -> None:
        self.bck_name = random_string()
        self.client = Client(CLUSTER_ENDPOINT)
        self.bck = self.client.bucket(self.bck_name)
        self.bck.create()
        self.local_test_files = (
            Path().absolute().joinpath("pytorch-plugin-test-" + random_string(8))
        )

    def tearDown(self) -> None:
        """
        Cleanup after each test, destroy the bucket if it exists
        """
        self.bck.delete(missing_ok=True)
        cleanup_local(str(self.local_test_files))

    def test_ais_dataset(self):
        num_objs = 10
        content_dict = {}
        for i in range(num_objs):
            content = create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"temp/obj{ i }"
            )
            content_dict[i] = content

        ais_dataset = AISMapDataset(ais_source_list=[self.bck])
        self.assertEqual(len(ais_dataset), num_objs)
        for i in range(num_objs):
            obj_name, content = ais_dataset[i]
            self.assertEqual(obj_name, f"temp/obj{ i }")
            self.assertEqual(content, content_dict[i])

    def test_ais_iter_dataset(self):
        num_objs = 10
        content_dict = {}
        for i in range(num_objs):
            content = create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"temp/obj{ i }"
            )
            content_dict[i] = content

        ais_iter_dataset = AISIterDataset(ais_source_list=self.bck)
        self.assertEqual(len(ais_iter_dataset), num_objs)
        for i, (obj_name, content) in enumerate(ais_iter_dataset):
            self.assertEqual(obj_name, f"temp/obj{ i }")
            self.assertEqual(content, content_dict[i])

    def test_multishard_stream(self):
        self.local_test_files.mkdir()
        bucket = self.client.bucket(self.bck_name)
        # Create two shards and store them in the bucket
        shard1_content_dict = {
            "file1.txt": b"Content of file one",
            "file2.txt": b"Content of file two",
            "file3.txt": b"Content of file three",
        }
        shard1_archive_name = "test_multishard_shard1.tar"
        shard1_archive_path = self.local_test_files.joinpath(shard1_archive_name)
        create_archive(shard1_archive_path, shard1_content_dict)
        shard1_obj = bucket.object(obj_name=shard1_archive_name)
        shard1_obj.get_writer().put_file(shard1_archive_path)

        shard2_content_dict = {
            "file1.cls": b"1",
            "file2.cls": b"2",
            "file3.cls": b"3",
        }
        shard2_archive_name = "test_multishard_shard2.tar"
        shard2_archive_path = self.local_test_files.joinpath(shard2_archive_name)
        create_archive(shard2_archive_path, shard2_content_dict)
        shard2_obj = bucket.object(obj_name=shard2_archive_name)
        shard2_obj.get_writer().put_file(shard2_archive_path)

        shard1 = DataShard(
            client_url=CLUSTER_ENDPOINT,
            bucket_name=self.bck_name,
            prefix="test_multishard_shard1.tar",
        )
        shard2 = DataShard(
            client_url=CLUSTER_ENDPOINT,
            bucket_name=self.bck_name,
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

        bucket: Bucket = self.client.bucket(self.bck_name)

        shard_one_dict = {
            "sample_1.cls": b"Class content of sample one",
            "sample_1.jpg": b"Jpg content of sample one",
            "sample_1.png": b"Png content of sample one",
            "sample_2.cls": b"Class content of sample two",
            "sample_2.jpg": b"Jpg content of sample two",
            "sample_2.png": b"Png content of sample two",
        }
        shard_one_archive_name = "shard_1.tar"
        shard_one_archive_path = self.local_test_files.joinpath(shard_one_archive_name)
        create_archive(shard_one_archive_path, shard_one_dict)
        shard_one_obj = bucket.object(obj_name=shard_one_archive_name)
        shard_one_obj.get_writer().put_file(shard_one_archive_path)

        shard_two_dict = {
            "sample_3.cls": b"Class content of sample three",
            "sample_3.jpg": b"Jpg content of sample three",
            "sample_3.png": b"Png content of sample three",
            "sample_4.cls": b"Class content of sample four",
            "sample_4.jpg": b"Jpg content of sample four",
            "sample_4.png": b"Png content of sample four",
        }
        shard_two_archive_name = "shard_2.tar"
        shard_two_archive_path = self.local_test_files.joinpath(shard_two_archive_name)
        create_archive(shard_two_archive_path, shard_two_dict)
        shard_two_obj = bucket.object(obj_name=shard_two_archive_name)
        shard_two_obj.get_writer().put_file(shard_two_archive_path)

        # Expected output from the reader
        expected_sample_dicts = [
            {
                "cls": b"Class content of sample one",
                "jpg": b"Jpg content of sample one",
                "png": b"Png content of sample one",
            },
            {
                "cls": b"Class content of sample two",
                "jpg": b"Jpg content of sample two",
                "png": b"Png content of sample two",
            },
            {
                "cls": b"Class content of sample three",
                "jpg": b"Jpg content of sample three",
                "png": b"Png content of sample three",
            },
            {
                "cls": b"Class content of sample four",
                "jpg": b"Jpg content of sample four",
                "png": b"Png content of sample four",
            },
        ]

        sample_basenames = ["sample_1", "sample_2", "sample_3", "sample_4"]

        # Test shard_reader with prefixes

        url_shard_reader = AISShardReader(
            bucket_list=[bucket],
            prefix_map={bucket: "shard_1.tar"},
        )

        for i, (basename, content_dict) in enumerate(url_shard_reader):
            self.assertEqual(basename, sample_basenames[i])
            self.assertEqual(content_dict, expected_sample_dicts[i])

        # Test shard_reader with bucket_params
        bck_shard_reader = AISShardReader(bucket_list=[bucket])

        for i, (basename, content_dict) in enumerate(bck_shard_reader):
            self.assertEqual(basename, sample_basenames[i])
            self.assertEqual(content_dict, expected_sample_dicts[i])


if __name__ == "__main__":
    unittest.main()
