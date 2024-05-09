"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from pathlib import Path
import torchdata.datapipes.iter as torch_pipes

from aistore.sdk import Client
from aistore.sdk.errors import AISError, ErrBckNotFound
from aistore.sdk.dataset.data_shard import DataShard
from aistore.pytorch import (
    AISFileLister,
    AISFileLoader,
    AISDataset,
    AISIterDataset,
    AISMultiShardStream,
)
from tests.integration import CLUSTER_ENDPOINT
from tests.utils import (
    create_and_put_object,
    random_string,
    destroy_bucket,
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
        self.client.bucket(self.bck_name).create()
        self.local_test_files = (
            Path().absolute().joinpath("pytorch-plugin-test-" + random_string(8))
        )

    def tearDown(self) -> None:
        """
        Cleanup after each test, destroy the bucket if it exists
        """
        destroy_bucket(self.client, self.bck_name)
        cleanup_local(str(self.local_test_files))

    def test_filelister_with_prefix_variations(self):
        num_objs = 10

        # create 10 objects in the /temp dir
        for i in range(num_objs):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"temp/obj{ i }"
            )

        # create 10 objects in the / dir
        for i in range(num_objs):
            obj_name = f"obj{ i }"
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=obj_name
            )

        prefixes = [
            ["ais://" + self.bck_name],
            ["ais://" + self.bck_name + "/"],
            ["ais://" + self.bck_name + "/temp/", "ais://" + self.bck_name + "/obj"],
        ]
        for prefix in prefixes:
            urls = AISFileLister(url=CLUSTER_ENDPOINT, source_datapipe=prefix)
            ais_loader = AISFileLoader(url=CLUSTER_ENDPOINT, source_datapipe=urls)
            with self.assertRaises(TypeError):
                len(urls)
            self.assertEqual(len(list(urls)), 20)
            self.assertEqual(sum(1 for _ in ais_loader), 20)

    def test_incorrect_inputs(self):
        prefixes = ["ais://asdasd"]

        # AISFileLister: Bucket not found
        try:
            list(AISFileLister(url=CLUSTER_ENDPOINT, source_datapipe=prefixes))
        except ErrBckNotFound as err:
            self.assertEqual(err.status_code, 404)

        # AISFileLoader: incorrect inputs
        url_list = [[""], ["ais:"], ["ais://"], ["s3:///unkown-bucket"]]

        for url in url_list:
            with self.assertRaises(AISError):
                s3_loader_dp = AISFileLoader(url=CLUSTER_ENDPOINT, source_datapipe=url)
                for _ in s3_loader_dp:
                    pass

    def test_torch_library(self):
        # Tests the torch library imports of aistore
        torch_pipes.AISFileLister(
            url=CLUSTER_ENDPOINT, source_datapipe=["ais://" + self.bck_name]
        )
        torch_pipes.AISFileLoader(
            url=CLUSTER_ENDPOINT, source_datapipe=["ais://" + self.bck_name]
        )

    def test_ais_dataset(self):
        num_objs = 10
        content_dict = {}
        for i in range(num_objs):
            content = create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"temp/obj{ i }"
            )
            content_dict[i] = content

        ais_dataset = AISDataset(
            client_url=CLUSTER_ENDPOINT, urls_list=["ais://" + self.bck_name]
        )
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

        ais_iter_dataset = AISIterDataset(
            client_url=CLUSTER_ENDPOINT, urls_list=["ais://" + self.bck_name]
        )
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
        shard1_obj.put_file(shard1_archive_path)

        shard2_content_dict = {
            "file1.cls": b"1",
            "file2.cls": b"2",
            "file3.cls": b"3",
        }
        shard2_archive_name = "test_multishard_shard2.tar"
        shard2_archive_path = self.local_test_files.joinpath(shard2_archive_name)
        create_archive(shard2_archive_path, shard2_content_dict)
        shard2_obj = bucket.object(obj_name=shard2_archive_name)
        shard2_obj.put_file(shard2_archive_path)

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
        dataset = AISMultiShardStream(data_sorces=[shard1, shard2])
        combined_content = list(
            zip(shard1_content_dict.values(), shard2_content_dict.values())
        )

        for i, content in enumerate(dataset):
            self.assertEqual(content, combined_content[i])


if __name__ == "__main__":
    unittest.main()
