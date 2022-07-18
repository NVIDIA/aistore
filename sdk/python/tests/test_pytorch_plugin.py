"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

import unittest
from aistore import Client
from aistore.client.errors import AISError, ErrBckNotFound
from tests import CLUSTER_ENDPOINT
from aistore.pytorch import AISFileLister, AISFileLoader
from tests.utils import create_and_put_object, random_name

# pylint: disable=unused-variable
class TestPytorchPlugin(unittest.TestCase):
    def setUp(self) -> None:
        self.bck_name = random_name()
        self.client = Client(CLUSTER_ENDPOINT)
        self.client.bucket(self.bck_name).create()

    def tearDown(self) -> None:
        # Try to destroy bucket if there is one left.
        try:
            self.client.bucket(self.bck_name).delete()
        except ErrBckNotFound:
            pass

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


if __name__ == "__main__":
    unittest.main()
