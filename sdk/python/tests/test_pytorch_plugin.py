"""
Test class for AIStore PyTorch Plugin
Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

import random
import string
import unittest
from aistore.client.api import Client
from aistore.client.errors import ErrBckNotFound
import tempfile
from tests import CLUSTER_ENDPOINT
from aistore.pytorch.aisio import AISFileListerIterDataPipe


# pylint: disable=unused-variable
class TestPytorchPlugin(unittest.TestCase):
    def setUp(self) -> None:
        letters = string.ascii_lowercase
        self.bck_name = ''.join(random.choice(letters) for _ in range(10))
        self.client = Client(CLUSTER_ENDPOINT)
        self.client.create_bucket(self.bck_name)
        num_objs = 10

        for i in range(num_objs):
            object_body = "test string" * random.randrange(1, 10)
            content = object_body.encode('utf-8')
            obj_name = f"temp/obj{ i }"
            with tempfile.NamedTemporaryFile() as f:
                f.write(content)
                f.flush()
                self.client.put_object(self.bck_name, obj_name, f.name)

    def tearDown(self) -> None:
        # Try to destroy bucket if there is one left.
        try:
            self.client.destroy_bucket(self.bck_name)
        except ErrBckNotFound:
            pass

    def test_filelister_with_prefix_variations(self):
        prefixes = ['ais://' + self.bck_name]
        urls = AISFileListerIterDataPipe(url=CLUSTER_ENDPOINT, source_datapipe=prefixes)
        with self.assertRaises(TypeError):
            len(urls)
        self.assertEqual(len(list(urls)), 10)

        prefixes = ['ais://' + self.bck_name + "/"]
        urls = AISFileListerIterDataPipe(url=CLUSTER_ENDPOINT, source_datapipe=prefixes)
        with self.assertRaises(TypeError):
            len(urls)
        self.assertEqual(len(list(urls)), 10)

        prefixes = ['ais://' + self.bck_name + "/temp/"]
        urls = AISFileListerIterDataPipe(url=CLUSTER_ENDPOINT, source_datapipe=prefixes)
        with self.assertRaises(TypeError):
            len(urls)
        self.assertEqual(len(list(urls)), 10)

    def test_filelister_with_invalid_bck_name(self):
        prefixes = ['ais://asdasd']
        with self.assertRaises(ErrBckNotFound):
            list(AISFileListerIterDataPipe(url=CLUSTER_ENDPOINT, source_datapipe=prefixes))


if __name__ == '__main__':
    unittest.main()
