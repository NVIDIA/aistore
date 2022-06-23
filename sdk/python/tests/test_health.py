"""
Test class for AIStore Python API function health
Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

import unittest

from aistore.client.api import Client
from . import CLUSTER_ENDPOINT


class TestClusterHealth(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.client = Client(CLUSTER_ENDPOINT)

    def test_health(self):
        self.assertEqual(self.client.health(), True)


if __name__ == '__main__':
    unittest.main()