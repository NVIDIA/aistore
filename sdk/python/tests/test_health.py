"""
Test class for AIStore Python API function health
Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

import unittest

from aistore.client.api import Client
from . import CLUSTER_ENDPOINT


class TestClusterHealth(unittest.TestCase):  # pylint: disable=unused-variable
    def test_health_success(self):
        self.assertEqual(Client(CLUSTER_ENDPOINT).is_aistore_running(), True)

    def test_health_failure(self):
        # url not exisiting or URL down
        self.assertEqual(Client("http://localhost:1234").is_aistore_running(), False)


if __name__ == '__main__':
    unittest.main()
