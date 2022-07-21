#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
#

# Default provider is AIS, so all Cloud-related tests are skipped.

import unittest

from aistore import Client
from . import CLUSTER_ENDPOINT


class TestClusterOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.client = Client(CLUSTER_ENDPOINT)

    def test_health_success(self):
        self.assertEqual(Client(CLUSTER_ENDPOINT).cluster().is_aistore_running(), True)

    def test_health_failure(self):
        # url not exisiting or URL down
        self.assertEqual(
            Client("http://localhost:1234").cluster().is_aistore_running(), False
        )

    def test_cluster_map(self):
        smap = self.client.cluster().get_info()

        self.assertIsNotNone(smap)
        self.assertIsNotNone(smap.proxy_si)
        self.assertNotEqual(len(smap.pmap), 0)
        self.assertNotEqual(len(smap.tmap), 0)
        self.assertNotEqual(smap.version, 0)
        self.assertIsNot(smap.uuid, "")
