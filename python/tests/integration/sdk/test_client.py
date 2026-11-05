#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
"""Verify client requests across serialization and forked processes."""

import pickle
import unittest
from multiprocessing import get_all_start_methods

from aistore.sdk import Client
from aistore.sdk.errors import ClientClosedError
from tests.integration import CLUSTER_ENDPOINT
from tests.integration.sdk import DEFAULT_TEST_CLIENT, TEST_RETRY_CONFIG
from tests.utils import run_forked_test


class TestClient(unittest.TestCase):
    """Exercise process transfer through client requests to a running cluster."""

    def setUp(self) -> None:
        """Reuse the integration client and its cluster interface."""
        self.client = DEFAULT_TEST_CLIENT
        self.cluster = self.client.cluster()

    def test_context_manager(self):
        """Context exit blocks requests while handles and cached data remain accessible."""
        with Client(CLUSTER_ENDPOINT, retry_config=TEST_RETRY_CONFIG) as client:
            cluster = client.cluster()
            clone = cluster.client.clone()
            uuid = cluster.get_info().uuid
            self.assertEqual(uuid, clone.get_smap().uuid)
        self.assertTrue(client.closed)
        self.assertEqual(uuid, clone.get_smap().uuid)
        new_clone = cluster.client.clone()
        obj = client.bucket("unused").object("unused")
        for operation, kwargs in (
            (client.cluster().get_info, {}),
            (cluster.get_info, {}),
            (cluster.is_ready, {}),
            (obj.head, {}),
            (clone.get_smap, {"force_update": True}),
            (new_clone.get_smap, {}),
        ):
            with self.subTest(operation=operation.__name__):
                with self.assertRaises(ClientClosedError):
                    operation(**kwargs)

    def test_client_pickle(self):
        """Original and deserialized clients can query the same cluster."""
        uuid = self.cluster.get_info().uuid
        restored = pickle.loads(pickle.dumps(self.client))
        restored_cluster = restored.cluster()
        self.addCleanup(restored.close)
        self.assertEqual(restored_cluster.get_info().uuid, uuid)
        self.assertEqual(self.cluster.get_info().uuid, uuid)

    @unittest.skipUnless("fork" in get_all_start_methods(), "requires fork")
    def test_client_fork(self):
        """The parent and forked child can each query the cluster."""
        uuid = self.cluster.get_info().uuid

        def child():
            """Issue a request through the inherited client in the child process."""
            self.assertEqual(self.client.cluster().get_info().uuid, uuid)

        run_forked_test(child)
        self.assertEqual(self.cluster.get_info().uuid, uuid)
