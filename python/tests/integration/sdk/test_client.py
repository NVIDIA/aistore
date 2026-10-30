#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#
"""Verify client requests across serialization and forked processes."""

import pickle
import unittest
from multiprocessing import get_all_start_methods

import pytest

from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import run_forked_test


class TestClient(unittest.TestCase):
    """Exercise process transfer through client requests to a running cluster."""

    def setUp(self) -> None:
        """Reuse the integration client and its cluster interface."""
        self.client = DEFAULT_TEST_CLIENT
        self.cluster = self.client.cluster()

    def test_client_pickle(self):
        """Original and deserialized clients can query the same cluster."""
        uuid = self.cluster.get_info().uuid
        restored = pickle.loads(pickle.dumps(self.client))
        restored_cluster = restored.cluster()
        self.addCleanup(restored_cluster.client.session_manager.close)
        self.assertEqual(restored_cluster.get_info().uuid, uuid)
        self.assertEqual(self.cluster.get_info().uuid, uuid)

    @pytest.mark.skipif("fork" not in get_all_start_methods(), reason="requires fork")
    def test_client_fork(self):
        """The parent and forked child can each query the cluster."""
        uuid = self.cluster.get_info().uuid

        def child():
            """Issue a request through the inherited client in the child process."""
            self.assertEqual(self.client.cluster().get_info().uuid, uuid)

        run_forked_test(child)
        self.assertEqual(self.cluster.get_info().uuid, uuid)
