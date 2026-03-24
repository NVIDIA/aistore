#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""Integration tests for Native Bucket Inventory (NBI) operations."""

import unittest

import pytest

from tests.integration import REMOTE_SET, REMOTE_BUCKET
from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.const import TEST_TIMEOUT

from aistore.sdk import ListObjectFlag
from aistore.sdk.types import NBIInfo


@unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
@pytest.mark.nonparallel("only one inventory per bucket, tests cannot run concurrently")
class TestNBIOps(unittest.TestCase):
    """Integration tests for NBI."""

    def setUp(self):
        self.client = DEFAULT_TEST_CLIENT
        provider, bck_name = REMOTE_BUCKET.split("://")
        self.bck = self.client.bucket(bck_name, provider=provider)
        # Clean slate
        try:
            self.bck.destroy_inventory()
        except Exception:  # pylint: disable=broad-except
            pass

    def tearDown(self):
        try:
            self.bck.destroy_inventory()
        except Exception:  # pylint: disable=broad-except
            pass

    def _create_and_wait(self, name="", force=False):
        """Helper to create an inventory and wait for completion."""
        job_id = self.bck.create_inventory(name=name, force=force)
        self.assertTrue(job_id)
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT)
        return job_id

    # ---- create ----

    def test_create_inventory(self):
        """Create an inventory and verify it returns a job ID."""
        self._create_and_wait()
        inv = self.bck.show_inventory()
        self.assertEqual(len(inv), 1)

    def test_create_inventory_with_name(self):
        """Create a named inventory and verify it appears in show."""
        self._create_and_wait(name="named-inv")
        inv = self.bck.show_inventory()
        found = any(i.name == "named-inv" for i in inv.values())
        self.assertTrue(found, "Named inventory 'named-inv' not found")

    def test_create_without_force_fails_when_exists(self):
        """Only one inventory per bucket is supported.

        Creating a second inventory without force=True should fail,
        regardless of the name. The original inventory remains intact.
        """
        self._create_and_wait(name="first-inv")
        inv_before = self.bck.show_inventory()
        self.assertEqual(len(inv_before), 1)

        # Second create without force — should fail
        with self.assertRaises(Exception):
            self._create_and_wait(name="second-inv", force=False)

        # Original inventory still intact
        inv_after = self.bck.show_inventory()
        self.assertEqual(len(inv_after), 1)
        found = any(i.name == "first-inv" for i in inv_after.values())
        self.assertTrue(found)

    def test_create_default_name_without_force_fails_when_exists(self):
        """Creating with default name also fails when inventory exists."""
        self._create_and_wait(name="existing-inv")

        with self.assertRaises(Exception):
            self._create_and_wait(force=False)

    def test_create_with_force_replaces_existing(self):
        """Creating with force=True should remove old and create new."""
        self._create_and_wait(name="old-inv")
        inv = self.bck.show_inventory()
        self.assertEqual(len(inv), 1)

        self._create_and_wait(name="new-inv", force=True)
        inv = self.bck.show_inventory()
        self.assertEqual(len(inv), 1)
        found_old = any(i.name == "old-inv" for i in inv.values())
        found_new = any(i.name == "new-inv" for i in inv.values())
        self.assertFalse(found_old, "Old inventory should be gone")
        self.assertTrue(found_new, "New inventory should exist")

    # ---- show ----

    def test_show_inventory_metadata(self):
        """Verify show_inventory returns proper metadata fields."""
        self._create_and_wait()
        inv = self.bck.show_inventory()
        self.assertGreater(len(inv), 0)

        for _, info in inv.items():
            self.assertIsInstance(info, NBIInfo)
            self.assertTrue(info.name)
            self.assertTrue(info.bucket)
            self.assertGreaterEqual(info.ntotal, 0)
            self.assertGreaterEqual(info.chunks, 0)
            self.assertGreater(info.started, 0)
            self.assertGreater(info.finished, 0)

    def test_show_inventory_empty(self):
        """Show inventory on a bucket with no inventories."""
        inv = self.bck.show_inventory()
        self.assertEqual(len(inv), 0)

    def test_show_inventory_by_name(self):
        """Show a specific inventory by name."""
        self._create_and_wait(name="filter-test")
        inv = self.bck.show_inventory(name="filter-test")
        self.assertEqual(len(inv), 1)
        self.assertIn("filter-test", [i.name for i in inv.values()])

    # ---- destroy ----

    def test_destroy_inventory_all(self):
        """Destroy all inventories for a bucket."""
        self._create_and_wait()
        inv = self.bck.show_inventory()
        self.assertGreater(len(inv), 0)

        self.bck.destroy_inventory()
        inv = self.bck.show_inventory()
        self.assertEqual(len(inv), 0)

    def test_destroy_inventory_by_name(self):
        """Destroy a specific named inventory."""
        self._create_and_wait(name="keep-inv")

        self.bck.destroy_inventory(name="keep-inv")
        inv = self.bck.show_inventory()
        self.assertEqual(len(inv), 0)

    # ---- list with inventory ----

    def test_list_objects_with_nbi_flag(self):
        """List objects using NBI flag (single inventory)."""
        self._create_and_wait()
        result = self.bck.list_objects(flags=[ListObjectFlag.NBI], page_size=100)
        # Verify the call succeeds; entry count depends on bucket contents
        self.assertIsNotNone(result.entries)

    def test_list_objects_with_inventory_name(self):
        """List objects using a specific inventory name."""
        self._create_and_wait(name="list-test")
        result = self.bck.list_objects(inventory_name="list-test", page_size=100)
        self.assertIsNotNone(result.entries)

    def test_list_objects_iter_with_inventory_name(self):
        """Iterate objects using a specific inventory name."""
        self._create_and_wait(name="iter-test")
        entries = list(
            self.bck.list_objects_iter(inventory_name="iter-test", page_size=100)
        )
        self.assertIsNotNone(entries)

    def test_list_all_objects_with_inventory_name(self):
        """List all objects using a specific inventory name."""
        self._create_and_wait(name="all-test")
        entries = self.bck.list_all_objects(inventory_name="all-test", page_size=100)
        self.assertIsNotNone(entries)
