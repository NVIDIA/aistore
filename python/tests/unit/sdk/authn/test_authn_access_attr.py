#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from aistore.sdk.authn.access_attr import AccessAttr


class TestAuthNAccessAttr(unittest.TestCase):
    """
    Unit tests for AccessAttr, verifying bitwise flag combinations, inclusion, and descriptions.
    """

    def test_access_none(self):
        self.assertFalse(AccessAttr.ACCESS_NONE & AccessAttr.GET)
        self.assertFalse(AccessAttr.ACCESS_NONE & AccessAttr.PUT)
        self.assertFalse(AccessAttr.ACCESS_NONE & AccessAttr.ACCESS_RO)
        self.assertFalse(AccessAttr.ACCESS_NONE & AccessAttr.ACCESS_RW)
        self.assertEqual(AccessAttr.ACCESS_NONE, AccessAttr(0))

    def test_simple_combination_of_access_attrs(self):
        combined = AccessAttr.GET | AccessAttr.PUT | AccessAttr.OBJ_DELETE
        self.assertTrue(combined & AccessAttr.GET)
        self.assertTrue(combined & AccessAttr.PUT)
        self.assertTrue(combined & AccessAttr.OBJ_DELETE)
        self.assertFalse(combined & AccessAttr.ADMIN)

    def test_access_all(self):
        self.assertTrue(AccessAttr.ACCESS_ALL & AccessAttr.GET)
        self.assertTrue(AccessAttr.ACCESS_ALL & AccessAttr.ADMIN)
        self.assertEqual(AccessAttr.ACCESS_ALL & AccessAttr.ACCESS_NONE, 0)

    def test_describe_combined_access(self):
        combined = AccessAttr.GET | AccessAttr.PUT | AccessAttr.OBJ_DELETE
        description = AccessAttr.describe(combined)
        self.assertIn("GET", description)
        self.assertIn("PUT", description)
        self.assertIn("DELETE", description)
        self.assertNotIn("ADMIN", description)

    def test_describe_derived_access(self):
        description = AccessAttr.describe(AccessAttr.ACCESS_RO)
        self.assertIn("GET", description)
        self.assertIn("OBJ_HEAD", description)
        self.assertIn("LIST_BUCKETS", description)
        self.assertIn("BCK_HEAD", description)
        self.assertIn("OBJ_LIST", description)
        self.assertNotIn("PUT", description)
        self.assertNotIn("ADMIN", description)

        description = AccessAttr.describe(AccessAttr.ACCESS_RW)
        self.assertIn("GET", description)
        self.assertIn("OBJ_HEAD", description)
        self.assertIn("LIST_BUCKETS", description)
        self.assertIn("BCK_HEAD", description)
        self.assertIn("OBJ_LIST", description)
        self.assertIn("PUT", description)
        self.assertIn("APPEND", description)
        self.assertIn("OBJ_DELETE", description)
        self.assertIn("OBJ_MOVE", description)
        self.assertNotIn("ADMIN", description)
        self.assertNotIn("PROMOTE", description)

    def test_access_ro(self):
        self.assertTrue(AccessAttr.ACCESS_RO & AccessAttr.GET)
        self.assertTrue(AccessAttr.ACCESS_RO & AccessAttr.OBJ_HEAD)
        self.assertFalse(AccessAttr.ACCESS_RO & AccessAttr.PUT)
        self.assertFalse(AccessAttr.ACCESS_RO & AccessAttr.ADMIN)

    def test_access_rw(self):
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.GET)
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.PUT)
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.OBJ_DELETE)
        self.assertFalse(AccessAttr.ACCESS_RW & AccessAttr.ADMIN)

    def test_access_cluster(self):
        self.assertTrue(AccessAttr.ACCESS_CLUSTER & AccessAttr.LIST_BUCKETS)
        self.assertTrue(AccessAttr.ACCESS_CLUSTER & AccessAttr.CREATE_BUCKET)
        self.assertTrue(AccessAttr.ACCESS_CLUSTER & AccessAttr.ADMIN)
        self.assertFalse(AccessAttr.ACCESS_CLUSTER & AccessAttr.GET)

    def test_access_all_includes_all_derived_roles(self):
        self.assertTrue(AccessAttr.ACCESS_ALL & AccessAttr.ACCESS_RW)
        self.assertTrue(AccessAttr.ACCESS_ALL & AccessAttr.ACCESS_CLUSTER)
        self.assertTrue(AccessAttr.ACCESS_ALL & AccessAttr.PROMOTE)
        self.assertFalse(AccessAttr.ACCESS_ALL & AccessAttr.ACCESS_NONE)


if __name__ == "__main__":
    unittest.main()
