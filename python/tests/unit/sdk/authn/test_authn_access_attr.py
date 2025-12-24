#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from aistore.sdk.authn.access_attr import AccessAttr


class TestAuthNAccessAttr(unittest.TestCase):
    """
    Unit tests for AccessAttr, verifying bitwise flag combinations, inclusion, and descriptions.
    """

    def test_simple_combination_of_access_attrs(self):
        combined = AccessAttr.GET | AccessAttr.PUT | AccessAttr.OBJ_DELETE
        self.assertTrue(combined & AccessAttr.GET)
        self.assertTrue(combined & AccessAttr.PUT)
        self.assertTrue(combined & AccessAttr.OBJ_DELETE)
        self.assertFalse(combined & AccessAttr.ADMIN)

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
        self.assertIn("BCK_HEAD", description)
        self.assertIn("OBJ_LIST", description)
        self.assertNotIn("PUT", description)
        self.assertNotIn("ADMIN", description)

        description = AccessAttr.describe(AccessAttr.ACCESS_RW)
        self.assertIn("GET", description)
        self.assertIn("OBJ_HEAD", description)
        self.assertIn("BCK_HEAD", description)
        self.assertIn("OBJ_LIST", description)
        self.assertIn("PUT", description)
        self.assertIn("APPEND", description)
        self.assertIn("OBJ_DELETE", description)
        self.assertIn("OBJ_MOVE", description)
        self.assertIn("PROMOTE", description)
        self.assertNotIn("ADMIN", description)

        description = AccessAttr.describe(AccessAttr.ACCESS_SU)
        self.assertIn("GET", description)
        self.assertIn("OBJ_HEAD", description)
        self.assertIn("LIST_BUCKETS", description)
        self.assertIn("BCK_HEAD", description)
        self.assertIn("OBJ_LIST", description)
        self.assertIn("PUT", description)
        self.assertIn("APPEND", description)
        self.assertIn("OBJ_DELETE", description)
        self.assertIn("OBJ_MOVE", description)
        self.assertIn("PROMOTE", description)
        self.assertIn("OBJ_UPDATE", description)
        self.assertIn("PATCH", description)
        self.assertIn("BCK_SET_ACL", description)
        self.assertIn("SHOW_CLUSTER", description)
        self.assertIn("CREATE_BUCKET", description)
        self.assertIn("DESTROY_BUCKET", description)
        self.assertIn("MOVE_BUCKET", description)
        self.assertIn("ADMIN", description)

    def test_access_ro(self):
        self.assertTrue(AccessAttr.ACCESS_RO & AccessAttr.GET)
        self.assertTrue(AccessAttr.ACCESS_RO & AccessAttr.OBJ_HEAD)
        self.assertFalse(AccessAttr.ACCESS_RO & AccessAttr.PUT)

    def test_access_rw(self):
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.GET)
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.PUT)
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.OBJ_DELETE)
        self.assertTrue(AccessAttr.ACCESS_RW & AccessAttr.PROMOTE)

    def test_cluster_access_ro(self):
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RO & AccessAttr.LIST_BUCKETS)
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RO & AccessAttr.SHOW_CLUSTER)
        self.assertFalse(AccessAttr.CLUSTER_ACCESS_RO & AccessAttr.CREATE_BUCKET)

    def test_cluster_access_rw(self):
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RW & AccessAttr.LIST_BUCKETS)
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RW & AccessAttr.SHOW_CLUSTER)
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RW & AccessAttr.CREATE_BUCKET)
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RW & AccessAttr.DESTROY_BUCKET)
        self.assertTrue(AccessAttr.CLUSTER_ACCESS_RW & AccessAttr.MOVE_BUCKET)

    def test_access_bucket_admin(self):
        self.assertTrue(AccessAttr.ACCESS_BUCKET_ADMIN & AccessAttr.PATCH)
        self.assertTrue(AccessAttr.ACCESS_BUCKET_ADMIN & AccessAttr.BCK_SET_ACL)
        self.assertTrue(AccessAttr.ACCESS_BUCKET_ADMIN & AccessAttr.OBJ_UPDATE)

    def test_access_su(self):
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.GET)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.OBJ_HEAD)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.LIST_BUCKETS)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.BCK_HEAD)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.OBJ_LIST)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.PUT)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.APPEND)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.OBJ_DELETE)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.OBJ_MOVE)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.PROMOTE)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.OBJ_UPDATE)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.PATCH)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.BCK_SET_ACL)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.SHOW_CLUSTER)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.CREATE_BUCKET)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.DESTROY_BUCKET)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.MOVE_BUCKET)
        self.assertTrue(AccessAttr.ACCESS_SU & AccessAttr.ADMIN)
