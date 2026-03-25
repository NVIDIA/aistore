#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""Integration tests for ListObjectFlag with directory-like object layouts."""

import unittest

from aistore.sdk import ListObjectFlag
from tests.integration.sdk.parallel_test_base import ParallelTestBase


class TestListObjectFlagOps(ParallelTestBase):
    """
    Tests ListObjectFlag options that require a specific object layout
    (nested paths, virtual directories) beyond what test_bucket_ops covers.
    """

    def setUp(self):
        super().setUp()
        self.bck = self._create_bucket()
        self.content = b"flag-test-content"

        # Flat objects (immediate children of prefix/)
        self.flat_names = [
            f"{self.obj_prefix}/alpha.txt",
            f"{self.obj_prefix}/beta.bin",
            f"{self.obj_prefix}/gamma.txt",
        ]
        # Objects nested under sub-directories
        self.nested_names = [
            f"{self.obj_prefix}/subdir/one.txt",
            f"{self.obj_prefix}/subdir/two.txt",
            f"{self.obj_prefix}/subdir/deep/three.txt",
        ]
        self.all_names = sorted(self.flat_names + self.nested_names)

        for name in self.all_names:
            self.bck.object(name).get_writer().put_content(self.content)

    # ------------------------------------------------------------------
    # NO_RECURSION: non-recursive listing returns only immediate children
    #   and virtual directories, not objects nested deeper.
    # ------------------------------------------------------------------
    def test_no_recursion(self):
        objects = self.bck.list_all_objects(
            prefix=f"{self.obj_prefix}/",
            flags=[ListObjectFlag.NO_RECURSION],
        )
        returned_names = sorted(obj.name for obj in objects)
        # Expect the 3 flat objects + the virtual dir "subdir/"
        expected_flat = sorted(self.flat_names)
        for obj in objects:
            is_flat = obj.name in expected_flat
            is_vdir = obj.name == f"{self.obj_prefix}/subdir/"
            self.assertTrue(
                is_flat or is_vdir,
                f"unexpected entry: {obj.name}",
            )
        # Nested objects should NOT appear
        for nested in self.nested_names:
            self.assertNotIn(nested, returned_names)

    # ------------------------------------------------------------------
    # NO_RECURSION + NO_DIRS: immediate children and virtual dirs;
    # nested objects beyond one level deep should not appear.
    # ------------------------------------------------------------------
    def test_no_recursion_no_dirs(self):
        objects = self.bck.list_all_objects(
            prefix=f"{self.obj_prefix}/",
            flags=[ListObjectFlag.NO_RECURSION, ListObjectFlag.NO_DIRS],
        )
        returned_names = sorted(obj.name for obj in objects)
        # Expect flat objects + the virtual dir entry for subdir/
        expected = sorted(self.flat_names + [f"{self.obj_prefix}/subdir/"])
        self.assertEqual(expected, returned_names)
        # Nested objects should NOT appear
        for nested in self.nested_names:
            self.assertNotIn(nested, returned_names)

    # ------------------------------------------------------------------
    # Prefix filtering: different prefixes return correct subsets
    # ------------------------------------------------------------------
    def test_prefix_filtering(self):
        # Only objects under subdir/
        objects = self.bck.list_all_objects(
            prefix=f"{self.obj_prefix}/subdir/",
        )
        returned_names = sorted(obj.name for obj in objects)
        self.assertEqual(sorted(self.nested_names), returned_names)

        # Direct subdir children + virtual dir for deep/
        objects = self.bck.list_all_objects(
            prefix=f"{self.obj_prefix}/subdir/",
            flags=[ListObjectFlag.NO_RECURSION, ListObjectFlag.NO_DIRS],
        )
        returned_names = sorted(obj.name for obj in objects)
        expected = sorted(
            [
                f"{self.obj_prefix}/subdir/deep/",
                f"{self.obj_prefix}/subdir/one.txt",
                f"{self.obj_prefix}/subdir/two.txt",
            ]
        )
        self.assertEqual(expected, returned_names)

    # ------------------------------------------------------------------
    # Props: request specific properties and verify they are populated
    # ------------------------------------------------------------------
    def test_props_name_size_checksum(self):
        objects = self.bck.list_all_objects(
            prefix=self.obj_prefix,
            props="name,size,checksum",
        )
        self.assertEqual(len(self.all_names), len(objects))
        for obj in objects:
            self.assertTrue(obj.size > 0)
            self.assertTrue(len(obj.checksum) > 0, "expected non-empty checksum")

    # ------------------------------------------------------------------
    # Page size: verify pagination respects page_size
    # ------------------------------------------------------------------
    def test_page_size(self):
        resp = self.bck.list_objects(
            prefix=self.obj_prefix,
            page_size=2,
        )
        self.assertLessEqual(len(resp.entries), 2)

    # ------------------------------------------------------------------
    # list_objects_iter: iterate with flags
    # ------------------------------------------------------------------
    def test_iter_with_name_size_flag(self):
        results = list(
            self.bck.list_objects_iter(
                prefix=self.obj_prefix,
                flags=[ListObjectFlag.NAME_SIZE],
            )
        )
        self.assertEqual(len(self.all_names), len(results))
        for obj in results:
            self.assertTrue(obj.size > 0)


if __name__ == "__main__":
    unittest.main()
