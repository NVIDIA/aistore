#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
#
from pathlib import Path
from typing import List, Tuple, Dict
import unittest
import boto3

from aistore.sdk import Object
from tests.integration import (
    REMOTE_SET,
    REMOTE_BUCKET,
)
from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import (
    random_string,
    create_and_put_object,
    cleanup_local,
)
from tests import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from tests.integration.boto3 import AWS_REGION
from tests.const import TEST_TIMEOUT_LONG, OBJECT_COUNT, SUFFIX_NAME, SMALL_FILE_SIZE


class ParallelTestBase(unittest.TestCase):
    """
    This class should be used with all tests that work with remote buckets or run in parallel against a common cluster.
    It provides helper methods for dealing with remote buckets and objects and tracking them for proper cleanup.
    This includes prefixing all objects with a unique value and deleting all objects after tests finish
        to avoid collisions with multiple instances using the same bucket.
    To use this class with another test class, simply inherit from this rather than TestCase.
    To extend setUp behavior in a child class, define them as normal for a TestCase then call
        super().setUp() before adding additional setup steps (same process for tearDown)
    """

    def setUp(self) -> None:
        self.client = DEFAULT_TEST_CLIENT
        self.buckets = []
        self.obj_prefix = f"{self._testMethodName}-{random_string(6)}"
        self._s3_client = None
        self._local_test_files = None

        if REMOTE_SET:
            self.cloud_objects = []
            provider, bck_name = REMOTE_BUCKET.split("://")
            self.bucket = self.client.bucket(bck_name, provider=provider)
        else:
            self.bucket = self._create_bucket()

    @property
    def s3_client(self):
        if not self._s3_client:
            self._s3_client = boto3.client(
                "s3",
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
        return self._s3_client

    @property
    def local_test_files(self):
        if not self._local_test_files:
            self._local_test_files = (
                Path().absolute().joinpath("bucket-ops-test-" + random_string(8))
            )
            self._local_test_files.mkdir(exist_ok=True)
        return self._local_test_files

    def tearDown(self) -> None:
        """
        Cleanup after each test, destroy the bucket if it exists
        """
        if REMOTE_SET:
            entries = self.bucket.list_all_objects(prefix=self.obj_prefix)
            obj_names = [entry.name for entry in entries]
            obj_names.extend(self.cloud_objects)
            if len(obj_names) > 0:
                job_id = self.bucket.objects(obj_names=obj_names).delete()
                self.client.job(job_id).wait(timeout=TEST_TIMEOUT_LONG)
        for bck_name in self.buckets:
            self.client.bucket(bck_name).delete(missing_ok=True)
        if self._local_test_files:
            cleanup_local(str(self.local_test_files))

    def _create_bucket(self, prefix="test-bck"):
        """
        Create a bucket and store its name for later cleanup
        """
        bck_name = f"{prefix}-{random_string(8)}"
        bck = self.client.bucket(bck_name)
        bck.create(exist_ok=True)
        self._register_for_post_test_cleanup(names=[bck_name], is_bucket=True)
        return bck

    def _register_for_post_test_cleanup(self, names: List[str], is_bucket: bool):
        """
        Register objects or buckets for post-test cleanup

        Args:
            names (List[str]): Names of buckets or objects
            is_bucket (bool): True if we are storing a bucket; False for an object
        """
        if is_bucket:
            self.buckets.extend(names)
        elif REMOTE_SET:
            self.cloud_objects.extend(names)

    def _create_object(self):
        """
        Create an object and track it for later cleanup

        Returns:
            The object created
        """
        obj_name = f"{self.obj_prefix}-{random_string(6)}"
        obj = self.bucket.object(obj_name=obj_name)
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)
        return obj

    def _create_object_with_content(self, obj_size=None) -> Tuple["Object", bytes]:
        """
        Create an object with the given content and track for later cleanup

        Args:
            obj_size: Size of the content in the created object

        Returns:
            Tuple of the object name and the content of the object created
        """
        obj_name = f"{self.obj_prefix}-{random_string(6)}"
        obj, content = create_and_put_object(
            client=self.client,
            bck=self.bucket.as_model(),
            obj_name=obj_name,
            obj_size=obj_size,
        )
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)
        return obj, content

    def _create_objects(
        self, num_obj=OBJECT_COUNT, suffix="", obj_size=SMALL_FILE_SIZE
    ) -> Dict[str, bytes]:
        """
        Create a list of objects using a unique test prefix and track them for later cleanup
        Args:
            num_obj: Number of objects to create
            suffix: Optional suffix for each object name
        """
        res = {}
        obj_names = [self.obj_prefix + str(i) + suffix for i in range(num_obj)]
        for obj_name in obj_names:
            _, content = create_and_put_object(
                self.client,
                bck=self.bucket.as_model(),
                obj_name=obj_name,
                obj_size=obj_size,
            )
            res[obj_name] = content
        self._register_for_post_test_cleanup(names=obj_names, is_bucket=False)
        return res

    def _check_all_objects_cached(self, num_obj, expected_cached):
        """
        List all objects with this test prefix and validate the cache status
        Args:
            num_obj: Number of objects we expect to find
            expected_cached: Whether we expect them to be cached
        """
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).entries
        self.assertEqual(num_obj, len(objects))
        self._validate_objects_cached(objects, expected_cached)

    def _validate_objects_cached(self, objects, expected_cached):
        """
        Validate that all objects provided are either cached or not
        Args:
            objects: List of objects to check
            expected_cached: Whether we expect them to be cached
        """
        self.assertTrue(len(objects) > 0)
        for obj in objects:
            self.assertTrue(obj.is_ok())
            if expected_cached:
                self.assertTrue(obj.is_cached())
            else:
                self.assertFalse(obj.is_cached())

    def _verify_cached_objects(self, expected_object_count, cached_range):
        """
        List each of the objects and verify the correct count and that all objects matching
        the cached range are cached and all others are not

        Args:
            expected_object_count: expected number of objects to list
            cached_range: object indices that should be cached, all others should not
        """
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).entries
        self.assertEqual(expected_object_count, len(objects))
        cached_names = {self.obj_prefix + str(x) + SUFFIX_NAME for x in cached_range}
        cached_objs = []
        evicted_objs = []
        for obj in objects:
            if obj.name in cached_names:
                cached_objs.append(obj)
            else:
                evicted_objs.append(obj)
        if len(cached_objs) > 0:
            self._validate_objects_cached(cached_objs, True)
        if len(evicted_objs) > 0:
            self._validate_objects_cached(evicted_objs, False)
