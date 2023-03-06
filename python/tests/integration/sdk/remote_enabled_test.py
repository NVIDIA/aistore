import unittest

from aistore.sdk.const import PROVIDER_AIS

from aistore import Client
from tests.integration import REMOTE_SET, REMOTE_BUCKET, CLUSTER_ENDPOINT
from tests.utils import random_string, destroy_bucket, create_and_put_objects


class RemoteEnabledTest(unittest.TestCase):
    """
    This class is intended to be used with all tests that work with remote buckets.
    It provides helper methods for dealing with remote buckets and objects and tracking them for proper cleanup.
    This includes prefixing all objects with a unique value and deleting all objects after tests finish
        to avoid collisions with multiple instances using the same bucket.
    To use this class with another test class, simply inherit from this rather than TestCase.
    To extend setUp behavior in a child class, define them as normal for a TestCase then call
        super().setUp() before adding additional setup steps (same process for tearDown)
    """

    def setUp(self) -> None:
        self.bck_name = random_string()
        self.client = Client(CLUSTER_ENDPOINT)
        self.buckets = []
        self.obj_prefix = f"{self._testMethodName}-{random_string(6)}-"

        if REMOTE_SET:
            self.cloud_objects = []
            provider, bck_name = REMOTE_BUCKET.split("://")
            self.bucket = self.client.bucket(bck_name, provider=provider)
            self.provider = provider
        else:
            self.provider = PROVIDER_AIS
            self.bucket = self._create_bucket(self.bck_name)

    def tearDown(self) -> None:
        """
        Cleanup after each test, destroy the bucket if it exists
        """
        for bck in self.buckets:
            destroy_bucket(self.client, bck)
        if REMOTE_SET:
            self.bucket.objects(obj_names=self.cloud_objects).delete()

    def _create_bucket(self, bck_name, provider=PROVIDER_AIS):
        """
        Create a bucket and store its name for later cleanup
        Args:
            bck_name: Name of new bucket
            provider: Provider for new bucket
        """
        bck = self.client.bucket(bck_name, provider=provider)
        self.buckets.append(bck_name)
        bck.create()
        return bck

    def _create_objects(self, num_obj, suffix=""):
        """
        Create a list of objects using a unique test prefix and track them for later cleanup
        Args:
            num_obj: Number of objects to create
            suffix: Optional suffix for each object name
        """
        obj_names = create_and_put_objects(
            self.client,
            self.bucket,
            self.obj_prefix,
            suffix,
            num_obj,
        )
        if REMOTE_SET:
            self.cloud_objects.extend(obj_names)
        return obj_names

    def _check_all_objects_cached(self, num_obj, expected_cached):
        """
        List all objects with this test prefix and validate the cache status
        Args:
            num_obj: Number of objects we expect to find
            expected_cached: Whether we expect them to be cached
        """
        objects = self.bucket.list_objects(
            props="name,cached", prefix=self.obj_prefix
        ).get_entries()
        self.assertEqual(num_obj, len(objects))
        self._validate_objects_cached(objects, expected_cached)

    def _validate_objects_cached(self, objects, expected_cached):
        """
        Validate that all objects provided are either cached or not
        Args:
            objects: List of objects to check
            expected_cached: Whether we expect them to be cached
        """
        for obj in objects:
            self.assertTrue(obj.is_ok())
            if expected_cached:
                self.assertTrue(obj.is_cached())
            else:
                self.assertFalse(obj.is_cached())
