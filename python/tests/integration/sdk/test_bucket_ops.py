#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import unittest
import requests

from aistore.sdk import Client
from aistore.sdk.const import ProviderAIS
from aistore.sdk.errors import ErrBckNotFound, InvalidBckProvider

from tests.utils import create_and_put_object, random_string
from tests.integration import CLUSTER_ENDPOINT, REMOTE_BUCKET

# If remote bucket is not set, skip all cloud-related tests
REMOTE_SET = REMOTE_BUCKET != "" and not REMOTE_BUCKET.startswith(ProviderAIS + ":")


class TestBucketOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.bck_name = random_string()

        self.client = Client(CLUSTER_ENDPOINT)
        self.buckets = []
        self.provider = None
        self.cloud_bck = None
        self.obj_prefix = "test_bucket_ops-"
        if REMOTE_SET:
            self.cloud_objects = []
            self.provider, self.cloud_bck = REMOTE_BUCKET.split("://")
            self._cleanup_objects()

    def tearDown(self) -> None:
        # Try to destroy all temporary buckets if there are left.
        for bck_name in self.buckets:
            try:
                self.client.bucket(bck_name).delete()
            except ErrBckNotFound:
                pass
        # If we are using a remote bucket for this specific test, delete the objects instead of the full bucket
        if self.cloud_bck:
            bucket = self.client.bucket(self.cloud_bck, provider=self.provider)
            for obj_name in self.cloud_objects:
                bucket.objects(obj_range=obj_name).delete()

    def _cleanup_objects(self):
        cloud_bck = self.client.bucket(self.cloud_bck, self.provider)
        # Clean up any other objects created with the test prefix, potentially from aborted tests
        object_names = [
            x.name for x in cloud_bck.list_objects(self.obj_prefix).get_entries()
        ]
        if len(object_names) > 0:
            job_id = cloud_bck.objects(obj_names=object_names).delete()
            self.client.job().wait_for_job(job_id=job_id, timeout=30)

    def test_bucket(self):
        res = self.client.cluster().list_buckets()
        count = len(res)
        self.create_bucket(self.bck_name)
        res = self.client.cluster().list_buckets()
        count_new = len(res)
        self.assertEqual(count + 1, count_new)

    def create_bucket(self, bck_name):
        self.buckets.append(bck_name)
        bucket = self.client.bucket(bck_name)
        bucket.create()
        return bucket

    def test_head_bucket(self):
        self.create_bucket(self.bck_name)
        self.client.bucket(self.bck_name).head()
        self.client.bucket(self.bck_name).delete()
        try:
            self.client.bucket(self.bck_name).head()
        except requests.exceptions.HTTPError as err:
            self.assertEqual(err.response.status_code, 404)

    def test_rename_bucket(self):
        from_bck_n = self.bck_name + "from"
        to_bck_n = self.bck_name + "to"
        self.create_bucket(from_bck_n)
        res = self.client.cluster().list_buckets()
        count = len(res)

        bck_obj = self.client.bucket(from_bck_n)
        self.assertEqual(bck_obj.name, from_bck_n)
        job_id = bck_obj.rename(to_bck=to_bck_n)
        self.assertNotEqual(job_id, "")

        # wait for rename to finish
        self.client.job().wait_for_job(job_id=job_id)

        # check if objects name has changed
        self.client.bucket(to_bck_n).head()

        # new bucket should be created and accessible
        self.assertEqual(bck_obj.name, to_bck_n)

        # old bucket should be inaccessible
        try:
            self.client.bucket(from_bck_n).head()
        except requests.exceptions.HTTPError as err:
            self.assertEqual(err.response.status_code, 404)

        # length of buckets before and after rename should be same
        res = self.client.cluster().list_buckets()
        count_new = len(res)
        self.assertEqual(count, count_new)

    def test_copy_bucket(self):
        from_bck = self.bck_name + "from"
        to_bck = self.bck_name + "to"
        self.create_bucket(from_bck)
        self.create_bucket(to_bck)

        job_id = self.client.bucket(from_bck).copy(to_bck)
        self.assertNotEqual(job_id, "")
        self.client.job().wait_for_job(job_id=job_id)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict_bucket(self):
        obj_name = "test_evict_bucket-"
        create_and_put_object(
            self.client,
            bck_name=self.cloud_bck,
            provider=self.provider,
            obj_name=obj_name,
        )
        bucket = self.client.bucket(self.cloud_bck, provider=self.provider)
        objects = bucket.list_objects(
            props="name,cached", prefix=obj_name
        ).get_entries()
        self.verify_objects_cache_status(objects, True)

        bucket.evict()

        objects = bucket.list_objects(
            props="name,cached", prefix=obj_name
        ).get_entries()
        self.verify_objects_cache_status(objects, False)

    def test_evict_bucket_local(self):
        bucket = self.create_bucket(self.bck_name)
        with self.assertRaises(InvalidBckProvider):
            bucket.evict()

    def verify_objects_cache_status(self, objects, expected_status):
        self.assertTrue(len(objects) > 0)
        for obj in objects:
            self.assertTrue(obj.is_ok())
            if expected_status:
                self.assertTrue(obj.is_cached())
            else:
                self.assertFalse(obj.is_cached())


if __name__ == "__main__":
    unittest.main()
