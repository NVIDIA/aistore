from aistore.sdk.const import UTF_ENCODING

from tests.integration.boto3 import NUM_BUCKETS, NUM_OBJECTS, OBJECT_LENGTH
from tests.integration.boto3.base_test import BaseBotoTest
from tests.utils import random_string


class BotoTest(BaseBotoTest):
    """Test basic S3 operations on an AIS cluster using the boto3 client"""

    def test_create_bucket(self):
        bucket_names = {random_string(20) for _ in range(NUM_BUCKETS)}
        for name in bucket_names:
            self.client.create_bucket(Bucket=name)
        existing_buckets = self.client.list_buckets()
        existing_bucket_names = {b.get("Name") for b in existing_buckets.get("Buckets")}
        self.assertEqual(bucket_names, existing_bucket_names)

    def test_update_read_bucket(self):
        bucket_name = self.create_bucket()
        objects = [(str(i), random_string(OBJECT_LENGTH)) for i in range(NUM_OBJECTS)]
        for key, body in objects:
            self.client.put_object(Bucket=bucket_name, Key=key, Body=body)
        existing_objects = [
            self.client.get_object(Bucket=bucket_name, Key=key)
            .get("Body")
            .read()
            .decode(UTF_ENCODING)
            for key, body in objects
        ]
        object_bodies = [body for key, body in objects]
        self.assertEqual(object_bodies, existing_objects)

    def test_delete_bucket(self):
        bucket_name = self.create_bucket()
        self.client.delete_bucket(Bucket=bucket_name)
        self.assertEqual([], self.client.list_buckets().get("Buckets"))
