from unittest import TestCase

import boto3

# pylint: disable=unused-import,unused-variable
from aistore.botocore_patch import botocore

from tests.integration import CLUSTER_ENDPOINT
from tests.integration.boto3 import NUM_BUCKETS, NUM_OBJECTS, OBJECT_LENGTH, AWS_REGION
from tests.utils import random_string


class BotoTest(TestCase):
    def setUp(self) -> None:
        self.client = boto3.client(
            "s3", region_name=AWS_REGION, endpoint_url=CLUSTER_ENDPOINT + "/s3"
        )
        self.clean_up()

    def tearDown(self) -> None:
        self.clean_up()

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
        for (key, body) in objects:
            self.client.put_object(Bucket=bucket_name, Key=key, Body=body)
        existing_objects = [
            self.client.get_object(Bucket=bucket_name, Key=key)
            .get("Body")
            .read()
            .decode("utf-8")
            for key, body in objects
        ]
        object_bodies = [body for key, body in objects]
        self.assertEqual(object_bodies, existing_objects)

    def test_delete_bucket(self):
        bucket_name = self.create_bucket()
        self.client.delete_bucket(Bucket=bucket_name)
        self.assertEqual([], self.client.list_buckets().get("Buckets"))

    def test_multipart_upload(self):
        key = "object-name"
        data_len = 100
        num_parts = 4
        chunk_size = int(data_len / num_parts)
        data = random_string(data_len)
        parts = [data[i * chunk_size : (i + 1) * chunk_size] for i in range(num_parts)]
        bucket_name = self.create_bucket()
        response = self.client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response.get("UploadId")
        offset = 0
        for part_num, part_data in enumerate(parts):
            self.client.upload_part(
                Body=part_data,
                Bucket=bucket_name,
                Key=key,
                PartNumber=part_num + 1,
                UploadId=upload_id,
            )
            offset += len(part_data)
        self.client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [{"PartNumber": part_num + 1} for part_num in range(num_parts)]
            },
        )
        response = self.client.get_object(Bucket=bucket_name, Key=key)
        uploaded_data = response["Body"].read().decode("utf-8")
        self.assertEqual(data, uploaded_data)

    def create_bucket(self):
        bucket_name = random_string(20)
        self.client.create_bucket(Bucket=bucket_name)
        return bucket_name

    def clean_up(self):
        existing_bucket_names = [
            b.get("Name") for b in self.client.list_buckets().get("Buckets")
        ]
        for bucket in existing_bucket_names:
            self.client.delete_bucket(Bucket=bucket)
