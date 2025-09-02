from unittest import TestCase

import boto3

from tests import AWS_ACCESS_KEY_ID, AWS_SESSION_TOKEN, AWS_SECRET_ACCESS_KEY
from tests.integration import CLUSTER_ENDPOINT
from tests.integration.boto3 import AWS_REGION
from tests.utils import random_string

# pylint: disable=unused-import,unused-variable
from aistore.botocore_patch import botocore


class BaseBotoTest(TestCase):
    """Base test class for boto3 S3 operations on AIS cluster"""

    def setUp(self) -> None:
        """
        Set up boto3 S3 client for AIS cluster testing
        """
        self.client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            endpoint_url=CLUSTER_ENDPOINT + "/s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
        )
        self.clean_up()

    def tearDown(self) -> None:
        self.clean_up()

    def create_bucket(self):
        """Create a bucket with a random name"""
        bucket_name = random_string(20)
        self.client.create_bucket(Bucket=bucket_name)
        return bucket_name

    def clean_up(self):
        """Clean up all buckets"""
        existing_bucket_names = [
            b.get("Name") for b in self.client.list_buckets().get("Buckets")
        ]
        for bucket in existing_bucket_names:
            self.client.delete_bucket(Bucket=bucket)
