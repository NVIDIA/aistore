import unittest

from aistore.sdk import Client
from aistore.sdk.cluster import Cluster
from aistore.sdk.etl import Etl
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import Namespace
from aistore.sdk.job import Job


class TestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.client = Client(self.endpoint)

    def test_bucket(self):
        bck_name = "bucket_123"
        provider = "bucketProvider"
        namespace = Namespace(uuid="id", name="namespace")
        bucket = self.client.bucket(bck_name, provider, namespace)
        self.assertEqual(self.endpoint, bucket.client.endpoint)
        self.assertIsInstance(bucket.client, RequestClient)
        self.assertEqual(bck_name, bucket.name)
        self.assertEqual(provider, bucket.provider)
        self.assertEqual(namespace, bucket.namespace)

    def test_cluster(self):
        res = self.client.cluster()
        self.assertEqual(self.endpoint, res.client.endpoint)
        self.assertIsInstance(res.client, RequestClient)
        self.assertIsInstance(res, Cluster)

    def test_job(self):
        res = self.client.job()
        self.assertEqual(self.endpoint, res.client.endpoint)
        self.assertIsInstance(res.client, RequestClient)
        self.assertIsInstance(res, Job)

    def test_etl(self):
        res = self.client.etl()
        self.assertEqual(self.endpoint, res.client.endpoint)
        self.assertIsInstance(res.client, RequestClient)
        self.assertIsInstance(res, Etl)
