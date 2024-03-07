#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import patch

from aistore.sdk import Client
from aistore.sdk.cluster import Cluster
from aistore.sdk.etl import Etl
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import Namespace
from aistore.sdk.job import Job
from tests.unit.sdk.test_utils import test_cases


class TestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.client = Client(self.endpoint)

    @patch("aistore.sdk.client.RequestClient")
    def test_init_defaults(self, mock_request_client):
        Client(self.endpoint)
        mock_request_client.assert_called_with(self.endpoint, False, None, None)

    @test_cases(
        (True, None, None),
        (False, "ca_cert_location", None),
        (False, None, 30.0),
        (False, None, (10, 30.0)),
    )
    @patch("aistore.sdk.client.RequestClient")
    def test_init(self, test_case, mock_request_client):
        skip_verify, ca_cert, timeout = test_case
        Client(self.endpoint, skip_verify=skip_verify, ca_cert=ca_cert, timeout=timeout)
        mock_request_client.assert_called_with(
            self.endpoint, skip_verify, ca_cert, timeout
        )

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
        job_id = "1234"
        job_kind = "test kind"
        res = self.client.job(job_id, job_kind)
        self.assertIsInstance(res, Job)
        self.assertEqual(job_id, res.job_id)
        self.assertEqual(job_kind, res.job_kind)

    def test_etl(self):
        etl_name = "my-etl"
        res = self.client.etl(etl_name)
        self.assertIsInstance(res, Etl)
        self.assertEqual(etl_name, res.name)
